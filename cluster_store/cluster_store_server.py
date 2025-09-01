import socket
import threading
import json
import sys
import os
import time

PORT = int(os.environ.get("SERVER_PORT"))
HOST = "0.0.0.0"

CLUSTER_STORE_PORTS = [6001, 6002, 6003]

MONITOR_HOST = os.environ.get('MONITOR_HOST', 'monitor')
MONITOR_PORT = 6000

SERVER_ROLE = "backup" 

STORE_DATA = {} 

def send_monitor_update(role, port):
    """Envia uma mensagem de status para o monitor."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2)
            s.connect((MONITOR_HOST, MONITOR_PORT))
            
            message = {
                "type": "store_status",
                "data": {
                    "port": port,
                    "role": role
                },
                "timestamp": time.time()
            }
            s.sendall(json.dumps(message).encode('utf-8'))
    except Exception as e:
        print(f"❌ Erro ao enviar status para o monitor: {e}")

def handle_client_connection(conn, addr):
    """Lida com as requisições de um cliente (Cluster Sync) ou de outro servidor."""
    print(f"Conexão aceita de {addr}")
    try:
        while True:
            data = conn.recv(1024)
            if not data:
                break
            
            message = json.loads(data.decode('utf-8'))
            print(f"Mensagem recebida: {message}")
            
            response = process_message(message)
            
            conn.sendall(json.dumps(response).encode('utf-8'))

    except (json.JSONDecodeError, ConnectionResetError) as e:
        print(f"Erro na comunicação com {addr}: {e}")
    finally:
        conn.close()
        print(f"Conexão com {addr} encerrada.")

def propagate_update_to_backups(data, all_ports, my_port):
    backup_ports = [p for p in all_ports if p != my_port]
    
    if not backup_ports:
        print("Não há backups para propagar a atualização.")
        return True
    
    print(f"Propagando atualização para backups nas portas: {backup_ports}")
    
    update_message = {
        "action": "update_backup",
        "data": data 
    }
    
    all_backups_succeeded = True
    
    for port in backup_ports:
        # ✅ CORREÇÃO: Mapeia a porta para o nome do serviço
        host_map = {6001: "store-primary", 6002: "store-backup1", 6003: "store-backup2"}
        host = host_map.get(port)
        if not host:
            print(f"⚠️ Erro: Não foi possível mapear a porta {port} para um host.")
            all_backups_succeeded = False
            continue

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect((host, port))
                
                s.sendall(json.dumps(update_message).encode('utf-8'))
                
                response_data = s.recv(1024)
                if response_data:
                    response = json.loads(response_data.decode('utf-8'))
                    if response.get("status") == "SUCCESS":
                        print(f"Backup na porta {port} atualizado com sucesso.")
                    else:
                        print(f"Backup na porta {port} falhou ao atualizar: {response.get('error')}")
                        all_backups_succeeded = False
                    
        except (socket.timeout, socket.error) as e:
            print(f"Falha ao conectar ou comunicar com o backup na porta {port}: {e}")
            all_backups_succeeded = False
            
    return all_backups_succeeded


def process_message(message):
    global STORE_DATA, SERVER_ROLE, PORT, CLUSTER_STORE_PORTS
    action = message.get("action")
    
    if action == "write":
        if SERVER_ROLE == "primary":
            data = message.get("data")
            key = f"chave_{os.getpid()}"
            STORE_DATA[key] = data
            print(f"Dados escritos: {STORE_DATA}")
            
            # Atualiza o monitor com o status do primário
            send_monitor_update("store_status", {"port": PORT, "role": "PRIMARY"})
            
            propagate_update_to_backups(STORE_DATA, CLUSTER_STORE_PORTS, PORT)
            
            return {"status": "SUCCESS", "message": "Dados escritos e propagados."}
        else:
            return {"status": "FAILED", "error": "Este servidor não é o primário."}
            
    elif action == "read":
        return {"status": "SUCCESS", "data": STORE_DATA}
        
    elif action == "update_backup":
        if SERVER_ROLE == "backup":
            updated_data = message.get("data")
            STORE_DATA.update(updated_data)
            print(f"Dados atualizados pelo primário: {STORE_DATA}")
            
            # Atualiza o monitor com o status do backup
            send_monitor_update("store_status", {"port": PORT, "role": "BACKUP"})
            
            return {"status": "SUCCESS", "message": "Backup atualizado com sucesso."}
        else:
            return {"status": "FAILED", "error": "Requisição 'update_backup' recebida por um servidor que não é backup."}

    else:
        return {"status": "FAILED", "error": "Ação desconhecida."}

if __name__ == "__main__":
    if PORT == 6001:
        SERVER_ROLE = "primary"
        print("Este servidor é o primário inicial.")
    else:
        print("Este servidor é um backup.")
        
    # Envia o status inicial para o monitor
    send_monitor_update("store_status", {"port": PORT, "role": SERVER_ROLE.upper()})

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((HOST, PORT))
    server_socket.listen(5)
    print(f"Servidor do Cluster Store rodando em {HOST}:{PORT}")

    try:
        while True:
            conn, addr = server_socket.accept()
            thread = threading.Thread(target=handle_client_connection, args=(conn, addr))
            thread.daemon = True
            thread.start()
    except KeyboardInterrupt:
        print("\nServidor encerrado pelo usuário.")
    finally:
        server_socket.close()
