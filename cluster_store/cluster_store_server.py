import datetime
import socket
import threading
import json
import sys
import os
import time
import hashlib 
import signal
import random

PORT = int(os.environ.get("SERVER_PORT"))
HOST = "0.0.0.0"

CLUSTER_STORE_PORTS = [6001, 6002, 6003]

ALL_CLUSTER_SERVERS = ["store-primary:6001", "store-backup1:6002", "store-backup2:6003"]
MY_SERVER_ADDRESS = f"{socket.gethostname()}:{PORT}"

MONITOR_HOST = os.environ.get('MONITOR_HOST', 'monitor')
MONITOR_PORT = 6000

SERVER_ROLE = "backup" 

STORE_DATA = {} 

HEARTBEAT_TIMEOUT = 10
ELECTION_TIMEOUT = 5
BACKUP_PROMOTION_TIMEOUT = 8
PING_INTERVAL = 3

server_status = {}
last_heartbeat = {} 
is_in_election = False
election_votes = {}

def exit_handler(signum, frame):
    print("Received shutdown signal. Writing final hash to log...")
    os.environ['TZ'] = 'America/Sao_Paulo'
    time.tzset()
    final_hash = calculate_store_hash()
    log_entry = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Final Hash for {SERVER_ROLE.upper()} on port {PORT}: {final_hash}\n"
    
    try:
        with open("/logs/store_hashes.log", "a") as log_file:
            log_file.write(log_entry)
            log_file.flush()
        print(f"✅ Final hash written to /logs/store_hashes.log: {final_hash}")
    except Exception as e:
        print(f"❌ Error writing to log file: {e}")
    sys.exit(0)

def calculate_store_hash():
    """
    Calcula um hash SHA-256 dos dados do store para verificação.
    Ordena as chaves do dicionário para garantir um hash consistente
    """
    sorted_data = json.dumps(STORE_DATA, sort_keys=True).encode('utf-8')
    return hashlib.sha256(sorted_data).hexdigest()

def monitor_heartbeat_thread_func():
    while True:
        send_monitor_update(SERVER_ROLE.upper(), PORT)
        time.sleep(3)

def send_monitor_update(role, port):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2)
            s.connect((MONITOR_HOST, MONITOR_PORT))
            
            message = {
                "type": "store_status",
                "data": {
                    "port": port,
                    "role": role,
                    "store_hash": calculate_store_hash(),
                    "timestamp": time.time()
                },
                "timestamp": time.time()
            }
            s.sendall(json.dumps(message).encode('utf-8'))
    except Exception as e:
        print(f"❌ Erro ao enviar status para o monitor: {e}")


# Funções de Detecção e Recuperação de Falhas
def ping_servers_thread():
    global last_heartbeat, server_status
    
    while True:
        current_time = time.time()
        
        # Envia PING para todos os outros servidores
        for server in ALL_CLUSTER_SERVERS:
            if server != MY_SERVER_ADDRESS:
                host, port = server.split(':')
                port = int(port)
                
                try:
                    ping_successful = send_ping(host, port)
                    if ping_successful:
                        last_heartbeat[server] = current_time
                        if server_status.get(server) != "ACTIVE":
                            print(f"✅ Server {server} is back online")
                        server_status[server] = "ACTIVE"
                    else:
                        # Verifica se o servidor expirou
                        if server in last_heartbeat:
                            time_since_last = current_time - last_heartbeat[server]
                            if time_since_last > HEARTBEAT_TIMEOUT:
                                if server_status.get(server) != "FAILED":
                                    print(f"🚨 Server {server} detected as FAILED (no response for {time_since_last:.1f}s)")
                                    server_status[server] = "FAILED"
                                    handle_server_failure(server)
                        else:
                            # Primeira tentativa de ping - dá um tempo antes de marcar como falhado
                            last_heartbeat[server] = current_time
                            if server not in server_status:
                                server_status[server] = "UNKNOWN"
                            print(f"🔍 First ping attempt to {server} failed, will retry...")
                            
                except Exception as e:
                    print(f"❌ Error pinging {server}: {e}")
                    # Só marca como falhado se estivermos tentando há um tempo
                    if server in last_heartbeat:
                        time_since_last = current_time - last_heartbeat[server]
                        if time_since_last > HEARTBEAT_TIMEOUT:
                            server_status[server] = "FAILED"
        
        time.sleep(PING_INTERVAL)

def send_ping(host, port):
    """Envia mensagem PING para um servidor e aguarda PONG."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2)
            s.connect((host, port))
            
            ping_message = {
                "action": "ping",
                "sender": MY_SERVER_ADDRESS,
                "timestamp": time.time()
            }
            s.sendall(json.dumps(ping_message).encode('utf-8'))
            
            response_data = s.recv(1024)
            if response_data:
                response = json.loads(response_data.decode('utf-8'))
                return response.get("status") == "PONG"
            return False
    except:
        return False

def handle_server_failure(failed_server):
    """Manipula a falha de um servidor baseado no papel atual e servidor falhado."""
    global SERVER_ROLE, is_in_election
    
    print(f"🔧 Handling failure of server: {failed_server}")
    
    # Caso 1.1: Servidor backup falha sem nenhuma requisição pendente
    if "backup" in failed_server.lower() and SERVER_ROLE == "primary":
        print(f"📢 Backup server {failed_server} failed. Primary continuing operations.")
        log_fault_event(f"BACKUP_FAILURE: {failed_server} is down")
        
    # Caso 1.2: Servidor backup falha enquanto manipula uma requisição
    elif "backup" in failed_server.lower() and SERVER_ROLE == "backup":
        print(f"📢 Another backup server {failed_server} failed.")
        log_fault_event(f"PEER_BACKUP_FAILURE: {failed_server} is down")
        
    # Caso 1.3: Servidor primário falha (caso mais crítico)
    elif ("primary" in failed_server.lower() or ":6001" in failed_server) and SERVER_ROLE == "backup":
        if not is_in_election:
            print(f"🚨 PRIMARY SERVER {failed_server} FAILED! Starting election process...")
            log_fault_event(f"PRIMARY_FAILURE: {failed_server} is down - Starting election")
            start_election()
        else:
            print(f"⚠️ Primary {failed_server} failed but election already in progress")
    else:
        print(f"ℹ️ Server failure detected but no action needed (role: {SERVER_ROLE}, failed: {failed_server})")

def start_election():
    """Inicia processo de eleição de líder entre servidores backup."""
    global is_in_election, election_votes, SERVER_ROLE
    
    if is_in_election:
        return
        
    is_in_election = True
    election_votes = {}
    
    print(f"🗳️ Starting election process from {MY_SERVER_ADDRESS}")
    
    """Envia mensagem de eleição para todos os servidores backup ativos"""
    active_backups = [s for s in ALL_CLUSTER_SERVERS 
                     if s != MY_SERVER_ADDRESS and 
                        server_status.get(s, "UNKNOWN") == "ACTIVE" and 
                        ":6001" not in s]
    
    if not active_backups:
        promote_to_primary()
        return
    
    election_message = {
        "action": "election",
        "candidate": MY_SERVER_ADDRESS,
        "candidate_priority": PORT,
        "timestamp": time.time()
    }
    
    votes_received = 0
    for backup_server in active_backups:
        host, port = backup_server.split(':')
        port = int(port)
        
        try:
            vote = send_election_message(host, port, election_message)
            if vote:
                votes_received += 1
                election_votes[backup_server] = vote
        except Exception as e:
            print(f"❌ Failed to get vote from {backup_server}: {e}")
    
    # Maioria simples ou maior prioridade vence
    total_servers = len(active_backups) + 1 
    if votes_received >= len(active_backups) // 2:
        promote_to_primary()
    else:
        print(f"❌ Election failed. Only got {votes_received} votes from {len(active_backups)} servers")
        is_in_election = False
        
        threading.Timer(ELECTION_TIMEOUT, retry_election).start()

def send_election_message(host, port, message):
    """Envia mensagem de eleição e retorna resultado do voto."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(3)
            s.connect((host, port))
            
            s.sendall(json.dumps(message).encode('utf-8'))
            
            response_data = s.recv(1024)
            if response_data:
                response = json.loads(response_data.decode('utf-8'))
                return response.get("vote", False)
            return False
    except:
        return False

def retry_election():
    """Tenta eleição novamente após timeout."""
    global is_in_election
    is_in_election = False
    
    # Verifica se o primário ainda está fora
    primary_server = "store-primary:6001"
    if server_status.get(primary_server, "FAILED") == "FAILED":
        start_election()

def promote_to_primary():
    """Promove este servidor backup para primário."""
    global SERVER_ROLE, is_in_election
    
    print(f"🎉 PROMOTING {MY_SERVER_ADDRESS} TO PRIMARY!")
    SERVER_ROLE = "primary"
    is_in_election = False
    
    # Anuncia novo papel para monitor e outros servidores
    send_monitor_update("PRIMARY", PORT)
    announce_new_primary()
    
    log_fault_event(f"PROMOTION: {MY_SERVER_ADDRESS} promoted to PRIMARY")

def announce_new_primary():
    """Anuncia para todos os servidores que este servidor agora é primário."""
    announcement = {
        "action": "new_primary_announcement",
        "new_primary": MY_SERVER_ADDRESS,
        "timestamp": time.time()
    }
    
    for server in ALL_CLUSTER_SERVERS:
        if server != MY_SERVER_ADDRESS and server_status.get(server) == "ACTIVE":
            host, port = server.split(':')
            port = int(port)
            
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(2)
                    s.connect((host, port))
                    s.sendall(json.dumps(announcement).encode('utf-8'))
                    print(f"📢 Announced new primary to {server}")
            except Exception as e:
                print(f"❌ Failed to announce to {server}: {e}")

def log_fault_event(event):
    """Registra eventos de tolerância a falhas."""
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"[{timestamp}] FAULT_EVENT: {event} (Server: {MY_SERVER_ADDRESS})\n"
    
    try:
        with open("/logs/fault_tolerance.log", "a") as log_file:
            log_file.write(log_entry)
            log_file.flush()
        print(f"📝 Fault event logged: {event}")
    except Exception as e:
        print(f"❌ Error writing to fault log: {e}")

def handle_client_connection(conn, addr):
    """Lida com as requisições de um cliente (Cluster Sync) ou de outro servidor."""
    print(f"Conexão aceita de {addr}")
    try:
        while True:
            data = conn.recv(4096)  # Aumentado de 1024 para 4096 bytes
            if not data:
                break
            
            try:
                message = json.loads(data.decode('utf-8'))
                print(f"Mensagem recebida: {message.get('action', 'unknown')} from {addr}")
                
                response = process_message(message)
                
                conn.sendall(json.dumps(response).encode('utf-8'))
            except json.JSONDecodeError as e:
                print(f"❌ Erro JSON de {addr}: {e}")
                error_response = {"status": "FAILED", "error": "Invalid JSON format"}
                conn.sendall(json.dumps(error_response).encode('utf-8'))
                break

    except (ConnectionResetError, BrokenPipeError) as e:
        print(f"Cliente {addr} desconectou: {type(e).__name__}")
    except Exception as e:
        print(f"Erro na comunicação com {addr}: {e}")
    finally:
        conn.close()
        print(f"Conexão com {addr} encerrada.")

def propagate_update_to_backups(data, all_servers, my_host):
    """Propaga a atualização para todos os servidores de backup ativos."""
    backup_servers = []
    for s in all_servers:
        if s != my_host and f":{PORT}" not in s:  # Excluir qualquer servidor na mesma porta
            server_status_check = server_status.get(s, "UNKNOWN")
            if server_status_check != "FAILED":
                backup_servers.append(s)
    
    if not backup_servers:
        print("Não há backups ativos para propagar a atualização.")
        return True
    
    print(f"Propagando atualização para backups em: {backup_servers}")
    
    # Enviar apenas os dados novos, não o STORE_DATA completo
    update_message = {
        "action": "update_backup",
        "data": data 
    }
    
    successful_updates = 0
    
    for host_and_port in backup_servers:
        try:
            host, port = host_and_port.split(':')
            port = int(port)
            
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(3)
                s.connect((host, port))
                
                # Verificar tamanho da mensagem para evitar truncamento
                message_json = json.dumps(update_message)
                if len(message_json) > 1000:
                    print(f"⚠️ Mensagem grande ({len(message_json)} bytes) para {host}:{port} - enviando dados resumidos")
                    # Enviar apenas o último item adicionado
                    last_key = max(data.keys()) if data else None
                    if last_key:
                        smaller_update = {
                            "action": "update_backup",
                            "data": {last_key: data[last_key]}
                        }
                        message_json = json.dumps(smaller_update)
                
                s.sendall(message_json.encode('utf-8'))
                
                response_data = s.recv(1024)
                if response_data:
                    try:
                        response = json.loads(response_data.decode('utf-8'))
                        if response.get("status") == "SUCCESS":
                            print(f"✅ Backup {host}:{port} atualizado com sucesso")
                            successful_updates += 1
                            server_status[host_and_port] = "ACTIVE"
                        else:
                            print(f"❌ Backup {host}:{port} falhou: {response.get('error', 'Unknown error')}")
                            server_status[host_and_port] = "FAILED"
                    except json.JSONDecodeError as je:
                        print(f"❌ Resposta JSON inválida de {host}:{port}: {je}")
                        server_status[host_and_port] = "FAILED"
                else:
                    print(f"❌ Backup {host}:{port} não respondeu")
                    server_status[host_and_port] = "FAILED"
                    
        except (socket.timeout, socket.error, ConnectionRefusedError) as e:
            print(f"❌ Falha na conexão com backup {host_and_port}: {type(e).__name__}")
            server_status[host_and_port] = "FAILED"
        except Exception as e:
            print(f"❌ Erro inesperado com backup {host_and_port}: {e}")
            server_status[host_and_port] = "FAILED"
    
    if successful_updates > 0:
        print(f"✅ Update propagated to {successful_updates}/{len(backup_servers)} backup(s)")
        return True
    else:
        print("❌ Failed to update any backup servers")
        return False

def process_message(message):
    global STORE_DATA, SERVER_ROLE, PORT, CLUSTER_STORE_PORTS, is_in_election
    action = message.get("action")
    
    # Cuida de mensagens PING
    if action == "ping":
        return {"status": "PONG", "server": MY_SERVER_ADDRESS, "role": SERVER_ROLE}

    # Cuida de mensagens de eleição
    elif action == "election":
        candidate = message.get("candidate")
        candidate_priority = message.get("candidate_priority", 9999)
        
        # Vota para o candidato se ele tiver prioridade maior (número de porta menor)
        my_priority = PORT
        vote = candidate_priority < my_priority
        
        print(f"🗳️ Received election from {candidate} (priority {candidate_priority}). My vote: {vote}")
        
        return {"status": "SUCCESS", "vote": vote, "voter": MY_SERVER_ADDRESS}
    
    elif action == "new_primary_announcement":
        new_primary = message.get("new_primary")
        print(f"📢 Acknowledged new primary: {new_primary}")
        
        # Atualiza status do server
        server_status[new_primary] = "ACTIVE"
        
        return {"status": "SUCCESS", "message": "New primary acknowledged"}
    
    elif action == "write":
        if SERVER_ROLE == "primary":
            data = message.get("data")
            key = f"chave_{os.getpid()}_{int(time.time())}"
            STORE_DATA[key] = data
            print(f"✅ Dados escritos: {key} = {data}")
            
            # Atualiza o monitor com o status do primário
            send_monitor_update(SERVER_ROLE.upper(), PORT)

            # Propagar apenas o novo item, não todo o STORE_DATA
            new_data = {key: data}
            propagation_success = propagate_update_to_backups(new_data, ALL_CLUSTER_SERVERS, MY_SERVER_ADDRESS)
            
            if propagation_success:
                return {"status": "SUCCESS", "message": "Dados escritos e propagados para backups."}
            else:
                return {"status": "SUCCESS", "message": "Dados escritos no primário. Alguns backups podem estar indisponíveis."}
        else:
            return {"status": "FAILED", "error": "Este servidor não é o primário."}
            
    elif action == "read":
        # Retornar dados locais sempre, independente do role
        print(f"📖 Leitura solicitada - Retornando dados locais")
        return {"status": "SUCCESS", "data": STORE_DATA, "server": MY_SERVER_ADDRESS, "role": SERVER_ROLE}
        
    elif action == "update_backup":
        if SERVER_ROLE == "backup":
            updated_data = message.get("data")
            if updated_data:
                # Atualizar apenas os novos dados recebidos
                STORE_DATA.update(updated_data)
                print(f"✅ Backup atualizado com {len(updated_data)} novos itens")
                
                # Atualiza o monitor com o status do backup
                send_monitor_update(SERVER_ROLE.upper(), PORT)
                
                return {"status": "SUCCESS", "message": f"Backup atualizado com {len(updated_data)} itens."}
            else:
                return {"status": "FAILED", "error": "Nenhum dado fornecido para atualização."}
        else:
            return {"status": "FAILED", "error": "Requisição 'update_backup' recebida por um servidor que não é backup."}

    else:
        return {"status": "FAILED", "error": "Ação desconhecida."}

if __name__ == "__main__":

    signal.signal(signal.SIGTERM, exit_handler)
    signal.signal(signal.SIGINT, exit_handler)
    
    if PORT == 6001:
        SERVER_ROLE = "primary"
        print("Este servidor é o primário inicial.")
    else:
        print("Este servidor é um backup.")
        
    for server in ALL_CLUSTER_SERVERS:
        if server != MY_SERVER_ADDRESS:
            server_status[server] = "UNKNOWN"
    
    print(f"Waiting 5 seconds before starting ping threads to allow other servers to start...")
    time.sleep(5)
    
    # Envia o status inicial para o monitor
    send_monitor_update(SERVER_ROLE.upper(), PORT)
    
    heartbeat_thread = threading.Thread(target=monitor_heartbeat_thread_func)
    heartbeat_thread.daemon = True
    heartbeat_thread.start()

    ping_thread = threading.Thread(target=ping_servers_thread)
    ping_thread.daemon = True
    ping_thread.start()

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