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
MY_SERVER_ADDRESS = f"{socket.gethostname()}:{PORT}" # Get the container's hostname

MONITOR_HOST = os.environ.get('MONITOR_HOST', 'monitor')
MONITOR_PORT = 6000

SERVER_ROLE = "backup" 

STORE_DATA = {} 

# 🆕 Fault tolerance configurations
HEARTBEAT_TIMEOUT = 10  # seconds
ELECTION_TIMEOUT = 5    # seconds
BACKUP_PROMOTION_TIMEOUT = 8  # seconds
PING_INTERVAL = 3       # seconds

# 🆕 Fault tolerance state
server_status = {}  # Track status of other servers
last_heartbeat = {}  # Track last heartbeat from servers
is_in_election = False
election_votes = {}

def exit_handler(signum, frame):
    """Handles graceful shutdown by writing the final hash and then exiting."""
    print("Received shutdown signal. Writing final hash to log...")
    # Define o fuso horário para o Brasil (Horário de Brasília)
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
    """Calculates a SHA-256 hash of the store data for verification."""
    # Sort the dictionary keys to ensure a consistent hash
    sorted_data = json.dumps(STORE_DATA, sort_keys=True).encode('utf-8')
    return hashlib.sha256(sorted_data).hexdigest()

def monitor_heartbeat_thread_func():
    """Thread for sending periodic status updates to the monitor."""
    while True:
        send_monitor_update(SERVER_ROLE.upper(), PORT)
        time.sleep(3)  # Send a heartbeat every 3 seconds

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
                    "role": role,
                    "store_hash": calculate_store_hash(),
                    "timestamp": time.time()
                },
                "timestamp": time.time()
            }
            s.sendall(json.dumps(message).encode('utf-8'))
    except Exception as e:
        print(f"❌ Erro ao enviar status para o monitor: {e}")

# 🆕 Fault Detection and Recovery Functions

def ping_servers_thread():
    """Thread to periodically ping other servers and detect failures."""
    global last_heartbeat, server_status
    
    while True:
        current_time = time.time()
        
        # Send PING to all other servers
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
                        # Check if server has timed out
                        if server in last_heartbeat:
                            time_since_last = current_time - last_heartbeat[server]
                            if time_since_last > HEARTBEAT_TIMEOUT:
                                if server_status.get(server) != "FAILED":
                                    print(f"🚨 Server {server} detected as FAILED (no response for {time_since_last:.1f}s)")
                                    server_status[server] = "FAILED"
                                    handle_server_failure(server)
                        else:
                            # First ping attempt - give some time before marking as failed
                            last_heartbeat[server] = current_time
                            if server not in server_status:
                                server_status[server] = "UNKNOWN"
                            print(f"🔍 First ping attempt to {server} failed, will retry...")
                            
                except Exception as e:
                    print(f"❌ Error pinging {server}: {e}")
                    # Only mark as failed if we've been trying for a while
                    if server in last_heartbeat:
                        time_since_last = current_time - last_heartbeat[server]
                        if time_since_last > HEARTBEAT_TIMEOUT:
                            server_status[server] = "FAILED"
        
        time.sleep(PING_INTERVAL)

def send_ping(host, port):
    """Send PING message to a server and wait for PONG."""
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
    """Handle the failure of a server based on current role and failed server."""
    global SERVER_ROLE, is_in_election
    
    print(f"🔧 Handling failure of server: {failed_server}")
    
    # Case 1.1: Backup server fails without any pending request
    if "backup" in failed_server.lower() and SERVER_ROLE == "primary":
        print(f"📢 Backup server {failed_server} failed. Primary continuing operations.")
        log_fault_event(f"BACKUP_FAILURE: {failed_server} is down")
        
    # Case 1.2: Backup server fails while handling a request
    elif "backup" in failed_server.lower() and SERVER_ROLE == "backup":
        print(f"📢 Another backup server {failed_server} failed.")
        log_fault_event(f"PEER_BACKUP_FAILURE: {failed_server} is down")
        
    # Case 1.3: Primary server fails (most critical case)
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
    """Start leader election process among backup servers."""
    global is_in_election, election_votes, SERVER_ROLE
    
    if is_in_election:
        return
        
    is_in_election = True
    election_votes = {}
    
    print(f"🗳️ Starting election process from {MY_SERVER_ADDRESS}")
    
    # Send election message to all active backup servers
    active_backups = [s for s in ALL_CLUSTER_SERVERS 
                     if s != MY_SERVER_ADDRESS and 
                        server_status.get(s, "UNKNOWN") == "ACTIVE" and 
                        ":6001" not in s]  # Exclude failed primary
    
    if not active_backups:
        # No other backups available, promote self immediately
        promote_to_primary()
        return
    
    election_message = {
        "action": "election",
        "candidate": MY_SERVER_ADDRESS,
        "candidate_priority": PORT,  # Higher port = lower priority
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
    
    # Simple majority or highest priority wins
    total_servers = len(active_backups) + 1  # +1 for self
    if votes_received >= len(active_backups) // 2:  # Majority of available servers
        promote_to_primary()
    else:
        print(f"❌ Election failed. Only got {votes_received} votes from {len(active_backups)} servers")
        is_in_election = False
        
        # Wait and try again
        threading.Timer(ELECTION_TIMEOUT, retry_election).start()

def send_election_message(host, port, message):
    """Send election message and return vote result."""
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
    """Retry election after timeout."""
    global is_in_election
    is_in_election = False
    
    # Check if primary is still down
    primary_server = "store-primary:6001"
    if server_status.get(primary_server, "FAILED") == "FAILED":
        start_election()

def promote_to_primary():
    """Promote this backup server to primary."""
    global SERVER_ROLE, is_in_election
    
    print(f"🎉 PROMOTING {MY_SERVER_ADDRESS} TO PRIMARY!")
    SERVER_ROLE = "primary"
    is_in_election = False
    
    # Announce new role to monitor and other servers
    send_monitor_update("PRIMARY", PORT)
    announce_new_primary()
    
    log_fault_event(f"PROMOTION: {MY_SERVER_ADDRESS} promoted to PRIMARY")

def announce_new_primary():
    """Announce to all servers that this server is now primary."""
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
    """Log fault tolerance events."""
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
            # ✅ CORRIGIDO: Receber mensagens maiores para evitar truncamento
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
    # ✅ CORRIGIDO: Filtrar corretamente backups ativos, excluindo a si mesmo
    backup_servers = []
    for s in all_servers:
        # ✅ CORREÇÃO PRINCIPAL: Usar comparação mais robusta para excluir a si mesmo
        if s != my_host and f":{PORT}" not in s:  # Excluir qualquer servidor na mesma porta
            server_status_check = server_status.get(s, "UNKNOWN")
            if server_status_check != "FAILED":
                backup_servers.append(s)
    
    if not backup_servers:
        print("Não há backups ativos para propagar a atualização.")
        return True
    
    print(f"Propagando atualização para backups em: {backup_servers}")
    
    # ✅ CORRIGIDO: Enviar apenas os dados novos, não o STORE_DATA completo
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
                
                # ✅ CORRIGIDO: Verificar tamanho da mensagem para evitar truncamento
                message_json = json.dumps(update_message)
                if len(message_json) > 1000:  # Se mensagem muito grande, enviar em partes
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
    
    # 🆕 Handle PING messages
    if action == "ping":
        return {"status": "PONG", "server": MY_SERVER_ADDRESS, "role": SERVER_ROLE}
    
    # 🆕 Handle election messages
    elif action == "election":
        candidate = message.get("candidate")
        candidate_priority = message.get("candidate_priority", 9999)
        
        # Vote for candidate if they have higher priority (lower port number)
        my_priority = PORT
        vote = candidate_priority < my_priority
        
        print(f"🗳️ Received election from {candidate} (priority {candidate_priority}). My vote: {vote}")
        
        return {"status": "SUCCESS", "vote": vote, "voter": MY_SERVER_ADDRESS}
    
    # 🆕 Handle new primary announcements
    elif action == "new_primary_announcement":
        new_primary = message.get("new_primary")
        print(f"📢 Acknowledged new primary: {new_primary}")
        
        # Update server status
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

            # ✅ CORRIGIDO: Propagar apenas o novo item, não todo o STORE_DATA
            new_data = {key: data}
            propagation_success = propagate_update_to_backups(new_data, ALL_CLUSTER_SERVERS, MY_SERVER_ADDRESS)
            
            if propagation_success:
                return {"status": "SUCCESS", "message": "Dados escritos e propagados para backups."}
            else:
                return {"status": "SUCCESS", "message": "Dados escritos no primário. Alguns backups podem estar indisponíveis."}
        else:
            return {"status": "FAILED", "error": "Este servidor não é o primário."}
            
    elif action == "read":
        # ✅ CORRIGIDO: Retornar dados locais sempre, independente do role
        print(f"📖 Leitura solicitada - Retornando dados locais")
        return {"status": "SUCCESS", "data": STORE_DATA, "server": MY_SERVER_ADDRESS, "role": SERVER_ROLE}
        
    elif action == "update_backup":
        if SERVER_ROLE == "backup":
            updated_data = message.get("data")
            if updated_data:
                # ✅ CORRIGIDO: Atualizar apenas os novos dados recebidos
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
        
    # Initialize server status tracking
    for server in ALL_CLUSTER_SERVERS:
        if server != MY_SERVER_ADDRESS:
            server_status[server] = "UNKNOWN"
    
    print(f"🕐 Waiting 5 seconds before starting ping threads to allow other servers to start...")
    time.sleep(5)  # Give other servers time to start up
    
    # Envia o status inicial para o monitor
    send_monitor_update(SERVER_ROLE.upper(), PORT)
    
    # 🆕 Start the heartbeat thread
    heartbeat_thread = threading.Thread(target=monitor_heartbeat_thread_func)
    heartbeat_thread.daemon = True
    heartbeat_thread.start()

    # 🆕 Start the ping/fault detection thread
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