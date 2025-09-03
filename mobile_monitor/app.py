from flask import Flask, render_template, jsonify
import socket
import threading
import json
import time
import signal, sys
app = Flask(__name__)

# Global state for both clusters
sync_status = {f"sync_{i+1}": {'active': 'INACTIVE', 'last_update': 0, 'client_id': ''} for i in range(5)}
store_status = {f"store_{i+1}": {'role': 'UNKNOWN', 'last_update': 0, 'port': ''} for i in range(3)}

# A map to assign a fixed slot for a given client_id
client_id_to_sync_slot = {}
store_port_to_slot = {}
CLUSTER_STORE_PORTS = [6001, 6002, 6003]

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/status')
def get_status():
    current_time = time.time()
    
    # Clean up old statuses for both clusters (if no update in 5 seconds, consider inactive)
    for sync_key, sync_data in sync_status.items():
        if current_time - sync_data['last_update'] > 5:
            sync_status[sync_key]['active'] = 'INACTIVE'
    
    for store_key, store_data in store_status.items():
        if current_time - store_data['last_update'] > 5:
            store_status[store_key]['role'] = 'INACTIVE'
    
    return jsonify({
        'syncs': sync_status,
        'stores': store_status
    })

def handler(signum, frame):
    print("🛑 Monitor shutting down...")
    sys.exit(0)

signal.signal(signal.SIGTERM, handler)

def start_monitoring_server():
    def server_thread():
        try:
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind(('0.0.0.0', 6000))
            server_socket.listen(10)
            print("🚀 Monitor server started on port 6000")
            
            while True:
                try:
                    conn, addr = server_socket.accept()
                    client_thread = threading.Thread(target=handle_client, args=(conn, addr))
                    client_thread.daemon = True
                    client_thread.start()
                except Exception as e:
                    pass  # Silencioso para conexões
                
        except Exception as e:
            print(f"❌ Monitor server error: {e}")
    
    def handle_client(conn, addr):
        try:
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                
                try:
                    message = json.loads(data.decode('utf-8'))
                    
                    # --- Logic for Cluster Sync nodes ---
                    if message.get('type') == 'sync_status':
                        client_data = message.get('data', {})
                        client_id = client_data.get('client_id', '')
                        status = client_data.get('status', 'INACTIVE')

                        # Só printar mudanças de status importantes
                        if status in ['ENTERING_CRITICAL', 'LEAVING_CRITICAL']:
                            if client_id not in client_id_to_sync_slot:
                                for i in range(5):
                                    key = f"sync_{i+1}"
                                    if sync_status[key]['client_id'] in ('', str(client_id)):
                                        client_id_to_sync_slot[client_id] = key
                                        break

                            if client_id in client_id_to_sync_slot:
                                sync_key = client_id_to_sync_slot[client_id]
                                sync_status[sync_key]['active'] = status
                                sync_status[sync_key]['last_update'] = time.time()
                                sync_status[sync_key]['client_id'] = str(client_id)
                                
                                # Só printar status críticos
                                if status == 'ENTERING_CRITICAL':
                                    print(f"🔴 {sync_key} (Client {client_id}): ENTERING CRITICAL SECTION")
                                elif status == 'LEAVING_CRITICAL':
                                    print(f"🟢 {sync_key} (Client {client_id}): LEAVING CRITICAL SECTION")
                        else:
                            # Para outros status, atualizar silenciosamente
                            if client_id in client_id_to_sync_slot:
                                sync_key = client_id_to_sync_slot[client_id]
                                sync_status[sync_key]['active'] = status
                                sync_status[sync_key]['last_update'] = time.time()
                                sync_status[sync_key]['client_id'] = str(client_id)

                    # --- Logic for Cluster Store nodes ---
                    elif message.get('type') == 'store_status':
                        store_data = message.get('data', {})
                        port = store_data.get('port', '')
                        role = store_data.get('role', 'UNKNOWN')
                        
                        if port not in store_port_to_slot:
                            for i in range(3):
                                key = f"store_{i+1}"
                                if store_status[key]['port'] in ('', port):
                                    store_port_to_slot[port] = key
                                    break
                        
                        if port in store_port_to_slot:
                            store_key = store_port_to_slot[port]
                            old_role = store_status[store_key]['role']
                            store_status[store_key]['role'] = role
                            store_status[store_key]['last_update'] = time.time()
                            store_status[store_key]['port'] = port
                            
                            # Só printar mudanças importantes de role
                            if old_role != role and role in ['PRIMARY', 'BACKUP']:
                                print(f"💾 {store_key} (port {port}): {role}")

                except json.JSONDecodeError:
                    pass  # Silencioso para JSON inválido
                except Exception as e:
                    pass  # Silencioso para outros erros
                
        except Exception as e:
            pass  # Silencioso para erros de conexão
        finally:
            conn.close()
    
    server_thread = threading.Thread(target=server_thread)
    server_thread.daemon = True
    server_thread.start()

def start_conflict_logger():
    def logger_thread():
        last_conflict_syncs = set()
        while True:
            time.sleep(0.5)  # Verificar a cada 0.5s
            active_syncs = [key for key in sync_status if sync_status[key]['active'] == 'ENTERING_CRITICAL']
            
            if len(active_syncs) > 1:
                current_conflict_syncs = set(active_syncs)
                # Só printar se for um novo conflito
                if current_conflict_syncs != last_conflict_syncs:
                    print(f"💥 CONFLICT DETECTED! Multiple syncs in critical section: {active_syncs}")
                    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                    with open("conflict.log", "a") as log_file:
                        log_file.write(f"[{timestamp}] 💥 Conflict! Active syncs: {active_syncs}\n")
                    last_conflict_syncs = current_conflict_syncs
            else:
                last_conflict_syncs = set()
    
    thread = threading.Thread(target=logger_thread)
    thread.daemon = True
    thread.start()

if __name__ == '__main__':
    print("🎯 Starting Mobile Monitor Server...")
    start_monitoring_server()
    start_conflict_logger()
    
    import subprocess
    try:
        result = subprocess.run(['hostname', '-I'], capture_output=True, text=True)
        local_ip = result.stdout.strip().split()[0]
        print(f"🌐 Access from mobile: http://{local_ip}:4999")
    except:
        print("🌐 Access from mobile: http://YOUR_IP:4999")
    
    print("💻 Local access: http://localhost:4999")
    
    # Executar Flask em modo silencioso
    import logging
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)
    
    app.run(host='0.0.0.0', port=4999, debug=False)