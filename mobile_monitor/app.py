from flask import Flask, render_template, jsonify
import socket
import threading
import json
import time
import signal, sys
app = Flask(__name__)

# Estado global para ambos os clusters
sync_status = {f"sync_{i+1}": {'active': 'INACTIVE', 'last_update': 0, 'client_id': ''} for i in range(5)}
store_status = {f"store_{i+1}": {'role': 'UNKNOWN', 'last_update': 0, 'port': ''} for i in range(3)}

# Mapeamento para atribuir um slot fixo para um dado client_id
client_id_to_sync_slot = {}
store_port_to_slot = {}
CLUSTER_STORE_PORTS = [6001, 6002, 6003]

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/status')
def get_status():
    current_time = time.time()
    
    # Limpa status antigos para ambos os clusters (se não há atualização em 5 segundos, considera inativo)
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
                    
                    # Lógica para nós do Cluster Sync
                    if message.get('type') == 'sync_status':
                        client_data = message.get('data', {})
                        client_id = client_data.get('client_id', '')
                        status = client_data.get('status', 'INACTIVE')

                        # Só imprime mudanças de status importantes
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
                                
                                # Só imprime status críticos
                                if status == 'ENTERING_CRITICAL':
                                    print(f"🔴 {sync_key} (Client {client_id}): ENTERING CRITICAL SECTION")
                                elif status == 'LEAVING_CRITICAL':
                                    print(f"🟢 {sync_key} (Client {client_id}): LEAVING CRITICAL SECTION")
                    
                    # Lógica para nós do Cluster Store
                    elif message.get('type') == 'store_status':
                        store_data = message.get('data', {})
                        port = store_data.get('port', '')
                        role = store_data.get('role', 'UNKNOWN')
                        
                        if port not in store_port_to_slot:
                            for i in range(3):
                                key = f"store_{i+1}"
                                if store_status[key]['port'] in ('', str(port)):
                                    store_port_to_slot[port] = key
                                    break
                        
                        if port in store_port_to_slot:
                            store_key = store_port_to_slot[port]
                            store_status[store_key]['role'] = role
                            store_status[store_key]['last_update'] = time.time()
                            store_status[store_key]['port'] = str(port)
                            
                            # Imprime mudanças de papel importantes
                            if role in ['PRIMARY', 'BACKUP']:
                                print(f"🏪 {store_key} (Port {port}): Role changed to {role}")
                    
                    # Lógica para clientes
                    elif message.get('type') == 'client_status':
                        client_data = message.get('data', {})
                        client_id = client_data.get('client_id', '')
                        status = client_data.get('status', 'UNKNOWN')
                        
                        print(f"👤 Client {client_id}: {status}")
                
                except json.JSONDecodeError:
                    print(f"❌ Invalid JSON from {addr}")
                except Exception as e:
                    print(f"❌ Error processing message from {addr}: {e}")
                    
        except Exception as e:
            print(f"❌ Client connection error: {e}")
        finally:
            try:
                conn.close()
            except:
                pass
    
    # Inicia thread do servidor
    server_thread = threading.Thread(target=server_thread)
    server_thread.daemon = True
    server_thread.start()

if __name__ == '__main__':
    print("🚀 Starting Mobile Monitor...")
    
    # Inicia servidor de monitoramento
    start_monitoring_server()
    
    # Inicia servidor Flask
    app.run(host='0.0.0.0', port=5000, debug=False)