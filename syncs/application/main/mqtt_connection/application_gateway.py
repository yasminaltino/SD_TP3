import threading
import queue
import time
import json
import socket
import os
import queue
import sys

from mqtt_client_connection import MqttClientConnection
from cluster_store.cluster_store_client import ClusterStoreClient

# Configurações de conexão com o broker
MQTT_BROKER_HOST = os.environ.get('MQTT_BROKER_HOST', 'localhost')
MQTT_BROKER_PORT = 1883
MQTT_USER = "your_mqtt_user"
MQTT_PASSWORD = "your_mqtt_password"
MQTT_TOPIC = "BCC362"

# Define um ID para a aplicação.
PROCESS_ID = os.getpid()
MQTT_CLIENT_NAME = f"MyApplicationGateway_{PROCESS_ID}"

NETWORK_LISTEN_HOST = "0.0.0.0"
NETWORK_LISTEN_PORT = int(sys.argv[1])

# Configurações do Cluster Store
CLUSTER_STORE_SERVERS = ['store-primary:6001', 'store-backup1:6002', 'store-backup2:6003']

# Filas compartilhadas
incoming_network_messages_queue = queue.Queue()
incoming_mqtt_messages_queue = queue.Queue()
outgoing_mqtt_publish_queue = queue.Queue()

# Armazenamento global de conexões de clientes
client_connections = {}
client_waiting_for_access = {} 

mqtt_connection_manager = None
cluster_store_client = ClusterStoreClient(CLUSTER_STORE_SERVERS)

MONITOR_HOST = os.environ.get('MONITOR_HOST', 'monitor')
MONITOR_PORT = 6000

def send_monitor_update(message_type, data):
    """Envia uma mensagem de status para o monitor."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2)
            s.connect((MONITOR_HOST, MONITOR_PORT))
            
            message = {
                "type": message_type,
                "data": data,
                "timestamp": time.time()
            }
            s.sendall(json.dumps(message).encode('utf-8'))
            
    except Exception as e:
        pass

def network_listener_thread_func():
    """Função da thread para escutar conexões de rede recebidas."""
    print(f"🚀 SYNC {PROCESS_ID}: Network Listener started on port {NETWORK_LISTEN_PORT}")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((NETWORK_LISTEN_HOST, NETWORK_LISTEN_PORT))
            s.listen(5)

            while True:
                conn, addr = s.accept()
                client_handler_thread = threading.Thread(target=handle_network_client_connection, args=(conn, addr))
                client_handler_thread.daemon = True
                client_handler_thread.start()
    except Exception as e:
        print(f"❌ Error in network listener thread: {e}")

def handle_network_client_connection(conn, addr):
    """Manipula troca de dados com um único cliente de rede conectado."""
    current_client_id = None
    
    try:
        while True:
            data = conn.recv(1024)
            if not data:
                break
            message_content = data.decode('utf-8').strip()

            try:
                json_data = json.loads(message_content)
                client_id = json_data.get('client_id', '')
                current_client_id = client_id
                
                client_connections[client_id] = conn
                
                if json_data.get('command') == 'REQUEST_ACCESS':
                    client_waiting_for_access[client_id] = conn
                    
            except json.JSONDecodeError:
                pass

            message_data = {
                "source": "network",
                "client_address": addr[0],
                "client_port": addr[1],
                "payload": message_content,
                "connection": conn
            }
            incoming_network_messages_queue.put(message_data)

    except Exception as e:
        pass
    finally:
        if current_client_id:
            print(f"🔌 Client {current_client_id} disconnected")
            done_payload = json.dumps({
                "command": "DONE", 
                "client_id": current_client_id, 
                "sync_id": PROCESS_ID, 
                "timestamp": f"{time.time()}"
            })
            outgoing_mqtt_publish_queue.put({"topic": MQTT_TOPIC, "payload": done_payload})
            client_connections.pop(current_client_id, None)
            client_waiting_for_access.pop(current_client_id, None)
        conn.close()

def send_response_to_client(client_id, response_data):
    """Envia uma resposta diretamente para um cliente específico via socket."""
    if client_id in client_connections:
        try:
            conn = client_connections[client_id]
            response_json = json.dumps(response_data)
            conn.sendall(response_json.encode('utf-8'))
            
            if response_data.get('status') == 'GRANTED':
                print(f"✅ GRANTED access to client {client_id}")
                client_waiting_for_access.pop(client_id, None)
            elif response_data.get('status') == 'SUCCESS':
                print(f"✅ Client {client_id} completed store operation")
                
            return True
        except Exception as e:
            client_connections.pop(client_id, None)
            client_waiting_for_access.pop(client_id, None)
            return False
    else:
        return False

def mqtt_publisher_loop_func(mqtt_client_instance):
    """Esta função será executada em uma thread para processar requisições de publicação MQTT."""
    while True:
        try:
            message_to_publish = outgoing_mqtt_publish_queue.get(timeout=1)
            topic = message_to_publish["topic"]
            payload = message_to_publish["payload"]
            qos = message_to_publish.get("qos", 1)

            if mqtt_client_instance and hasattr(mqtt_client_instance, '_state') and mqtt_client_instance.is_connected():
                result, mid = mqtt_client_instance.publish(topic, payload, qos)
                payload_data = json.loads(payload)
                command = payload_data.get('command', '')
                if command in ['DONE', 'GRANT_ACCESS']:
                    print(f"📡 MQTT: {command} for client {payload_data.get('client_id', 'unknown')}")
            else:
                outgoing_mqtt_publish_queue.put(message_to_publish)
                time.sleep(1)

        except queue.Empty:
            pass
        except Exception as e:
            pass
        time.sleep(0.01)

def application_logic_thread_func():
    """Thread principal da lógica da aplicação."""
    print(f"🧠 SYNC {PROCESS_ID}: Application Logic started")
    resource_queue = queue.Queue()
    
    while True:
        try:
            # Processa mensagens de rede recebidas
            if not incoming_network_messages_queue.empty():
                message = incoming_network_messages_queue.get()

                try:
                    json_data = json.loads(message['payload'])
                    command = json_data.get('command', '')
                    client_id = json_data.get('client_id', '')

                    if command == "REQUEST_ACCESS":
                        # Publica REQUEST_ACCESS no MQTT (para coordenação com outros syncs)
                        access_payload = json.dumps({
                            "command": command, 
                            "client_id": client_id, 
                            "sync_id": PROCESS_ID, 
                            "timestamp": f"{time.time()}"
                        })
                        outgoing_mqtt_publish_queue.put({"topic": MQTT_TOPIC, "payload": access_payload})
                        
                        # Verifica se o recurso está livre
                        if resource_queue.empty():
                            resource_queue.put(client_id)
                            send_monitor_update("sync_status", {
                                "client_id": client_id,
                                "status": "ENTERING_CRITICAL",
                                "sync_id": PROCESS_ID
                            })
                            
                            response_data = {
                                "status": "GRANTED",
                                "message": "Access granted immediately"
                            }
                            send_response_to_client(client_id, response_data)
                        else:
                            # Recurso ocupado, cliente deve aguardar
                            response_data = {
                                "status": "WAIT",
                                "message": "Resource busy, please wait"
                            }
                            send_response_to_client(client_id, response_data)
                    
                    elif command == "STORE_ACTION":
                        # Executa ação no Cluster Store
                        action = json_data.get('action', '')
                        
                        if action == "write":
                            data = json_data.get('data', '')
                            success = cluster_store_client.send_write_request(data)
                        elif action == "read":
                            data = cluster_store_client.send_read_request()
                            success = data is not None
                        else:
                            success = False
                        
                        if success:
                            response_data = {
                                "status": "SUCCESS",
                                "message": f"Store {action} completed",
                                "data": data if action == "read" else None
                            }
                        else:
                            response_data = {
                                "status": "ERROR",
                                "message": f"Store {action} failed"
                            }
                        
                        send_response_to_client(client_id, response_data)
                        
                        # Libera o recurso após a operação
                        if not resource_queue.empty():
                            resource_queue.get()
                            send_monitor_update("sync_status", {
                                "client_id": client_id,
                                "status": "LEAVING_CRITICAL",
                                "sync_id": PROCESS_ID
                            })
                            
                            # Notifica outros syncs que o recurso foi liberado
                            done_payload = json.dumps({
                                "command": "DONE", 
                                "client_id": client_id, 
                                "sync_id": PROCESS_ID, 
                                "timestamp": f"{time.time()}"
                            })
                            outgoing_mqtt_publish_queue.put({"topic": MQTT_TOPIC, "payload": done_payload})
                
                except json.JSONDecodeError:
                    print(f"❌ Invalid JSON from client")
                except Exception as e:
                    print(f"❌ Error processing network message: {e}")
            
            # Processa mensagens MQTT recebidas
            if not incoming_mqtt_messages_queue.empty():
                mqtt_message = incoming_mqtt_messages_queue.get()
                
                try:
                    payload_data = json.loads(mqtt_message['payload'])
                    command = payload_data.get('command', '')
                    sender_sync_id = payload_data.get('sync_id', '')
                    
                    if command == "REQUEST_ACCESS" and sender_sync_id != PROCESS_ID:
                        # Outro sync está solicitando acesso
                        client_id = payload_data.get('client_id', '')
                        
                        if not resource_queue.empty():
                            # Recurso ocupado, adiciona à fila
                            resource_queue.put(client_id)
                        else:
                            # Recurso livre, concede acesso
                            resource_queue.put(client_id)
                    
                    elif command == "DONE" and sender_sync_id != PROCESS_ID:
                        # Outro sync liberou o recurso
                        if not resource_queue.empty():
                            resource_queue.get()
                            
                            # Verifica se há clientes aguardando
                            if not resource_queue.empty():
                                next_client_id = resource_queue.get()
                                resource_queue.put(next_client_id)
                                
                                # Concede acesso ao próximo cliente
                                response_data = {
                                    "status": "GRANTED",
                                    "message": "Access granted from queue"
                                }
                                send_response_to_client(next_client_id, response_data)
                
                except json.JSONDecodeError:
                    print(f"❌ Invalid JSON from MQTT")
                except Exception as e:
                    print(f"❌ Error processing MQTT message: {e}")
            
            time.sleep(0.01)
            
        except Exception as e:
            print(f"❌ Error in application logic: {e}")
            time.sleep(1)

def publish_queue_state(resource_queue):
    queue_state = {
        "command": "QUEUE_UPDATE",
        "queue": list(resource_queue.queue),
        "sync_id": PROCESS_ID,
        "timestamp": time.time()
    }
    outgoing_mqtt_publish_queue.put({
        "topic": MQTT_TOPIC, 
        "payload": json.dumps(queue_state)
    })

def announce_sync_presence():
    """Anuncia a presença deste sync para outros syncs"""
    announce_payload = json.dumps({
        "command": "SYNC_ANNOUNCE",
        "sync_id": PROCESS_ID,
        "port": NETWORK_LISTEN_PORT,
        "timestamp": time.time()
    })
    outgoing_mqtt_publish_queue.put({"topic": MQTT_TOPIC, "payload": announce_payload})

if __name__ == "__main__":
    print(f"🚀 Starting Application Gateway (SYNC {PROCESS_ID})...")

    mqtt_connection_manager = MqttClientConnection(
        broker_ip=MQTT_BROKER_HOST,
        port=MQTT_BROKER_PORT,
        client_name=MQTT_CLIENT_NAME,
        username=MQTT_USER,
        password=MQTT_PASSWORD
    )

    mqtt_connection_manager.set_callback_userdata("incoming_mqtt_messages_queue", incoming_mqtt_messages_queue)
    mqtt_connection_manager.start_connection()

    actual_mqtt_paho_client = mqtt_connection_manager.get_mqtt_client_instance()

    if actual_mqtt_paho_client:
        mqtt_publisher_thread = threading.Thread(target=mqtt_publisher_loop_func, args=(actual_mqtt_paho_client,))
        mqtt_publisher_thread.daemon = True
        mqtt_publisher_thread.start()
    else:
        print("❌ Could not start MQTT publisher thread")
        exit(1)

    network_listener_thread = threading.Thread(target=network_listener_thread_func)
    network_listener_thread.daemon = True
    network_listener_thread.start()

    application_logic_thread = threading.Thread(target=application_logic_thread_func)
    application_logic_thread.daemon = True
    application_logic_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n🛑 SYNC {PROCESS_ID}: Shutting down...")
    finally:
        if mqtt_connection_manager:
            mqtt_connection_manager.end_connection()
        print(f"🛑 SYNC {PROCESS_ID}: Stopped.")