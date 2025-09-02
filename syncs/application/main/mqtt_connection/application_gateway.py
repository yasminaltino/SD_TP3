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

# --- Configuration ---
# - Broker Connection Configs
MQTT_BROKER_HOST = os.environ.get('MQTT_BROKER_HOST', 'localhost')
MQTT_BROKER_PORT = 1883
MQTT_USER = "your_mqtt_user"
MQTT_PASSWORD = "your_mqtt_password"
MQTT_TOPIC = "BCC362"

# defines an ID for the application. Weak approach, should be changed on a formal application
PROCESS_ID = os.getpid()
MQTT_CLIENT_NAME = f"MyApplicationGateway_{PROCESS_ID}"


NETWORK_LISTEN_HOST = "0.0.0.0"
NETWORK_LISTEN_PORT = int(sys.argv[1])

# --- Cluster Store Config ---
CLUSTER_STORE_PORTS = [6001, 6002, 6003]
CLUSTER_STORE_HOST = os.environ.get('CLUSTER_STORE_HOST', 'cluster_store_primary')

CLUSTER_STORE_SERVERS = [
    "cluster_store_primary:6001",
    "cluster_store_backup1:6002",
    "cluster_store_backup2:6003"
]


# --- Shared Queues ---
incoming_network_messages_queue = queue.Queue()
incoming_mqtt_messages_queue = queue.Queue()
outgoing_mqtt_publish_queue = queue.Queue()

# --- Global Client Connection Storage ---
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
        print(f"❌ Erro ao enviar status para o monitor: {e}")

def network_listener_thread_func():
    """Thread function for listening for incoming network connections."""
    print(f"Network Listener started on {NETWORK_LISTEN_HOST}:{NETWORK_LISTEN_PORT}")
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
        print(f"Error in network listener thread: {e}")

def handle_network_client_connection(conn, addr):
    """Handles data exchange with a single connected network client."""
    print(f"Accepted network connection from {addr}")
    current_client_id = None
    
    try:
        while True:
            data = conn.recv(1024)
            if not data:
                print(f"Client {addr} disconnected.")
                break
            message_content = data.decode('utf-8').strip()
            print(f"Raw Network Message from {addr}: '{message_content}'")

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
            print(f"Network message queued for processing: {message_content}")

    except Exception as e:
        print(f"Error handling network client {addr}: {e}")
    finally:
        if current_client_id:
            # ✅ CORREÇÃO: Publica uma mensagem de DONE quando um cliente se desconecta
            print(f"Client {current_client_id} disconnected, publishing DONE message.")
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
    """Send a response directly to a specific client via socket."""
    if client_id in client_connections:
        try:
            conn = client_connections[client_id]
            response_json = json.dumps(response_data)
            conn.sendall(response_json.encode('utf-8'))
            print(f"Response sent to client {client_id}: {response_json}")
            
            if response_data.get('status') == 'GRANTED':
                client_waiting_for_access.pop(client_id, None)
                
            return True
        except Exception as e:
            print(f"Error sending response to client {client_id}: {e}")
            client_connections.pop(client_id, None)
            client_waiting_for_access.pop(client_id, None)
            return False
    else:
        print(f"No active connection found for client {client_id}")
        return False

def mqtt_publisher_loop_func(mqtt_client_instance):
    """This function will be run in a thread to process outgoing MQTT publish requests."""
    print("MQTT Publisher Loop Thread started.")
    while True:
        try:
            message_to_publish = outgoing_mqtt_publish_queue.get(timeout=1)
            topic = message_to_publish["topic"]
            payload = message_to_publish["payload"]
            qos = message_to_publish.get("qos", 1)

            if mqtt_client_instance and hasattr(mqtt_client_instance, '_state') and mqtt_client_instance.is_connected():
                result, mid = mqtt_client_instance.publish(topic, payload, qos)
                if result == 0:
                    print(f"MQTT Published: Topic='{topic}', Payload='{payload}', QoS={qos}, MID={mid}")
                else:
                    print(f"MQTT Publish failed for topic {topic} with result {result}")
            else:
                print("MQTT client not connected, requeueing message...")
                outgoing_mqtt_publish_queue.put(message_to_publish)
                time.sleep(1)

        except queue.Empty:
            pass
        except Exception as e:
            print(f"Error in MQTT publisher loop: {e}")
        time.sleep(0.01)

def application_logic_thread_func():
    """Main application logic thread."""
    print("Application Logic Thread started.")
    resource_queue = queue.Queue()
    
    while True:
        try:
            # --- Process Incoming Network Messages ---
            if not incoming_network_messages_queue.empty():
                message = incoming_network_messages_queue.get()
                print(f"Processing Network Message: {message['payload']}")

                try:
                    json_data = json.loads(message['payload'])
                    print(f"Parsed JSON data: {json_data}")
                    
                    command = json_data.get('command', '')
                    client_id = json_data.get('client_id', '')

                    if command == "REQUEST_ACCESS":
                        send_response_to_client(client_id, {
                            "status": "WAIT",
                            "message": "Access request received. Waiting for resource."
                        })
                        
                        send_monitor_update("sync_status", {"client_id": client_id, "status": "WAITING"})

                        
                        access_payload = json.dumps({
                            "command": command, 
                            "client_id": client_id, 
                            "sync_id": PROCESS_ID, 
                            "timestamp": f"{time.time()}"
                        })
                        outgoing_mqtt_publish_queue.put({"topic": MQTT_TOPIC, "payload": access_payload})
                        print(">> Logic: Queued REQUEST_ACCESS publish to MQTT.")
                        
                    elif command == "STORE_ACTION":
                         # 1. Obter a ação (leitura ou escrita) e os dados do cliente
                        action = json_data.get("action")
                        data = json_data.get("data")
                        
                        send_monitor_update("sync_status", {"client_id": client_id, "status": "ENTERING_CRITICAL"})

                        
                        # 2. Chamar o ClusterStoreClient para realizar a operação
                        
                        if action == "write":
                            success = cluster_store_client.send_write_request(data)
                        elif action == "read":
                            result = cluster_store_client.send_read_request()
                            if result:
                                success = True
                            else:
                                success = False
                            
                        # 3. Notificar o cliente do resultado e publicar o 'DONE'
                        if success:
                            send_response_to_client(client_id, {"status": "SUCCESS", "message": "Store action completed"})
                            
                            done_payload = json.dumps({
                                "command": "DONE", 
                                "client_id": client_id, 
                                "sync_id": PROCESS_ID, 
                                "timestamp": f"{time.time()}"
                            })
                            outgoing_mqtt_publish_queue.put({"topic": MQTT_TOPIC, "payload": done_payload})
                        else:
                            send_response_to_client(client_id, {"status": "FAILED", "message": "Store action failed"})
                            done_payload = json.dumps({
                                "command": "DONE", 
                                "client_id": client_id, 
                                "sync_id": PROCESS_ID, 
                                "timestamp": f"{time.time()}"
                            })
                            outgoing_mqtt_publish_queue.put({"topic": MQTT_TOPIC, "payload": done_payload})
                            
                    elif command == "DONE":
                        # ✅ CORREÇÃO: Nova lógica para lidar com o DONE
                        print(f">> Logic: 'DONE' message received from client {client_id} via sync {sync_id}.")
                        # A remoção do cliente da fila só deve ocorrer se ele for o primeiro
                        if not resource_queue.empty() and resource_queue.queue[0] == client_id:
                            finished_client = resource_queue.get()
                            print(f">> Logic: Client {finished_client} removed from queue")
                            
                            send_monitor_update("sync_status", {"client_id": finished_client, "status": "LEAVING_CRITICAL"})

                            # Verifica se ainda há clientes na fila
                            if not resource_queue.empty():
                                next_client = resource_queue.queue[0]
                                print(f">> Logic: Next client in queue is: {next_client}")
                                
                                # Publica o GRANT_ACCESS para o próximo cliente
                                grant_payload = json.dumps({
                                    "command": "GRANT_ACCESS",
                                    "client_id": next_client,
                                    "sync_id": PROCESS_ID,
                                    "timestamp": f"{time.time()}"
                                })
                                outgoing_mqtt_publish_queue.put({"topic": MQTT_TOPIC, "payload": grant_payload})
                                print(f">> Logic: Published GRANT_ACCESS for client {next_client} to MQTT")

                        # A notificação de DONE para o cliente final é redundante, pois ele já tem a resposta do STORE_ACTION.
                        # Podemos remover a chamada para send_response_to_client aqui.

                except json.JSONDecodeError:
                    print("Message is not valid JSON...")

            # --- Process Incoming MQTT Messages ---
            if not incoming_mqtt_messages_queue.empty():
                message = incoming_mqtt_messages_queue.get()
                print(f"Processing MQTT Message: Topic='{message['topic']}'")

                if message['topic'] == "BCC362":
                    try:
                        payload_data = json.loads(message['payload'])
                        command = payload_data.get('command', '')
                        client_id = payload_data.get('client_id', '')
                        sync_id = payload_data.get('sync_id', '')
                        
                        if command == "DONE":
                            print(f">> Logic: 'DONE' message received from client {client_id} via sync {sync_id}.")
                            # A remoção do cliente da fila só deve ocorrer se ele for o primeiro
                            if not resource_queue.empty() and resource_queue.queue[0] == client_id:
                                finished_client = resource_queue.get()
                                print(f">> Logic: Client {finished_client} removed from queue")
                                
                                send_monitor_update("sync_status", {"client_id": finished_client, "status": "LEAVING_CRITICAL"})
                                
                                # Verifica se ainda há clientes na fila
                                if not resource_queue.empty():
                                    next_client = resource_queue.queue[0]
                                    print(f">> Logic: Next client in queue is: {next_client}")
                                    
                                    # Publica o GRANT_ACCESS para o próximo cliente
                                    grant_payload = json.dumps({
                                        "command": "GRANT_ACCESS",
                                        "client_id": next_client,
                                        "sync_id": PROCESS_ID,
                                        "timestamp": f"{time.time()}"
                                    })
                                    outgoing_mqtt_publish_queue.put({"topic": MQTT_TOPIC, "payload": grant_payload})
                                    print(f">> Logic: Published GRANT_ACCESS for client {next_client} to MQTT")
                        
                        elif command == "REQUEST_ACCESS":
                            print(f">> Logic: REQUEST_ACCESS from client {client_id} via sync {sync_id}")
                            resource_queue.put(client_id)
                            print(f">> Logic: Added client {client_id} to queue (position: {resource_queue.qsize()})")
                            
                            send_monitor_update("sync_status", {"client_id": client_id, "status": "WAITING"})
                            
                            # A concessão de acesso só deve ocorrer se a fila estava vazia antes
                            if resource_queue.qsize() == 1:
                                grant_payload = json.dumps({
                                    "command": "GRANT_ACCESS",
                                    "client_id": client_id,
                                    "sync_id": PROCESS_ID,
                                    "timestamp": f"{time.time()}"
                                })
                                outgoing_mqtt_publish_queue.put({"topic": MQTT_TOPIC, "payload": grant_payload})
                                print(f">> Logic: Published GRANT_ACCESS for client {client_id} (first in queue)")
                                
                        elif command == "GRANT_ACCESS":
                            print(f">> Logic: GRANT_ACCESS received for client {client_id} from sync {sync_id}")
                            
                            if client_id in client_waiting_for_access:
                                print(f">> Logic: Granting access to our client {client_id}")
                                response_sent = send_response_to_client(client_id, {
                                    "status": "GRANTED",
                                    "message": "Access to resource granted",
                                    "timestamp": time.time()
                                })
                                if response_sent:
                                    print(f">> Logic: Successfully notified client {client_id} of granted access")
                                else:
                                    print(f">> Logic: Failed to notify client {client_id}")
                            else:
                                print(f">> Logic: GRANT_ACCESS for client {client_id} not from this sync")
                                
                    except json.JSONDecodeError:
                        print(f">> Logic: Invalid JSON in MQTT message")

            time.sleep(0.1)
        except Exception as e:
            print(f"Error in application logic loop: {e}")
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
    """Announce this sync's presence to other syncs"""
    announce_payload = json.dumps({
        "command": "SYNC_ANNOUNCE",
        "sync_id": PROCESS_ID,
        "port": NETWORK_LISTEN_PORT,
        "timestamp": time.time()
    })
    outgoing_mqtt_publish_queue.put({"topic": MQTT_TOPIC, "payload": announce_payload})

if __name__ == "__main__":
    print("Starting Main Application Gateway...")

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
        print("Could not start MQTT publisher thread: MQTT client not initialized.")
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
        print("\nCtrl+C detected. Shutting down application...")
    finally:
        if mqtt_connection_manager:
            mqtt_connection_manager.end_connection()
        print("Application Gateway stopped.")
