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
CLUSTER_STORE_SERVERS = ['store-primary:6001', 'store-backup1:6002', 'store-backup2:6003']

# --- Shared Queues ---
incoming_network_messages_queue = queue.Queue()
incoming_mqtt_messages_queue = queue.Queue()
outgoing_mqtt_publish_queue = queue.Queue()

# --- Global Client Connection Storage ---
client_connections = {}
client_waiting_for_access = {}  # Track which clients are waiting

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
        pass  # Silencioso para não poluir

def network_listener_thread_func():
    """Thread function for listening for incoming network connections."""
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
    """Handles data exchange with a single connected network client."""
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
        pass  # Conexão perdida, normal
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
    """Send a response directly to a specific client via socket."""
    if client_id in client_connections:
        try:
            conn = client_connections[client_id]
            response_json = json.dumps(response_data)
            conn.sendall(response_json.encode('utf-8'))
            
            # Só imprimir respostas importantes
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
    """This function will be run in a thread to process outgoing MQTT publish requests."""
    while True:
        try:
            message_to_publish = outgoing_mqtt_publish_queue.get(timeout=1)
            topic = message_to_publish["topic"]
            payload = message_to_publish["payload"]
            qos = message_to_publish.get("qos", 1)

            if mqtt_client_instance and hasattr(mqtt_client_instance, '_state') and mqtt_client_instance.is_connected():
                result, mid = mqtt_client_instance.publish(topic, payload, qos)
                # Só logar mensagens importantes
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
    """Main application logic thread."""
    print(f"🧠 SYNC {PROCESS_ID}: Application Logic started")
    resource_queue = queue.Queue()
    
    while True:
        try:
            # --- Process Incoming Network Messages ---
            if not incoming_network_messages_queue.empty():
                message = incoming_network_messages_queue.get()

                try:
                    json_data = json.loads(message['payload'])
                    command = json_data.get('command', '')
                    client_id = json_data.get('client_id', '')

                    if command == "REQUEST_ACCESS":
                        # Publish REQUEST_ACCESS to MQTT (for coordination with other syncs)
                        access_payload = json.dumps({
                            "command": command, 
                            "client_id": client_id, 
                            "sync_id": PROCESS_ID, 
                            "timestamp": f"{time.time()}"
                        })
                        outgoing_mqtt_publish_queue.put({"topic": MQTT_TOPIC, "payload": access_payload})
                        print(f"🔄 REQUEST_ACCESS published to MQTT for client {client_id}")
                        
                    elif command == "STORE_ACTION":
                        action = json_data.get("action")
                        data = json_data.get("data")
                        
                        print(f"💾 Client {client_id} performing {action} operation")

                        # Chamar o ClusterStoreClient com timeout
                        try:
                            if action == "write":
                                success = cluster_store_client.send_write_request(data)
                            elif action == "read":
                                result = cluster_store_client.send_read_request()
                                success = bool(result)
                                if success:
                                    print(f"✅ Read successful - got {len(result) if result else 0} items")
                                else:
                                    print("❌ Read failed or returned None")
                            else:
                                success = False
                                
                        except Exception as e:
                            print(f"❌ Cluster store operation failed: {e}")
                            success = False
                            
                        # Notificar resultado
                        if success:
                            send_response_to_client(client_id, {"status": "SUCCESS", "message": "Store action completed"})
                            print(f"✅ Client {client_id} operation {action} completed successfully")
                        else:
                            send_response_to_client(client_id, {"status": "FAILED", "message": "Store action failed"})
                            print(f"❌ Client {client_id} operation {action} failed")
                            
                        # Publicar DONE via MQTT (sempre, independente do sucesso)
                        done_payload = json.dumps({
                            "command": "DONE", 
                            "client_id": client_id, 
                            "sync_id": PROCESS_ID, 
                            "timestamp": f"{time.time()}"
                        })
                        outgoing_mqtt_publish_queue.put({"topic": MQTT_TOPIC, "payload": done_payload})
                        print(f"📡 DONE message published for client {client_id}")

                except json.JSONDecodeError:
                    pass

            # --- Process Incoming MQTT Messages ---
            if not incoming_mqtt_messages_queue.empty():
                message = incoming_mqtt_messages_queue.get()
                print(f"📨 Processing MQTT message from topic: {message['topic']}")

                if message['topic'] == "BCC362":
                    try:
                        payload_data = json.loads(message['payload'])
                        command = payload_data.get('command', '')
                        client_id = payload_data.get('client_id', '')
                        sync_id = payload_data.get('sync_id', '')
                        
                        print(f"📋 MQTT Command: {command} for client {client_id} from sync {sync_id}")
                        
                        if command == "DONE":
                            print(f"🏁 Client {client_id} finished (from sync {sync_id})")
                            if not resource_queue.empty():
                                finished_client = resource_queue.get()
                                print(f"✅ Client {finished_client} removed from queue (remaining: {resource_queue.qsize()})")
                                
                                send_monitor_update("sync_status", {"client_id": finished_client, "status": "LEAVING_CRITICAL"})
                                
                                # Check if there's a next client in queue
                                if not resource_queue.empty():
                                    next_client = resource_queue.queue[0]
                                    print(f"👉 Next client in queue is: {next_client} (queue size: {resource_queue.qsize()})")
                                    
                                    # Only send GRANT_ACCESS via MQTT for coordination
                                    grant_payload = json.dumps({
                                        "command": "GRANT_ACCESS",
                                        "client_id": next_client,
                                        "sync_id": PROCESS_ID,
                                        "timestamp": f"{time.time()}"
                                    })
                                    outgoing_mqtt_publish_queue.put({"topic": MQTT_TOPIC, "payload": grant_payload})
                                    print(f"📡 Published GRANT_ACCESS for client {next_client} to MQTT")
                                else:
                                    print("🔚 Queue is now empty")
                            else:
                                print("⚠️ Received DONE but queue is already empty")
                                    
                        elif command == "REQUEST_ACCESS":
                            print(f"🔄 REQUEST_ACCESS from client {client_id} via sync {sync_id}")
                            resource_queue.put(client_id)
                            print(f"📋 Added client {client_id} to queue (total position: {resource_queue.qsize()})")
                            print(f"🔍 Current queue: {list(resource_queue.queue)}")
                            
                            # If this is the first client, grant access immediately
                            if resource_queue.qsize() == 1:
                                grant_payload = json.dumps({
                                    "command": "GRANT_ACCESS",
                                    "client_id": client_id,
                                    "sync_id": PROCESS_ID,
                                    "timestamp": f"{time.time()}"
                                })
                                outgoing_mqtt_publish_queue.put({"topic": MQTT_TOPIC, "payload": grant_payload})
                                print(f"🎯 Published GRANT_ACCESS for client {client_id} (first in queue)")
                            else:
                                print(f"⏳ Client {client_id} waiting in position {resource_queue.qsize()}")
                                
                        elif command == "GRANT_ACCESS":
                            print(f"🎯 GRANT_ACCESS received for client {client_id} from sync {sync_id}")
                            
                            # Check if this client belongs to this sync and is waiting
                            if client_id in client_waiting_for_access:
                                print(f"✅ Granting access to our client {client_id}")
                                send_response_to_client(client_id, {
                                    "status": "GRANTED",
                                    "message": "Access to resource granted",
                                    "timestamp": time.time()
                                })
                                send_monitor_update("sync_status", {"client_id": client_id, "status": "ENTERING_CRITICAL"})
                            else:
                                print(f"ℹ️ GRANT_ACCESS for client {client_id} not from this sync")
                                
                    except json.JSONDecodeError:
                        pass

            time.sleep(0.1)
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
    """Announce this sync's presence to other syncs"""
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