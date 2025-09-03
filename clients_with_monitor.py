import socket
import time
import json
import os
import random
import datetime
import sys

# Configurações do cliente
SERVER_HOST = os.environ.get('SERVER_HOST', 'localhost')
SERVER_PORT = int(os.environ.get('SERVER_PORT', 5000))

# Configurações do monitor
MONITOR_HOST = os.environ.get('MONITOR_HOST', 'localhost')
MONITOR_PORT = 6000
client_id = os.getenv("CLIENT_ID", "0")

def log_event(event_type, client_id, message=""):
    """Registra eventos no arquivo de log."""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    log_entry = f"[{timestamp}] Client {client_id}: {event_type}"
    if message:
        log_entry += f" - {message}"
    log_entry += "\n"
    
    try:
        with open("log.txt", "a", encoding="utf-8") as log_file:
            log_file.write(log_entry)
    except Exception as e:
        print(f"Error writing to log: {e}")

def send_monitor_update(status):
    """Envia atualização de status para o aplicativo monitor."""
    try:
        monitor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        monitor_socket.settimeout(2)  # Timeout de 2 segundos
        monitor_socket.connect((MONITOR_HOST, MONITOR_PORT))
        
        message = json.dumps({
            "client_id": client_id,
            "status": status,
            "timestamp": time.time()
        })
        
        monitor_socket.sendall(message.encode('utf-8'))
        monitor_socket.close()
        print(f"✅ Monitor update sent: {status}")
        return True
        
    except Exception as e:
        print(f"❌ Failed to send monitor update: {e}")
        return False

class PersistentClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.socket = None
        self.connected = False
    
    def connect(self):
        """Estabelece conexão com o servidor de sincronização."""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            self.connected = True
            print(f"Client: Connected to server at {self.host}:{self.port}")
            return True
        except ConnectionRefusedError:
            print(f"Client: Connection refused. Is the server running on {self.host}:{self.port}?")
            return False
        except Exception as e:
            print(f"Client: Connection error: {e}")
            return False
    
    def send_message_and_wait_response(self, message, timeout=30):
        """Envia mensagem e aguarda resposta do servidor de sincronização."""
        if not self.connected:
            print("Client: Not connected to server.")
            return None
            
        try:
            self.socket.sendall(message.encode('utf-8'))
            print(f"Client: Sent message: '{message}'")
            
            self.socket.settimeout(timeout)
            response_data = self.socket.recv(1024)
            if response_data:
                response = response_data.decode('utf-8')
                print(f"Client: Received response: '{response}'")
                return response
            else:
                print("Client: No response received.")
                return None
                
        except socket.timeout:
            print(f"Client: Timeout waiting for response ({timeout}s)")
            return None
        except Exception as e:
            print(f"Client: Error during communication: {e}")
            return None
    
    def disconnect(self):
        """Fecha a conexão."""
        if self.socket and self.connected:
            self.socket.close()
            self.connected = False
            print("Client: Connection closed.")

if __name__ == "__main__":
    print(f"--- Starting Client Application (PID: {client_id}) ---")
    
    print("🔍 Testing monitor connection...")
    if send_monitor_update("TEST_CONNECTION"):
        print("✅ Monitor connection successful!")
    else:
        print("❌ Monitor connection failed - continuing anyway...")
    
    client = PersistentClient(SERVER_HOST, SERVER_PORT)
    
    if not client.connect():
        print("Client: Failed to connect. Exiting.")
        exit(1)
    
    try:
        for request_number in range(1, 51):  # O cliente acessa o recurso 50 vezes
            print(f"\n=== Request {request_number}/50 ===")
            
            json_message = {"command": "REQUEST_ACCESS", "client_id": f"{client_id}"}
            log_event("ACCESS_REQUEST", client_id, f"Request #{request_number}")
            
            response = client.send_message_and_wait_response(json.dumps(json_message))

            if response:
                try:
                    response_data = json.loads(response)
                    if response_data.get("status") == "GRANTED":
                        log_event("ACCESS_GRANTED", client_id, f"Request #{request_number}")
                        print(f"🎉 Client {client_id}: Access granted for request {request_number}!")
                        
                        action = random.choice(["write", "read"])
                        
                        if action == "write":
                            data_to_write = f"Data-from-client-{client_id}-at-{time.time()}"
                            store_message = {
                                "command": "STORE_ACTION",
                                "client_id": f"{client_id}",
                                "action": action,
                                "data": data_to_write
                            }
                            print(f"Client {client_id}: Enviando requisição de escrita para o sync...")
                        else:
                            store_message = {
                                "command": "STORE_ACTION",
                                "client_id": f"{client_id}",
                                "action": action
                            }
                            print(f"Client {client_id}: Enviando requisição de leitura para o sync...")

                        # Envia a mensagem para o Sync e espera a resposta da operação
                        store_response = client.send_message_and_wait_response(json.dumps(store_message))
                        
                        if store_response:
                            try:
                                store_response_data = json.loads(store_response)
                                if store_response_data.get("status") == "SUCCESS":
                                    log_event("STORE_ACTION_SUCCESS", client_id, f"Action: {action}, Response: {store_response}")
                                    print(f"✅ Client {client_id}: Operação de '{action}' no Cluster Store concluída com sucesso.")
                                else:
                                    log_event("STORE_ACTION_FAILED", client_id, f"Action: {action}, Error: {store_response}")
                                    print(f"❌ Client {client_id}: Falha na operação de '{action}' no Cluster Store.")
                            except json.JSONDecodeError:
                                log_event("ERROR", client_id, f"Resposta inválida do sync: {store_response}")
                                print(f"❌ Resposta inválida do sync.")
                        
                        time.sleep(random.uniform(1, 3))
                        
                    elif response_data.get("status") == "WAIT":
                        log_event("ACCESS_DENIED", client_id, f"Request #{request_number}")
                        print(f"⏳ Client {client_id}: Access denied, need to wait.")
                    else:
                        log_event("UNEXPECTED_RESPONSE", client_id, f"Request #{request_number} - Response: {response}")
                        print(f"❓ Unexpected response: {response}")
                        break
                        
                except json.JSONDecodeError:
                    log_event("ERROR", client_id, f"Request #{request_number} - Invalid JSON response: {response}")
                    print(f"❌ Invalid JSON response: {response}")
                    break
            else:
                log_event("ERROR", client_id, f"Request #{request_number} - No response received")
                print(f"❌ No response received")
                break
        
        print(f"\n🏁 Client {client_id}: Completed all requests.")
        log_event("COMPLETED", client_id, "Finished all requests")
            
    except KeyboardInterrupt:
        log_event("INTERRUPTED", client_id, "Client interrupted by user")
        print(f"\n⏹️ Client {client_id}: Interrupted by user.")
    finally:
        send_monitor_update("CLIENT_FINISHED")
        client.disconnect()
        print(f"--- Client {client_id} application finished. ---")
