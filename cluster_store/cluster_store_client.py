import json
import random
import socket
import time

class ClusterStoreClient:
    def __init__(self, servers):
        """
        Initializes the client with a list of server addresses (e.g., ['store-primary:6001', 'store-backup1:6002']).
        """
        self.servers = servers
        self.primary_index = 0
        self.primary_host, self.primary_port = self.servers[self.primary_index].split(':')
        self.primary_port = int(self.primary_port)
        self.conns = {}
        
    def find_current_primary(self):
        """Find which server is currently accepting writes (the primary)."""
        
        for i, server in enumerate(self.servers):
            host, port = server.split(':')
            port = int(port)
            
            try:
                conn = self.get_connection(host, port)
                if not conn:
                    continue
                    
                # Send a ping to check if this server is primary
                test_message = {"action": "ping"}
                conn.sendall(json.dumps(test_message).encode('utf-8'))
                
                response_data = conn.recv(4096)
                if response_data:
                    response = json.loads(response_data.decode('utf-8'))
                    if response.get("status") == "PONG" and response.get("role") == "primary":
                        print(f"✅ Found primary at {host}:{port}")
                        self.primary_index = i
                        self.primary_host, self.primary_port = host, port
                        return True
                    else:
                        role = response.get("role", "unknown")
                        print(f"ℹ️ {host}:{port} is {role}")
                        continue
                        
            except Exception as e:
                print(f"❌ Error testing {host}:{port}: {e}")
                # Close failed connection
                key = f"{host}:{port}"
                if key in self.conns:
                    try:
                        self.conns[key].close()
                    except:
                        pass
                    del self.conns[key]
                continue
                
        print("❌ No primary server found!")
        return False
        
    def get_connection(self, host, port):
        """Get or create a connection for a specific host and port."""
        key = f"{host}:{port}"
        if key not in self.conns:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(8) 
                s.connect((host, port))
                self.conns[key] = s
                return s
            except(socket.error, socket.timeout) as e:
                print (f"Failed to connect to server at {host}:{port}: {e}")
                return None
        return self.conns[key]
    
    def close_connections(self):
        """Close all active connections."""
        for conn in self.conns.values():
            conn.close()
        self.conns = {}
    
    def send_write_request(self, data):
        """Send a write request to the primary server."""
        max_retries = 3
        for attempt in range(max_retries):
            print(f"✍️ Write attempt {attempt + 1}: Trying primary {self.primary_host}:{self.primary_port}")
            try:
                conn = self.get_connection(self.primary_host, self.primary_port)
                if not conn:
                    print(f"❌ Failed to connect to primary")
                    if not self.find_current_primary():
                        continue
                    # Retry with new primary
                    conn = self.get_connection(self.primary_host, self.primary_port)
                    if not conn:
                        continue

                message = {"action": "write", "data": data}
                conn.sendall(json.dumps(message).encode('utf-8'))
                
                response_data = conn.recv(4096)  # Increased buffer size
                if response_data:
                    response = json.loads(response_data.decode('utf-8'))
                    if response.get("status") == "SUCCESS":
                        print(f"✅ Write successful to primary {self.primary_host}:{self.primary_port}")
                        return True
                    elif "não é o primário" in response.get('error', '').lower():
                        print(f"⚠️ Server {self.primary_host}:{self.primary_port} is no longer primary, searching for new primary...")
                        if self.find_current_primary():
                            continue  # Retry with new primary
                        else:
                            return False
                    else:
                        error_msg = response.get('error', 'unknown error')
                        print(f"❌ Primary reported error: {error_msg}")
                        return False
                else:
                    print(f"❌ No response from primary")
                    return False
                    
            except (socket.timeout, socket.error) as e:
                print(f"❌ Communication error with primary: {e}")
                # Close the failed connection
                key = f"{self.primary_host}:{self.primary_port}"
                if key in self.conns:
                    try:
                        self.conns[key].close()
                    except:
                        pass
                    del self.conns[key]
                
                # Try to find new primary
                if not self.find_current_primary():
                    time.sleep(1)
                    continue
                time.sleep(1)

        print("❌ Write failed after all retries")
        return False
    
    def send_read_request(self):
        """Send a read request to a random replica."""
        max_attempts = 3
        for attempt in range(max_attempts):
            random_server = random.choice(self.servers)
            read_host, read_port = random_server.split(':')
            read_port = int(read_port)
            
            print(f"📖 Attempt {attempt + 1}: Trying to read from {read_host}:{read_port}")
            
            try:
                conn = self.get_connection(read_host, read_port)
                if not conn:
                    print(f"❌ Failed to connect to {read_host}:{read_port}")
                    continue

                message = {"action": "read"}
                conn.sendall(json.dumps(message).encode('utf-8'))

                response_data = conn.recv(4096)  # Increased buffer size
                if response_data:
                    response = json.loads(response_data.decode('utf-8'))
                    if response.get("status") == "SUCCESS":
                        data = response.get("data")
                        print(f"✅ Read successful from {read_host}:{read_port} - got {len(data) if data else 0} items")
                        return data
                    else:
                        error_msg = response.get('error', 'unknown error')
                        print(f"❌ Server {read_host}:{read_port} reported error: {error_msg}")
                        continue
                else:
                    print(f"❌ No response from {read_host}:{read_port}")
                    continue
                    
            except (socket.timeout, socket.error) as e:
                print(f"❌ Connection error with {read_host}:{read_port}: {e}")
                # Close the failed connection
                key = f"{read_host}:{read_port}"
                if key in self.conns:
                    try:
                        self.conns[key].close()
                    except:
                        pass
                    del self.conns[key]
                continue
                
        print("❌ Failed to read after all attempts")
        return None
