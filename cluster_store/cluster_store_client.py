# In cluster_store_client.py

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
        
    def get_connection(self, host, port):
        """Get or create a connection for a specific host and port."""
        key = f"{host}:{port}"
        if key not in self.conns:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(5)
                s.connect((host, port))
                self.conns[key] = s
                return s
            except(socket.error, socket.timeout) as e:
                print (f"Falha ao conectar com o servidor em {host}:{port}: {e}")
                return None
        return self.conns[key]
    
    def close_connections(self):
        """Fecha todas as conexões ativas."""
        for conn in self.conns.values():
            conn.close()
        self.conns = {}
    
    def send_write_request(self, data):
        """Envia uma requisição de escrita para o servidor primário."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                conn = self.get_connection(self.primary_host, self.primary_port)
                if not conn:
                    self.handle_primary_failure()
                    continue

                message = {"action": "write", "data": data}
                conn.sendall(json.dumps(message).encode('utf-8'))
                
                response_data = conn.recv(1024)
                if response_data:
                    response = json.loads(response_data.decode('utf-8'))
                    if response.get("status") == "SUCCESS":
                        print(f"Escrita bem-sucedida no primário {self.primary_host}:{self.primary_port}.")
                        return True
                    else:
                        print(f"Primário {self.primary_host}:{self.primary_port} reportou falha na escrita: {response.get('error')}")
                        return False
            except (socket.timeout, socket.error) as e:
                print(f"Falha de comunicação com o primário {self.primary_host}:{self.primary_port}: {e}")
                self.handle_primary_failure()
                time.sleep(1)

        print("Não foi possível realizar a escrita após várias tentativas.")
        return False
    
    def send_read_request(self):
        """Envia uma requisição de leitura para uma réplica aleatória."""
        random_server = random.choice(self.servers)
        read_host, read_port = random_server.split(':')
        read_port = int(read_port)
        
        try:
            conn = self.get_connection(read_host, read_port)
            if not conn:
                print(f"Falha ao conectar para leitura em {read_host}:{read_port}.")
                return None

            message = {"action": "read"}
            conn.sendall(json.dumps(message).encode('utf-8'))

            response_data = conn.recv(1024)
            if response_data:
                response = json.loads(response_data.decode('utf-8'))
                if response.get("status") == "SUCCESS":
                    print(f"Leitura bem-sucedida da réplica {read_host}:{read_port}.")
                    return response.get("data")
                else:
                    print(f"Réplica {read_host}:{read_port} reportou falha na leitura: {response.get('error')}")
                    return None
        except (socket.timeout, socket.error) as e:
            print(f"Falha de comunicação com a réplica {read_host}:{read_port}: {e}")
            return None
        
    def handle_primary_failure(self):
        """Lógica simples de eleição de novo primário."""
        print(f"Primário em {self.primary_host}:{self.primary_port} parece ter falhado. Iniciando eleição...")
        
        key = f"{self.primary_host}:{self.primary_port}"
        if key in self.conns:
            self.conns[key].close()
            del self.conns[key]

        self.primary_index = (self.primary_index + 1) % len(self.servers)
        self.primary_host, self.primary_port = self.servers[self.primary_index].split(':')
        self.primary_port = int(self.primary_port)
        print(f"Novo primário eleito: {self.primary_host}:{self.primary_port}.")