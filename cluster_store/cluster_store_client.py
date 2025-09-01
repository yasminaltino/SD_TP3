# --- Configuration for the Cluster Store ---
import json
import random
import socket
import time

class ClusterStoreClient:
    def __init__(self, host, ports):
        self.host = host
        self.ports = ports
        self.primary_index = 0
        self.primary_port = self.ports[self.primary_index]
        self.conns = {}
        
    def get_connection(self, port):
        if port not in self.conns:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(5)
                s.connect((self.host, port))
                self.conns[port] = s
                return s
            except(socket.error, socket.timeout) as e:
                print (f"Falha ao conectar com o servidor na porta {port}: {e}")
                return None
        return self.conns[port]
    
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
                conn = self.get_connection(self.primary_port)
                if not conn:
                    # Falha na conexão com o primário atual.
                    self.handle_primary_failure()
                    continue # Tentar novamente com o novo primário

                message = {"action": "write", "data": data}
                conn.sendall(json.dumps(message).encode('utf-8'))
                
                # Espera a resposta do primário
                response_data = conn.recv(1024)
                if response_data:
                    response = json.loads(response_data.decode('utf-8'))
                    if response.get("status") == "SUCCESS":
                        print(f"Escrita bem-sucedida no primário {self.primary_port}.")
                        return True
                    else:
                        print(f"Primário {self.primary_port} reportou falha na escrita: {response.get('error')}")
                        return False

            except (socket.timeout, socket.error) as e:
                print(f"Falha de comunicação com o primário {self.primary_port}: {e}")
                self.handle_primary_failure()
                time.sleep(1) # Aguarda antes de tentar novamente

        print("Não foi possível realizar a escrita após várias tentativas.")
        return False
    
    
    def send_read_request(self):
        """Envia uma requisição de leitura para uma réplica aleatória."""
        random_port = random.choice(self.ports)
        try:
            conn = self.get_connection(random_port)
            if not conn:
                print(f"Falha ao conectar para leitura na porta {random_port}.")
                return None

            message = {"action": "read"}
            conn.sendall(json.dumps(message).encode('utf-8'))

            response_data = conn.recv(1024)
            if response_data:
                response = json.loads(response_data.decode('utf-8'))
                if response.get("status") == "SUCCESS":
                    print(f"Leitura bem-sucedida da réplica {random_port}.")
                    return response.get("data")
                else:
                    print(f"Réplica {random_port} reportou falha na leitura: {response.get('error')}")
                    return None
        except (socket.timeout, socket.error) as e:
            print(f"Falha de comunicação com a réplica {random_port}: {e}")
            return None
        
    def handle_primary_failure(self):
        """Lógica simples de eleição de novo primário."""
        print(f"Primário na porta {self.primary_port} parece ter falhado. Iniciando eleição...")
        
        # Fecha a conexão com o primário falho
        if self.primary_port in self.conns:
            self.conns[self.primary_port].close()
            del self.conns[self.primary_port]

        # Simplesmente promove o próximo nó na lista
        self.primary_index = (self.primary_index + 1) % len(self.ports)
        self.primary_port = self.ports[self.primary_index]
        print(f"Novo primário eleito: porta {self.primary_port}.")
        
        # Em um sistema real, aqui o novo primário precisaria ser notificado
        # e garantir que ele tem o estado mais recente.