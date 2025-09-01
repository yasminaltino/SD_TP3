import paho.mqtt.client as mqtt
from paho.mqtt.client import Client, CallbackAPIVersion
# from .callbacks import on_connect, on_subscribe, on_message # Remove this line
import callbacks # Import the module directly

class MqttClientConnection:
    def __init__(self, broker_ip: str, port: int, client_name: str,
                 keepalive=60, 
                 username: str = None, password: str = None):
        
        self.__broker_ip = broker_ip
        self.__port = port
        self.__client_name = client_name
        self.__keepalive = keepalive
        self.__username = username    
        self.__password = password   
        self.__mqtt_client = None
        self.__userdata_for_callbacks = {} # New: to store data for callbacks

    def set_callback_userdata(self, key, value):
        """Allows setting data that will be passed to callbacks."""
        self.__userdata_for_callbacks[key] = value

    def start_connection(self):
        mqtt_client = Client(client_id=self.__client_name, callback_api_version=CallbackAPIVersion.VERSION2)
        
        if self.__username and self.__password:
            mqtt_client.username_pw_set(self.__username, self.__password)
            
        mqtt_client.on_connect = callbacks.on_connect
        mqtt_client.on_subscribe = callbacks.on_subscribe
        mqtt_client.on_message = callbacks.on_message
        
        # Set userdata for callbacks, useful for passing queues etc.
        # This makes the __userdata_for_callbacks dictionary available to the callbacks
        mqtt_client.user_data_set(self.__userdata_for_callbacks)
        
        try:
            mqtt_client.connect(host=self.__broker_ip, port=self.__port, keepalive=self.__keepalive)
            self.__mqtt_client = mqtt_client
            self.__mqtt_client.loop_start()
            print("MQTT Client connection started in background loop.")
        except Exception as e:
            print(f"Erro ao tentar conectar ao broker: {e}")
            
    def end_connection(self):
        try:
            if self.__mqtt_client:
                self.__mqtt_client.loop_stop()
                self.__mqtt_client.disconnect()
                print("Conexão MQTT encerrada.")
                return True
            else:
                print("Cliente MQTT não inicializado.")
                return False
        except Exception as e:
            print(f"Erro ao encerrar a conexão: {e}")
            return False

    def get_mqtt_client_instance(self):
        """Returns the internal paho-mqtt client instance."""
        return self.__mqtt_client