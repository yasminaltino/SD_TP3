from .mqtt_connection.mqtt_client_connection import MqttClientConnection
import time, os
from dotenv import load_dotenv

load_dotenv()

# Configurações do broker MQTT
BROKER_ADDRESS = os.getenv("MQTT_BROKER_ADDRESS")
BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", 1883)) 
MQTT_USERNAME = os.getenv("MQTT_USERNAME") 
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "python_subscriber_default") 

def start():
    """Inicia a conexão MQTT e mantém o processo ativo."""
    mqtt_client_connection = MqttClientConnection(
        BROKER_ADDRESS, BROKER_PORT,
        CLIENT_ID, 60,
        MQTT_USERNAME, MQTT_PASSWORD
    )
    mqtt_client_connection.start_connection()

    # Mantém o processo ativo
    while True: time.sleep(0.001)