import json
import queue

TOPIC = "BCC362"

def on_connect(client, userdata, flags, rc, properties=None):
    """Callback executado quando o MQTT Sync se conecta."""
    if rc == 0:
        print(f"Cliente conectado com sucesso: {client}")
        client.subscribe(TOPIC, qos=1)
    else:
        print(f'Erro ao me conectar! codigo={rc}')
        
def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    """Callback executado quando a inscrição é bem-sucedida."""
    print(f'Cliente Subscribed at {TOPIC}')
    print(f'QOS:{granted_qos}')
    
def on_message(client, userdata, message, properties=None):
    """Callback executado quando uma mensagem MQTT é recebida."""
    print('Mensagem MQTT recebida!')
    topic = message.topic
    payload = message.payload.decode()
    
    # Userdata contém a fila para mensagens MQTT recebidas
    incoming_mqtt_messages_queue = userdata.get("incoming_mqtt_messages_queue")
    
    if incoming_mqtt_messages_queue:
        message_data = {
            "source": "mqtt",
            "topic": topic,
            "payload": payload
        }
        incoming_mqtt_messages_queue.put(message_data)
        print(f"MQTT Message queued for processing: Topic='{topic}', Payload='{payload}'")
    else:
        print("Warning: incoming_mqtt_messages_queue not provided to on_message callback.")