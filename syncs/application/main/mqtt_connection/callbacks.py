import json
# from syncs.application.controllers.controller import Controller # Remove if not directly used in callbacks
import queue # Import queue

TOPIC = "BCC362"

def on_connect(client, userdata, flags, rc, properties=None):
    """Callback for when the MQTT Sync connects"""
    if rc == 0:
        print(f"Cliente conectado com sucesso: {client}")
        client.subscribe(TOPIC, qos=1)
        # Assuming userdata might contain a reference to the main app's components
        # For now, it's just a placeholder, but could be used to pass queues
    else:
        print(f'Erro ao me conectar! codigo={rc}')
        
def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    """Callback for when the subscription is successful"""
    print(f'Cliente Subscribed at {TOPIC}')
    print(f'QOS:{granted_qos}')
    
def on_message(client, userdata, message, properties=None):
    """Callback for when an MQTT message is received"""
    print('Mensagem MQTT recebida!')
    topic = message.topic
    payload = message.payload.decode()
    
    # Userdata will now contain the queue for incoming MQTT messages
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

    # Original logic (can be moved to the main logic thread)
    # if topic == "BCC362":
    #     try:
    #         if(str(payload) == "DONE"):
    #             print("Processo concluído. Atualizando fila...")
    #         elif(str(payload) == "REQUEST"):
    #             print("Processo novo requisitado. Atualizando fila...")
    #     except ValueError:
    #         print(f'Payload inválido: {payload}')