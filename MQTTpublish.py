"""
    Implementa um cliente publish mqtt usando a biblioteca paho-mqtt e o broker de testes test.mosca.io
"""

import time
from paho.mqtt import client as mqtt

broker = 'broker.mqttdashboard.com'
port = 1883
topic = "meu/teste"
client_id = 'python-mqtt-2022'
user_name = 'isau-testemqtt'
password = 'public'

def connect_mqtt():
    #função padrão de quando usamos a biblioteca paho-mqtt
    #implementa a menssagem de retorno qu o cliente nos dará
    #de acordo com a resposta CONNACK do servidor
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print('Conectado ao MQTT Broker!!!!!')
        else:
            print('Falha na conecção, Erro:', rc)

    def on_message(client, userdata, msg):
        print(msg.topic, str(msg.payload))

    #instancia o objeto client
    client = mqtt.Client(client_id)
    client.username_pw_set(user_name, password)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(broker, port)
    return client

def publish(client):
    msg_cont = 0
    while True:
        time.sleep(1)
        msg = 'contador: ' + str(msg_cont)

        # esse metodo retorna uma lista com duas posições result: [0, 1]
        # na posição 0 da lista esta o status da publicação, se foi ou não publicado
        result = client.publish(topic, msg, qos=0, retain=True)
        if result[0] == 0:
            print('A mensagem', msg, 'foi puplicada no topico', topic)
        else:
            print('falha no envio da mensagem', f'({msg})', 'ao topico', topic)

        msg_cont += 1


def run():
    client = connect_mqtt()
    client.loop_start()
    publish(client)

if __name__ == '__main__':
    run()







