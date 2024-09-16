from argparse import ArgumentError
import ssl
from django.db.models import Avg
from datetime import timedelta, datetime
from receiver.models import Data, Measurement
import paho.mqtt.client as mqtt
import schedule
import time
from django.conf import settings

client = mqtt.Client(settings.MQTT_USER_PUB)
# client = mqtt.Client(client_id=settings.MQTT_USER_PUB, protocol=mqtt.MQTTv311)  # Puedes ajustar el protocolo según el que estés usando


def analyze_data():
    # Consulta todos los datos de la última hora, los agrupa por estación y variable
    # Compara el promedio con los valores límite que están en la base de datos para esa variable.
    # Si el promedio se excede de los límites, se envia un mensaje de alerta.

    print("Calculando alertas...")

    data = Data.objects.filter(
        base_time__gte=datetime.now() - timedelta(hours=1))
    aggregation = data.annotate(check_value=Avg('avg_value')) \
        .select_related('station', 'measurement') \
        .select_related('station__user', 'station__location') \
        .select_related('station__location__city', 'station__location__state',
                        'station__location__country') \
        .values('check_value', 'station__user__username',
                'measurement__name',
                'measurement__max_value',
                'measurement__min_value',
                'station__location__city__name',
                'station__location__state__name',
                'station__location__country__name')
    alerts = 0
    for item in aggregation:
        alert = False

        variable = item["measurement__name"]
        max_value = item["measurement__max_value"] or 0
        min_value = item["measurement__min_value"] or 0

        country = item['station__location__country__name']
        state = item['station__location__state__name']
        city = item['station__location__city__name']
        user = item['station__user__username']

        if item["check_value"] > max_value or item["check_value"] < min_value:
            alert = True

        if alert:
            message = "ALERT {} {} {}".format(variable, min_value, max_value)
            topic = '{}/{}/{}/{}/in'.format(country, state, city, user)
            print(datetime.now(), "Sending alert to {} {}".format(topic, variable))
            print("Mensaje: ", message)
            setup_mqtt()
            client.publish(topic, message)
            alerts += 1

    print(len(aggregation), "dispositivos revisados")
    print(alerts, "alertas enviadas")
    
def get_measurements():
    measurements = Measurement.objects.all()
    return list(measurements)


def analyze_temp_average():
    '''
    Analiza el promedio de la temperatura y envía alertas si está fuera de los límites.
    '''
    print("Calculando alertas de temperatura...")
    measurements = get_measurements()
    print("Mediciones: ", measurements)
    # Consulta solo para la variable 'temperatura' y últimos 2 minutos
    data = Data.objects.filter(
        base_time__gte=datetime.now() - timedelta(minutes=5),
        measurement__name="temperatura"  # Filtra solo la temperatura
    )
    # setup_mqtt()
    # print(data)

    # Agrupa y promedia los datos
    aggregation = data.annotate(check_value=Avg('avg_value')) \
        .select_related('station', 'measurement') \
        .select_related('station__user', 'station__location') \
        .select_related('station__location__city', 'station__location__state',
                        'station__location__country') \
        .values('check_value', 'station__user__username',
                'measurement__name',
                'measurement__max_value',
                'measurement__min_value',
                'station__location__city__name',
                'station__location__state__name',
                'station__location__country__name')

    alerts = 0
    for item in aggregation:
        led_state = "off"  # Estado inicial del LED

        variable = item["measurement__name"]
        max_value = item["measurement__max_value"] or 0
        min_value = item["measurement__min_value"] or 0
        print("Max value: ", max_value)
        print("Min value: ", min_value)
        

        country = item['station__location__country__name']
        state = item['station__location__state__name']
        city = item['station__location__city__name']
        user = item['station__user__username']

        check_value = item["check_value"]
        print("Average value: ", item["check_value"])

        # Evaluar la medición en relación con los límites
        if check_value > max_value:
            led_state = "fast_blink" 
        elif check_value < min_value:
            led_state = "slow_blink"
        else:
            led_state = "off"
            
        print("Alert state: ", led_state)

        message = "state: {}".format(led_state)
        topic = '{}/{}/{}/{}/in'.format(country, state, city, user)
        print(datetime.now(), "Sending alert to {} {}".format(topic, variable))
        print("Mensaje: ", message)
        
        client.publish(String(topic), String(message))

        alerts += 1

    print(len(aggregation), "dispositivos revisados")
    print(alerts, "alertas enviadas")

    

def on_connect(client, userdata, flags, rc):
    '''
    Función que se ejecuta cuando se conecta al bróker.
    '''
    print("Conectando al broker MQTT...", mqtt.connack_string(rc))


def on_disconnect(client: mqtt.Client, userdata, rc):
    '''
    Función que se ejecuta cuando se desconecta del broker.
    Intenta reconectar al bróker.
    '''
    print("Desconectado con mensaje:" + str(mqtt.connack_string(rc)))
    print("Reconectando...")
    client.reconnect()


def setup_mqtt():
    '''
    Configura el cliente MQTT para conectarse al broker.
    '''
    print("Iniciando cliente MQTT...", settings.MQTT_HOST, settings.MQTT_PORT)
    global client
    try:
        client = mqtt.Client(settings.MQTT_USER_PUB)
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect

        if settings.MQTT_USE_TLS:
            client.tls_set(ca_certs=settings.CA_CRT_PATH,
                           tls_version=ssl.PROTOCOL_TLSv1_2, cert_reqs=ssl.CERT_NONE)

        client.username_pw_set(settings.MQTT_USER_PUB,
                               settings.MQTT_PASSWORD_PUB)
        client.connect(settings.MQTT_HOST, settings.MQTT_PORT)

    except Exception as e:
        print('Ocurrió un error al conectar con el bróker MQTT:', e)


def start_cron():
    '''
    Inicia el cron que se encarga de ejecutar la función analyze_data cada hora.
    Initia el cron que se encarga de ejecutar la función analyze_temp_average cada minuto.
    '''
    print("Iniciando cron de alertas generales...")
    schedule.every().hour.do(analyze_data)
    print("Iniciando Cron de alerta promedio de temperatura...")
    schedule.every(1).minutes.do(analyze_temp_average)
    print("Servicio de control iniciado")
    while 1:            
        schedule.run_pending()
        time.sleep(1)
