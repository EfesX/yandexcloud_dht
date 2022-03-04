import paho.mqtt.client as paho
import ssl
import Adafruit_DHT
import time
from gpiozero import CPUTemperature
import json
import time

sensor = Adafruit_DHT.DHT11
sensor_pin = 17

cnt_meas = 0


def on_message(clnt, userdata, msg):
    print(msg.topic+" "+str(msg.payload))

def on_connect(dfs, client, userdata, rc):
        print("Connected with result code "+str(rc))


mqttc = paho.Client()
mqttc.on_message = on_message
mqttc.on_connect = on_connect
#mqttc.tls_set("/home/efesxzc/rootCA.crt", "/home/efesxzc/cert_dev.pem", "/home/efesxzc/key_dev.pem", ssl.CERT_REQUIRED, ssl.PROTOCOL_TLS)
mqttc.tls_set("./cert/rootCA.crt", "./cert/cert_dev.pem", "./cert/key_dev.pem", ssl.CERT_REQUIRED, ssl.PROTOCOL_TLS)
mqttc.connect("mqtt.cloud.yandex.net", 8883, 60)
mqttc.loop_start()



while True:
    humidity, temperature = Adafruit_DHT.read_retry(sensor, sensor_pin)
    cpu = CPUTemperature()

    data = {
        'DeviceId' : 0,
        "MeasId"   : cnt_meas,
        "TimeStamp": time.strftime('%H:%M:%S %d/%m/%Y'),
        "Status" : "ERROR",
        "Values" : [
            {
                "Type"  : "Double",
                "Name"  : "Humidity",
                "Value" : humidity
            },
            {
                "Type"  : "Double",
                "Name"  : "Temperature",
                "Value" : temperature
            }
        ]
    }

    if humidity is not None and temperature is not None:
        data["Status"] = "OK"
        mqttc.publish("$devices/are2r23ji4jbk5c84c0k/events", json.dumps(data))
    else:
        data["Status"] = "ERROR"
        mqttc.publish("$devices/are2r23ji4jbk5c84c0k/events", json.dumps(data))
        break
    print("------------------------")
    print("Температура: {} град".format(temperature))
    print("Влажность:   {} %".format(humidity))
    print("Температура ЦПУ: {} град".format(cpu.temperature))
    time.sleep(60 * 60)
    cnt_meas = cnt_meas + 1

    mqttc.loop_stop()
