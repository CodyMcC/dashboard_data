import paho.mqtt.client as mqtt
import json
import time
import datetime
from helper.remote_insert import open_tunnel, db_connection, insert

from pathlib import Path

from simple_schedule import Schedule

import logging

logging.basicConfig(level=logging.ERROR)

# start_time = time.time()
s = Schedule()

# Get configs
with open(Path("~/.dashboard_data").expanduser(), 'r') as fh:
    config = json.load(fh)

debug = logging.WARNING

logging.basicConfig(level=debug, format=f'%(asctime)s %(levelname)s %(name)s Line:%(lineno)s %(message)s')
# format='%(asctime)s %levelname)s: %(message)s',
#                         datefmt='%m/%d/%Y %I:%M:%S %p'
logging.info(f"Logging level: {str(debug)}")

accessories = {"garage/battery_1/2111/TemperatureSensor": {"CurrentTemperature": "0"},
               "garage/battery_2/2222/TemperatureSensor": {"CurrentTemperature": "0"},
               "garage/battery_3/2333/TemperatureSensor": {"CurrentTemperature": "0"},
               "garage/ambient/2444/TemperatureSensor": {"CurrentTemperature": "0"}}


def update_values():
    ts = time.time()
    timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

    for topic in accessories:

        if "CurrentTemperature" in accessories[topic].keys():
            accessories[topic]["CurrentTemperature"] = str(
                9.0 / 5.0 * float(accessories[topic]["CurrentTemperature"]) + 32)  # Convert to F

        values = [(timestamp,
                   topic.split("/")[0].capitalize(),  # Location
                   topic.split("/")[1].replace("_", " ").capitalize(),  # Name
                   topic.split("/")[2],  # AID
                   topic.split("/")[3],  # Service
                   list(accessories[topic].keys())[0],  # Characteristic
                   list(accessories[topic].values())[0]  # Value
                   )]

        statement = "INSERT INTO things (time, location, name, aid, service, characteristic, value)" \
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)"

        tunnel = open_tunnel(config['remote'], config['ssh_key'], config['remote_username'])
        tunnel.start()

        db_conn = db_connection(config['database_user'],
                                config['database_password'],
                                config['database'],
                                port=tunnel.local_bind_port)

        insert(db_conn, statement, values)

        db_conn.close()
        tunnel.close()


def on_connect(client, userdata, flags, rc):
    for topic in accessories.keys():
        client.subscribe(topic)
    client.subscribe("$SYS/broker/uptime")  # This makes sure we get some messages at least


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    for topic in accessories.keys():
        if topic == msg.topic:
            print(msg.topic)
            accessories[topic].update(json.loads(msg.payload.decode()))

    if s.run_action("1_m"):
        update_values()
    if s.run_action("30_m"):
        exit()


def main():

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    logger = logging.getLogger(__name__)
    client.enable_logger(logger)

    client.connect("pi-server.local", 1883, 60)

    # Blocking call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a
    # manual interface.
    client.loop_forever()


if __name__ == "__main__":
    main()
