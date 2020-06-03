import paho.mqtt.client as mqtt
import json
import time

import datetime
import sys
import os
import logging
from simple_schedule import Schedule
from helper.remote_insert import open_tunnel, db_connection, insert
from pathlib import Path


debug = logging.WARNING

logging.basicConfig(level=debug, format=f'%(asctime)s %(levelname)s %(name)s Line:%(lineno)s %(message)s')
# format='%(asctime)s %levelname)s: %(message)s',
#                         datefmt='%m/%d/%Y %I:%M:%S %p'
logging.info(f"Logging level: {str(debug)}")

s = Schedule()

# Get configs
with open(Path("~/.dashboard_data").expanduser(), 'r') as fh:
    config = json.load(fh)


things = {
    "solar_w": {"topic": "N/6064054fad59/solarcharger/258/Yield/Power", "value": None},
    "solar_a": {"topic": "N/6064054fad59/solarcharger/258/Dc/0/Current", "value": None},
    "solar_v": {"topic": "N/6064054fad59/solarcharger/258/Dc/0/Voltage", "value": None},
    "soc": {"topic": "N/6064054fad59/battery/260/Soc", "value": None},
    "temp": {"topic": "N/6064054fad59/battery/260/Dc/0/Temperature", "value": None},
    "dc_load": {"topic": "N/6064054fad59/system/0/Dc/System/Power", "value": None},
    "ac_load": {"topic": "N/6064054fad59/system/0/Ac/Consumption/L1/Power", "value": None},
    "generator": {"topic": "N/6064054fad59/system/0/Ac/Genset/L1/Power", "value": None}

}


def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    # tracker.test()
    # tracker.maintain()
    for item in things:
        client.subscribe(things[item]["topic"])
    print("Finished subscribing")

    print("Sending keep alive")
    os.system("mosquitto_pub -m '' -t 'R/6064054fad59/system/0/Serial' -h venus")
    print("Sent keep alive")

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    # client.subscribe("N/6064054fad59/solarcharger/258/Yield/Power")
    # client.subscribe("N/6064054fad59/solarcharger/258/Dc/0/Current")


def update_values():
    ts = time.time()
    timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

    values = [(
        things["solar_w"]["value"],
        things["solar_a"]["value"],
        things["solar_v"]["value"],
        things["soc"]["value"],
        (9.0 / 5.0 * things["temp"]["value"] + 32),
        things["dc_load"]["value"],
        things["ac_load"]["value"],
        things["generator"]["value"],
        timestamp
    )]

    statement = f"INSERT INTO electric (solar_w, solar_a, solar_v, soc, temp, dc_load, ac_load, generator, time)" \
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

    tunnel = open_tunnel(config['remote'], config['ssh_key'], config['remote_username'])
    tunnel.start()

    db_conn = db_connection(config['database_user'],
                            config['database_password'],
                            config['database'],
                            port=tunnel.local_bind_port)

    insert(db_conn, statement, values)

    db_conn.close()
    tunnel.close()


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    for item in things:
        if things[item]['topic'] == msg.topic:
            things[item].update(json.loads(msg.payload.decode()))

    if s.run_action("1_m"):
        update_values()
        # Send keep alive
        os.system("mosquitto_pub -m '' -t 'R/6064054fad59/system/0/Serial' -h venus")
    if s.run_action("30_m"):
        exit()


def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect("venus", 1883, 60)

    # Blocking call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a
    # manual interface.
    client.loop_forever()


if __name__ == "__main__":
    main()
