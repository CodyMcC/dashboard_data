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


debug = logging.INFO

logging.basicConfig(level=debug, format=f'%(asctime)s %(levelname)s %(name)s Line:%(lineno)s %(message)s')
# format='%(asctime)s %levelname)s: %(message)s',
#                         datefmt='%m/%d/%Y %I:%M:%S %p'
logging.info(f"Logging level: {str(debug)}")

s = Schedule()

# Get configs
with open(Path("~/.dashboard_data").expanduser(), 'r') as fh:
    config = json.load(fh)


things = {
    "generator": {"topic": "N/6064054fad59/system/0/Ac/Genset/L1/Power", "value": None},
    "solar_w": {"topic": "N/6064054fad59/solarcharger/258/Yield/Power", "value": None},
    "solar_a": {"topic": "N/6064054fad59/solarcharger/258/Dc/0/Current", "value": None},
    "solar_v": {"topic": "N/6064054fad59/solarcharger/258/Dc/0/Voltage", "value": None},
    "soc": {"topic": "N/6064054fad59/battery/260/Soc", "value": None},
    "temp": {"topic": "N/6064054fad59/battery/260/Dc/0/Temperature", "value": None},
    "dc_load": {"topic": "N/6064054fad59/system/0/Dc/System/Power", "value": None},
    "ac_load": {"topic": "N/6064054fad59/system/0/Ac/Consumption/L1/Power", "value": None},


}


def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))

    for item in things:
        client.subscribe(things[item]["topic"])
    client.subscribe("$SYS/broker/uptime")  # This makes sure we get some messages at least

    os.system("mosquitto_pub -m '' -t 'R/6064054fad59/system/0/Serial' -h 192.168.10.172")


def update_values():
    rows = list()

    for key in things.keys():
        if things[key]["value"]:
            rows.append([key, things[key]["value"]])

    statement = "INSERT INTO electric (field, value) VALUES (%s, %s)"

    tunnel = open_tunnel(config['remote'], config['ssh_key'], config['remote_username'])
    tunnel.start()

    db_conn = db_connection(config['database_user'],
                            config['database_password'],
                            config['database'],
                            port=tunnel.local_bind_port)

    insert(db_conn, statement, rows)

    db_conn.close()
    tunnel.close()


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    for item in things:
        if things[item]['topic'] == msg.topic:
            things[item].update(json.loads(msg.payload.decode()))

    if s.run_action("1_m"):
        logging.info("Ran the 1 minute update")
        update_values()
    if s.run_action("30_m"):
        logging.info("Its been 30 minutes, exiting")
        exit()


def main():
    print("\n\n")
    logging.info("Starting new instance")
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect("192.168.10.172", 1883, 60)

    # Blocking call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a
    # manual interface.
    client.loop_forever()


if __name__ == "__main__":
    main()
