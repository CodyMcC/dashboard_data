import paho.mqtt.client as mqtt
import json
import time
import datetime
import sys
import os
import logging
import mysql.connector
from sshtunnel import SSHTunnelForwarder
from helper.remote_insert import open_tunnel, db_connection, insert

from pathlib import Path

from simple_schedule import Schedule

import logging

logging.basicConfig(level=logging.ERROR)

start_time = time.time()
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

#
# def open_tunnel():
#     remote = "ec2-3-230-77-95.compute-1.amazonaws.com"
#     key = os.path.expanduser("~/.ssh/linux-cloud")
#     server = SSHTunnelForwarder(
#         remote,
#         ssh_username="admin",
#         ssh_pkey=key,
#         remote_bind_address=('127.0.0.1', 3306)
#     )
#
#     return server


# def insert(db: str, statement: str, records: list):
#     """
#     Example:
#     statement = "INSERT INTO temperature (temperature, location, time) VALUES (%s, %s, %s)"
#     """
#
#     print("running the insert funciton")
#
#     server = open_tunnel()
#
#     server.start()
#
#     mydb = mysql.connector.connect(
#         #       host="ec2-3-230-77-95.compute-1.amazonaws.com",
#         host="127.0.0.1",
#         port=server.local_bind_port,
#         user="ssh",
#         passwd="GameraRodan",
#         database=db
#     )
#
#     print(1)
#
#     mycursor = mydb.cursor()
#
#     print(2)
#
#     # val = [(temp, location, timestamp)]
#     # mycursor.execute(sql, val)  # Only used for single entry
#     mycursor.executemany(statement, records)
#
#     print(3)
#
#     mydb.commit()
#
#     print(mycursor.rowcount, "record inserted.")
#     mydb.close()
#
#     server.close()


def update_values():
    print("\n\n\n\nTime to update values\n\n\n\n\n")

    ts = time.time()
    timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

    for topic in accessories:
        print(topic)

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

        print(values)
        print("\n\n\n")

        statement = "INSERT INTO things (time, location, name, aid, service, characteristic, value)" \
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)"

        print("pre insert")

        tunnel = open_tunnel(config['remote'], config['ssh_key'], config['remote_username'])
        tunnel.start()

        db_conn = db_connection(config['database_user'],
                                config['database_password'],
                                config['database'],
                                port=tunnel.local_bind_port)

        insert(db_conn, statement, values)

        db_conn.close()
        tunnel.close()

        print("post insert")


def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))

    for topic in accessories.keys():
        client.subscribe(topic)
    client.subscribe("$SYS/broker/uptime")  # This makes sure we get some messages at least
    print("Finished subscribing")


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    # print(msg.topic+" "+str(msg.payload))
    # for item in things:
    #     if things[item]['topic'] == msg.topic:
    #         things[item].update(json.loads(msg.payload.decode()))

    for topic in accessories.keys():
        if topic == msg.topic:
            print(msg.topic)
            accessories[topic].update(json.loads(msg.payload.decode()))
    #             print(accessories[topic])
    #     print(things)
    print("Got callback")

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
    print("Next line is forever looop")
    client.loop_forever()


if __name__ == "__main__":
    main()
