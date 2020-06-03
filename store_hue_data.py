import mysql.connector
import time
import datetime
import sys
import logging
import requests
import json
import argparse
from pathlib import Path
from sshtunnel import SSHTunnelForwarder
import os

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.DEBUG)

ssh_key = Path("~/.ssh/linux-cloud").expanduser()
remote_user = "admin"
database_user = "root"
hue_hostname = "192.168.10.189"


def get_args():
    parser = argparse.ArgumentParser(description="Collect Hue data and send to remote database")

    parser.add_argument("-a", "--api", required=True, help="Hue API key")
    parser.add_argument("-r", "--remote", required=True, help="Remote server to store the data")
    parser.add_argument("-p", "--db_password", required=True, help="The remote database password")


def open_tunnel(remote, ssh_keyremote_user):

    server = SSHTunnelForwarder(
        remote,
        ssh_username=remote_user,
        ssh_pkey=ssh_key,
        remote_bind_address=('127.0.0.1', 3306)
    )

    return server

def get_hue_data(api_key, hue_hostname):
    results = requests.get(f"http://{hue_hostname}/api/{api_key}/sensors")
    # logging.debug(results.data)

    sensors = json.loads(results.content.decode())

    sensor_whole = {}

    # Create a dictionary that uses the first part of the unique ID as the parent device
    # {"qwer-qwer-asdf-zxcv: {"name": "Bedroom Sensor", "temperature": 56.43}}

    for sensor in sensors:

        if 'uniqueid' in sensors[sensor]:

            unid = sensors[sensor]['uniqueid'][:23]  # The rest is too unique

            # New sensor
            if unid not in sensor_whole:
                sensor_whole[unid] = {}

            # the ZLLPresence sensor contains the frindly name
            if 'type' in sensors[sensor]:
                if sensors[sensor]['type'] == "ZLLPresence":
                    if 'name' in sensors[sensor]:
                        sensor_whole[unid].update({"name": sensors[sensor]["name"]})

            if "state" in sensors[sensor]:
                if "temperature" in sensors[sensor]["state"]:
                    # print(sensors[sensor]["state"])

                    # 4 digit temp C missing decimal i.e. 1943 should be 19.43
                    temp_s = str(sensors[sensor]["state"]["temperature"])
                    temp_c = str(sensors[sensor]["state"]["temperature"])
                    # Make sure temp_c is padded with zeros
                    if len(temp_c) == 3:
                        temp_c = "0" + temp_c
                    if len(temp_c) == 2:
                        temp_c = "00" + temp_c
                        # print(f"temp_c: {temp_c} - {sensor_whole[unid]}")
                    temp_c = float(temp_c[0:2] + "." + temp_c[2:4])  # Add in the decimal
                    temp_f = 9.0 / 5.0 * temp_c + 32  # Convert to F

                    sensor_whole[unid].update({"temperature": temp_f})

                if "lightlevel" in sensors[sensor]["state"]:
                    light = sensors[sensor]["state"]["lightlevel"]
                    sensor_whole[unid].update({"lightlevel": light})

    records_to_insert = []
    ts = time.time()
    timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

    for key in sensor_whole:
        # if "name" in sensor_whole[key]:
        if len(sensor_whole[key]) > 1:
            print(sensor_whole[key]['name'], "{0:.2f}".format(sensor_whole[key]['temperature']))

            temp = "{0:.2f}".format(sensor_whole[key]['temperature'])
            # temp = sensor_whole[key]['temperature']
            location = sensor_whole[key]['name']

            records_to_insert.append((timestamp,
                                      location.split(" ")[0].capitalize(),  # Homekit room
                                      "Temperature".capitalize(),  # Name
                                      "0",
                                      "TemperatureSensor",
                                      "CurrentTemperature",
                                      temp))

    return records_to_insert


# db1 = {'host': 'localhost', 'db': 'arcticwolf', 'user': 'root', 'password': 'GameraRodan'}
db1 = {'host': '******', 'db': 'arcticwolf', 'user': 'remote', 'password': '*******'}


def insert(db, statement, records):
    mydb = mysql.connector.connect(
        host=db['host'],
        user=db['user'],
        passwd=db['password'],
        database=db['db']
    )

    mycursor = mydb.cursor()

    sql = "INSERT INTO things (temperature, location, time) VALUES (%s, %s, %s)"

    # val = [(temp, location, timestamp)]
    # mycursor.execute(sql, val)  # Only used for single entry
    mycursor.executemany(statement, records)

    mydb.commit()

    print(mycursor.rowcount, "record inserted.")

args = get_args()
records = get_hue_data(args.api, hue_hostname)
# sql = "INSERT INTO temperature (temperature, location, time) VALUES (%s, %s, %s)"
sql = "INSERT INTO things (time, location, name, aid, service, characteristic, value)" \
            "VALUES (%s, %s, %s, %s, %s, %s, %s)"
insert(db1, sql, records)





