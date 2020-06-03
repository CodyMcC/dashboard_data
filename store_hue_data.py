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
from helper.remote_insert import open_tunnel, db_connection, insert
import os

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.DEBUG)

# ssh_key = Path("~/.ssh/linux-cloud").expanduser()
# remote_user = "admin"
# database_user = "root"
# hue_hostname = "192.168.10.189"


def get_args():
    parser = argparse.ArgumentParser(description="Collect Hue data and send to remote database")

    parser.add_argument("-a", "--api", required=True, help="Hue API key")
    parser.add_argument("-r", "--remote", required=True, help="Remote server to store the data")
    parser.add_argument("-p", "--db_password", required=True, help="The remote database password")

    return parser.parse_args()


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


def main():
    # args = get_args()

    # Get configs
    with open(Path("~/.dashboard_data").expanduser(), 'r') as fh:
        config = json.load(fh)

    records = get_hue_data(config['hue_api'], "192.168.10.189")  # config['hue_hostname']
    print("Got the recorsd")
    print(records)
    # sql = "INSERT INTO temperature (temperature, location, time) VALUES (%s, %s, %s)"
    sql = "INSERT INTO things (time, location, name, aid, service, characteristic, value)" \
          "VALUES (%s, %s, %s, %s, %s, %s, %s)"

    tunnel = open_tunnel(config['remote'], config['ssh_key'], config['remote_username'])
    tunnel.start()

    db_conn = db_connection(config['database_user'],
                            config['database_password'],
                            config['database'],
                            port=tunnel.local_bind_port)

    insert(db_conn, sql, records)

    db_conn.close()
    tunnel.close()


if __name__ == "__main__":
    main()


