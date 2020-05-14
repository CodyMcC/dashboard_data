import mysql.connector
from sshtunnel import SSHTunnelForwarder
from pathlib import Path


def open_tunnel(remote: str, ssh_key: str, user: str, remote_port=3306):
    """This will setup an ssh tunnel to the remote server using the mysql port.
    Get the local port with server.local_bind_port"""

    server = SSHTunnelForwarder(
        remote,
        ssh_username=user,
        ssh_pkey=ssh_key,
        remote_bind_address=('127.0.0.1', remote_port)
        )

    return server


def db_connection(user: str, password: str, port: int, database: str):
    """
    Example:
    statement = "INSERT INTO temperature (temperature, location, time) VALUES (%s, %s, %s)"
    """

    mydb = mysql.connector.connect(
        host="127.0.0.1",
        port=port,
        user=user,
        passwd=password,
        database=database
    )

    return mydb


def insert(database_conn: mysql.connector, statement: str, values: list, tunnel: SSHTunnelForwarder):

    tunnel.start()

    mycursor = database_conn.cursor()

    mycursor.executemany(statement, values)



