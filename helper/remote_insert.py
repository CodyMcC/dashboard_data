import mysql.connector
from sshtunnel import SSHTunnelForwarder
from pathlib import Path
import logging

_logger = logging.getLogger(__name__)



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


def db_connection(user: str, password: str, database: str, port=3306):
    """
    Example:
    statement = "INSERT INTO temperature (temperature, location, time) VALUES (%s, %s, %s)"
    """

    try:
        mydb = mysql.connector.connect(
            host="127.0.0.1",
            port=port,
            user=user,
            passwd=password,
            database=database,
            auth_plugin='mysql_native_password'
            # auth_plugin="caching_sha2_password"
        )
        if mydb.is_connected():
            _logger.info('Connected to MySQL database')
            return mydb
    except Exception as e:
        _logger.error(e)

    return None


def insert(database_conn: mysql.connector, statement: str, values: list):

    mycursor = database_conn.cursor()

    try:
        mycursor.executemany(statement, values)
        database_conn.commit()
    except Exception as e:
        print(mycursor.statement)
        print(e)

    mycursor.close()


