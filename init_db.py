#!/usr/bin/python
import psycopg2
from configparser import ConfigParser


create_users_table_cmd = """
CREATE TABLE IF NOT EXISTS users (
   username VARCHAR(50) PRIMARY KEY,
   password VARCHAR(50) NOT NULL
);
"""

def add_user_cmd(username, password):
    return """INSERT INTO users(username, password)\nVALUES ('{}', '{}')""".format(username, password)

drop_all_cmd = """
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO public;
"""


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(
            dbname = "postgres",
            user = "postgres",
            host = "psql-container",
            password = "postgres"
        )

        # create a cursor
        cur = conn.cursor()

	    # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return conn

def disconnect(conn):
    if conn is not None:
        conn.cursor().close()
        conn.close()
        print('Database connection closed.')

def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db

def use_db(commands):
    if not isinstance(commands, list):
        commands = [commands]
    conn = connect();
    cursor = conn.cursor()
    for command in commands:
        cursor.execute(command)
    conn.commit()
    disconnect(conn)

def reset_db():
    use_db([drop_all_cmd, create_users_table_cmd])

reset_db()
# use_db(add_user_cmd("Holas","van"))
