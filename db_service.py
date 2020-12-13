#!/usr/bin/python
import psycopg2
import os
import urllib.parse as urlparse


CREATE_POSTS_TABLE_CMD = "\
                CREATE TABLE IF NOT EXISTS posts (\
                    id SERIAL PRIMARY KEY,\
                    user_id INT NOT NULL,\
                    price INT NOT NULL,\
                    date DATE NOT NULL,\
                    is_blocked BOOLEAN DEFAULT false\
                );\
                "

INIT_CMD = CREATE_POSTS_TABLE_CMD

def add_post_query(user_id, price, date):
    return "\
                INSERT INTO posts(user_id, price, date)\
                VALUES ('{}', '{}', '{}')\
                RETURNING *".format(user_id, price, date)

def get_post_query(post_id):
    return "\
                SELECT * FROM posts WHERE id = '{}'".format(post_id)

def edit_post_cmd(price, date, is_blocked, post_id):
    return "\
                UPDATE posts SET price='{}', date='{}', is_blocked='{}' \
                WHERE id='{}' \
                RETURNING *".format(price, date, is_blocked, post_id)

def get_posts_from_user_query(user_id):
    return "\
                SELECT * FROM posts WHERE user_id = '{}'".format(user_id)

def delete_post_query(post_id):
    return "\
                DELETE FROM posts\
                WHERE id = '{}'\
                RETURNING *".format(post_id)

DROP_ALL_CMD = "\
                DROP SCHEMA public CASCADE;\
                CREATE SCHEMA public;\
                GRANT ALL ON SCHEMA public TO postgres;\
                GRANT ALL ON SCHEMA public TO public;\
                "

RESET_CMD = DROP_ALL_CMD + INIT_CMD

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        url = urlparse.urlparse(os.environ['DATABASE_URL'])
        conn = psycopg2.connect(
            dbname = url.path[1:],
            user = url.username,
            host = url.hostname,
            password = url.password,
            port = url.port
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
        conn.close()
        print('Database connection closed.')

def set_db(conn, command):
    with conn.cursor() as cursor:
        cursor.execute(command)
        conn.commit()

def use_db(conn, command):
    with conn.cursor() as cursor:
        cursor.execute(command)
        conn.commit()
        results = cursor.fetchall()
    # print(results)
    if len(results) == 1:
        return results[0]
    return results
