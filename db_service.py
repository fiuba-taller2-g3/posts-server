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
                    is_blocked BOOLEAN DEFAULT false,\
                    type VARCHAR(15)\
                );\
                "

CREATE_BOOKINGS_TABLE_CMD = "\
                CREATE TABLE IF NOT EXISTS bookings (\
                    id SERIAL PRIMARY KEY,\
                    user_id INT NOT NULL,\
                    post_id INT REFERENCES posts(id),\
                    beginDate DATE NOT NULL,\
                    endDate DATE NOT NULL\
                );\
                "

DROP_ALL_CMD = "\
                DROP SCHEMA public CASCADE;\
                CREATE SCHEMA public;\
                GRANT ALL ON SCHEMA public TO postgres;\
                GRANT ALL ON SCHEMA public TO public;\
                "

INIT_CMD = CREATE_POSTS_TABLE_CMD + CREATE_BOOKINGS_TABLE_CMD

RESET_CMD = DROP_ALL_CMD + INIT_CMD

def add_post_query(user_id, price, date, type):
    return "\
                INSERT INTO posts(user_id, price, date, type)\
                VALUES ('{}', '{}', '{}', '{}')\
                RETURNING *".format(user_id, price, date, type)

def add_booking_query(user_id, post_id, beginDate, endDate):
    return "\
                INSERT INTO bookings(user_id, post_id, beginDate, endDate)\
                VALUES ('{}', '{}', '{}', '{}')\
                RETURNING *".format(user_id, post_id, beginDate, endDate)

def overlapping_bookings_count_query(post_id, beginDate, endDate):
    return "\
                SELECT COUNT(*)\
                FROM bookings\
                WHERE post_id='{}'\
                AND (\
                    (beginDate BETWEEN '{}' AND ('{}'::date - '1 day'::interval))\
                    OR\
                    (endDate BETWEEN ('{}'::date + '1 day'::interval) AND '{}')\
                    OR\
                    (beginDate < '{}' AND endDate > '{}')\
                )".format(post_id, beginDate, endDate, beginDate, endDate, beginDate, endDate)

def get_post_query(post_id):
    return "\
                SELECT * FROM posts WHERE id = '{}'".format(post_id)

def edit_post_cmd(post_id, **fields):
    query = "\
                UPDATE posts SET "
    for key, value in fields.items():
        query += "{}='{}', ".format(key, value)
    query = query[:-2]
    query += "\
                WHERE id='{}' \
                RETURNING *".format(post_id)
    return query

def get_posts_query(user_id, type, minPrice, maxPrice):
    query = "\
                SELECT * FROM posts\
                WHERE id > 0 "
    if user_id:
        query += "AND user_id = '{}' ".format(user_id)
    elif type:
        query += "AND type = '{}' ".format(type)
    elif maxPrice:
        query += "AND price <= {} ".format(maxPrice)
    elif minPrice:
        query += "AND price >= {} ".format(minPrice)
    return query

def delete_post_query(post_id):
    return "\
                DELETE FROM posts\
                WHERE id = '{}'\
                RETURNING *".format(post_id)

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
        try:
            cursor.execute(command)
        except Exception as e:
            print("Error {}: {}\n".format(type(e).__name__, e.args))
        conn.commit()

def use_db(conn, command, many=False):
    with conn.cursor() as cursor:
        try:
            cursor.execute(command)
        except Exception as e:
            print("Error {}: {}\n".format(type(e).__name__, e.args))
        conn.commit()
        results = cursor.fetchall()
    print(results)
    if len(results) == 1 and not many:
        return results[0]
    return results
