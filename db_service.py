#!/usr/bin/python
import psycopg2
import os
import urllib.parse as urlparse

CREATE_POSTS_TABLE_CMD = "\
                CREATE TABLE IF NOT EXISTS posts (\
                    id SERIAL PRIMARY KEY,\
                    user_id INT NOT NULL,\
                    price REAL NOT NULL,\
                    date DATE NOT NULL,\
                    is_blocked BOOLEAN DEFAULT false,\
                    type VARCHAR(15),\
                    title VARCHAR(30),\
                    description VARCHAR(100),\
                    room_transaction VARCHAR(250)\
                );\
                "

CREATE_BOOKINGS_TABLE_CMD = "\
                CREATE TABLE IF NOT EXISTS bookings (\
                    id SERIAL,\
                    user_id INT,\
                    wallet_id INT,\
                    guest_user_id INT NOT NULL,\
                    guest_wallet_id INT NOT NULL,\
                    post_id INT REFERENCES posts(id),\
                    status VARCHAR(50),\
                    transaction VARCHAR(250) PRIMARY KEY,\
                    resTransaction VARCHAR(250),\
                    beginDate DATE,\
                    endDate DATE\
                );\
                "

DROP_ALL_CMD = "\
                DROP SCHEMA public CASCADE;\
                CREATE SCHEMA public;\
                GRANT ALL ON SCHEMA public TO postgres;\
                GRANT ALL ON SCHEMA public TO public;\
                "

INIT_CMD = CREATE_POSTS_TABLE_CMD + CREATE_BOOKINGS_TABLE_CMD + CREATE_BOOKINGS_TABLE_CMD

RESET_CMD = DROP_ALL_CMD + INIT_CMD


def add_post_query(user_id, price, date, type, title, description, roomTransaction):
    return "\
                INSERT INTO posts(user_id, price, date, type, title, description, room_transaction)\
                VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}')\
                RETURNING *".format(user_id, price, date, type, title, description, roomTransaction)

def get_bookings_query(guest_user_id, user_id, post_id, status, booking_id):
    query = "\
                SELECT * FROM bookings \
                "
    if booking_id:
        query += "WHERE id='{}'".format(booking_id)
    else:
        query += "WHERE id > 0 "
    if guest_user_id:
        query += "AND guest_user_id='{}' ".format(guest_user_id)
    if user_id:
        query += "AND user_id='{}' ".format(user_id)
    if status:
        query += "AND status='{}' ".format(status)
    if post_id:
        query += "AND post_id='{}'".format(post_id)
    return query

def add_booking_query(guest_user_id, guest_wallet_id, post_id, status, transaction, beginDate, endDate):
    return "\
                INSERT INTO bookings(guest_user_id, guest_wallet_id, post_id, status, transaction, beginDate, endDate)\
                VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}')\
                RETURNING *".format(guest_user_id, guest_wallet_id, post_id, status, transaction, beginDate, endDate)

def respond_booking_query(user_id, wallet_id, status, resTransaction, endDate, beginDate, guest_wallet_id, post_id):
    return "\
                UPDATE bookings\
                SET user_id='{}', wallet_id='{}', status='{}', resTransaction='{}'\
                WHERE endDate='{}'\
                AND beginDate='{}'\
                AND guest_wallet_id='{}'\
                AND post_id='{}'\
                RETURNING *".format(user_id, wallet_id, status, resTransaction, endDate, beginDate, guest_wallet_id, post_id)


def overlapping_bookings_count_query(post_id, beginDate, endDate):
    return "\
                SELECT COUNT(*)\
                FROM bookings\
                WHERE post_id='{}'\
                AND status='accepted'\
                AND (\
                    (beginDate BETWEEN '{}' AND ('{}'::date))\
                    OR\
                    (endDate BETWEEN ('{}'::date) AND '{}')\
                    OR\
                    (beginDate < '{}' AND endDate > '{}')\
                )".format(post_id, beginDate, endDate, beginDate, endDate, beginDate, endDate)

def overlapping_bookings_query(post_id, beginDate, endDate):
    return "\
                SELECT *\
                FROM bookings\
                WHERE post_id='{}'\
                AND status='pending'\
                AND (\
                    (beginDate BETWEEN '{}' AND ('{}'::date))\
                    OR\
                    (endDate BETWEEN ('{}'::date) AND '{}')\
                    OR\
                    (beginDate < '{}' AND endDate > '{}')\
                )".format(post_id, beginDate, endDate, beginDate, endDate, beginDate, endDate)

def get_post_query(post_id):
    return "\
                SELECT * FROM posts WHERE id = '{}'".format(post_id)

def get_post_transaction_query(post_id):
    return"\
                SELECT room_transaction\
                FROM posts\
                WHERE id='{}'".format(post_id)

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
            dbname=url.path[1:],
            user=url.username,
            host=url.hostname,
            password=url.password,
            port=url.port
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
