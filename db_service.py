#!/usr/bin/python
import psycopg2
import os
import urllib.parse as urlparse
import json

CREATE_FEEDBACK_TABLE_CMD = "\
                CREATE TABLE IF NOT EXISTS feedback (\
                    id SERIAL PRIMARY KEY,\
                    post_id INT REFERENCES posts(id),\
                    user_id INT NOT NULL,\
                    date DATE NOT NULL,\
                    comment VARCHAR(400),\
                    stars INT\
                );\
                "

CREATE_POSTS_TABLE_CMD = "\
                CREATE TABLE IF NOT EXISTS posts (\
                    id SERIAL PRIMARY KEY,\
                    availability_dates json NOT NULL,\
                    availability_type VARCHAR(50) NOT NULL,\
                    bathrooms VARCHAR(10) NOT NULL,\
                    bedrooms VARCHAR(10) NOT NULL,\
                    beds VARCHAR(10) NOT NULL,\
                    beds_distribution JSON NOT NULL,\
                    date DATE NOT NULL,\
                    description VARCHAR(100) NOT NULL,\
                    guests VARCHAR(10) NOT NULL,\
                    images json NOT NULL,\
                    is_blocked BOOLEAN DEFAULT false,\
                    location json NOT NULL,\
                    price DOUBLE PRECISION NOT NULL,\
                    services json NOT NULL,\
                    title VARCHAR(30),\
                    type VARCHAR(15),\
                    user_id INT NOT NULL,\
                    wallet_id INT NOT NULL,\
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
                    transaction VARCHAR(250),\
                    resTransaction VARCHAR(250),\
                    beginDate DATE,\
                    endDate DATE,\
                    creationDate DATE NOT NULL DEFAULT CURRENT_DATE,\
                    PRIMARY KEY (guest_wallet_id, post_id, beginDate, endDate)\
                );\
                "

DROP_ALL_CMD = "\
                DROP SCHEMA public CASCADE;\
                CREATE SCHEMA public;\
                GRANT ALL ON SCHEMA public TO postgres;\
                GRANT ALL ON SCHEMA public TO public;\
                "

INIT_CMD = CREATE_POSTS_TABLE_CMD + CREATE_BOOKINGS_TABLE_CMD + CREATE_BOOKINGS_TABLE_CMD + CREATE_FEEDBACK_TABLE_CMD

RESET_CMD = DROP_ALL_CMD + INIT_CMD


def add_feedback_query(user_id, post_id, date, comment, stars):
    query = "\
                INSERT INTO feedback(user_id, post_id, date, comment, stars)\
                VALUES ('{}', '{}', '{}', ".format(user_id, post_id, date)
    query += "'{}', ".format(comment) if comment else "NULL, "
    query += "'{}'".format(stars) if stars else "NULL"
    query += ")RETURNING *"
    return query


def get_feedback_query(user_id, post_id, date, mandatoryComment, mandatoryStars):
    query = "\
                    SELECT * FROM feedback\
                    WHERE id > 0 "
    if user_id:
        query += "AND user_id = '{}' ".format(user_id)
    if post_id:
        query += "AND post_id = '{}' ".format(post_id)
    if date:
        query += "AND date = '{}' ".format(date)
    if mandatoryStars:
        query += "AND stars IS NOT NULL "
    if mandatoryComment:
        query += "AND comment IS NOT NULL "
    return query


def count_bookings_query(post_id, guest_user_id):
    return "\
        SELECT COUNT(*) FROM bookings \
        WHERE guest_user_id='{}' \
        AND post_id='{}' \
        AND status='accepted'".format(guest_user_id, post_id)


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


def add_post_query(body):
    return "\
                INSERT INTO posts(availability_dates, availability_type, bathrooms, bedrooms, beds, beds_distribution, " \
           "date, description, guests, images, is_blocked, location, price, services, title, type, user_id, wallet_id, room_transaction)\
                VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')\
                RETURNING *".format(json.dumps(body["availability_dates"]), body["availability_type"],
                                    body["bathrooms"],
                                    body["bedrooms"],
                                    body["beds"], json.dumps(body["beds_distribution"]), body["date"],
                                    body["description"], body["guests"],
                                    json.dumps(body["images"]),
                                    body["is_blocked"], json.dumps(body["location"]), body["price"],
                                    json.dumps(body["services"]), body["title"], body["type"].lower(), body["user_id"],
                                    body["wallet_id"], body["room_transaction"])


def add_booking_query(user_id, wallet_id, guest_user_id, guest_wallet_id, post_id, status, transaction, beginDate, endDate):
    return "\
                INSERT INTO bookings(user_id, wallet_id, guest_user_id, guest_wallet_id, post_id, status, transaction, beginDate, endDate)\
                VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')\
                RETURNING *".format(user_id, wallet_id, guest_user_id, guest_wallet_id, post_id, status, transaction, beginDate, endDate)


def respond_booking_query(user_id, wallet_id, status, resTransaction, endDate, beginDate, guest_wallet_id, post_id):
    return "\
                UPDATE bookings\
                SET user_id='{}', wallet_id='{}', status='{}', resTransaction='{}'\
                WHERE endDate='{}'\
                AND beginDate='{}'\
                AND guest_wallet_id='{}'\
                AND post_id='{}'\
                RETURNING *".format(user_id, wallet_id, status, resTransaction, endDate, beginDate, guest_wallet_id,
                                    post_id)


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


def get_user_id_of_post_query(post_id):
    return "\
                SELECT user_id FROM posts WHERE id = '{}'".format(post_id)


def get_post_transaction_query(post_id):
    return "\
                SELECT room_transaction\
                FROM posts\
                WHERE id='{}'".format(post_id)


def get_post_owner_wallet_id_query(post_id):
    return "\
                SELECT wallet_id\
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


def get_posts_query(user_id, type, minPrice, maxPrice, hide_user_id):
    query = "\
                SELECT * FROM posts\
                WHERE id > 0 "
    if user_id:
        query += "AND user_id = '{}' ".format(user_id)
    if hide_user_id:
        query += "AND user_id != '{}' ".format(hide_user_id)
    if type:
        query += "AND type = '{}' ".format(type)
    if maxPrice:
        query += "AND price <= {} ".format(maxPrice)
    if minPrice:
        query += "AND price >= {} ".format(minPrice)
    return query


def delete_post_query(post_id):
    return "\
                DELETE FROM posts\
                WHERE id = '{}'\
                RETURNING *".format(post_id)


def count_posts_between_dates(from_date, to_date):
    return "\
                SELECT date as date_key, COUNT(*) as value\
                FROM posts\
                WHERE (date >= '{}' AND date <= '{}')\
                GROUP BY date".format(from_date, to_date)


def count_bookings_between_dates(from_date, to_date):
    return "\
                SELECT creationDate, COUNT(*)\
                FROM bookings\
                WHERE (creationDate >= '{}' AND creationDate <= '{}')\
                GROUP BY creationDate".format(from_date, to_date)


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
