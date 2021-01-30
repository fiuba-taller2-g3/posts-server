import json

import requests
from flask import Flask, request, make_response, jsonify
from db_service import *

app = Flask(__name__)

conn = connect()
set_db(conn, INIT_CMD)

try:
    payments_base_url = os.environ['PAYMENTS_URL']
except KeyError:
    payments_base_url = 'https://payments-server-develop.herokuapp.com/'


@app.route('/')
def hello():
    response = 'db: UP\n'
    use_db(conn, 'SELECT NOW();')
    return response


@app.route('/posts', methods=['DELETE'])
def reset_posts():
    set_db(conn, RESET_CMD)
    return make_response("DB Reseted", 200)


@app.route('/posts', methods=['POST'])
def new_post():
    body = request.json
    post_id, availability_dates, availability_type, bathrooms, bedrooms, beds, beds_distribution, date, description, guests, images, installations, is_blocked, location, price, security, services, title, type, user_id, wallet_id, = use_db(conn, add_post_query(body))

    response = requests.post(payments_base_url + 'room',
                             json={"creatorId": body['wallet_id'], "price": body['price']},
                             headers={'Content-Type': 'application/json'})

    print('new post:', response.content)

    if response.status_code == 200:
        return make_response(
            jsonify(id=post_id, user_id=user_id, price=price, date=date.strftime('%Y-%m-%d'), is_blocked=is_blocked,
                    type=type), 201)


@app.route('/posts/<post_id>')
def visualize_post(post_id):
    post_id, availability_dates, availability_type, bathrooms, bedrooms, beds, beds_distribution, date, description, guests, images, installations, is_blocked, location, price, security, services, title, type, user_id, wallet_id, = use_db(conn, get_post_query(post_id))
    return make_response(
        jsonify(id=post_id, user_id=user_id, price=price, date=date.strftime('%Y-%m-%d'), is_blocked=is_blocked,
                type=type, title=title, description=description, availability_dates=availability_dates, availability_type=availability_type,
                bathrooms=bathrooms, bedrooms=bedrooms, beds=beds, beds_distribution=beds_distribution, guests=guests, images=images,
                installations=installations, location=location, security=security, services=services, wallet_id=wallet_id), 200)


@app.route('/posts/<post_id>', methods=['PATCH'])
def edit_post(post_id):
    body = request.json
    body.pop("id", None)
    post_id, availability_dates, availability_type, bathrooms, bedrooms, beds, beds_distribution, date, description, guests, images, installations, is_blocked, location, price, security, services, title, type, user_id, wallet_id, = use_db(conn, edit_post_cmd(post_id, **body))
    return make_response(
        jsonify(id=post_id, user_id=user_id, price=price, date=date.strftime('%Y-%m-%d'), is_blocked=is_blocked,
                type=type, title=title, description=description, availability_dates=availability_dates,
                availability_type=availability_type,
                bathrooms=bathrooms, bedrooms=bedrooms, beds=beds, beds_distribution=beds_distribution, guests=guests,
                images=images,
                installations=installations, location=location, security=security, services=services,
                wallet_id=wallet_id), 201)


@app.route('/posts/<post_id>', methods=['DELETE'])
def delete_post(post_id):
    post_id, availability_dates, availability_type, bathrooms, bedrooms, beds, beds_distribution, date, description, guests, images, installations, is_blocked, location, price, security, services, title, type, user_id, wallet_id, = use_db(conn, delete_post_query(post_id))
    return make_response(
        jsonify(id=post_id, user_id=user_id, price=price, date=date.strftime('%Y-%m-%d'), is_blocked=is_blocked,
                type=type, title=title, description=description, availability_dates=availability_dates,
                availability_type=availability_type,
                bathrooms=bathrooms, bedrooms=bedrooms, beds=beds, beds_distribution=beds_distribution, guests=guests,
                images=images,
                installations=installations, location=location, security=security, services=services,
                wallet_id=wallet_id), 200)


@app.route('/posts')
def search_posts():
    user_id = request.args.get('user_id')
    type = request.args.get('type')
    minPrice = request.args.get('minPrice')
    maxPrice = request.args.get('maxPrice')
    beginDate = request.args.get('beginDate')
    endDate = request.args.get('endDate')
    posts = use_db(conn, get_posts_query(user_id, type, minPrice, maxPrice), many=True)
    parsed_posts = []
    for post_id, availability_dates, availability_type, bathrooms, bedrooms, beds, beds_distribution, date, description, guests, images, installations, is_blocked, location, price, security, services, title, type, user_id, wallet_id, in posts:
        overlap = False
        if beginDate and endDate:
            overlap, = use_db(conn, overlapping_bookings_count_query(post_id, beginDate, endDate))
        if not overlap:
            parsed_posts.append({"id": post_id, "user_id": user_id, "price": price, "date": date.strftime('%Y-%m-%d'),
                                 "is_blocked": is_blocked, "type": type, "title": title, "description": description,
                                 "availability_dates": availability_dates, "availability_type": availability_type,
                                 "bathrooms": bathrooms, "bedrooms": bedrooms, "beds": beds, "beds_distribution": beds_distribution,
                                 "images": images, "installations": installations, "location": location, "security": security,
                                 "services": services, "wallet_id": wallet_id})
    return make_response(jsonify(parsed_posts), 200)


@app.route('/bookings', methods=['POST'])
def new_booking():
    body = request.json
    overlap, = use_db(conn, overlapping_bookings_count_query(body['post_id'], body['begin_date'], body['end_date']))
    if overlap:
        return make_response(jsonify({"error": "Alojamiento no disponible durante el rango de fechas ingresado"}), 409)
    booking_id, user_id, post_id, begin_date, end_date, = use_db(conn,
                                                                 add_booking_query(body['user_id'], body['post_id'],
                                                                                   body['begin_date'],
                                                                                   body['end_date']))
    return make_response(
        jsonify(post_id=post_id, user_id=user_id, booking_id=booking_id, begin_date=begin_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d')), 201)


if __name__ == '__main__':
    try:
        app.run(port=os.environ['PORT'])
    except KeyError:
        app.run()
    disconnect(conn)
