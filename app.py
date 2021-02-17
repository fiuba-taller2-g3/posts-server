import json

import requests
import datetime
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
    response = requests.post(payments_base_url + 'room',
                             json={"creatorId": body['wallet_id'], "price": body['price']},
                             headers={'Content-Type': 'application/json'})


    if response.status_code == 200:
        print('new room: ', response.json()['roomTransaction'])
        roomTransaction = response.json()['roomTransaction']
        body['room_transaction'] = roomTransaction
        post_id, availability_dates, availability_type, bathrooms, bedrooms, beds, beds_distribution, date, description, guests, images, installations, is_blocked, location, price, security, services, title, type, user_id, wallet_id, room_transaction, = use_db(conn, add_post_query(body))
        return make_response(
        jsonify(id=post_id, user_id=user_id, price=price, date=date.strftime('%Y-%m-%d'), is_blocked=is_blocked,
                type=type, title=title, description=description, availability_dates=availability_dates, availability_type=availability_type,
                bathrooms=bathrooms, bedrooms=bedrooms, beds=beds, beds_distribution=beds_distribution, guests=guests, images=images,
                installations=installations, location=location, security=security, services=services, wallet_id=wallet_id, room_transaction=room_transaction), 201)


@app.route('/posts/<post_id>')
def visualize_post(post_id):
    post_id, availability_dates, availability_type, bathrooms, bedrooms, beds, beds_distribution, date, description, guests, images, installations, is_blocked, location, price, security, services, title, type, user_id, wallet_id, room_transaction, = use_db(conn, get_post_query(post_id))
    return make_response(
        jsonify(id=post_id, user_id=user_id, price=price, date=date.strftime('%Y-%m-%d'), is_blocked=is_blocked,
                type=type, title=title, description=description, availability_dates=availability_dates, availability_type=availability_type,
                bathrooms=bathrooms, bedrooms=bedrooms, beds=beds, beds_distribution=beds_distribution, guests=guests, images=images,
                installations=installations, location=location, security=security, services=services, wallet_id=wallet_id, room_transaction=room_transaction), 200)


@app.route('/posts/<post_id>', methods=['PATCH'])
def edit_post(post_id):
    body = request.json
    body.pop("id", None)
    post_id, availability_dates, availability_type, bathrooms, bedrooms, beds, beds_distribution, date, description, guests, images, installations, is_blocked, location, price, security, services, title, type, user_id, wallet_id, room_transaction, = use_db(conn, edit_post_cmd(post_id, **body))
    return make_response(
        jsonify(id=post_id, user_id=user_id, price=price, date=date.strftime('%Y-%m-%d'), is_blocked=is_blocked,
                type=type, title=title, description=description, availability_dates=availability_dates,
                availability_type=availability_type,
                bathrooms=bathrooms, bedrooms=bedrooms, beds=beds, beds_distribution=beds_distribution, guests=guests,
                images=images,
                installations=installations, location=location, security=security, services=services,
                wallet_id=wallet_id, room_transaction=room_transaction), 201)


@app.route('/posts/<post_id>', methods=['DELETE'])
def delete_post(post_id):
    post_id, availability_dates, availability_type, bathrooms, bedrooms, beds, beds_distribution, date, description, guests, images, installations, is_blocked, location, price, security, services, title, type, user_id, wallet_id, room_transaction, = use_db(conn, delete_post_query(post_id))
    return make_response(
        jsonify(id=post_id, user_id=user_id, price=price, date=date.strftime('%Y-%m-%d'), is_blocked=is_blocked,
                type=type, title=title, description=description, availability_dates=availability_dates,
                availability_type=availability_type,
                bathrooms=bathrooms, bedrooms=bedrooms, beds=beds, beds_distribution=beds_distribution, guests=guests,
                images=images,
                installations=installations, location=location, security=security, services=services,
                wallet_id=wallet_id, room_transaction=room_transaction), 200)


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
    for post_id, availability_dates, availability_type, bathrooms, bedrooms, beds, beds_distribution, date, description, guests, images, installations, is_blocked, location, price, security, services, title, type, user_id, wallet_id, room_transaction, in posts:
        overlap = False
        if beginDate and endDate:
            overlap, = use_db(conn, overlapping_bookings_count_query(post_id, beginDate, endDate))
        if not overlap:
            parsed_posts.append({"id": post_id, "user_id": user_id, "price": price, "date": date.strftime('%Y-%m-%d'),
                                 "is_blocked": is_blocked, "type": type, "title": title, "description": description,
                                 "availability_dates": availability_dates, "availability_type": availability_type,
                                 "bathrooms": bathrooms, "bedrooms": bedrooms, "beds": beds, "beds_distribution": beds_distribution,
                                 "guests": guests, "images": images, "installations": installations, "location": location, "security": security,
                                 "services": services, "wallet_id": wallet_id, "room_transaction" : room_transaction})
    return make_response(jsonify(parsed_posts), 200)

@app.route('/bookings', methods=['GET'])
def get_bookings():
    guest_user_id = request.args.get('guest_user_id')
    user_id = request.args.get('user_id')
    post_id = request.args.get('post_id')
    status = request.args.get('status')
    booking_id = request.args.get('booking_id')
    bookings = use_db(conn, get_bookings_query(guest_user_id, user_id, post_id, status, booking_id), many=True)
    parsed_bookings = []
    for b_id, u_id, w_id, gu_id, gw_id, p_id, status, tx, res_tx, begin_date, end_date in bookings:
        parsed_bookings.append({"booking_id":b_id, "user_id":u_id, "wallet_id":w_id,
                                "guest_user_id": gu_id,"guest_wallet_id": gw_id, "post_id":p_id, "status":status,
                                "transaction":tx, "response_transaction":res_tx,
                                "begin_date":begin_date.strftime('%Y-%m-%d'), "end_date":end_date.strftime('%Y-%m-%d')})
    return make_response(jsonify(parsed_bookings), 200)


@app.route('/bookings', methods=['POST'])
def new_booking():
    body = request.json
    roomTransaction = use_db(conn, get_post_transaction_query(body['post_id']))[0]
    beginDate = datetime.datetime.strptime(body['begin_date'], '%Y-%m-%d')
    endDate = datetime.datetime.strptime(body['end_date'], '%Y-%m-%d')
    response = requests.post(payments_base_url + 'booking', json={  "wallet_id" : body['wallet_id'],
                                                                    "room_transaction" : roomTransaction,
                                                                    "day" : beginDate.day,
                                                                    "month" : beginDate.month,
                                                                    "year" : beginDate.year,
                                                                    "end_day" : endDate.day,
                                                                    "end_month" : endDate.month,
                                                                    "end_year" : endDate.year})
    if response.status_code == 200:
        b_id, u_id, w_id, gu_id, gw_id, p_id, status, tx, res_tx, begin_date, end_date, = use_db(conn, add_booking_query( body['user_id'],
                                                                                                                    body['wallet_id'],
                                                                                                                    body['post_id'],
                                                                                                                    'pending',
                                                                                                                    response.json()['intentTransaction'],
                                                                                                                    body['begin_date'],
                                                                                                                    body['end_date']
                                                                                                                    ))
        return make_response(
            jsonify(post_id=p_id, guest_user_id=gu_id, guest_wallet_id=gw_id, booking_id=b_id, begin_date=body['begin_date'],
                    end_date=body['end_date'], status=status, transaction=tx), 201)
    return make_response(response.content, response.status_code)

@app.route('/acceptance', methods=['POST'])
def accept_booking():
    body = request.json
    roomTransaction = use_db(conn, get_post_transaction_query(body['post_id']))[0]
    beginDate = datetime.datetime.strptime(body['begin_date'], '%Y-%m-%d')
    endDate = datetime.datetime.strptime(body['end_date'], '%Y-%m-%d')
    response = requests.post(payments_base_url + 'acceptance', json={   "wallet_id" : body['wallet_id'],
                                                                        "guest_wallet_id" : body['guest_wallet_id'],
                                                                        "room_transaction" : roomTransaction,
                                                                        "day" : beginDate.day,
                                                                        "month" : beginDate.month,
                                                                        "year" : beginDate.year,
                                                                        "end_day" : endDate.day,
                                                                        "end_month" : endDate.month,
                                                                        "end_year" : endDate.year})
    if response.status_code == 200:
        # TODO Notificar al guest que se acepto la reserva
        b_id, u_id, w_id, gu_id, gw_id, p_id, status, tx, res_tx, begin_date, end_date, = use_db(conn, respond_booking_query(body['user_id'],
                                                                                                                body['wallet_id'],
                                                                                                                'accepted',
                                                                                                                response.json()['acceptTransaction'],
                                                                                                                body['end_date'],
                                                                                                                body['begin_date'],
                                                                                                                body['guest_wallet_id'],
                                                                                                                body['post_id']
                                                                                                                ))
        acceptResponse = make_response(
            jsonify(post_id=p_id, guest_user_id=gu_id, guest_wallet_id=gw_id, booking_id=b_id, begin_date=body['begin_date'],
                    user_id=u_id, wallet_id=w_id, end_date=body['end_date'], status=status, transaction=tx, acceptTrasaction=res_tx), 201)
        overlappingBookings = use_db(conn, overlapping_bookings_query(body['post_id'], body['begin_date'], body['end_date']), many=True)
        for b_id, u_id, w_id, gu_id, gw_id, p_id, status, tx, res_tx, begin_date, end_date in overlappingBookings:
            response = requests.post(payments_base_url + 'rejectance', json={   "wallet_id" : body['wallet_id'],
                                                                                "guest_wallet_id" : gw_id,
                                                                                "room_transaction" : roomTransaction,
                                                                                "day" : beginDate.day,
                                                                                "month" : beginDate.month,
                                                                                "year" : beginDate.year,
                                                                                "end_day" : endDate.day,
                                                                                "end_month" : endDate.month,
                                                                                "end_year" : endDate.year})
            if response.status_code == 200:
                # TODO Notificar al guest que se rechazo la reserva
                resValues = use_db(conn,respond_booking_query(
                                                                body['user_id'],
                                                                body['wallet_id'],
                                                                'rejected',
                                                                response.json()['rejectTransaction'],
                                                                end_date,
                                                                begin_date,
                                                                gw_id,
                                                                p_id
                                                                ))
            else:
                print("Fallo el rechazo", response.content)
        return acceptResponse
    return make_response(response.content, response.status_code)


if __name__ == '__main__':
    try:
        app.run(port=os.environ['PORT'])
    except KeyError:
        app.run()
    disconnect(conn)
