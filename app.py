import json

from datetime import datetime
from flask import Flask, request, make_response, jsonify
from db_service import *

app = Flask(__name__)

conn = connect()
set_db(conn, INIT_CMD)

@app.route('/')
def hello():
    response = 'db: UP\n'
    use_db(conn,'SELECT NOW();')
    return response

@app.route('/posts', methods=['DELETE'])
def reset_posts():
    set_db(conn, RESET_CMD)
    return make_response("DB Reseted", 200)

@app.route('/posts', methods=['POST'])
def new_post():
    body = request.json
    post_id, user_id, price, date, is_blocked, type, = use_db(conn, add_post_query(body['user_id'], body['price'], body['date'], body['type']))
    return make_response(jsonify(id=post_id, user_id=user_id, price=price, date=date.strftime('%Y-%m-%d'), is_blocked=is_blocked, type=type), 201)

@app.route('/posts/<post_id>')
def visualize_post(post_id):
    post_id, user_id, price, date, is_blocked, type, = use_db(conn, get_post_query(post_id))
    return make_response(jsonify(id=post_id, user_id=user_id, price=price, date=date.strftime('%Y-%m-%d'), is_blocked=is_blocked, type=type), 200)

@app.route('/posts/<post_id>', methods=['PATCH'])
def edit_post(post_id):
    body = request.json
    body.pop("id", None)
    post_id, user_id, price, date, is_blocked, type, = use_db(conn, edit_post_cmd(post_id, **body))
    return make_response(jsonify(id=post_id, user_id=user_id, price=price, date=date.strftime('%Y-%m-%d'), is_blocked=is_blocked, type=type), 201)

@app.route('/posts/<post_id>', methods=['DELETE'])
def delete_post(post_id):
    post_id, user_id, price, date, is_blocked, type, = use_db(conn, delete_post_query(post_id))
    return make_response(jsonify(id=post_id, user_id=user_id, price=price, date=date.strftime('%Y-%m-%d'), is_blocked=is_blocked, type=type), 200)

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
    for post_id, user_id, price, date, is_blocked, type in posts:
        overlap = False
        if beginDate and endDate:
            overlap, = use_db(conn, overlapping_bookings_count_query(post_id, beginDate, endDate))
        if not overlap:
            parsed_posts.append({"id":post_id, "user_id":user_id, "price":price, "date":date.strftime('%Y-%m-%d'), "is_blocked":is_blocked, "type":type})
    return make_response(jsonify(parsed_posts), 200)

@app.route('/bookings', methods=['POST'])
def new_booking():
    body = request.json
    overlap, = use_db(conn, overlapping_bookings_count_query(body['post_id'], body['beginDate'], body['endDate']))
    if overlap:
        return make_response(jsonify({"error": "Alojamiento no disponible durante el rango de fechas ingresado"}), 409)
    booking_id, user_id, post_id, beginDate, endDate, = use_db(conn, add_booking_query(body['user_id'], body['post_id'], body['beginDate'], body['endDate']))
    return make_response(jsonify(post_id=post_id, user_id=user_id, booking_id=booking_id, beginDate=beginDate.strftime('%Y-%m-%d'), endDate=endDate.strftime('%Y-%m-%d')), 201)

if __name__ == '__main__':
    try:
        app.run(port=os.environ['PORT'])
    except KeyError:
        app.run()
    disconnect(conn)
