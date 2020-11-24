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
    try:
        use_db(conn,'\SELECT NOW();')
    except Exception as e:
        response = "Error {}: {}\n".format(type(e).__name__, e.args)
    return response

@app.route('/posts/reset', methods=['DELETE'])
def reset_posts():
    set_db(conn, RESET_CMD)
    return make_response("DB Reseted", 200)

@app.route('/posts', methods=['POST'])
def new_post():
    body = request.json
    post_id, = use_db(conn, add_post_query(body['user_id'], body['price'], body['date']))
    return make_response(jsonify(id=post_id), 201)

@app.route('/posts/<post_id>')
def visualize_post(post_id):
    user_id, price, date = use_db(conn, get_post_query(post_id))
    return make_response(jsonify(user_id=user_id, date=date.strftime('%Y-%m-%d') , price=price), 200)

@app.route('/posts/<post_id>', methods=['PATCH'])
def edit_post(post_id):
    return use_db(conn, )

@app.route('/posts/<post_id>', methods=['DELETE'])
def delete_post(post_id):
    id, = use_db(conn, delete_post_query(post_id))
    return make_response("Post {} deleted successfully".format(id), 200)

@app.route('/posts')
def visualize_posts_from_user():
    user_id = request.args.get('user_id')
    posts = use_db(conn, get_posts_query(user_id))
    parsed_posts = []
    for id, user_id, price, date, is_blocked in posts:
        parsed_posts.append({"id":id, "user_id":user_id, "price":price, "date":date.strftime('%Y-%m-%d'), "is_blocked":is_blocked})
    return make_response(jsonify(parsed_posts), 200)

if __name__ == '__main__':
    try:
        app.run(port=os.environ['PORT'])
    except KeyError:
        app.run()
    disconnect(conn)
