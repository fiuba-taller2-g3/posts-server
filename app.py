import requests
import datetime
import geopy.distance
import sys

from math import floor
from flask import Flask, request, make_response, jsonify
from db_service import *

from fcm_service import send_notification
from tokens_service import save_token, reset_tokens, get_tokens

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


@app.route('/feedback', methods=['POST'])
def new_feedback():
    body = request.json
    if use_db(conn, count_bookings_query(body['post_id'], body['user_id']))[0] == 0:
        return make_response({"error": "No puedes calificar este alojamiento si nunca reservaste ahi"}, 400)
    feedback_id, post_id, user_id, date, comment, stars, = use_db(conn, add_feedback_query(body['user_id'],
                                                                                           body['post_id'],
                                                                                           body['date'],
                                                                                           body.get('comment'),
                                                                                           body.get('stars')
                                                                                           ))
    return make_response(
        jsonify(
            feedback_id=feedback_id,
            post_id=post_id,
            user_id=user_id,
            date=date.strftime('%Y-%m-%d'),
            coment=comment,
            stars=stars
        )
    )


@app.route('/feedback')
def get_feedbacks():
    user_id = request.args.get('user_id')
    post_id = request.args.get('post_id')
    date = request.args.get('date')
    mandatoryComment = request.args.get('mandatoryComment', False)
    mandatoryStars = request.args.get('mandatoryStars', False)
    feedbacks = []
    for feedback_id, post_id, user_id, date, comment, stars in use_db(conn, get_feedback_query(user_id,
                                                                                               post_id,
                                                                                               date,
                                                                                               mandatoryComment == True,
                                                                                               mandatoryStars == True
                                                                                               ), many=True):
        feedbacks.append({'feedback_id': feedback_id, 'post_id': post_id,
                          'user_id': user_id, 'date': date.strftime('%Y-%m-%d'),
                          'comment': comment, 'stars': stars})
    return make_response(jsonify(feedbacks), 200)


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
        post_id, availability_dates, availability_type, bathrooms, bedrooms, beds, beds_distribution, date, description, guests, images, is_blocked, location, price, services, title, type, user_id, wallet_id, room_transaction, = use_db(
            conn, add_post_query(body))
        return make_response(
            jsonify(id=post_id, user_id=user_id, price=price, date=date.strftime('%Y-%m-%d'), is_blocked=is_blocked,
                    type=type, title=title, description=description, availability_dates=availability_dates,
                    availability_type=availability_type,
                    bathrooms=bathrooms, bedrooms=bedrooms, beds=beds, beds_distribution=beds_distribution,
                    guests=guests, images=images,
                    location=location, services=services, wallet_id=wallet_id, room_transaction=room_transaction), 201)


@app.route('/posts/<post_id>')
def visualize_post(post_id):
    post_id, availability_dates, availability_type, bathrooms, bedrooms, beds, beds_distribution, date, description, guests, images, is_blocked, location, price, services, title, type, user_id, wallet_id, room_transaction, = use_db(
        conn, get_post_query(post_id))
    return make_response(
        jsonify(id=post_id, user_id=user_id, price=price, date=date.strftime('%Y-%m-%d'), is_blocked=is_blocked,
                type=type, title=title, description=description, availability_dates=availability_dates,
                availability_type=availability_type,
                bathrooms=bathrooms, bedrooms=bedrooms, beds=beds, beds_distribution=beds_distribution, guests=guests,
                images=images,
                location=location, services=services, wallet_id=wallet_id, room_transaction=room_transaction), 200)


@app.route('/posts/<post_id>', methods=['PATCH'])
def edit_post(post_id):
    body = request.json
    body.pop("id", None)
    if body.get('price'):
        roomTransaction = use_db(conn, get_post_transaction_query(post_id))[0]
        owner_w_id = use_db(conn, get_post_owner_wallet_id_query(post_id))[0]
        response = requests.patch(payments_base_url + 'room',
                                  json={'wallet_id': owner_w_id, 'room_transaction': roomTransaction,
                                        'price': body.get('price')})
        if response.status_code != 200:
            return make_response(response.content, 500)
    post_id, availability_dates, availability_type, bathrooms, bedrooms, beds, beds_distribution, date, description, guests, images, is_blocked, location, price, services, title, type, user_id, wallet_id, room_transaction, = use_db(
        conn, edit_post_cmd(post_id, **body))
    return make_response(
        jsonify(id=post_id, user_id=user_id, price=price, date=date.strftime('%Y-%m-%d'), is_blocked=is_blocked,
                type=type, title=title, description=description, availability_dates=availability_dates,
                availability_type=availability_type,
                bathrooms=bathrooms, bedrooms=bedrooms, beds=beds, beds_distribution=beds_distribution, guests=guests,
                images=images,
                location=location, services=services,
                wallet_id=wallet_id, room_transaction=room_transaction), 201)


@app.route('/posts/<post_id>', methods=['DELETE'])
def delete_post(post_id):
    roomTransaction = use_db(conn, get_post_transaction_query(post_id))[0]
    owner_w_id = use_db(conn, get_post_owner_wallet_id_query(post_id))[0]
    response = requests.delete(payments_base_url + 'room',
                               json={'room_transaction': roomTransaction, 'wallet_id': owner_w_id})
    if response.status_code != 200:
        return make_response(response.content, 500)
    post_id, availability_dates, availability_type, bathrooms, bedrooms, beds, beds_distribution, date, description, guests, images, is_blocked, location, price, services, title, type, user_id, wallet_id, room_transaction, = use_db(
        conn, delete_post_query(post_id))
    return make_response(
        jsonify(id=post_id, user_id=user_id, price=price, date=date.strftime('%Y-%m-%d'), is_blocked=is_blocked,
                type=type, title=title, description=description, availability_dates=availability_dates,
                availability_type=availability_type,
                bathrooms=bathrooms, bedrooms=bedrooms, beds=beds, beds_distribution=beds_distribution, guests=guests,
                images=images,
                location=location, services=services,
                wallet_id=wallet_id, room_transaction=room_transaction), 200)

def get_posts_query_wrapper(user_id, type, minPrice, maxPrice, bodyBeginDate, bodyEndDate, lng, lat, hide_user_id ,maxDistance):
    posts = use_db(conn, get_posts_query(user_id, type, minPrice, maxPrice, hide_user_id), many=True)
    parsed_posts = []
    for post_id, availability_dates, availability_type, bathrooms, bedrooms, beds, beds_distribution, date, description, guests, images, is_blocked, location, price, services, title, type, user_id, wallet_id, room_transaction, in posts:
        closeEnough = True
        if lng and lat:
            maxDistance = float(maxDistance) if maxDistance else 100.0
            postCoords = (float(location['lng']), float(location['lat']))
            searchCoords = (float(lng), float(lat))
            distance = geopy.distance.geodesic(postCoords, searchCoords).km
            closeEnough = distance <= maxDistance
        overlap = False
        availableRoom = True
        if bodyBeginDate and bodyEndDate:
            overlap, = use_db(conn, overlapping_bookings_count_query(post_id, bodyBeginDate, bodyEndDate))
            avBeginDate = datetime.datetime.strptime(availability_dates['start_date'], '%Y-%m-%d')
            avEndDate = datetime.datetime.strptime(availability_dates['end_date'], '%Y-%m-%d')
            beginDate = datetime.datetime.strptime(bodyBeginDate, '%Y-%m-%d')
            endDate = datetime.datetime.strptime(bodyEndDate, '%Y-%m-%d')
            availableRoom = avBeginDate <= beginDate <= avEndDate and avBeginDate <= endDate <= avEndDate
        if not overlap and availableRoom and closeEnough and not is_blocked:
            parsed_posts.append({"id": post_id, "user_id": user_id, "price": price, "date": date.strftime('%Y-%m-%d'),
                                 "is_blocked": is_blocked, "type": type, "title": title, "description": description,
                                 "availability_dates": availability_dates, "availability_type": availability_type,
                                 "bathrooms": bathrooms, "bedrooms": bedrooms, "beds": beds,
                                 "beds_distribution": beds_distribution, "recommended" : False,
                                 "guests": guests, "images": images, "location": location,
                                 "services": services, "wallet_id": wallet_id, "room_transaction": room_transaction})
    return parsed_posts

def loose_filters(minPrice, maxPrice, beginDate, endDate, maxDistance):
    loosenMinPrice = None
    loosenMaxPrice = None
    loosenBeginDate = None
    loosenEndDate = None
    loosenMaxDistance = None
    if beginDate and endDate:
        beginDate = datetime.datetime.strptime(beginDate, '%Y-%m-%d')
        endDate = datetime.datetime.strptime(endDate, '%Y-%m-%d')
        diffDays = (endDate - beginDate).days
        loosenBeginDate = beginDate + datetime.timedelta(days=floor(diffDays/5))
        loosenEndDate = endDate - datetime.timedelta(days=floor(diffDays/5))
        loosenBeginDate = loosenBeginDate.strftime('%Y-%m-%d')
        loosenEndDate = loosenEndDate.strftime('%Y-%m-%d')
    if maxDistance:
        loosenMaxDistance = maxDistance * 2
    if minPrice:
        loosenMinPrice = float(minPrice) * 0.75
    if maxPrice:
        loosenMaxPrice = float(maxPrice) * 1.25
    return loosenMinPrice, loosenMaxPrice, loosenBeginDate, loosenEndDate, loosenMaxDistance


@app.route('/posts')
def search_posts():
    user_id = request.args.get('user_id')
    type = request.args.get('type')
    if type: type = type.lower()
    minPrice = request.args.get('minPrice')
    maxPrice = request.args.get('maxPrice')
    bodyBeginDate = request.args.get('beginDate')
    bodyEndDate = request.args.get('endDate')
    lng = request.args.get('lng')
    lat = request.args.get('lat')
    maxDistance = request.args.get('maxDistance')
    hide_user_id = request.args.get('hide_user_id')
    includeRecommendations = request.args.get('includeRecommendations', False)
    searchPosts = get_posts_query_wrapper(
        user_id, type, minPrice, maxPrice, bodyBeginDate, bodyEndDate, lng, lat, hide_user_id, maxDistance
    )
    recommendedPosts = []
    if bool(includeRecommendations):
        minPrice, maxPrice, bodyBeginDate, bodyEndDate, maxDistance = loose_filters(
            minPrice, maxPrice, bodyBeginDate, bodyEndDate, maxDistance
        )
        withRecommendedPosts = get_posts_query_wrapper(
            user_id, type, minPrice, maxPrice, bodyBeginDate, bodyEndDate, lng, lat, hide_user_id, maxDistance
        )
        recommendedPosts = [post for post in withRecommendedPosts if post not in searchPosts]
        for post in recommendedPosts:
            post["recommended"] = True
    print("Searched: ", len(searchPosts), " Recommended: ", len(recommendedPosts))
    return make_response(jsonify(searchPosts + recommendedPosts), 200)


@app.route('/bookings', methods=['GET'])
def get_bookings():
    guest_user_id = request.args.get('guest_user_id')
    user_id = request.args.get('user_id')
    post_id = request.args.get('post_id')
    status = request.args.get('status')
    booking_id = request.args.get('booking_id')
    bookings = use_db(conn, get_bookings_query(guest_user_id, user_id, post_id, status, booking_id), many=True)
    parsed_bookings = []
    for b_id, u_id, w_id, gu_id, gw_id, p_id, status, tx, res_tx, begin_date, end_date, creation_date in bookings:
        parsed_bookings.append({"booking_id": b_id, "user_id": u_id, "wallet_id": w_id,
                                "guest_user_id": gu_id, "guest_wallet_id": gw_id, "post_id": p_id, "status": status,
                                "transaction": tx, "response_transaction": res_tx, "creation_date": creation_date.strftime('%Y-%m-%d'),
                                "begin_date": begin_date.strftime('%Y-%m-%d'),
                                "end_date": end_date.strftime('%Y-%m-%d')})
    return make_response(jsonify(parsed_bookings), 200)


@app.route('/bookings', methods=['POST'])
def new_booking():
    body = request.json
    # TODO Validar availavility
    roomTransaction = use_db(conn, get_post_transaction_query(body['post_id']))[0]
    beginDate = datetime.datetime.strptime(body['begin_date'], '%Y-%m-%d')
    endDate = datetime.datetime.strptime(body['end_date'], '%Y-%m-%d')
    response = requests.post(payments_base_url + 'booking', json={"wallet_id": body['wallet_id'],
                                                                  "room_transaction": roomTransaction,
                                                                  "day": beginDate.day,
                                                                  "month": beginDate.month,
                                                                  "year": beginDate.year,
                                                                  "end_day": endDate.day,
                                                                  "end_month": endDate.month,
                                                                  "end_year": endDate.year})
    if response.status_code == 200:
        host_id = use_db(conn, get_user_id_of_post_query(body['post_id']))
        print("host_id:", host_id)
        print('sarasa', str(host_id[0]))
        sys.stdout.flush()
        # TODO Notificar al host que intentaron reservar
        send_notification(str(host_id[0]), "Intentaron reservar tu alojamiento",
                          "Desde el " + str(beginDate) + " hasta el " + str(endDate) + "|host")
        b_id, u_id, w_id, gu_id, gw_id, p_id, status, tx, res_tx, begin_date, end_date, creation_date, = use_db(conn,
                                                                                                 add_booking_query(
                                                                                                     body.get('host_user_id'),
                                                                                                     body.get('host_wallet_id'),
                                                                                                     body['user_id'],
                                                                                                     body['wallet_id'],
                                                                                                     body['post_id'],
                                                                                                     'pending',
                                                                                                     response.json()[
                                                                                                         'intentTransaction'],
                                                                                                     body['begin_date'],
                                                                                                     body['end_date']
                                                                                                 ))
        return make_response(
            jsonify(post_id=p_id, guest_user_id=gu_id, guest_wallet_id=gw_id, booking_id=b_id,
                    begin_date=body['begin_date'], creation_date=creation_date.strftime('%Y-%m-%d'),
                    end_date=body['end_date'], status=status, transaction=tx), 201)
    return make_response(response.content, 500)


@app.route('/rejectance', methods=['POST'])
def reject_booking():
    body = request.json
    roomTransaction = use_db(conn, get_post_transaction_query(body['post_id']))[0]
    beginDate = datetime.datetime.strptime(body['begin_date'], '%Y-%m-%d')
    endDate = datetime.datetime.strptime(body['end_date'], '%Y-%m-%d')
    response = requests.post(payments_base_url + 'rejectance', json={"wallet_id": body['wallet_id'],
                                                                     "guest_wallet_id": body['guest_wallet_id'],
                                                                     "room_transaction": roomTransaction,
                                                                     "day": beginDate.day,
                                                                     "month": beginDate.month,
                                                                     "year": beginDate.year,
                                                                     "end_day": endDate.day,
                                                                     "end_month": endDate.month,
                                                                     "end_year": endDate.year})
    if response.status_code == 200:
        send_notification(body['user_id'], "Reservación rechazada", "Volvé a intentarlo" + "|guest")
        b_id, u_id, w_id, gu_id, gw_id, p_id, status, tx, res_tx, begin_date, end_date, creation_date, = use_db(conn, respond_booking_query(
            body['user_id'],
            body['wallet_id'],
            'rejected',
            response.json()['rejectTransaction'],
            body['end_date'],
            body['begin_date'],
            body['guest_wallet_id'],
            body['post_id']
        ))
        return make_response(
            jsonify(post_id=p_id, guest_user_id=gu_id, guest_wallet_id=gw_id, booking_id=b_id,
                    begin_date=body['begin_date'], creation_date=creation_date.strftime('%Y-%m-%d'),
                    user_id=u_id, wallet_id=w_id, end_date=body['end_date'], status=status, transaction=tx,
                    rejectTrasaction=res_tx),
            201)
    return make_response(response.content, 500)


@app.route('/acceptance', methods=['POST'])
def accept_booking():
    body = request.json
    roomTransaction = use_db(conn, get_post_transaction_query(body['post_id']))[0]
    beginDate = datetime.datetime.strptime(body['begin_date'], '%Y-%m-%d')
    endDate = datetime.datetime.strptime(body['end_date'], '%Y-%m-%d')
    response = requests.post(payments_base_url + 'acceptance', json={"wallet_id": body['wallet_id'],
                                                                     "guest_wallet_id": body['guest_wallet_id'],
                                                                     "room_transaction": roomTransaction,
                                                                     "day": beginDate.day,
                                                                     "month": beginDate.month,
                                                                     "year": beginDate.year,
                                                                     "end_day": endDate.day,
                                                                     "end_month": endDate.month,
                                                                     "end_year": endDate.year})
    if response.status_code == 200:
        # TODO Notificar al guest que se acepto la reserva
        send_notification(body['guest_user_id'], "Reservación confirmada", "¡Que disfrutes tu alojamiento!")
        b_id, u_id, w_id, gu_id, gw_id, p_id, status, tx, res_tx, begin_date, end_date, creation_date, = use_db(conn,
                                                                                                 respond_booking_query(
                                                                                                     body['user_id'],
                                                                                                     body['wallet_id'],
                                                                                                     'accepted',
                                                                                                     response.json()[
                                                                                                         'acceptTransaction'],
                                                                                                     body['end_date'],
                                                                                                     body['begin_date'],
                                                                                                     body[
                                                                                                         'guest_wallet_id'],
                                                                                                     body['post_id']
                                                                                                 ))
        acceptResponse = make_response(
            jsonify(post_id=p_id, guest_user_id=gu_id, guest_wallet_id=gw_id, booking_id=b_id,
                    begin_date=body['begin_date'], creation_date=creation_date.strftime('%Y-%m-%d'),
                    user_id=u_id, wallet_id=w_id, end_date=body['end_date'], status=status, transaction=tx,
                    acceptTrasaction=res_tx), 201)
        overlappingBookings = use_db(conn,
                                     overlapping_bookings_query(body['post_id'], body['begin_date'], body['end_date']),
                                     many=True)
        for b_id, u_id, w_id, gu_id, gw_id, p_id, status, tx, res_tx, begin_date, end_date, creation_date in overlappingBookings:
            response = requests.post(payments_base_url + 'rejectance', json={"wallet_id": body['wallet_id'],
                                                                             "guest_wallet_id": gw_id,
                                                                             "room_transaction": roomTransaction,
                                                                             "day": beginDate.day,
                                                                             "month": beginDate.month,
                                                                             "year": beginDate.year,
                                                                             "end_day": endDate.day,
                                                                             "end_month": endDate.month,
                                                                             "end_year": endDate.year})
            if response.status_code == 200:
                # TODO Notificar al guest que se rechazo la reserva
                send_notification(body['guest_user_id'], "Reservación rechazada", "Volvé a intentarlo")
                resValues = use_db(conn, respond_booking_query(
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
    return make_response(response.content, 500)


# endpoint para pruebas internas
@app.route('/notifications', methods=['POST'])
def notifications():
    user_id = request.args.get('user_id')
    result = send_notification(str(user_id), "default title", "default message")
    print(result)

    return make_response(result, 200)


@app.route('/tokens', methods=['POST'])
def save_tokens():
    save_token(request.json['user_id'], request.json['token_id'])
    return make_response("{\"msg\" : \"ok\"}", 201)


@app.route('/posts/metrics')
def metrics_posts():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    res = use_db(conn, count_posts_between_dates(from_date, to_date), many=True)
    if res is not []:
        return make_response(json.dumps([{"name": row[0].strftime('%d-%m-%Y'), "value": row[1]} for row in res]), 200)
    else:
        print("no hay publicaciones")
        sys.stdout.flush()
        return make_response("{\"msg\" : \"empty\"}", 204)


@app.route('/bookings/metrics')
def metrics_bookings():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    res = use_db(conn, count_bookings_between_dates(from_date, to_date), many=True)
    if res is not []:
        return make_response(json.dumps([{"name": row[0].strftime('%d-%m-%Y'), "value": row[1]} for row in res]), 200)
    else:
        print("no hay bookings")
        sys.stdout.flush()
        return make_response("{\"msg\" : \"empty\"}", 204)


@app.route('/tokens', methods=['DELETE'])
def delete_tokens():
    reset_tokens()
    return make_response("{\"msg\" : \"ok\"}", 200)


@app.route('/tokens')
def tokens():
    return make_response(json.dumps(get_tokens()), 200)


if __name__ == '__main__':
    try:
        app.run(port=os.environ['PORT'])
    except KeyError:
        app.run()
    disconnect(conn)
