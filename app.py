from flask import Flask, request
from init_db import *

app = Flask(__name__)


@app.route('/')
def hello():
    response = 'db: UP\n'
    conn = connect()
    try:
        use_db(conn,'SELECT NOW();')
    except Exception as e:
        response = "Error {}: {}\n".format(type(e).__name__, e.args)
    finally:
        disconnect(conn)

    return response


if __name__ == '__main__':
    try:
        app.run(port=os.environ['PORT'])
    except KeyError:
        app.run()
