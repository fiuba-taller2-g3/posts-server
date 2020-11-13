from flask import Flask, request
from init_db import *

app = Flask(__name__)


@app.route('/')
def hello():

    return 'Hello World!\n'


if __name__ == '__main__':
    try:
        app.run(port=os.environ['PORT'])
    except KeyError:
        app.run()
