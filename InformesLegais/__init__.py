from flask import Flask
from .cadastros_bp import cadastros
import os


def create_app():

    app = Flask(__name__)
    app.register_blueprint(cadastros)

    @app.route('/')
    def hello_world():  # put application's code here
        return 'Hello World!'

    app.config['DEBUG'] = True

    app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')

    return app




