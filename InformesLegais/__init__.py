from flask import Flask
from .cadastros_bp import cadastros
from .extracoes_bp import extracoes
import os

from celery import Celery




from .celery import celery_app

from .tasks import extrair_posicao_jcot_unique





def create_app():

    app = Flask(__name__)
    app.register_blueprint(cadastros)
    app.register_blueprint(extracoes)

    @app.route('/')
    def hello_world():  # put application's code here
        return 'Hello World!'

    app.config['DEBUG'] = True

    app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')

    return app




