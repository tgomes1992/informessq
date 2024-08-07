from flask import Flask
from .cadastros_bp import cadastros
from .extracoes_bp import extracoes
from .geracaoInformes_bp import geracao_informes_bp
import os

from celery import Celery




from .celery import celery_app

from .tasks import extrair_posicao_jcot_unique





def create_app():

    app = Flask(__name__)
    app.secret_key = '762ebc2bbcee5fd6ecabea179063bb30'
    app.register_blueprint(cadastros)
    app.register_blueprint(extracoes)
    app.register_blueprint(geracao_informes_bp)

    @app.route('/')
    def hello_world():  # put application's code here
        return 'Hello World!'

    app.config['DEBUG'] = True

    app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')

    return app




