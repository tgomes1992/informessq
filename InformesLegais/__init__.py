from flask import Flask , render_template
from .blueprints import *
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
    app.register_blueprint(conroles_bp)

    @app.route('/')
    def home():  # put application's code here
        return render_template("main_template.html")

    app.config['DEBUG'] = True

    app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')

    return app




