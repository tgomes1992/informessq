from flask import Flask , render_template , jsonify
from .blueprints import *
import os
from celery import Celery
from .Services import TaskService



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

    @app.route("/task")
    def criar_tasks():
        id = TaskService().start_task("Teste")



        return jsonify({"message":id})

    @app.route("/finish_task/<string:id>")
    def finishtasks(id):
        id = TaskService().finish_task(id)

        id['_id'] = str(id['_id'])
        id['id'] = str(id['id'])

        return jsonify(id)


    @app.route("/home")
    def new_home():


        return render_template("new_home.html")


    app.config['DEBUG'] = True
    app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')

    return app




