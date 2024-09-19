from InformesLegais.celery import celery_app
from InformesLegais.db import db
from InformesLegais.Services.ConsolidadordePosicoes import ControllerConsolidaPosicoes
from flask import Flask
import os
from dotenv import load_dotenv
from GERACAO_5401.extracao_5401_quantidades_o2 import o2Api
import pandas as pd



@celery_app.task(name="Consolidar Posições")
def task_consolidar_posicoes(data):
    app = Flask(__name__)

    app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')

    with app.app_context():
        consolidador = ControllerConsolidaPosicoes()
        consolidador.get_posicoesjcot(data)


    return "Posições Consolidadas com sucesso!"