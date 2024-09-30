from InformesLegais.celery import celery_app
from InformesLegais.db import db
from InformesLegais.Services.ConsolidadordePosicoes import ControllerConsolidaPosicoes
from flask import Flask
import os
from dotenv import load_dotenv
from GERACAO_5401.extracao_5401_quantidades_o2 import o2Api
import pandas as pd
from InformesLegais.Services.TaskService import TaskService


@celery_app.task(name="Consolidar Posições")
def task_consolidar_posicoes(data ,  task_id):
    app = Flask(__name__)

    app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')

    service_task = TaskService()



    with app.app_context():
        consolidador = ControllerConsolidaPosicoes()
        consolidador.get_posicoesjcot(data)

    service_task.finish_task(task_id)

    return "Posições Consolidadas com sucesso!"