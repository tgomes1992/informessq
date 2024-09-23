from InformesLegais.celery import celery_app 
from InformesLegais.db import db 
from flask import Flask
import os
from dotenv import load_dotenv
from GERACAO_5401.extracao_5401_quantidades_o2 import o2Api
import pandas as pd





@celery_app.task(name="Buscar dados de Investidor")
def get_dados_investidor(cpfcnpj , header ):
    api = o2Api("thiago.conceicao", "DBCE0923-9CE3-4597-9E9A-9EAE7479D897")

    app = Flask(__name__)
    app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')

    with app.app_context():
        dados = api.get_dados_investidor_unique(cpfcnpj, header)
        db.investidoreso2.insert_one(dados)

    return f"Extração de Investidores Concluída com sucesso"









@celery_app.task(name="Atualizar Investidores")
def atualizar_investidores_o2():
    api = o2Api("thiago.conceicao", "DBCE0923-9CE3-4597-9E9A-9EAE7479D897")

    app = Flask(__name__)

    app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')

    with app.app_context():
        investidores = db.investidores5401.aggregate([{"$group": {'_id': '$identificacao'}}])

        df = pd.DataFrame(investidores)

        lista = list(df['_id'])

        api.get_dados_investidores_multiple(lista, db)

    return f"Extração de Investidores Concluída com sucesso"

