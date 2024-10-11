import time

from InformesLegais.celery import celery_app
from InformesLegais.db import db 
from JCOTSERVICE import RelPosicaoFundoCotistaService
from flask import Flask
import os
from dotenv import load_dotenv
from GERACAO_5401.extracao_5401_quantidades_o2 import o2Api
import pandas as pd




load_dotenv()

@celery_app.task(name="Extrair Posições Jcot")
def extrair_posicao_jcot_unique(fundo):
    '''função para realizar extração de posições do jcot '''
    service = RelPosicaoFundoCotistaService("roboescritura", "Senh@123")
    posicao = service.get_posicoes_json(fundo)

    try:
        app = Flask(__name__)
        app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')
        with app.app_context():
            trava = db.posicoesjcot.find_one({"fundo": fundo['codigo'] , "data": fundo['dataPosicao']})
            if not trava:
                db.posicoesjcot.insert_many(posicao)
    
    except Exception as e:
        print (e)
        pass
    
    return {"message": f"Extração do fundo {fundo['codigo']} Concluída"}


@celery_app.task(name="Extrair Posições Jcot Geral")
def extrair_posicao_jcot():
    '''cria as mensagens para extrair em lote do jcot'''
    try:
        app = Flask(__name__)
        app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')



        with app.app_context():
            fundos = db.fundos.find({})

            db.posicoesjcot.delete_many({})

            for fundo in fundos:

                fundo['_id'] = str(fundo['_id'])
                fundo['dataPosicao'] = "2024-06-28"
                fundo['fundo'] = fundo['codigo']
                extrair_posicao_jcot_unique.delay(fundo)

    except Exception as e:
        print(e)
        pass

    return {"message": f"Fundos enviados para extração jcot Concluída"}




def get_valor_de_cota_jcot(ativo_o2):
    try:          
        valor_de_cota = db.posicoesjcot.find_one({"fundo": str(ativo_o2['cd_jcot']) , "data": str(ativo_o2['data'])  })
        return valor_de_cota['valor_cota']
    except Exception as e:
        print (e)        
        return '0'


@celery_app.task
def extrair_posicao_o2_geral():
    api = o2Api("thiago.conceicao", "DBCE0923-9CE3-4597-9E9A-9EAE7479D897")

    headers = {
        'Authorization': f'Bearer {api.get_token()}',
        'Content-Type': 'application/json'
    }

    app = Flask(__name__)
    app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')

    with app.app_context():

        db.posicoeso2.delete_many({})

        fundos = db.ativoso2.find({"cd_jcot": {"$ne": "Sem Código"}})

        for item in fundos:
            item['_id'] = str(item['_id'])
            item['data'] = '2024-06-30'
            extrair_posicao_o2.delay(item, headers)

    return f"Ativos o2 enviados para extração"




@celery_app.task(name="Extrair Posições O2")
def extrair_posicao_o2(ativo_o2 , header):
    api = o2Api("thiago.conceicao", "DBCE0923-9CE3-4597-9E9A-9EAE7479D897")

    app = Flask(__name__)
    app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')
    with app.app_context():

        valor_de_cota = get_valor_de_cota_jcot(ativo_o2)
        df_posicao = api.get_posicao( ativo_o2['data'] , ativo_o2['descricao'] ,  ativo_o2['cd_jcot'] , header , valor_de_cota , ativo_o2)

        if not df_posicao.empty:
            db.posicoeso2.insert_many(df_posicao.to_dict('records'))


    print (f"Extração do fundo {ativo_o2['cd_escritural']} concluída")
    return f"Extração da posição do ativo {ativo_o2['descricao']} Concluída"


