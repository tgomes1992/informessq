from flask import Blueprint , request , jsonify , render_template
from JCOTSERVICE import ListFundosService , RelPosicaoFundoCotistaService
from bson.objectid import ObjectId
from InformesLegais.db import db
from InformesLegais.tasks import extrair_posicao_jcot_unique
import pandas as pd
from GERACAO_5401 import Extracao_Quantidades_O2 , client
from GERACAO_5401.extracao_5401_quantidades_o2 import o2Api
from datetime import datetime
from ..tasks import extrair_posicao_o2 , atualizar_investidores_o2


extracoes = Blueprint('extracoes', __name__ , url_prefix='/extracoes')





@extracoes.route("/extrair_jcot")
def extrair_posicoes_jcot():    
    fundos = db.fundos.find({})
    for fundo in fundos:
        fundo['_id'] = str(fundo['_id'])
        fundo['dataPosicao'] = "2024-06-28"
        fundo['fundo'] = fundo['codigo']
        extrair_posicao_jcot_unique.delay(fundo)

    
    return jsonify({"message": "Fundo enviados para extracao"})


@extracoes.route("/atualizar_ativos_intactus")
def atualizar_ativos_02():
    '''atualizar a base de ativos do o2 '''
    db.ativoso2.delete_many({})
    extrator_intactus = Extracao_Quantidades_O2( client, datetime(2024,6,28) )
    fundos = extrator_intactus.get_lista_fundos()
    db.ativoso2.insert_many(fundos)

    return jsonify({"messsage": "Ativos Atualizados"})


@extracoes.route("/extrair_posicao")
def extrair_posicoes_o2():
    '''extracao de posição'''
    api = o2Api("thiago.conceicao", "DBCE0923-9CE3-4597-9E9A-9EAE7479D897")

    headers = {
        'Authorization': f'Bearer {api.get_token()}',
        'Content-Type': 'application/json'
    }
    #consulta de fundos que possuem código jcot

    db.posicoeso2.delete_many({})

    fundos  = db.ativoso2.find({"cd_jcot": { "$ne": "Sem Código" }})
    
    # fundos  = db.ativoso2.find({"cd_jcot": '10681'})
     
    #criando as tasks para baixar as posições do o2
    for item in fundos:
        item['_id'] = str(item['_id'])
        item['data'] = '2024-06-28'
        extrair_posicao_o2.delay(item , headers)
    
    return jsonify({"messsage": "Posiçoes Extraídas"})


@extracoes.route("/get_investidores_o2")
def get_investidores():
    '''extracao de investidores o2'''
    api = o2Api("thiago.conceicao", "DBCE0923-9CE3-4597-9E9A-9EAE7479D897")
    #consulta de fundos que possuem código jcot

    db.investidoreso2.delete_many({})
    
    atualizar_investidores_o2.delay()


    
    return jsonify({"messsage": "Posiçoes Extraídas"})