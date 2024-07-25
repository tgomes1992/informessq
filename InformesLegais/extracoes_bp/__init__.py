from flask import Blueprint , request , jsonify , render_template
from JCOTSERVICE import ListFundosService , RelPosicaoFundoCotistaService
from bson.objectid import ObjectId
from InformesLegais.db import db
from InformesLegais.tasks import extrair_posicao_jcot_unique
import pandas as pd
from GERACAO_5401 import Extracao_Quantidades_O2 , client
from datetime import datetime
from ..tasks import extrair_posicao_o2


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
    fundos  = db.ativoso2.find({"cd_jcot": { "$ne": "Sem Código" }})
    for item in fundos:
        extrair_posicao_o2.delay(item)
    
    return jsonify({"messsage": "Posiçoes Extraídas"})
