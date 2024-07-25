from flask import Blueprint , request , jsonify , render_template
from JCOTSERVICE import ListFundosService , RelPosicaoFundoCotistaService
from bson.objectid import ObjectId
from InformesLegais.db import db
from InformesLegais.tasks import extrair_posicao_jcot_unique
import pandas as pd

extracoes = Blueprint('extracoes', __name__ , url_prefix='/extracoes')


@extracoes.route("/extrair_jcot")
def extrair_posicoes_jcot():
    
    fundos = db.fundos.find({})
    
    # for fundo in fundos:
    fundo = {}
    # fundo['_id'] = str(fundo['_id'])
    fundo['dataPosicao'] = "2024-06-28"
    # fundo['fundo'] = fundo['codigo']
    fundo['codigo'] = '1944'
    extrair_posicao_jcot_unique.delay(fundo)
 
    
    return jsonify({"message": "Fundo enviados para extracao"})