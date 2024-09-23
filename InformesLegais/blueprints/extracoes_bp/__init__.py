from flask import Blueprint , request , jsonify , render_template
from InformesLegais.db import db
from GERACAO_5401 import Extracao_Quantidades_O2 , client
from datetime import datetime
from InformesLegais.Services import ServiceInvestidores , ServiceExtracaoJcotO2


extracoes = Blueprint('extracoes', __name__ , url_prefix='/extracoes')


@extracoes.route("/extrair_jcot")
def extrair_posicoes_jcot():    


    data = datetime.today()
    ServiceExtracaoJcotO2().ExtracaoJcot(data)

    
    return jsonify({"message": "Fundo enviados para extracao"})



@extracoes.route("/extrair_posicao_o2")
def extrair_posicoes_o2():
    '''extracao de posição'''

    data = datetime.today()
    ServiceExtracaoJcotO2().Extracaoo2(data)

    return jsonify({"messsage": "Posiçoes Extraídas"})



@extracoes.route("/atualizar_ativos_intactus")
def atualizar_ativos_02():
    '''atualizar a base de ativos do o2 '''
    db.ativoso2.delete_many({})
    extrator_intactus = Extracao_Quantidades_O2( client, datetime(2024,6,28) )
    fundos = extrator_intactus.get_lista_fundos()
    db.ativoso2.insert_many(fundos)

    return jsonify({"messsage": "Ativos Atualizados"})



@extracoes.route("/get_investidores_o2")
def get_investidores():
    '''extracao de investidores o2'''

    ServiceInvestidores().ExtracaoDadosInvestidores()


    return jsonify({"messsage": "Dados de Investidores Enviados para Extração"})