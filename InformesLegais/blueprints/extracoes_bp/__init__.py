from flask import Blueprint , request , jsonify , render_template
from InformesLegais.db import db
from GERACAO_5401 import Extracao_Quantidades_O2 , client
from datetime import datetime
from InformesLegais.Services import ServiceInvestidores , ServiceExtracaoJcotO2


extracoes = Blueprint('extracoes', __name__ , url_prefix='/extracoes')


@extracoes.route("/extrair_jcot")
def extrair_posicoes_jcot():    


    data_request = request.args.get("data")

    data_datetime = datetime.strptime(data_request , "%Y-%m-%d")
    ServiceExtracaoJcotO2().ExtracaoJcot(data_datetime)

    
    return jsonify({"message": "Fundos Jcot enviados para extração"})



@extracoes.route("/extrair_posicao_o2")
def extrair_posicoes_o2():
    '''extracao de posição'''

    data_request = request.args.get("data")

    data_datetime = datetime.strptime(data_request, "%Y-%m-%d")

    ServiceExtracaoJcotO2().Extracaoo2(data_datetime)

    return jsonify({"messsage": "Posições o2 enviadas para extração com sucesso"})



@extracoes.route("/atualizar_ativos_intactus")
def atualizar_ativos_02():
    '''atualizar a base de ativos do o2 '''
    #todo atualizar essa tarefa , para buscar os dados do o2 , com as suas respectivas correlações corretas
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