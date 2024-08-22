from InformesLegais.celery import celery_app 
from .db import db 
from JCOTSERVICE import RelPosicaoFundoCotistaService
from flask import Flask
import os
from dotenv import load_dotenv
from GERACAO_5401.extracao_5401_quantidades_o2 import o2Api
from GERACAO_5401.Fundo5401 import Fundo5401
from GERACAO_5401.Documento5401 import Documento5401
from GERACAO_5401.xml_5401 import XML_5401
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import pandas as pd

load_dotenv()

def job_criar_fundos(cnpj, documento_5401):
    gerador_fundo_5401 = Fundo5401(cnpj)
    fundo = gerador_fundo_5401.transforma_posicao_posicao_informe()

    if fundo is not None:

        documento_5401.adicionar_fundos(fundo)
    else:
        pass

@celery_app.task(name="Extrair Posições Jcot")
def extrair_posicao_jcot_unique(fundo):
    '''função para realizar extração de posições do jcot '''
    service = RelPosicaoFundoCotistaService("roboescritura", "Senh@123")
    posicao = service.get_posicoes_json(fundo)
    try:
        app = Flask(__name__)
        app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')
        print (app.config)
        with app.app_context():
            trava = db.posicoesjcot.find_one({"fundo": fundo['codigo'] , "data": fundo['dataPosicao']})
            if not trava:
                db.posicoesjcot.insert_many(posicao)
    
    except Exception as e:
        print (e)
        pass
    
    return {"message": f"Extração do fundo {fundo['codigo']} Concluída"}

def get_valor_de_cota_jcot(ativo_o2):
    try:          
        valor_de_cota = db.posicoesjcot.find_one({"fundo": str(ativo_o2['cd_jcot']) , "data": str(ativo_o2['data'])  })
        return valor_de_cota['valor_cota']
    except Exception as e:
        print (e)        
        return '0'

@celery_app.task(name="Extrair Posições O2")
def extrair_posicao_o2(ativo_o2 , header):
    api = o2Api("thiago.conceicao", "DBCE0923-9CE3-4597-9E9A-9EAE7479D897")


    app = Flask(__name__)
    app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')
    with app.app_context():

        print (ativo_o2)
        
        valor_de_cota = get_valor_de_cota_jcot(ativo_o2)
        
        df_posicao = api.get_posicao( ativo_o2['data'] , ativo_o2['descricao'] ,  ativo_o2['cd_jcot'] , header , valor_de_cota , ativo_o2)

        if not df_posicao.empty:
            db.posicoeso2.insert_many(df_posicao.to_dict('records'))

        # trava = db.posicoeso2.find_one({"fundo": fundo['codigo'], "data": fundo['dataPosicao']})

    print (f"Extração do fundo {ativo_o2['cd_escritural']} concluída")
    return {"message": f"Extração da posição do ativo {ativo_o2['descricao']} Concluída"}


@celery_app.task(name="Atualizar Investidores")
def atualizar_investidores_o2():
    api = o2Api("thiago.conceicao", "DBCE0923-9CE3-4597-9E9A-9EAE7479D897")


    app = Flask(__name__)
    app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')
    with app.app_context():
        investidores = db.posicoeso2.aggregate([{"$group": {'_id': '$cpfcnpjInvestidor'}}])

        df = pd.DataFrame(investidores)

        lista = list(df['_id'])

        api.get_dados_investidores_multiple(lista, db)


    print (f"Investidores Realizada com sucesso")
    return {"message": f"Extração de Investidores Concluída com sucesso"}



@celery_app.task(name="GERAR 5401")
def gerar_5401_por_adm(adm):
    '''adm precisa ser uma string com o cnpj com 14 digitos do adm'''
    app = Flask(__name__)
    app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')
    with app.app_context():

        # fundos_por_adm = db.fundos.find({"administrador": adm})
        #
        # df = pd.DataFrame.from_dict(fundos_por_adm)
        # cnpjs = list(df['cnpj'].drop_duplicates())
        documento_5401 = Documento5401()
        criacao_fundos = partial(job_criar_fundos, documento_5401=documento_5401)

        cnpjs = [adm]
        try:
            with ThreadPoolExecutor() as executor:
                executor.map(criacao_fundos, cnpjs)
            documento = documento_5401.retornar_arquivo_5401_completo()
            ajustador = XML_5401(documento)
            ajustador.ajustar_arquivo_5401()
            ajustador.reescrever_xml(f"{adm}.xml")

        except Exception as e:
            print(e)


