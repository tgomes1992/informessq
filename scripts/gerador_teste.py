from functools import partial
from InformesLegais.celery import celery_app 
from InformesLegais.db import db 
from flask import Flask
import os
from GERACAO_5401.Fundo5401 import Fundo5401
from GERACAO_5401.Fundo5401_base import Fundo5401 as Fundo5401_base
from GERACAO_5401.Documento5401 import Documento5401
from GERACAO_5401.xml_5401 import XML_5401
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import pandas as pd
from InformesLegais.Services.TaskService import TaskService



def job_criar_fundos(cnpj, documento_5401):
    gerador_fundo_5401 = Fundo5401(cnpj)
    fundo = gerador_fundo_5401.transforma_posicao_posicao_informe()
    if fundo is not None:
        documento_5401.adicionar_fundos(fundo)
    else:
        pass


def job_criar_fundos_json(cnpj):
    gerador_fundo_5401 = Fundo5401_base(cnpj)
    fundo = gerador_fundo_5401.transforma_posicao_posicao_informe()
    if fundo is not None:
        gerador_fundo_5401.client['informes_legais']['5401'].insert_one(fundo)
        # documento_5401.adicionar_fundos(fundo)
    else:
        pass





@celery_app.task(name="GERAR 5401")
def gerar_5401_por_adm(adm ):
    '''adm precisa ser uma string com o cnpj com 14 digitos do adm'''
    app = Flask(__name__)
    app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')
    with app.app_context():

        fundos_por_adm = db.fundos.find({"administrador": adm})

        df = pd.DataFrame.from_dict(fundos_por_adm)
        cnpjs = list(df['cnpj'].drop_duplicates())

        documento_5401 = Documento5401()
        criacao_fundos = partial(job_criar_fundos, documento_5401=documento_5401)

        try:
            print ("geração iniciada !!!")
            print (adm)

            with ThreadPoolExecutor() as executor:
                executor.map(criacao_fundos, cnpjs)
            documento = documento_5401.retornar_arquivo_5401_completo()
            ajustador = XML_5401(documento)
            ajustador.ajustar_arquivo_5401()
            ajustador.reescrever_xml(f"{adm}.xml")
            # print (id)
            # TaskService().finish_task(id)

        except Exception as e:
            print(e)





@celery_app.task(name="GERAR 5401 JSON")
def gerar_5401_por_adm_json(adm , id):
    '''adm precisa ser uma string com o cnpj com 14 digitos do adm'''
    app = Flask(__name__)
    app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')
    with app.app_context():

        fundos_por_adm = db.fundos.find({"administrador": adm})

        df = pd.DataFrame.from_dict(fundos_por_adm)
        cnpjs = list(df['cnpj'].drop_duplicates())

        documento_5401 = Documento5401()
        criacao_fundos = partial(job_criar_fundos_json)

        try:
            print ("geração iniciada !!!")
            print (adm)

            with ThreadPoolExecutor() as executor:
                executor.map(criacao_fundos, cnpjs)

            print (id)
            TaskService().finish_task(id)

        except Exception as e:
            print(e)

