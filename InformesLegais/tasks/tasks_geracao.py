from functools import partial
from InformesLegais.celery import celery_app 
from InformesLegais.db import db 
from flask import Flask
import os
from GERACAO_5401.Fundo5401 import Fundo5401
from GERACAO_5401.Fundo5401_175 import Fundo5401_175
from GERACAO_5401.Fundo5401_base import Fundo5401 as Fundo5401_base
from GERACAO_5401.Documento5401 import Documento5401
from GERACAO_5401.xml_5401 import XML_5401
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import pandas as pd
from InformesLegais.Services.TaskService import TaskService



def job_criar_fundos(cnpj, documento_5401 , data):
    gerador_fundo_5401 = Fundo5401(cnpj , data)
    fundo = gerador_fundo_5401.transforma_posicao_posicao_informe()
    if fundo is not None:
        documento_5401.adicionar_fundos(fundo)
    else:
        pass
    
    
    

def job_criar_fundos_175(cnpj, documento_5401 , data):
    gerador_fundo_5401 = Fundo5401_175(cnpj , data)
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
def gerar_5401_por_adm(adm ,  data, id ,  tipo):
    '''adm precisa ser uma string com o cnpj com 14 digitos do adm'''
    app = Flask(__name__)
    app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')
    with app.app_context():

        print (data)


        fundos_por_adm = db.fundos.find({"administrador": adm})

        # db.posicaoconsolidada.delete_many({})

        df = pd.DataFrame.from_dict(fundos_por_adm)
        cnpjs = list(df['cnpj'].drop_duplicates())

        documento_5401 = Documento5401(adm , data , tipo)
        criacao_fundos = partial(job_criar_fundos, documento_5401=documento_5401 ,  data=data)

        try:
            print ("geração iniciada !!!")
            print (adm)

            with ThreadPoolExecutor() as executor:
                executor.map(criacao_fundos, cnpjs)
            documento = documento_5401.retornar_arquivo_5401_completo()
            ajustador = XML_5401(documento)
            ajustador.ajustar_arquivo_5401()
            ajustador.reescrever_xml(f"{adm}_{data.replace('-' , '')}.xml")
            print (id)
            TaskService().finish_task(id)

        except Exception as e:
            print(e)



@celery_app.task(name="GERAR 5401 175")
def gerar_5401_por_adm_175(adm ,  data, id ,tipo ):
    '''adm precisa ser uma string com o cnpj com 14 digitos do adm'''
    app = Flask(__name__)
    app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')
    print ("task")
    
    with app.app_context():

        fundos_por_adm = db.fundos.find({"administrador": adm})

        # db.posicaoconsolidada.delete_many({})

        try:
            
            
            df = pd.DataFrame.from_dict(fundos_por_adm)
            cnpjs = list(df['cnpj'].drop_duplicates())

            documento_5401 = Documento5401(adm , data , tipo )
            criacao_fundos = partial(job_criar_fundos_175, documento_5401=documento_5401 ,  data=data)

            print ("geração iniciada !!!")
            print (adm)

            with ThreadPoolExecutor() as executor:
                executor.map(criacao_fundos, cnpjs)
            documento = documento_5401.retornar_arquivo_5401_completo()
            ajustador = XML_5401(documento)
            ajustador.ajustar_arquivo_5401()
            ajustador.reescrever_xml(f"{adm}_{data.replace('-' , '')}_175.xml")
            print (id)
            TaskService().finish_task(id)

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

