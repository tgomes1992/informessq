from dotenv import load_dotenv
from flask import Flask
import pandas as pd
from InformesLegais.db import db
import os
from concurrent.futures import ThreadPoolExecutor
from pymongo import MongoClient
from . import ServiceInvestidores
from functools import partial
from decimal import Decimal

load_dotenv()


class ControllerConsolidaPosicoes:

    '''classe responsável por consolidar as posições bolsa e cetip do jcot
     em uma única collection'''

    def __init__(self):
        self.app = Flask(__name__)
        self.client = MongoClient("mongodb://localhost:27017",
             connectTimeoutMS=30000,  # Connection timeout in milliseconds (30 seconds)
             socketTimeoutMS=60000,   # Socket timeout in milliseconds (60 seconds)
             serverSelectionTimeoutMS=40000   )
        self.db = self.client['informes_legais']
        self.app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')
        self.service_investidores = ServiceInvestidores()


    def job_processar_posicao_jcot(self  , posicao_jcot):
        data = "2024-09-30"
        self.processar_posicoes_o2(posicao_jcot ,data)




    def get_posicoesjcot(self , data):
        with self.app.app_context():
            fundos = db.fundos.find({"codigo":"10961_SUB01"})

            codigos = [fundo['codigo'] for fundo in fundos]

            jcot = db.posicoesjcot.find({"data": data ,  'fundo': "10961_SUB01"})
            df = pd.DataFrame.from_dict(jcot)


            df_cetip_bolsa = df[df['cpfcnpjCotista'].isin(['9358105000191' , '9346601000125' ])]
            df_escritural = df[~df['cpfcnpjCotista'].isin(['9358105000191' , '9346601000125' ])]
            db.posicaoconsolidada.delete_many({"data": data})
            df_escritural['tipoCotista'] = df['cpfcnpjCotista'].apply(self.service_investidores.get_tipo_cotista_5401)
            self.db['posicaoconsolidada'].insert_many(df_escritural.to_dict('records'))
            funcao_p = partial(self.job_processar_posicao_jcot , data = data)
            with ThreadPoolExecutor() as executor:
                executor.map(self.job_processar_posicao_jcot  , df_cetip_bolsa.to_dict('records'))



    def processar_posicoes_o2(self , posicao_jcot, data):
        posicoes_o2 = self.get_posicoes_o2(posicao_jcot, data)

        try:
            for item in posicoes_o2.to_dict('records'):
                print (item)
                ndict = {
                    "cd_cotista": str(item['cpfcnpjInvestidor']) ,
                    "nmCotista": item['nomeInvestidor'],
                    "cpfcnpjCotista": str(item['cpfcnpjInvestidor']),
                    "totalCotista": "",
                    "qtCotas": str(Decimal(str(item['quantidadeTotalDepositada'])) ),
                    "vlAplicacao": str(round(item['quantidadeTotalDepositada'] * float(posicao_jcot['valor_cota']) , 2)),
                    "vlCorrigido":  str(round(item['quantidadeTotalDepositada'] * float(posicao_jcot['valor_cota']) , 2)),
                    "vlIof": "0",
                    "vlIr": "0",
                    "vlResgate": str(round(item['quantidadeTotalDepositada'] * float(posicao_jcot['valor_cota']) , 2)),
                    "vlRendimento": 0,
                    "fundo": posicao_jcot['fundo'],
                    "data": posicao_jcot['data'],
                    "valor_cota": posicao_jcot['valor_cota'],
                    "tipoCotista": self.service_investidores.get_tipo_cotista_5401(str(item['cpfcnpjInvestidor']))
                }

                with self.app.app_context():
                    self.db['posicaoconsolidada'].insert_one(ndict)
        except Exception as e:
            print(e)


    def get_posicoes_o2(self , item, data):
        try:
            if item['cpfcnpjCotista'] == '9346601000125':
                posicoeso2 = self.db['posicoeso2'].find({'cd_jcot': item['fundo'],  "depositaria":"BOLSA" , "data": data})
                return pd.DataFrame(posicoeso2)
            elif item['cpfcnpjCotista'] == '9358105000191':
                posicoeso2 = self.db['posicoeso2'].find({'cd_jcot': item['fundo'], "depositaria": "CETIP" ,  "data": data})
                return pd.DataFrame(posicoeso2)
            else:
                return pd.DataFrame(item)
        except Exception as e:
            print (e)

