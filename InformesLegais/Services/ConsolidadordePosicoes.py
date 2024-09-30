from dotenv import load_dotenv
from flask import Flask
import pandas as pd
from InformesLegais.db import db
import os
from concurrent.futures import ThreadPoolExecutor
from pymongo import MongoClient

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


    def job_processar_posicao_jcot(self  , posicao_jcot):
        self.processar_posicoes_o2(posicao_jcot)




    def get_posicoesjcot(self , data):
        with self.app.app_context():
            fundos = db.fundos.find({})
            codigos = [fundo['codigo'] for fundo in fundos]
            jcot = db.posicoesjcot.find({"data": data})

            df = pd.DataFrame.from_dict(jcot)

            df_cetip_bolsa = df[df['cpfcnpjCotista'].isin(['9358105000191' , '9346601000125' ])]

            df_escritural = df[~df['cpfcnpjCotista'].isin(['9358105000191' , '9346601000125' ])]

            db.posicaoconsolidada.delete_many({})


            self.db['posicaoconsolidada'].insert_many(df_escritural.to_dict('records'))

            with ThreadPoolExecutor() as executor:
                executor.map(self.job_processar_posicao_jcot , df_cetip_bolsa.to_dict('records'))


            # for item in jcot:
            #     if '9358105000191' in item['cpfcnpjCotista'] or '9346601000125' in item['cpfcnpjCotista']:
            #         self.processar_posicoes_o2(item)
            #     else:
            #         db.posicaoconsolidada.insert_one(item)


    def processar_posicoes_o2(self , posicao_jcot):
        posicoes_o2 = self.get_posicoes_o2(posicao_jcot)


        try:
            for item in posicoes_o2.to_dict('records'):
                ndict = {
                    "cd_cotista": str(item['cpfcnpjInvestidor']) ,
                    "nmCotista": item['nomeInvestidor'],
                    "cpfcnpjCotista": str(item['cpfcnpjInvestidor']),
                    "totalCotista": "",
                    "qtCotas": str(item['quantidadeTotalDepositada']),
                    "vlAplicacao": str(round(item['quantidadeTotalDepositada'] * float(posicao_jcot['valor_cota']) , 2)),
                    "vlCorrigido":  str(round(item['quantidadeTotalDepositada'] * float(posicao_jcot['valor_cota']) , 2)),
                    "vlIof": "0",
                    "vlIr": "0",
                    "vlResgate": str(round(item['quantidadeTotalDepositada'] * float(posicao_jcot['valor_cota']) , 2)),
                    "vlRendimento": 0,
                    "fundo": posicao_jcot['fundo'],
                    "data": posicao_jcot['data'],
                    "valor_cota": posicao_jcot['valor_cota'],
                }

                with self.app.app_context():
                    self.db['posicaoconsolidada'].insert_one(ndict)
        except Exception as e:
            print(e)


    def get_posicoes_o2(self , item):
        try:
            if item['cpfcnpjCotista'] == '9346601000125':
                posicoeso2 = self.db['posicoeso2'].find({'cd_jcot': item['fundo'],  "depositaria":"BOLSA"})
                return pd.DataFrame(posicoeso2)
            elif item['cpfcnpjCotista'] == '9358105000191':
                posicoeso2 = self.db['posicoeso2'].find({'cd_jcot': item['fundo'], "depositaria": "CETIP"})
                return pd.DataFrame(posicoeso2)
            else:
                return pd.DataFrame(item)
        except Exception as e:
            print (e)

