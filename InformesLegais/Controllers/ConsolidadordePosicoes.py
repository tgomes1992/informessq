from dotenv import load_dotenv
from flask import Flask
import pandas as pd
from InformesLegais.db import db
import os

load_dotenv()


class ControllerConsolidaPosicoes:

    '''classe responsável por consolidar as posições bolsa e cetip do jcot
     em uma única collection'''

    def __init__(self):
        self.app = Flask(__name__)
        self.app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')

    def get_posicoesjcot(self):
        with self.app.app_context():
            jcot = db.posicoesjcot.find({})
            for item in jcot:
                if '9358105000191' in item['cpfcnpjCotista'] or '9346601000125' in item['cpfcnpjCotista']:
                    self.processar_posicoes_o2(item)
                else:
                    db.posicaoconsolidada.insert_one(item)


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
                    db.posicaoconsolidada.insert_one(ndict)
        except Exception as e:
            print(e)


    def get_posicoes_o2(self , item):

        with self.app.app_context():
            try:
                if item['cpfcnpjCotista'] == '9346601000125':
                    posicoeso2 = db.posicoeso2.find({'cd_jcot': item['fundo'],  "depositaria":"BOLSA"})
                    return pd.DataFrame(posicoeso2)
                elif item['cpfcnpjCotista'] == '9358105000191':
                    posicoeso2 = db.posicoeso2.find({'cd_jcot': item['fundo'], "depositaria": "CETIP"})
                    return pd.DataFrame(posicoeso2)
                else:
                    return pd.DataFrame(item)
            except Exception as e:
                print (e)

