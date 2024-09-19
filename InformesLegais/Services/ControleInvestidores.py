from dotenv import load_dotenv
from flask import Flask
import pandas as pd
from InformesLegais.db import db
import os
from flask import Flask
from intactus.osapi import  o2Api



class ServiceInvestidores:

    '''service responsável por controlar toda a interação com os investidores'''

    def __init__(self):
        self.app = Flask(__name__)
        self.app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')
        self.api = o2Api("thiago.conceicao", "DBCE0923-9CE3-4597-9E9A-9EAE7479D897")


    def get_investidores_a_buscar(self):

        with self.app.app_context():

            investidores_o2 = db.investidoreso2.find({})

            df_investidores_o2 = pd.DataFrame.from_dict(investidores_o2)

            lista_cpfs_cnpjs = [str(item) for item in df_investidores_o2]

            investidores =  db.investidoreso2.aggregate([
            {
                '$group': {
                    '_id': '$cpfcnpj'
                }
            }
            ])
            investidores_a_buscar = [str(item['_id']) for item in investidores if item['_id'] not in lista_cpfs_cnpjs ]

            return investidores_a_buscar

    def get_dados_o2(self):
        investidores_a_buscar = self.get_investidores_a_buscar()

        with self.app.app_context():

            for

            self.api.get_dados_investidores(investidores_a_buscar)



    def buscar_investidores_5401(self):

        '''buscar os investidores com posição para o 5401'''

        with self.app.app_context():
            investidores_jcot = db.posicoesjcot.aggregate([
            {
                '$group': {
                    '_id': {
                        'cpfcnpj': '$cpfcnpjCotista',
                        'nome': '$nmCotista'
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'cpfcnpj': '$_id.cpfcnpj',
                    'nome': '$_id.nome'
                }
            }
        ])
            investidores_o2 = db.posicoeso2.aggregate([
            {
                '$match': {
                    'depositaria': {
                        '$in': [
                            'CETIP', 'BOLSA'
                        ]
                    }
                }
            }, {
                '$group': {
                    '_id': {
                        'cpfcnpj': '$cpfcnpjInvestidor',
                        'nome': '$nomeInvestidor'
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'cpfcnpj': '$_id.cpfcnpj',
                    'nome': '$_id.nome'
                }
            }
        ])
            df_jcot = pd.DataFrame.from_dict(investidores_jcot)
            df_o2 = pd.DataFrame.from_dict(investidores_o2)

            return pd.concat([df_jcot, df_o2])


    def consolidar_investidores(self):
        investidores = self.buscar_investidores_5401()

        with self.app.app_context():

            db.investidores5401.insert_many(investidores.to_dict("records"))

        #todo lógica para atualizar esses investidores com os dados do o2 , caso não esteja na tabela do o2



    def relatorio_de_investidores(self):
        pass

    def classificar_investidor_5401(self):
        pass