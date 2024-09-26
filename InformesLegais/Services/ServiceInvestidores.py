import os
import pandas as pd
from .ServiceBase import  ServiceBase
from InformesLegais.tasks import get_dados_investidor


class ServiceInvestidores(ServiceBase):

    def get_investidores_a_buscar(self):

        with self.app.app_context():

            investidores_o2 = self.db.investidoreso2.find({})

            df_investidores_o2 = pd.DataFrame.from_dict(investidores_o2)

            lista_cpfs_cnpjs = [str(item) for item in df_investidores_o2]

            investidores =  self.db.investidoreso2.aggregate([
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
            self.api.get_dados_investidores(investidores_a_buscar)

    def buscar_investidores_5401(self):

        '''buscar os investidores com posição para o 5401'''

        with self.app.app_context():
            investidores_jcot = self.db.posicoesjcot.aggregate([
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
            investidores_o2 = self.db.posicoeso2.aggregate([
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
            self.db.investidores5401.insert_many(investidores.to_dict("records"))


    def ExtracaoDadosInvestidores(self):
        with self.app.app_context():
            self.consolidar_investidores()

            investidores = self.db.investidores5401.aggregate([
                {
                    '$group': {
                        '_id': '$cpfcnpj'
                    }
                }
            ])

            header = {
                'Authorization': f'Bearer {self.api.get_token()}',
                'Content-Type': 'application/json'
            }

            df = pd.DataFrame.from_dict(investidores)

            investidores_o2 = self.db.investidoreso2.aggregate([
                {
                    '$group': {
                        '_id': '$cpfcnpj'
                    }
                }
            ])
            df_o2 = pd.DataFrame.from_dict(investidores_o2)

            investidores_o2_ajustado = [item['_id'] for item in df_o2.to_dict("records")]

            filtro = ~df['_id'].isin(investidores_o2_ajustado)
            a_buscar = df[filtro]

            for item in a_buscar.to_dict(orient='records')[0:20000]:
                get_dados_investidor.delay(item['_id'], header)
