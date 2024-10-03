import os
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from .ServiceBase import  ServiceBase
from InformesLegais.tasks import get_dados_investidor
from ValidadorCpfCnpj import ValidadorCpf , ValidadorCnpj



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


    def get_tipo_cotista_5401(self , investidor):
        db_investidor = self.client['informes_legais']['investidoreso2']
        investidor_cadastrado = db_investidor.find_one({'cpfcnpj': int(investidor)})
        try:
            tipos_de_cotista_O2 = {'NÃO CLASSIFICADO': 1, 'PESSOA FISICA NÃO RESIDENTE': 3, 'PESSOA FISICA PARAISO FISCAL': 3,
                                   'INSTITUIÇÃO FINANCEIRA': 3, 'PESSOA JURIDICA SEM FINS LUCRATIVOS': 4,
                                   'PESSOA JURIDICA NÃO RESIDENTE': 4, 'PESSOA JURIDICA PARAISO FISCAL': 4,
                                   'FUNDO DE INVESTIMENTO': 2, 'FUNDO DE INVESTIMENTO IMOBILIÁRIO': 2,
                                   'PESSOA FISICA': 1, 'PESSOA JURIDICA': 2,
                                   'SOCIEDADE LTDA, ECON. MISTA, ANON., POR COTAS, ETC': 2,
                                   'BOLSA DE VALORES E MERCADORIAS': 12, 'CONDOMINIO': 13,
                                   'COOPERATIVA': 2, 'CONSORCIO': 15, 'INST. DE CARATER FILANTROPICO, RECREATIVO, CULT.': 2,
                                   'BANCO COMERCIAL/ MULTIPLO': 17, 'BANCO DE INVESTIMENTO': 18,
                                   'SOCIEDADE DE SEGURO, PREVIDENCIA E CAPITALIZAÇÃO': 19,
                                   'SOCIEDADE DTVM': 20, 'SOCIEDADE CTVM E CAMBIO': 2,
                                   'SOCIEDADE FINANCEIRA E DE CREDITO IMOBILIARIO': 22,
                                   'SOCIEDADE DE ARRENDAMENTO MERCANTIL (LEASING)': 23,
                                   'FUNDO DE PREVIDENCIA PRIVADA – ABERTO': 2,
                                   'FUNDO DE PREVIDENCIA PRIVADA – FECHADO': 2, 'CLUBE DE INVESTIMENTO': 26, 'TEMPLO DE QUALQUER CULTO': 2,
                                   'ENTIDADE SINDICAL DOS TRABALHADORES': 2, 'DEPOSITARIO DE ADR': 29,
                                   'FUNDO DE PLANO DE BENEF. DE SOCIEDADE SEGURADORA': 30,
                                   'UNIAO, EST., MUNIC. OU DIST. FED., AUTAR. OU FUND.': 2, 'PARTIDO POLITICO E SUAS FUNDACOES': 32}

            return str(tipos_de_cotista_O2[investidor_cadastrado['nomePerfilTributarioInvestidor']])
        except Exception as e :
            if ValidadorCpf(investidor).validarCpf():
                return "1"
            elif ValidadorCnpj(investidor).validarCnpj():
                return "2"
            else:
                return "5"

    def atualizar_cotistas(self, investidor):
        print(investidor)

        tipoCotista = self.get_tipo_cotista_5401(investidor['_id'])

        filter = {"cpfcnpjCotista": str(investidor['_id'])}

        update_operation = {"$set": {"tipoCotista": tipoCotista}}

        atualizar = self.client['informes_legais']['posicaoconsolidada'].find(filter)

        for item in atualizar:
            self.client['informes_legais']['posicaoconsolidada'].update_one(
                {'_id': item['_id']}, update_operation
            )




    def AtualizarTipodeCotistas(self):

        aggreagation = [
                    {
                        '$group': {
                            '_id': '$cpfcnpjCotista'
                        }
                    }
                        ]

        cotistas_a_atualizar = self.client['informes_legais']['posicaoconsolidada'].aggregate(aggreagation)

        cotistas = pd.DataFrame.from_dict(cotistas_a_atualizar)

        with ThreadPoolExecutor(max_workers=5) as executor:
            executor.map(self.atualizar_cotistas ,  cotistas.to_dict('records'))
