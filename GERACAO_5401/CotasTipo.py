
'''classe responsável por definir a tipagem de cotas entre os fundos'''

from pymongo import MongoClient
import pandas as pd



class CotasTipo():

    ''' Classe que realiza a criação e definição dos tipos de cota para cada fundo
        deve receber apenas as classes de cota que possuem posição ,
         para classificar os tipos de acordo com o proposto no 54011
    '''

    def __init__(self ,  CNPJ_EMISSOR):
        self.client = MongoClient('localhost', 27017)
        self.cnpj_emissor = CNPJ_EMISSOR



    def atribuir_tipos_de_cota(self, df):
        '''função que define os tipos de cada uma das cotas'''
        tipos_de_cota = []
        dataframe_ajustado = []
        cotas_trocaveis = ['2' , '3' , '4' , '5']

        for item in df.to_dict("records"):
            if item['tipo'] not in tipos_de_cota:
                tipos_de_cota.append(item['tipo'])


        for item in tipos_de_cota:
            if item in cotas_trocaveis:
                df_cota = df[df['tipo'] == item]
                contagem = 1


                for item in df_cota.to_dict("records"):
                    if len(df_cota.to_dict("records")) == 1:
                        item['tipo'] = f"{item['tipo']}"
                        dataframe_ajustado.append(item)
                    else:
                        item['tipo'] = f"{item['tipo']}.{str(contagem).zfill(2)}"
                        dataframe_ajustado.append(item)
                        contagem += 1
            else:

                for cota in df.to_dict("records"):
                    dataframe_ajustado.append(cota)


        return pd.DataFrame.from_dict(dataframe_ajustado)




    def buscar_cotas(self):
        cotas = self.client['informes_legais']['fundos'].find({"cnpj": self.cnpj_emissor})
        df = pd.DataFrame(cotas)
        novo_df = self.atribuir_tipos_de_cota(df)

        return novo_df[['codigo' , 'tipo']]


