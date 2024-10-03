from pymongo import MongoClient
import pandas as pd
import xml.etree.ElementTree as ET
from xml.dom import minidom
from .CotasTipo import CotasTipo
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from ValidadorCpfCnpj import ValidadorCpf , ValidadorCnpj
from pymongo import MongoClient
import polars as pl


class Fundo5401():


    def __init__(self , CNPJ_EMISSOR):
        self.CNPJ_EMISSOR = CNPJ_EMISSOR
        self.client = MongoClient('localhost', 27017)
        self.lista_cotistas = []


    def consultar_fundos_5401(self):
        fundos = self.client['informes_legais']['fundos'].find({"cnpj": self.CNPJ_EMISSOR})
        base = [fundo for fundo in fundos]
        return base

    def atribuir_tipo_cota(self , df , df_cotas):

        n_df_2 =  df.with_columns(
                pl.col("fundo").map_elements(lambda x : df_cotas[df_cotas['codigo']== x]['tipo'].values[0]).alias("cotatipo")
            )



        return n_df_2

    def get_tipo_cota(self , cota):
        consulta_tipos = MongoClient('localhost', 27017)

        base = consulta_tipos['informes_legais']['fundos'].find({})

        df_fundos = pd.DataFrame(list(base))
        try:
            return df_fundos[df_fundos['codigo'] == cota]['tipo'].values[0]
        except:
            return "5"

    def consultar_posicoes_jcot(self):
        fundos = self.consultar_fundos_5401()
        df_fundos = pd.DataFrame.from_dict(fundos)

        lista_codigos = [item['codigo'] for item in df_fundos.to_dict('records')]

        aggregate = [
            {
                '$match': {
                    'fundo': {
                        '$in': lista_codigos
                    }
                }
            }
        ]

        try:
            posicoes_jcot = self.client['informes_legais']['posicaoconsolidada'].aggregate(aggregate)

            # df = pd.DataFrame(posicoes_jcot)

            df = pl.DataFrame(posicoes_jcot)

            n_df = df.with_columns([
                pl.col("cd_cotista").map_elements(lambda x: x.strip()).alias("cd_cotista"),
                pl.col("cpfcnpjCotista").map_elements(lambda x: x.strip()).alias("cpfcnpjCotista"),
                pl.col("vlCorrigido").map_elements(float).alias("vlCorrigido"),
                pl.col("qtCotas").map_elements(float).alias("qtCotas"),
            ])


            print ("df gerado")

            cotas_jcot = CotasTipo(self.CNPJ_EMISSOR)
            cotas_df = cotas_jcot.buscar_cotas(df_fundos)


            print("df formatado")

            return self.atribuir_tipo_cota(n_df, cotas_df)
        except Exception as e :
            print (e)
            return pd.DataFrame()

    def formatar_cotista(self, cotista): 

        if ValidadorCpf(str(cotista)).validarCpf():
            return str(cotista).zfill(11)
        elif ValidadorCnpj(str(cotista)).validarCnpj():
            return str(cotista).zfill(14)
        else:
            return str(cotista)


    def get_tipo_cotista_5401(self , investidor):
        db_investidor = self.client['informes_legais']['investidoreso2']
        investidor_cadastrado = db_investidor.find_one({'cpfcnpj': int(investidor)})
        try:
            tipos_de_cotista_O2 = {'NÃO CLASSIFICADO': 0, 'PESSOA FISICA NÃO RESIDENTE': 3, 'PESSOA FISICA PARAISO FISCAL': 3,
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
            if len(investidor) == 11:
                return "1"
            elif len(investidor)== 14:
                return "2"
            else:
                return "5"



    def criar_cotistas_unico(self, cotista):
    #todo criar função para validar o tipo de cotista e a sua respectiva classificação
        
        #todo lógica de validação do tipo de cotista

        dados = {
            'tipoPessoa':  cotista['tipoCotista'] ,
            "identificacao":  self.formatar_cotista(cotista["cpfcnpjCotista"]),
            "cotas":  []
            }

        try:
            dados_formatado = self.formatar_cotista(cotista["cpfcnpjCotista"])


            if cotista['cpfcnpjCotista'] == '09358105000191':
                dados['classificacao'] = str(3)

            else:
                dados['classificacao'] = str(1)


            if cotista['cpfcnpjCotista'] == '02332886000104':
                dados['classificacao'] = str(2)

            else:
                dados['classificacao'] = str(1)

            return dados
        except Exception as e:
            print("erro cotistas", e)


    def criar_cotistas(self):
        cotistas = ET.Element('cotistas')
        return cotistas

    def montar_cotistas(self):
        cotistas = ET.Element('cotistas')
        return cotistas

    def criar_fundo(self, cnpj_fundo, quantidade_cotas, quantidade_cotistas, plFundo):

        return {
            "cnpjFundo" :  cnpj_fundo ,
            "quantidadeCotas": quantidade_cotas,
            "quantidadeCotistas": quantidade_cotistas ,
            "plFundo":  plFundo ,
            "cotistas":  []
        }

    def criar_cotas(self , lista_cotas):
        # todo criar função para pegar a lógica das cotas

        cotas = ET.Element('cotas')
        for cota in lista_cotas:
            ncota = ET.SubElement(cotas ,  'cota')
            ##todo depara de cada uma das classes do jcot

            ncota.set("tipoCota" , str(cota['cotatipo']))
            ncota.set("qtdeCotas" ,  str(round(cota['qtdeCotas'],2)))
            ncota.set("valorCota" , str(cota['valorCota']))

        return cotas


    def consultar_cotista_cetip(self, cnpj_emissor):
        posicoes = self.client['informes_legais']['posicoeso2'].find({"cnpjFundo": int(cnpj_emissor) , 'depositaria':{"$in": ['CETIP' , 'BOLSA']}})
        df_fundo = pd.DataFrame.from_dict(posicoes)
 

        return df_fundo


    def job_criar_cotista_unico(self , cotista , n_posicao ,  cotistas ):

        try:

            cotas_df = n_posicao.filter(pl.col('cpfcnpjCotista') ==  cotista['cpfcnpjCotista'])


            cotas_df_pre_ajustado = cotas_df.group_by(["fundo", "valor_cota", "cotatipo"]).agg(
                [
                    pl.col("vlCorrigido").sum().alias("vlCorrigido"),
                    pl.col("qtCotas").sum().alias("qtCotas")
                ]
            )
            # Rename columns to match the desired column names
            cotas_df_pre_ajustado = cotas_df_pre_ajustado.rename({
                "fundo": "tipo",
                "valor_cota": "valorCota",
                "cotatipo": "tipoCota",
                "vlCorrigido": "vlCorrigido",
                "qtCotas": "qtdeCotas"
            })

            # cotas_xml_elemento = self.criar_cotas(cotas_df_pre_ajustado.to_dict("records"))


            elemento_cotista = self.criar_cotistas_unico(cotista)

            cotas_records = cotas_df_pre_ajustado.select(["tipo", "tipoCota", "valorCota", "qtdeCotas"]).to_dicts()
            # elemento_cotista["cotas"] = cotas_df_pre_ajustado[['tipo' , 'tipoCota' , 'valorCota' ,  'qtdeCotas']].to_dict('records')
            elemento_cotista["cotas"] = cotas_records

            cotistas.append(elemento_cotista)

            print(elemento_cotista)

        except Exception as e:
            print (e ,  cotista)




    def transforma_posicao_posicao_informe(self):
        '''usa o df do fundo para transforma-lo no xml do 5401'''
        df_posicao = self.consultar_posicoes_jcot()[[ 'cd_cotista' , 'cpfcnpjCotista', 'fundo', 'valor_cota' , 'cotatipo' , 'vlCorrigido' , 'qtCotas' , "tipoCotista"]]


        n_posicao = df_posicao

        if not n_posicao.is_empty():

            print ("start_cotistas")

            total_cotistas = n_posicao.filter(~n_posicao.is_duplicated())

            xml_fundos = self.criar_fundo(self.CNPJ_EMISSOR , str(round(n_posicao.select(pl.col("qtCotas").sum()).item() ,2)) , len(total_cotistas) , str(round(n_posicao.select(pl.col("vlCorrigido").sum()).item() ,2)) )


            cotistas = n_posicao.select(["cpfcnpjCotista" , "tipoCotista"]).unique()

            lista_cotistas = [{"cpfcnpjCotista": item[0]  , "tipoCotista": item[1]} for item in cotistas.iter_rows()]

            job_cotista = partial(self.job_criar_cotista_unico ,  n_posicao=n_posicao , cotistas=xml_fundos['cotistas'])


            with ThreadPoolExecutor() as executor:
                executor.map( job_cotista , lista_cotistas)

            return xml_fundos

