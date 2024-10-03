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

        n_df_2 = df.with_columns(
            pl.col("fundo").map_elements(lambda x: df_cotas[df_cotas['codigo'] == x]['tipo'].values[0]).alias(
                "cotatipo")
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

            df = pl.DataFrame(posicoes_jcot)

            n_df = df.with_columns([
                pl.col("cd_cotista").map_elements(lambda x: x.strip()).alias("cd_cotista"),
                pl.col("cpfcnpjCotista").map_elements(lambda x: x.strip()).alias("cpfcnpjCotista"),
                pl.col("vlCorrigido").cast(float).alias("vlCorrigido"),
                pl.col("qtCotas").cast(float).alias("qtCotas"),
            ])

            cotas_jcot = CotasTipo(self.CNPJ_EMISSOR)
            cotas_df = cotas_jcot.buscar_cotas(df_fundos)


            return self.atribuir_tipo_cota(n_df, cotas_df)
        except Exception as e :
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


        try:
            dados_formatado = self.formatar_cotista(cotista["cpfcnpjCotista"])
            cotista_elemento = ET.Element("cotista")

            cotista_elemento.set("tipoPessoa", cotista['tipoCotista'])

            cotista_elemento.set("identificacao", dados_formatado)

            if cotista['cpfcnpjCotista'] == '09358105000191':
                cotista_elemento.set('classificacao', str(3))
            else:
                cotista_elemento.set('classificacao', str(1))

            if cotista['cpfcnpjCotista'] == '02332886000104':
                cotista_elemento.set('classificacao', str(2))
            else:
                cotista_elemento.set('classificacao', str(1))

            return cotista_elemento
        except Exception as e:
            print("erro cotistas", e)


    def criar_cotistas(self):
        cotistas = ET.Element('cotistas')
        return cotistas

    def montar_cotistas(self):
        cotistas = ET.Element('cotistas')
        return cotistas

    def criar_fundo(self, cnpj_fundo, quantidade_cotas, quantidade_cotistas, plFundo):
        fundo = ET.Element("fundo")

        fundo.set("cnpjFundo", cnpj_fundo)
        fundo.set("quantidadeCotas", str(quantidade_cotas))
        fundo.set('quantidadeCotistas', str(quantidade_cotistas))
        fundo.set("plFundo",plFundo)

        return fundo

    def criar_cotas(self , lista_cotas):
        # todo criar função para pegar a lógica das cotas

        cotas = ET.Element('cotas')
        for cota in lista_cotas:
            ncota = ET.SubElement(cotas ,  'cota')

            ncota.set("tipoCota" , str(cota['cotatipo']))
            ncota.set("qtdeCotas" ,  str(round(cota['qtdeCotas'],2)))
            ncota.set("valorCota" , str(cota['valorCota']))

        return cotas


    def consultar_cotista_cetip(self, cnpj_emissor):
        posicoes = self.client['informes_legais']['posicoeso2'].find({"cnpjFundo": int(cnpj_emissor) , 'depositaria':{"$in": ['CETIP' , 'BOLSA']}})
        df_fundo = pd.DataFrame.from_dict(posicoes)
 

        return df_fundo

    def job_criar_cotista_cetip(self ,  cotista , df_com_tipo_cota , lista_de_cotas):
        try:
            cotas_df = df_com_tipo_cota[df_com_tipo_cota['cpfcnpjInvestidor'] == cotista['cpfcnpjInvestidor']]            
            cotas_df_pre_ajustado = cotas_df.groupby(['cd_jcot', 'cotatipo' ,'valor_cota'])[
                ['quantidadeTotalDepositada']].sum().reset_index()
            cotas_df_pre_ajustado.columns = ['tipo', 'cotatipo', 'valorCota' , 'qtdeCotas']
            cotas_df_pre_ajustado['vlCorrigido'] = str(cotas_df_pre_ajustado['qtdeCotas'].apply(float) * cotas_df_pre_ajustado['valorCota'].apply(float))
            elemento_cotista = self.criar_cotistas_unico(cotista)

            print (cotas_df_pre_ajustado)


            cotas_xml_elemento = self.criar_cotas(cotas_df_pre_ajustado.to_dict("records"))
            elemento_cotista.append(cotas_xml_elemento)
            lista_de_cotas.append(elemento_cotista)
        except Exception as e:
            print (e)

    def transformar_cotistas_cetip(self, df_cetip):
        '''função que vai gerar o xml dos cotistas cetipados'''
        cotistas = df_cetip.drop_duplicates('cpfcnpjInvestidor')

        df_cetip['fundo'] = df_cetip['cd_jcot']

        tipo_cota = CotasTipo(self.CNPJ_EMISSOR)

        df_tipo_cota = tipo_cota.buscar_cotas()

        df_com_tipo_cota = self.atribuir_tipo_cota(df_cetip ,df_tipo_cota)

        lista_de_cotistas_cetip  = []


        cotistas['cpfcnpjCotista'] = cotistas['cpfcnpjInvestidor'].apply(str)

        inicio = partial(self.job_criar_cotista_cetip , df_com_tipo_cota=df_com_tipo_cota , lista_de_cotas=lista_de_cotistas_cetip)

        with ThreadPoolExecutor(max_workers=10) as executor:
            executor.map( inicio, cotistas.to_dict("records")  )

        return lista_de_cotistas_cetip


    def job_criar_cotista_unico(self , cotista , n_posicao ,  xml_cotistas ):

        try:

            cotas_df = n_posicao.filter(pl.col('cpfcnpjCotista') == cotista['cpfcnpjCotista'])


            cotas_df_pre_ajustado = cotas_df.group_by(["fundo", "valor_cota", "cotatipo"]).agg(
                [
                    pl.col("vlCorrigido").sum().alias("vlCorrigido"),
                    pl.col("qtCotas").sum().round(8).alias("qtCotas")
                ]
            )

            cotas_df_pre_ajustado = cotas_df_pre_ajustado.rename({
                "fundo": "tipo",
                "valor_cota": "valorCota",
                "cotatipo": "tipoCota",
                "vlCorrigido": "vlCorrigido",
                "qtCotas": "qtdeCotas"
            })



            cotas_records = cotas_df_pre_ajustado.to_dicts()

            root = ET.Element("cotas")

            for record in cotas_records:
                cota = ET.SubElement(root, "cota")
                for key, value in record.items():
                    if key in ['tipoCota', 'qtdeCotas', 'valorCota']: 
                        cota.set(key, str(value))  

            elemento_cotista = self.criar_cotistas_unico(cotista)

            elemento_cotista.append(root)

            xml_cotistas.append(elemento_cotista)

        except Exception as e:
            print (e ,  cotista)




    def transforma_posicao_posicao_informe(self):
        '''usa o df do fundo para transforma-lo no xml do 5401'''
        df_posicao = self.consultar_posicoes_jcot()[[ 'cd_cotista' , 'cpfcnpjCotista', 'fundo', 'valor_cota' , 'cotatipo' , 'vlCorrigido' , 'qtCotas' ,  "tipoCotista"]]
        n_posicao = df_posicao

        if not n_posicao.is_empty():
            total_cotistas = n_posicao.filter(~n_posicao.is_duplicated())
            xml_fundos = self.criar_fundo(self.CNPJ_EMISSOR , str(round(n_posicao['qtCotas'].sum() ,2)) , len(total_cotistas) ,  str(round(n_posicao['vlCorrigido'].sum() , 2)) )
            xml_cotistas = self.montar_cotistas()
            cotistas = n_posicao.select(["cpfcnpjCotista" , "tipoCotista"]).unique()
            lista_cotistas = [{"cpfcnpjCotista": item[0], "tipoCotista": item[1]} for item in cotistas.iter_rows()]
            job_cotista = partial(self.job_criar_cotista_unico ,  n_posicao=n_posicao , xml_cotistas=xml_cotistas)

            with ThreadPoolExecutor() as executor:
                executor.map( job_cotista , lista_cotistas)

            xml_fundos.append(xml_cotistas)

            return xml_fundos


    def ajustar_df_cetip(self , df_cetip):

        if not df_cetip.empty:
            mapper_rename = {
                "cpfcnpjInvestidor": "cpfcnpjCotista" ,
                'quantidadeTotalDepositada': 'qtCotas' ,
                'cd_jcot': 'fundo'
            }
            df_cetip.rename(columns=mapper_rename , inplace=True)
            df_cetip['data'] = df_cetip['data'].apply(lambda x :x[0:10])
            df_cetip['cotatipo'] = df_cetip['fundo'].apply(lambda x : self.get_tipo_cota( x))
            df_cetip['vlCorrigido'] = df_cetip['qtCotas'].apply(float) * df_cetip['valor_cota'].apply(float)

            df_posicao = df_cetip[[ 'cpfcnpjCotista', 'fundo', 'valor_cota' , 'cotatipo' , 'vlCorrigido' , 'qtCotas']]
            df_posicao['cpfcnpjCotista'] = df_posicao['cpfcnpjCotista'].apply(lambda x : str(x))
            df_posicao['cd_cotista']  = df_posicao['cpfcnpjCotista'].apply(lambda x : str(x))

            return df_posicao

        else :
            return pd.DataFrame()