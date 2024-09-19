from pymongo import MongoClient
import pandas as pd
import xml.etree.ElementTree as ET
from xml.dom import minidom
from .CotasTipo import CotasTipo
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from ValidadorCpfCnpj import ValidadorCpf , ValidadorCnpj
from pymongo import MongoClient

class Fundo5401():


    def __init__(self , CNPJ_EMISSOR):
        self.CNPJ_EMISSOR = CNPJ_EMISSOR
        self.client = MongoClient('localhost', 27017)


    def consultar_fundos_5401(self):
        fundos = self.client['informes_legais']['fundos'].find({"cnpj": self.CNPJ_EMISSOR})
        base = [fundo for fundo in fundos]
        return base

    def atribuir_tipo_cota(self , df , df_cotas):

        df['cotatipo'] = df['fundo'].apply(lambda x : df_cotas[df_cotas['codigo']== x]['tipo'].values[0])
        df['cd_cotista'] =  df['cd_cotista'].apply(lambda  x: str(x).strip())
        df['cpfcnpjCotista'] = df['cpfcnpjCotista'].apply(lambda x: str(x).strip())
        return df

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
        lista_codigos = [item['codigo'] for item in fundos]

        
        try:
        
            posicoes_jcot =  self.client["informes_legais"]["posicaoconsolidada"].find({"fundo": {"$in": lista_codigos} })
            df = pd.DataFrame(posicoes_jcot)

            df['qtCotas'] = df['qtCotas'].apply(float)
            df['vlCorrigido'] = df['vlCorrigido'].apply(float)

            cotas_jcot = CotasTipo(self.CNPJ_EMISSOR)
            cotas_df = cotas_jcot.buscar_cotas()

            # print(self.atribuir_tipo_cota(df, cotas_df))

            return self.atribuir_tipo_cota(df, cotas_df)
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
                                   'FUNDO DE INVESTIMENTO': 7, 'FUNDO DE INVESTIMENTO IMOBILIÁRIO': 8,
                                   'PESSOA FISICA': 1, 'PESSOA JURIDICA': 2,
                                   'SOCIEDADE LTDA, ECON. MISTA, ANON., POR COTAS, ETC': 11,
                                   'BOLSA DE VALORES E MERCADORIAS': 12, 'CONDOMINIO': 13,
                                   'COOPERATIVA': 14, 'CONSORCIO': 15, 'INST. DE CARATER FILANTROPICO, RECREATIVO, CULT.': 16,
                                   'BANCO COMERCIAL/ MULTIPLO': 17, 'BANCO DE INVESTIMENTO': 18,
                                   'SOCIEDADE DE SEGURO, PREVIDENCIA E CAPITALIZAÇÃO': 19,
                                   'SOCIEDADE DTVM': 20, 'SOCIEDADE CTVM E CAMBIO': 21,
                                   'SOCIEDADE FINANCEIRA E DE CREDITO IMOBILIARIO': 22,
                                   'SOCIEDADE DE ARRENDAMENTO MERCANTIL (LEASING)': 23,
                                   'FUNDO DE PREVIDENCIA PRIVADA – ABERTO': 24,
                                   'FUNDO DE PREVIDENCIA PRIVADA – FECHADO': 25, 'CLUBE DE INVESTIMENTO': 26, 'TEMPLO DE QUALQUER CULTO': 27,
                                   'ENTIDADE SINDICAL DOS TRABALHADORES': 28, 'DEPOSITARIO DE ADR': 29,
                                   'FUNDO DE PLANO DE BENEF. DE SOCIEDADE SEGURADORA': 30,
                                   'UNIAO, EST., MUNIC. OU DIST. FED., AUTAR. OU FUND.': 31, 'PARTIDO POLITICO E SUAS FUNDACOES': 32}

            return str(tipos_de_cotista_O2[investidor_cadastrado['nomePerfilTributarioInvestidor']])
        except Exception as e :
            return "1"



    def criar_cotistas_unico(self, cotista):
    #todo criar função para validar o tipo de cotista e a sua respectiva classificação
        
        #todo lógica de validação do tipo de cotista
    
    
        try:
            dados_formatado = self.formatar_cotista(cotista["cpfcnpjCotista"])
            cotista_elemento = ET.Element("cotista")
            cotista_elemento.set("tipoPessoa", '1') #todo ajustar o tipo de pessoa pois está mockado

            cotista_elemento.set("tipoPessoa", self.get_tipo_cotista_5401(cotista['cpfcnpjCotista']))

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
            ##todo depara de cada uma das classes do jcot

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

    def transforma_posicao_posicao_informe(self):
        '''usa o df do fundo para transforma-lo no xml do 5401'''
        df_posicao = self.consultar_posicoes_jcot()[[ 'cd_cotista' , 'cpfcnpjCotista', 'fundo', 'valor_cota' , 'cotatipo' , 'vlCorrigido' , 'qtCotas']]

        # df_cetip = self.consultar_cotista_cetip(self.CNPJ_EMISSOR)
        #
        # print (df_cetip)
        #
        # df_cetip_ajustado = self.ajustar_df_cetip(df_cetip)
        #
        # n_posicao = pd.concat([df_posicao , df_cetip_ajustado]).reset_index()

        n_posicao = df_posicao

        if not n_posicao.empty:


            total_cotistas = n_posicao['cpfcnpjCotista'].drop_duplicates().values

            xml_fundos = self.criar_fundo(self.CNPJ_EMISSOR , str(round(n_posicao['qtCotas'].sum() ,2)) , len(total_cotistas) ,  str(round(n_posicao['vlCorrigido'].sum() , 2)) )

            # criação do elemento cotistas
            xml_cotistas = self.montar_cotistas()

            # criar elemento cotista unico


            cotistas = n_posicao.drop_duplicates('cpfcnpjCotista')

            cotista_cetip = cotistas[cotistas['cd_cotista'].isin(['09358105000191 ' , '09346601000125 '  ]) ]

            for cotista in cotistas.to_dict('records'):
                # todo incluir nesse ponto a consulta das cotas do respectivo cotista , para o respectivo fundo


                if '9358105000191' not in cotista['cd_cotista'] and '9346601000125' not in  cotista['cd_cotista'] :
                    #consulta das cotas do cotista  
  
                    cotas_df = n_posicao[n_posicao['cpfcnpjCotista'] == cotista['cpfcnpjCotista']]
                    cotas_df_pre_ajustado  = cotas_df.groupby(['fundo', 'valor_cota' , 'cotatipo'])[['vlCorrigido' , 'qtCotas']].sum().reset_index()
                    cotas_df_pre_ajustado.columns = ['tipo', 'valorCota' , 'cotatipo' , 'vlCorrigido' , 'qtdeCotas' ]

                    cotas_xml_elemento = self.criar_cotas(cotas_df_pre_ajustado.to_dict("records"))
                    elemento_cotista = self.criar_cotistas_unico(cotista)
                    elemento_cotista.append(cotas_xml_elemento)

                    xml_cotistas.append(elemento_cotista)


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