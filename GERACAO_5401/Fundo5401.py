from pymongo import MongoClient
import pandas as pd
import xml.etree.ElementTree as ET
from xml.dom import minidom


class Fundo5401():


    def __init__(self , CNPJ_EMISSOR):
        self.CNPJ_EMISSOR = CNPJ_EMISSOR
        self.client = MongoClient('localhost', 27017)


    def consultar_fundos_5401(self):
        fundos = self.client['informes_legais']['fundos'].find({"cnpj": self.CNPJ_EMISSOR})
        base = [fundo for fundo in fundos]

        return base


    def consultar_posicoes_jcot(self):
        fundos = self.consultar_fundos_5401()
        lista_codigos = [item['codigo'] for item in fundos]
        posicoes_jcot =  self.client["informes_legais"]["posicoesjcot"].find({"fundo": {"$in": lista_codigos} })
        df = pd.DataFrame(posicoes_jcot)
        df['qtCotas'] = df['qtCotas'].apply(float)
        df['vlCorrigido'] = df['vlCorrigido'].apply(float)


        return df

    def criar_cotistas_unico(self, cotista):
    #todo criar função para validar o tipo de cotista e a sua respectiva classificação
        try:
            dados_formatado = cotista["cpfcnpjCotista"]
            cotista_elemento = ET.Element("cotista")
            cotista_elemento.set("tipoPessoa", '1')
            cotista_elemento.set("identificacao", cotista["cpfcnpjCotista"])

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
        print (lista_cotas)
        cotas = ET.Element('cotas')
        for cota in lista_cotas:
            ncota = ET.SubElement(cotas ,  'cota')
            ##todo depara de cada uma das classes do jcot

            ncota.set("tipoCota" , str(cota['tipo']))
            ncota.set("qtdeCotas" ,  str(round(cota['qtdeCotas'],2)))
            ncota.set("valorCota" , str(cota['valorCota']))

        return cotas


    def consultar_cotista_cetip(self, cnpj_emissor):
        posicoes = self.client['informes_legais']['posicoeso2'].find({"cnpjFundo": int(cnpj_emissor) , 'depositaria':'CETIP'})
        df_fundo = pd.DataFrame.from_dict(posicoes)

        return df_fundo


    def transformar_cotistas_cetip(self, df_cetip):
        '''função que vai gerar o xml dos cotistas cetipados'''
        cotistas = df_cetip.drop_duplicates('cpfcnpjInvestidor')

        lista_de_cotistas_cetip  = []

        cotistas['cpfcnpjCotista'] = cotistas['cpfcnpjInvestidor'].apply(str)

        for cotista in cotistas.to_dict('records'):

            cotas_df = df_cetip[df_cetip['cpfcnpjInvestidor'] == cotista['cpfcnpjInvestidor']]

            cotas_df_pre_ajustado  = cotas_df.groupby(['cd_jcot'])[['quantidadeTotalDepositada']].sum().reset_index()
            cotas_df_pre_ajustado.columns = ['tipo', 'qtdeCotas']
            # cotas_df_pre_ajustado.columns = ['tipo',  'valorCota' , 'vlCorrigido' , 'qtdeCotas' ]

            cotas_df_pre_ajustado['valorCota'] = 1
            cotas_df_pre_ajustado['vlCorrigido'] = 1000



            elemento_cotista = self.criar_cotistas_unico(cotista)

            cotas_xml_elemento = self.criar_cotas(cotas_df_pre_ajustado.to_dict("records"))
            elemento_cotista.append(cotas_xml_elemento)

            lista_de_cotistas_cetip.append(elemento_cotista)

        return lista_de_cotistas_cetip

    def transforma_posicao_posicao_informe(self):
        '''usa o df do fundo para transforma-lo no xml do 5401'''
        df_posicao = self.consultar_posicoes_jcot()


        total_cotistas = df_posicao['cpfcnpjCotista'].drop_duplicates().values

        xml_fundos = self.criar_fundo(self.CNPJ_EMISSOR , str(round(df_posicao['qtCotas'].sum() ,2)) , len(total_cotistas) ,  str(round(df_posicao['vlCorrigido'].sum() , 2)) )

        # criação do elemento cotistas
        xml_cotistas = self.montar_cotistas()

        # criar elemento cotista unico

        # cotistas = [item for item in df_posicao.to_dict("records")]

        cotistas = df_posicao.drop_duplicates('cpfcnpjCotista')

        cotista_cetip = cotistas[cotistas['cd_cotista'] == '09358105000191 ']

        for cotista in cotistas.to_dict('records'):
            # todo incluir nesse ponto a consulta das cotas do respectivo cotista , para o respectivo fundo

            if '9358105000191' not in cotista['cpfcnpjCotista']:
                #consulta das cotas do cotista
                cotas_df = df_posicao[df_posicao['cpfcnpjCotista'] == cotista['cpfcnpjCotista']]
                cotas_df_pre_ajustado  = cotas_df.groupby(['fundo', 'valor_cota'])[['vlCorrigido' , 'qtCotas']].sum().reset_index()
                cotas_df_pre_ajustado.columns = ['tipo',  'valorCota' , 'vlCorrigido' , 'qtdeCotas' ]
                cotas_xml_elemento = self.criar_cotas(cotas_df_pre_ajustado.to_dict("records"))
                elemento_cotista = self.criar_cotistas_unico(cotista)
                elemento_cotista.append(cotas_xml_elemento)

                xml_cotistas.append(elemento_cotista)



        if not cotista_cetip.empty:
            df_cetip = self.consultar_cotista_cetip(self.CNPJ_EMISSOR)
            cotistas_cetip = self.transformar_cotistas_cetip(df_cetip)
            for item in cotistas_cetip:
                print (item)
                xml_cotistas.append(item)
            pass



        xml_fundos.append(xml_cotistas)



        return xml_fundos




