import pandas as pd
from intactus.osapi import o2Api
from sqlalchemy import create_engine
from datetime import datetime , timedelta
import threading
import numpy as np
import time
import concurrent.futures



class Extracao_Quantidades_O2():

    def __init__(self , mongoclient , data):
        '''a data precisa ser um objeto datetime'''
        self.client = mongoclient
        self.db =  self.client['posicoes_o2_5401']
        self.colection  = self.db['posicoes_o2']
        self.api = o2Api("thiago.conceicao" , "DBCE0923-9CE3-4597-9E9A-9EAE7479D897")
        self.data = data
        
        
        
    def get_ativos_o2(self):
        return self.api.get_ativos()


    def ajustar_codigo_escritural(self,codigo_dict):
        for item in codigo_dict:
            if item['nomeOrigemCodigoInstrumentoFinanceiro'] == 'ESCRITURAL':  
                return item['descricao']
            else:
                return ""
        
    def ajustar_codigo_jcot(self,codigo_dict):
        for item in codigo_dict:
            if item['nomeOrigemCodigoInstrumentoFinanceiro'] == 'JCOT':  
                try:
                    return item['descricao']
                except Exception as e:
                    print (e)
            else:
                return ""
            
    def get_cd_origem_instrumento_financeiro( self,base_dict , tipo):
        if base_dict['nomeOrigemCodigoInstrumentoFinanceiro'] == tipo:
            return base_dict['descricao']
        else:
            return  False
        

    def get_cd_jcot_lista(self,lista_base_dict ,  tipo):
        cd_jcot = []
        for item in lista_base_dict:
            teste =  self.get_cd_origem_instrumento_financeiro(item , tipo)
            if teste:
                cd_jcot.append(teste)
        try:
            return cd_jcot[0]
        except Exception as e:
            return "Sem Código"


    def ajustar_data_fim_relacionamento(self,string):
        try:
            return datetime.strptime(string[0:10] , "%Y-%m-%d")
        except:
            return datetime(9999,12,31)

    def get_lista_fundos(self):
        # self.db['ativos_o2'].delete_many({})
        ativos = self.get_ativos_o2()
        print (ativos)
        fundos = ['FIP' , 'FII' , 'FIM' , 'FIDC' , 'FIA' , 'FIC FIDC' , 'FIC FIM' , 'RECIBO DE SUBSCRIÇÃO - FUNDO IMOBILIARIO' , 'FIAGRO' , 'FIP-IE' , "CFF" ]
        fundos_a_buscar = ativos
        fundos_a_buscar['cd_escritural'] = fundos_a_buscar['codigosInstrumentosFinanceiros'].apply(self.ajustar_codigo_escritural)
        fundos_a_buscar['cd_jcot'] = fundos_a_buscar['codigosInstrumentosFinanceiros'].apply(lambda x : self.get_cd_jcot_lista(x, 'JCOT'))
        fundos_a_buscar['dataFimRelacionamento'] = fundos_a_buscar['dataFimRelacionamento'].apply(self.ajustar_data_fim_relacionamento)
        filtro = fundos_a_buscar['dataFimRelacionamento'] >= self.data
        fundos_ativos = fundos_a_buscar[filtro]
        return fundos_ativos.to_dict("records")


    def submit_task(self,index , listas_de_fundos):
        lista = listas_de_fundos[index]
        self.api.get_posicao_list_fintools(self.data.strftime("%Y-%m-%d"), lista, self.colection , index)


    def get_posicoes_o2(self , fundos):

        self.colection.delete_many({})
        listas_de_fundos = self.get_lista_fundos(fundos)

        pool = concurrent.futures.ThreadPoolExecutor(max_workers=7)

        for i in range(len(listas_de_fundos)):
            pool.submit(self.submit_task, i , listas_de_fundos)

        pool.shutdown(wait=True)
 

