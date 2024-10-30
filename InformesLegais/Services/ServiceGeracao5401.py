from .ServiceBase import ServiceBase
import pandas as pd
from functools import partial
from GERACAO_5401.Fundo5401 import Fundo5401
from GERACAO_5401.Documento5401 import Documento5401
from GERACAO_5401.xml_5401 import XML_5401
from concurrent.futures import ThreadPoolExecutor
from InformesLegais.tasks import gerar_5401_por_adm , gerar_5401_por_adm_175
from InformesLegais.Services.TaskService import TaskService

class ServiceGeracao5401(ServiceBase):


    def __init__(self , adm , data ):
        self.adm = adm
        self.data = data
        self.documento_5401 = Documento5401(adm,  data)

    def job_criar_fundos(self ,  cnpj , documento_5401):
        gerador_fundo_5401 = Fundo5401(cnpj)
        fundo = gerador_fundo_5401.transforma_posicao_posicao_informe()
        if fundo is not None:
            documento_5401.adicionar_fundos(fundo)
        else:
            pass

    def get_fundos_5401(self):
        with self.app.app_context():
            fundos_por_adm = self.db.fundos.find({"administrador": self.adm})
            df = pd.DataFrame.from_dict(fundos_por_adm)
            cnpjs = list(df['cnpj'].drop_duplicates())
            return cnpjs
    def get_dados_fundo_5401(self):
        criacao_fundos = partial(self.job_criar_fundos, documento_5401=self.documento_5401)
        cnpjs = self.get_fundos_5401()

        with ThreadPoolExecutor() as executor:
            executor.map(criacao_fundos, cnpjs)


    def gerar_arquivo(self):
        print ("Geração Iniciada !!! ")
        try:
            documento = self.documento_5401.retornar_arquivo_5401_completo()
            ajustador = XML_5401(documento)
            ajustador.ajustar_arquivo_5401()
            ajustador.reescrever_xml(f"{self.adm}.xml")
        except Exception as e:
            print (e)


    def processar_arquivo(self):
        #buscar os dados
        self.get_dados_fundo_5401()

        # escrever arquivos
        self.gerar_arquivo()

    def gerar_5401_por_adm(self,adm):
        id = TaskService().start_task(f"Geração 5401 {adm}")
        gerar_5401_por_adm.delay(adm , self.data , id)


    def gerar_5401_por_adm_175(self,adm):
        id = TaskService().start_task(f"Geração 5401 175 {adm}")
        gerar_5401_por_adm_175.delay(adm , self.data , id)
