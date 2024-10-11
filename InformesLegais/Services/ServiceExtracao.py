from .ServiceBase import ServiceBase
from InformesLegais.Services.TaskService import TaskService
from InformesLegais.tasks import extrair_posicao_jcot_unique ,  extrair_posicao_o2
from InformesLegais.tasks.tasks_controles import finalizar_task
from datetime import datetime
import pandas as pd
from celery import chord




class ServiceExtracaoJcotO2(ServiceBase):



    def Extracaoo2(self , data):
        # service para extração de dados do o2

        with self.app.app_context():
            headers = {
                'Authorization': f'Bearer {self.api.get_token()}',
                'Content-Type': 'application/json'
            }

            self.db.posicoeso2.delete_many({"data": data.strftime("%Y-%m-%d")})

            fundos = self.db.ativoso2.find({"cd_jcot": {"$ne": "Sem Código"}})

            task_list = []


            for item in fundos:
                item['_id'] = str(item['_id'])
                item['data'] = data.strftime("%Y-%m-%d")
                # extrair_posicao_o2.delay(item, headers)
                task_list.append(extrair_posicao_o2.s(item , headers))

            task = TaskService().start_task(f"Baixar posições o2 {data.strftime("%Y-%m-%d")}")

            chord(task_list)(finalizar_task.s(task))


    def ExtracaoJcot(self ,  data):
        '''a data precisa ser em formato datetime ,
            função que cria os jobs para extrair dados do jcot
        '''
        with self.app.app_context():

            fundos = self.db.fundos.find({})
            self.db.posicoesjcot.delete_many({"data": data.strftime("%Y-%m-%d")})

            task_list = []

            for fundo in fundos:
                fundo['_id'] = str(fundo['_id'])
                fundo['dataPosicao'] = data.strftime("%Y-%m-%d")
                fundo['fundo'] = fundo['codigo']
                task_list.append(extrair_posicao_jcot_unique.s(fundo))

            task = TaskService().start_task(f"Baixar posições Jcot {data.strftime("%d/%m/%Y")}")
            chord(task_list)(finalizar_task.s(task))

    def get_codigo_jcot(self ,  lista_codigos , tipo):
        for item in lista_codigos:
            if item['nomeOrigemCodigoInstrumentoFinanceiro'] == tipo:
                return item['descricao']

        return None



    def ExtracaoAtivoso2(self):
        ativos_intactus = self.api.get_ativos()

        ativos_intactus['cd_jcot'] =  ativos_intactus['codigosInstrumentosFinanceiros'].apply(lambda x : self.get_codigo_jcot( x,'JCOT'))
        ativos_intactus['cd_escritural'] =  ativos_intactus['codigosInstrumentosFinanceiros'].apply(lambda x : self.get_codigo_jcot( x,'ESCRITURAL'))

        return ativos_intactus[~ativos_intactus['cd_jcot'].isnull()].to_dict('records')
