from .ServiceBase import ServiceBase
from InformesLegais.tasks import extrair_posicao_jcot_unique ,  extrair_posicao_o2
from datetime import datetime
import pandas as pd




class ServiceExtracaoJcotO2(ServiceBase):



    def Extracaoo2(self , data):
        # service para extração de dados do o2

        with self.app.app_context():
            headers = {
                'Authorization': f'Bearer {self.api.get_token()}',
                'Content-Type': 'application/json'
            }

            self.db.posicoeso2.delete_many({})

            fundos = self.db.ativoso2.find({"cd_jcot": {"$ne": "Sem Código"}})

            for item in fundos:
                item['_id'] = str(item['_id'])
                item['data'] = data.strftime("%Y-%m-%d")
                extrair_posicao_o2.delay(item, headers)

    def ExtracaoJcot(self ,  data):
        '''a data precisa ser em formato datetime ,
            função que cria os jobs para extrair dados do jcot
        '''
        with self.app.app_context():

            fundos = self.db.fundos.find({})

            self.db.posicoesjcot.delete_many({})

            for fundo in fundos:
                fundo['_id'] = str(fundo['_id'])
                fundo['dataPosicao'] = data.strftime("%Y-%m-%d")
                fundo['fundo'] = fundo['codigo']
                extrair_posicao_jcot_unique.delay(fundo)

