from InformesLegais.db import db
from flask import Flask
import os
from datetime import datetime
import locale
import pandas as pd





# Set locale to Portuguese
locale.setlocale(locale.LC_TIME, 'pt_BR')
class ExtraInfos:




    def get_admins(self):
        app = Flask(__name__)

        app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')

        with app.app_context():

            adm = db.administradores.find({})
            df_adm  = pd.DataFrame.from_dict(adm)


            adm_formatados = [self.formatar_adms(item) for item in df_adm.to_dict("records")]



            return  adm_formatados


    def formatar_adms(self , adm_dict):
        return (adm_dict['cnpj'] ,f"{adm_dict['cnpj']} - {adm_dict['nome']}" )


    def periodos_posicao(self):
        app = Flask(__name__)

        app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')

        with app.app_context():

            periodos = db.posicaoconsolidada.aggregate([
            {
              '$project': {
                'periodo': { '$substr': ['$data', 0, 10] }
              }
            },
            { '$group': { '_id': '$periodo' } }
          ])

            periodos_formatados = [self.formatar_periodos(item['_id']) for item in periodos]



            return  periodos_formatados


    def periodos_posicao_jcot(self):
        app = Flask(__name__)

        app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')

        with app.app_context():

            periodos = db.posicoesjcot.aggregate([
            {
              '$project': {
                'periodo': { '$substr': ['$data', 0, 10] }
              }
            },
            { '$group': { '_id': '$periodo' } }
          ])

            periodos_formatados = [self.formatar_periodos(item['_id']) for item in periodos]



            return  periodos_formatados

    def formatar_periodos(self, periodo):
        periodo_datetime = datetime.strptime(periodo, '%Y-%m-%d').strftime('%d/%m/%Y')
        return (periodo, periodo_datetime)



