from InformesLegais.db import db
from flask import Flask
import os
from datetime import datetime
import locale


# Set locale to Portuguese
locale.setlocale(locale.LC_TIME, 'pt_BR')
class ExtraInfos:

    def periodos_posicao(self):
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

