from InformesLegais.celery import celery_app 
from .db import db 
from JCOTSERVICE import RelPosicaoFundoCotistaService
from flask import Flask
import os







@celery_app.task(name="Extrair Posições Jcot")
def extrair_posicao_jcot_unique(fundo):
    service = RelPosicaoFundoCotistaService("roboescritura", "Senh@123")

    
    posicao = service.get_posicoes_json(fundo)
    
    try:
        app = Flask(__name__)
        app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')
        with app.app_context():
            trava = db.posicoesjcot.find_one({"fundo": fundo['codigo'] , "data": fundo['dataPosicao']})

            if not trava:
                db.posicoesjcot.insert_many(posicao)
    
    except Exception as e:
        print (e)
        pass
    
    return {"message": f"Extração do fundo {fundo['codigo']} Concluída"}



