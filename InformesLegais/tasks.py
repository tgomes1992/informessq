from InformesLegais.celery import celery_app 
from .db import db 
from JCOTSERVICE import RelPosicaoFundoCotistaService
from flask import Flask
import os
from dotenv import load_dotenv
from GERACAO_5401.extracao_5401_quantidades_o2 import o2Api



load_dotenv()

@celery_app.task(name="Extrair Posições Jcot")
def extrair_posicao_jcot_unique(fundo):
    '''função para realizar extração de posições do jcot '''
    service = RelPosicaoFundoCotistaService("roboescritura", "Senh@123")
    posicao = service.get_posicoes_json(fundo)
    try:
        app = Flask(__name__)
        app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')
        print (app.config)
        with app.app_context():
            trava = db.posicoesjcot.find_one({"fundo": fundo['codigo'] , "data": fundo['dataPosicao']})
            if not trava:
                db.posicoesjcot.insert_many(posicao)
    
    except Exception as e:
        print (e)
        pass
    
    return {"message": f"Extração do fundo {fundo['codigo']} Concluída"}


@celery_app.task(name="Extrair Posições O2")
def extrair_posicao_o2(ativo_o2 , header):
    api = o2Api("thiago.conceicao", "DBCE0923-9CE3-4597-9E9A-9EAE7479D897")
    df_posicao = api.get_posicao( ativo_o2['data'] , ativo_o2['descricao'] ,  ativo_o2['cd_jcot'] , header)

    app = Flask(__name__)
    app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')
    with app.app_context():
        if not df_posicao.empty:
            db.posicoeso2.insert_many(df_posicao.to_dict('records'))

        # trava = db.posicoeso2.find_one({"fundo": fundo['codigo'], "data": fundo['dataPosicao']})

    print (f"Extração do fundo {ativo_o2['cd_escritural']} concluída")
    return {"message": f"Extração da posição do ativo {ativo_o2['descricao']} Concluída"}


