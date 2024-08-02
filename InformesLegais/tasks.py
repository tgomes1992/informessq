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


def get_valor_de_cota_jcot(ativo_o2):
    try:          
        valor_de_cota = db.posicoesjcot.find_one({"fundo": str(ativo_o2['cd_jcot']) , "data": str(ativo_o2['data'])  })
        return valor_de_cota['valor_cota']
    except Exception as e:
        print (e)        
        return '0'


@celery_app.task(name="Extrair Posições O2")
def extrair_posicao_o2(ativo_o2 , header):
    api = o2Api("thiago.conceicao", "DBCE0923-9CE3-4597-9E9A-9EAE7479D897")


    app = Flask(__name__)
    app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')
    with app.app_context():
        
        valor_de_cota = get_valor_de_cota_jcot(ativo_o2)
        
        df_posicao = api.get_posicao( ativo_o2['data'] , ativo_o2['descricao'] ,  ativo_o2['cd_jcot'] , header , valor_de_cota)

        if not df_posicao.empty:
            db.posicoeso2.insert_many(df_posicao.to_dict('records'))

        # trava = db.posicoeso2.find_one({"fundo": fundo['codigo'], "data": fundo['dataPosicao']})

    print (f"Extração do fundo {ativo_o2['cd_escritural']} concluída")
    return {"message": f"Extração da posição do ativo {ativo_o2['descricao']} Concluída"}




@celery_app.task(name="Atualizar Investidores")
def atualizar_investidores_o2():
    api = o2Api("thiago.conceicao", "DBCE0923-9CE3-4597-9E9A-9EAE7479D897")


    app = Flask(__name__)
    app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')
    with app.app_context():
        
        resultado = api.get_investidores()    
        db.investidoreso2.insert_many(resultado)
        

    print (f"Investidores Realizada com sucesso")
    return {"message": f"Extração de Investidores Concluída com sucesso"}



