from flask import Blueprint , request , jsonify , render_template
from GERACAO_5401.Fundo5401 import Fundo5401
from GERACAO_5401.Documento5401 import Documento5401
from GERACAO_5401.xml_5401 import XML_5401
from pymongo import MongoClient
import pandas as pd
from ..db import db
from concurrent.futures import ThreadPoolExecutor
from functools import partial


geracao_informes_bp = Blueprint('geracao_informes', __name__ , url_prefix='/geracao')



def job_criar_fundos(cnpj , documento_5401 ):
    print (cnpj)
    gerador_fundo_5401 = Fundo5401(cnpj)
    fundo = gerador_fundo_5401.transforma_posicao_posicao_informe()

    if fundo is not None:
 
        documento_5401.adicionar_fundos(fundo)
    else:
        pass


@geracao_informes_bp.route("/5401")
def gerar_informe_5401():

    adm = '02332886000104'

    fundos_por_adm = db.fundos.find({"administrador": adm})

    df = pd.DataFrame.from_dict(fundos_por_adm)
    cnpjs = list(df['cnpj'].drop_duplicates())

    documento_5401 = Documento5401()
    criacao_fundos = partial(job_criar_fundos, documento_5401=documento_5401 )
    
    cnpjs = ['46153220000156']

    try:
            
        with ThreadPoolExecutor(max_workers=7) as executor:
            executor.map(criacao_fundos, cnpjs)
            

        documento = documento_5401.retornar_arquivo_5401_completo()
        ajustador = XML_5401(documento)
        ajustador.ajustar_arquivo_5401()        
        ajustador.reescrever_xml('arqu.xml')            
        # documento_5401.escrever_arquivo()




    except Exception  as e:
        print (e)


    return jsonify({"message":"Arquivo gerado com sucesso"})
