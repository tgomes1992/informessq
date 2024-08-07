from flask import Blueprint , request , jsonify , render_template , redirect , url_for , flash
from GERACAO_5401.Fundo5401 import Fundo5401
from GERACAO_5401.Documento5401 import Documento5401
from GERACAO_5401.xml_5401 import XML_5401
from pymongo import MongoClient
import pandas as pd
from ..db import db
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from ..tasks import gerar_5401_por_adm


geracao_informes_bp = Blueprint('geracao_informes', __name__ , url_prefix='/geracao')



def job_criar_fundos(cnpj , documento_5401 ):
    gerador_fundo_5401 = Fundo5401(cnpj)
    fundo = gerador_fundo_5401.transforma_posicao_posicao_informe()

    if fundo is not None:
 
        documento_5401.adicionar_fundos(fundo)
    else:
        pass



@geracao_informes_bp.route("/gerar_arquivo")
def gerar_arquivo_5401():
    fundos = db.fundos.find({})
    adms = []
    for item in fundos:
        if item['administrador'] not in adms:
            adms.append(item['administrador'])




    return render_template("form_geracao_5401.html" , adms = adms)




@geracao_informes_bp.route("/5401" , methods=['POST'])
def gerar_informe_5401():

    adm = '02332886000104'

    try:

        adm = request.form['administrador']

        print (str(adm))

        gerar_5401_por_adm.delay('08387157000123')
        flash(f"Arquivo enviado para geração" ,  'succes')

    except Exception as e:
        flash(f"Erro ao enviar para geração -> {e}", 'danger')



    return redirect(url_for('geracao_informes.gerar_arquivo_5401'))
