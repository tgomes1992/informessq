from flask import Blueprint , request , jsonify , render_template , redirect , url_for , flash
from ..db import db
from ..tasks import gerar_5401_por_adm  , extrair_posicao_jcot_unique , extrair_posicao_o2 , extrair_posicao_jcot , extrair_posicao_o2_geral


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

    try:

        adm = request.form['administrador']

        # gerar_5401_por_adm.delay('08387157000123')

        print (adm)
        #
        # extrair_posicao_jcot.delay()
        #
        # extrair_posicao_o2_geral.delay()

        gerar_5401_por_adm.delay(adm)


        flash(f"Arquivo enviado para geração" ,  'succes')

    except Exception as e:
        flash(f"Erro ao enviar para geração -> {e}", 'danger')



    return redirect(url_for('geracao_informes.gerar_arquivo_5401'))
