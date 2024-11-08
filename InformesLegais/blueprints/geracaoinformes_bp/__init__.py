from flask import Blueprint , request , jsonify , render_template , redirect , url_for , flash
from InformesLegais.db import db
from .forms import SolicitarArquivo5401
from InformesLegais.Services.ServiceGeracao5401 import ServiceGeracao5401


geracao_informes_bp = Blueprint('geracao_informes', __name__ , url_prefix='/geracao')


@geracao_informes_bp.route("/gerar_arquivo")
def gerar_arquivo_5401():
    form = SolicitarArquivo5401()
    return render_template("form_geracao_5401.html" , form = form)


@geracao_informes_bp.route("/5401" , methods=['POST'])
def gerar_informe_5401():

    try:

        adm = request.form['administrador']
        data = request.form['data']
        tipo = request.form['tipos_5401']



        geracao = ServiceGeracao5401(adm , data , tipo)

        if tipo == "175":
            geracao.gerar_5401_por_adm_175(adm)
        else:
            geracao.gerar_5401_por_adm(adm)

        # geracao.gerar_5401(adm)


        flash(f"Arquivo enviado para geração" ,  'succes')

    except Exception as e:
        flash(f"Erro ao enviar para geração -> {e}", 'danger')



    return redirect(url_for('geracao_informes.gerar_arquivo_5401'))
