from flask import Blueprint , request , jsonify , render_template
from GERACAO_5401.Fundo5401 import Fundo5401
from GERACAO_5401.Documento5401 import Documento5401


geracao_informes_bp = Blueprint('geracao_informes', __name__ , url_prefix='/geracao')




@geracao_informes_bp.route("/5401")
def gerar_informe_5401():


    gerador_fundo_5401 = Fundo5401('26286853000125')

    documento_5401 = Documento5401()

    try:


        fundo = gerador_fundo_5401.transforma_posicao_posicao_informe()

        documento = documento_5401.documento_5401(fundo)

        Documento5401().escrever_arquivo(documento)




    except Exception  as e:
        print (e)


    return jsonify({"message":"Arquivo gerado com sucesso"})
