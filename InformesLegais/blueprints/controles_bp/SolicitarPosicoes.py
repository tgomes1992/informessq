from flask.views import MethodView
from flask import render_template, request, jsonify ,  redirect , url_for , flash
from . import conroles_bp
from .forms import BuscarPosicoesForm
from InformesLegais.Services import ServiceExtracaoJcotO2 ,  TaskService
from datetime import datetime

class SolicitarPosicoes(MethodView):

    def get(self):
        # Code to handle GET request
        form = BuscarPosicoesForm()

        return render_template("Controles/SolicitarPosicoes.html" ,  form = form)

    def post(self):
        # Code to handle POST request

        try:
            data_request = request.form["data"]
            data_datetime = datetime.strptime(data_request, "%Y-%m-%d")


            ServiceExtracaoJcotO2().ExtracaoJcot(data_datetime)

            ServiceExtracaoJcotO2().Extracaoo2(data_datetime)


            flash(f"Posições Enviadas para extração" ,  'succes')
        except Exception as e:
            flash(f"Algo deu errado {e}", 'warning')





        return redirect(url_for("controles.solicitarPosicoes"))


conroles_bp.add_url_rule('/buscarPosicoes', view_func=SolicitarPosicoes.as_view("solicitarPosicoes"))
