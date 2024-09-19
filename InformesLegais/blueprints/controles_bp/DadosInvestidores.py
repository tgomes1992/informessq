from flask.views import MethodView
from flask import render_template, request, jsonify
from . import conroles_bp
from .forms import ConsolidarPosicoesForm


class DadosInvestidores(MethodView):

    def get(self):
        # Code to handle GET request


        return render_template("Controles/Investidores.html")

    def post(self):
        # Code to handle POST request



        return jsonify({"message": "Posição enviadas para consolidação"})


conroles_bp.add_url_rule('/dados_investidores', view_func=DadosInvestidores.as_view("Investidores"))
