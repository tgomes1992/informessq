from flask.views import MethodView
from flask import render_template , request , jsonify
from . import conroles_bp
from .forms import ConsolidarPosicoesForm
from InformesLegais.tasks.tasks_controles import task_consolidar_posicoes


class ConsolidarPosicoes(MethodView):
    
    def get(self):
        # Code to handle GET request
        
        form  = ConsolidarPosicoesForm()
              
        return render_template("Controles/ConsolidadordePosicoes.html" , form=form)

    def post(self):
        # Code to handle POST request

        task_consolidar_posicoes.delay(request.form['data'])

        return jsonify({"message": "Posição enviadas para consolidação"})
    
    

conroles_bp.add_url_rule('/consolida', view_func=ConsolidarPosicoes.as_view("consolidarPosicoes"))
