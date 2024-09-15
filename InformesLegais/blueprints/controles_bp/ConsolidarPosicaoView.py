from flask.views import MethodView
from flask import render_template
from . import conroles_bp
from InformesLegais.Controllers import ConsolidadordePosicoes
from .forms import ConsolidarPosicoesForm


class ConsolidarPosicoes(MethodView):
    
    def get(self):
        # Code to handle GET request
        
        form  = ConsolidarPosicoesForm()
    
              
        return render_template("Controles/ConsolidadordePosicoes.html" , form=form)

    def post(self):
        # Code to handle POST request
        return "Create new user"
    
    

conroles_bp.add_url_rule('/consolida', view_func=ConsolidarPosicoes.as_view("consolidarPosicoes"))
