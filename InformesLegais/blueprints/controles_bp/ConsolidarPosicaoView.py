from flask.views import MethodView
from flask import render_template , request , jsonify , flash ,  redirect , url_for
from . import conroles_bp
from .forms import ConsolidarPosicoesForm
from InformesLegais.tasks.tasks_controles import task_consolidar_posicoes
from InformesLegais.Services import TaskService
from InformesLegais.utils.ExtraInfos import ExtraInfos


class ConsolidarPosicoes(MethodView):
    
    def get(self):
        # Code to handle GET request

        periodos = ExtraInfos().periodos_posicao_jcot()
        
        form = ConsolidarPosicoesForm(periodos)
              
        return render_template("Controles/ConsolidadordePosicoes.html" , form=form)

    def post(self):
        # Code to handle POST request

        service_tasks = TaskService()

        task_id = service_tasks.start_task(f"Consolidar Posições {request.form['data']}")

        task_consolidar_posicoes.delay(request.form['data'] ,  task_id)

        flash(f"Posições Enviadas para consolidação", 'succes')

        return redirect(url_for("controles.consolidarPosicoes"))
    

conroles_bp.add_url_rule('/consolida', view_func=ConsolidarPosicoes.as_view("consolidarPosicoes"))
