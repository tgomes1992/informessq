from flask import Blueprint , jsonify , render_template





conroles_bp = Blueprint('controles', __name__ , url_prefix='/controles')


@conroles_bp.route("/")
def home_controles():
    return render_template("Controles/main.html")


from .ConsolidarPosicaoView import *