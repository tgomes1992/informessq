from flask_wtf import FlaskForm
from wtforms import StringField,SelectField ,  SubmitField  , DateField
from wtforms.validators import DataRequired
from InformesLegais.utils.ExtraInfos import ExtraInfos






class ConsolidarPosicoesForm(FlaskForm):
    
    data = SelectField('Data', choices=ExtraInfos().periodos_posicao(),validators=[DataRequired()])
    
    submit = SubmitField('Consolidar')


class BuscarPosicoesForm(FlaskForm):
    data = DateField('Data', validators=[DataRequired()])

    submit = SubmitField('Solicitar Posicoes ')
