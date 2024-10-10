from flask_wtf import FlaskForm
from wtforms import StringField,SelectField ,  SubmitField  , DateField
from wtforms.validators import DataRequired
from InformesLegais.utils.ExtraInfos import ExtraInfos




class ConsolidarPosicoesForm(FlaskForm):
    data = SelectField('Data', validators=[DataRequired()])
    submit = SubmitField('Consolidar')

    def __init__(self, periodos, *args, **kwargs):
        super(ConsolidarPosicoesForm, self).__init__(*args, **kwargs)
        self.data.choices = periodos



class BuscarPosicoesForm(FlaskForm):
    data = DateField('Data', validators=[DataRequired()])

    submit = SubmitField('Solicitar Posicoes ')
