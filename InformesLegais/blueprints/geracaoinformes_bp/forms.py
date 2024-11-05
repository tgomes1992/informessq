from flask_wtf import FlaskForm
from wtforms import StringField,SelectField ,  SubmitField  , DateField
from wtforms.validators import DataRequired
from InformesLegais.utils.ExtraInfos import ExtraInfos
from dotenv import load_dotenv


load_dotenv()


class SolicitarArquivo5401(FlaskForm):
    data = SelectField('Data', validators=[DataRequired()])
    administrador = SelectField("Administrador", validators=[DataRequired()])
    tipos_5401 = SelectField("Tipos 5401", validators=[DataRequired()])

    submit = SubmitField('Gerar Arquivo')

    def __init__(self, *args, **kwargs):
        super(SolicitarArquivo5401, self).__init__(*args, **kwargs)
        infos = ExtraInfos()
        self.data.choices = infos.periodos_posicao()
        self.administrador.choices = infos.get_admins()
        self.tipos_5401.choices = infos.tipos_5401()



