from flask_wtf import FlaskForm
from wtforms import StringField,SelectField ,  SubmitField  , DateField
from wtforms.validators import DataRequired
from InformesLegais.utils.ExtraInfos import ExtraInfos
from dotenv import load_dotenv


load_dotenv()


class SolicitarArquivo5401(FlaskForm):
    data = SelectField('Data', validators=[DataRequired()])
    administrador = SelectField("Administrador", validators=[DataRequired()])
    submit = SubmitField('Consolidar')

    def __init__(self, *args, **kwargs):
        super(SolicitarArquivo5401, self).__init__(*args, **kwargs)

        # Create a new ExtraInfos instance for every new form instance
        infos = ExtraInfos()

        # Populate the 'data' field choices
        self.data.choices = infos.periodos_posicao()

        # Populate the 'administrador' field choices
        self.administrador.choices = infos.get_admins()



