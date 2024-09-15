from flask_wtf import FlaskForm
from wtforms import StringField,SelectField ,  SubmitField 
from wtforms.validators import DataRequired




class ConsolidarPosicoesForm(FlaskForm):
    
    data = SelectField('Data', choices=[('tech', 'Technology'), 
                                                ('health', 'Health'), 
                                                ('finance', 'Finance')], 
                                                validators=[DataRequired()])
    
    submit = SubmitField('Consolidar')
