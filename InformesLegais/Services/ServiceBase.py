from dotenv import load_dotenv
from flask import Flask
import pandas as pd
from InformesLegais.db import db
import os
from flask import Flask
from intactus.osapi import  o2Api



class ServiceBase:

    '''service responsável por controlar toda a interação com os investidores'''

    def __init__(self):
        self.app = Flask(__name__)
        self.app.config['MONGO_URI'] = os.environ.get('DB_URI_LOCAL')
        self.api = o2Api("thiago.conceicao", "DBCE0923-9CE3-4597-9E9A-9EAE7479D897")
        self.db = db


