
from pymongo import MongoClient
# from .extracao_5401_quantidades_o2 import o2Api


client = MongoClient('mongodb://localhost:27017/')

from .extracao_5401_cotas import ExtratorCotas
from .extracao_5401_quantidades_o2 import Extracao_Quantidades_O2



