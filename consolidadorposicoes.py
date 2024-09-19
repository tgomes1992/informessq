from InformesLegais.Services.ConsolidadordePosicoes import ControllerConsolidaPosicoes
from pymongo import MongoClient
from multiprocessing import Pool
import numpy as np



controller = ControllerConsolidaPosicoes()

client = MongoClient('localhost', 27017)
db = client['informes_legais']

fundos = db['fundos'].find({})
codigos = [fundo['codigo'] for fundo in fundos]


controller.get_posicoesjcot(codigos)



