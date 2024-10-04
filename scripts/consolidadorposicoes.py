from InformesLegais.Services import ControllerConsolidaPosicoes
from pymongo import MongoClient
from multiprocessing import Pool
import numpy as np




def consolidar_posicoes():

    controller = ControllerConsolidaPosicoes()

    client = MongoClient("mongodb://localhost:27017",
         connectTimeoutMS=30000,  # Connection timeout in milliseconds (30 seconds)
        socketTimeoutMS=60000,   # Socket timeout in milliseconds (60 seconds)
        serverSelectionTimeoutMS=40000   )


    controller.get_posicoesjcot('2024-09-30')



