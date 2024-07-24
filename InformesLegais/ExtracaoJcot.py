from JCOTSERVICE import ListFundosService , RelPosicaoFundoCotistaService
from .db import db



class ExtracaoJcot:

    def extrair_posicao(self , codigo_fundo):
        service = RelPosicaoFundoCotistaService("thiago", "tAman1993**")
        fundo = {"codigo": codigo_fundo ,"dataPosicao": '2024-06-28'}
        df = service.get_posicoes_json(fundo)
        db.posicoes_jcot.insert_many(fundo)

        return "Movimento"


