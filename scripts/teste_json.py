from GERACAO_5401.Fundo5401_base import Fundo5401
import xml.etree.ElementTree as ET





def gerar_arquivo_teste():

    gerador  = Fundo5401("20748515000181")
    dados = gerador.transforma_posicao_posicao_informe()

    with open("arquivo.json", 'w') as file:
        file.write(str(dados))

