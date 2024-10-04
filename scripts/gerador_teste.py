from GERACAO_5401.Fundo5401 import Fundo5401
import xml.etree.ElementTree as ET





gerador  = Fundo5401("20748515000181")


dados = gerador.transforma_posicao_posicao_informe()

with open("teste.xml" ,  'w') as file:
    file.write(ET.tostring(dados, encoding='unicode'))


