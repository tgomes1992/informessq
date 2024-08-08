from GERACAO_5401.xml_5401 import XML_5401
import os


caminho = os.path.join(r"C:\Projects\analise_5401\5401_DTVM_01_2024" ,"5401_DTVM_01_2024.xml")


arquivo = XML_5401(caminho ,  caminho)

arquivo.send_cotistas_to_db()