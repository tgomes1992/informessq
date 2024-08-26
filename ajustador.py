from GERACAO_5401.xml_5401 import XML_5401
import xml.etree.ElementTree as ET

arquivo = ET.parse('DTVM_07_2024.xml')

root = arquivo.getroot()


file = XML_5401(root)

file.ajustar_arquivo_5401()

file.reescrever_xml('DTVM_07_2024_pos_ajustes.xml')