from GERACAO_5401.xml_5401 import XML_5401
import xml.etree.ElementTree as ET

arquivo = ET.parse('202407_20240808_BACEN-Informe5401.xml')

file = XML_5401(arquivo)


file.ajustar_arquivo_5401()

file.reescrever_xml('XP.xml')