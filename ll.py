from GERACAO_5401.Fundo5401 import Fundo5401
from GERACAO_5401.Documento5401 import  Documento5401
from GERACAO_5401.xml_5401 import XML_5401

documento = Documento5401()
gerador  = Fundo5401("43121036000136")


dados = gerador.transforma_posicao_posicao_informe()


gerador.client['informes_legais']['fundos_consolidados'].insert(dados)



#
# fundo = gerador.transforma_posicao_posicao_informe()
#
# documento.adicionar_fundos(fundo)
#
# documento = documento.retornar_arquivo_5401_completo()
# ajustador = XML_5401(documento)
# ajustador.ajustar_arquivo_5401()
# ajustador.reescrever_xml(f"teste.xml")