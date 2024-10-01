from GERACAO_5401.Fundo5401_base import Fundo5401
import xml.etree.ElementTree as ET




gerador  = Fundo5401("43121036000136")


dados = gerador.transforma_posicao_posicao_informe()

print(dados)


#
#
# gerador.client['informes_legais']['5401'].insert_one(dados)
#
#
# dados = gerador.client['informes_legais']['5401'].find({})
#
#
# def create_fundo_xml(fundo):
#     fundos = ET.Element("fundos")
#
#     fundo_elem = ET.SubElement(fundos, "fundo",
#                                cnpjFundo=fundo['cnpjFundo'],
#                                quantidadeCotas=str(fundo['quantidadeCotas']),
#                                quantidadeCotistas=str(fundo['quantidadeCotistas']),
#                                plFundo=str(fundo['plFundo']))
#
#     cotistas_elem = ET.SubElement(fundo_elem, "cotistas")
#
#     for cotista in fundo['cotistas']:
#         cotista_elem = ET.SubElement(cotistas_elem, "cotista",
#                                      tipoPessoa=str(cotista['tipoPessoa']),
#                                      identificacao=cotista['identificacao'],
#                                      classificacao=str(cotista['classificacao']))
#
#         cotas_elem = ET.SubElement(cotista_elem, "cotas")
#
#         for cota in cotista['cotas']:
#             ET.SubElement(cotas_elem, "cota",
#                           tipoCota=str(cota['tipoCota']),
#                           qtdeCotas=str(cota['qtdeCotas']) ,
#                           valorCota=str(cota['valorCota']))
#
#     return ET.tostring(fundos, encoding="unicode")
#
#
# for item in dados:
#     print (create_fundo_xml(item))
