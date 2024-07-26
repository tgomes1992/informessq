from pymongo import MongoClient
import pandas as pd
import xml.etree.ElementTree as ET
from xml.dom import minidom



class Documento5401:



    def criar_documento_inicial(self):
        documento  = ET.Element("documento")
        documento.set("cnpj" , "36113886000191" )
        documento.set('codigoDocumento' ,  '5401')
        documento.set("tipoRemessa" , 'I')
        documento.set("dataBase" ,  '2024-05')
        documento.set('nomeResponsavel' ,  "THIAGO MENEZES")
        documento.set("emailResponsavel" , "thiago.menezes@oliveiratrust.com.br")
        documento.set("telefoneResponsavel" , "02135140000")
        return documento
    def criar_fundos(self , fundos_xml):
        fundos = ET.Element("fundos")
        fundos.append(fundos_xml)
        return fundos

    def documento_5401(self ,  fundo_xml):
        base = self.criar_documento_inicial()
        fundos = self.criar_fundos(fundo_xml)

        base.append(fundos)

        return base


    def escrever_arquivo(self , documento):
        file_name = "teste.xml"

        xml_string = minidom.parseString(ET.tostring(documento)).toprettyxml(indent=" ")

        with open(f'{file_name}', "w") as file:
            file.write(xml_string)
