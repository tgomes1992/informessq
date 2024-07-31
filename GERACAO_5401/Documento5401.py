from pymongo import MongoClient
import pandas as pd
import xml.etree.ElementTree as ET
from xml.dom import minidom



class Documento5401:


    def __init__(self):
        self.documento_5401 = self.documento_5401()
        self.elemento_fundos = self.criar_fundos()


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
    
    def criar_fundos(self):
        fundos = ET.Element("fundos")
        return fundos
    
    
    def adicionar_fundos(self, fundo):
        self.elemento_fundos.append(fundo)
    
    

    def documento_5401(self):
        base = self.criar_documento_inicial()
 
        return base
 
 
    def consolidar_arquivo(self):
        self.documento_5401.append(self.elemento_fundos)
        
 
    def retornar_arquivo_5401_completo(self):
        self.consolidar_arquivo()
        return self.documento_5401
 

    def escrever_arquivo(self):
        file_name = "teste.xml"
        
        self.consolidar_arquivo()
 
        xml_string = minidom.parseString(ET.tostring(self.documento_5401)).toprettyxml(indent=" ")

        with open(f'{file_name}', "w") as file:
            file.write(xml_string)
