from pymongo import MongoClient
import pandas as pd
import xml.etree.ElementTree as ET
from xml.dom import minidom
from pymongo import MongoClient



class Documento5401:


    def __init__(self , adm,  data):
        self.data = data
        self.documento_5401 = self.documento_5401(adm)
        self.elemento_fundos = self.criar_fundos()



    def get_dados_adm(self, adm):
        client = MongoClient('localhost', 27017)
        adm = client['informes_legais']['administradores'].find_one({"cnpj": adm})

        return dict(adm)

    def criar_documento_inicial(self ,  adm):

        dados_adm = self.get_dados_adm(adm)

        data_ajustada = "-".join(self.data.split('-')[0:2])

        documento  = ET.Element("documento")
        documento.set("cnpj" , dados_adm['cnpj'] )
        documento.set('codigoDocumento' ,  '5401')
        documento.set("tipoRemessa" , 'I')
        documento.set("dataBase" ,  data_ajustada)
        documento.set('nomeResponsavel' ,  dados_adm['representante'])
        documento.set("emailResponsavel" , "thiago.menezes@oliveiratrust.com.br")
        documento.set("telefoneResponsavel" , "02135140000")
        return documento
    
    def criar_fundos(self):
        fundos = ET.Element("fundos")
        return fundos
    
    
    def adicionar_fundos(self, fundo):
        self.elemento_fundos.append(fundo)
    
    

    def documento_5401(self , adm):
        base = self.criar_documento_inicial(adm)
 
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
