from io import BytesIO
import time
from numpy import fabs
import requests
import json
import pandas as pd
from datetime import timedelta, datetime
from sqlalchemy import create_engine
import sqlite3

from bs4 import BeautifulSoup
import pandas as pd


def criar_form_cetip(size):
    form_cetip = {
        'sort': "" , 
        'page': 1 ,
        'pageSize': size ,
        'group': "" ,
        'filter': "" ,
        'DepositariaID': 100578 ,
        'InvestidorID': "" ,
        'InstrumentoFinanceiroID':  "",
        'DataInicio': '03/08/2021' ,
        'DataFim': '03/08/2021' ,
    }
    return form_cetip




class IntactusRequests():    

    def criar_form_bolsa( self,size,data,  depositaria , instrumento=""):
        form_cetip = { "sort": "",
                        "page": 1,
                        "pageSize": size,
                        "group": "" ,
                        "filter": "" ,
                        "DepositariaID": depositaria,
                        "InvestidorID":  "",
                        "InstrumentoFinanceiroID": instrumento ,
                        "DataInicio": data,
                        "DataFim": data,
                        "TipoMovimentacaoID": ""}

        return form_cetip



        
    def login_intactus(self):
        login = "https://escriturador.oliveiratrust.com.br/intactus/iauth/autenticacao/login"
        # login_hml ="http://172.16.0.26/intactus/iauth/autenticacao/login"
        with requests.session() as s:
            r = s.get(login)
            soup = BeautifulSoup(r.content,'html.parser')
            token = soup.find('input',{'name' :"__RequestVerificationToken"})['value']
            form = {
                '__RequestVerificationToken': token,
                'usuario': "thiago.conceicao" ,
                'senha': "tAman1993**"
            }
            r = s.post(login,form)
            return s


    def get_email(self , int_id):
        with self.login_intactus() as s:
            r =  s.get(f"https://escriturador.oliveiratrust.com.br/intactus/icorp/emissor/editar/{int_id}")
            soup  = BeautifulSoup(r.content, "html")
            email  = soup.find("input", {"id": "Email"})
            return email['value']
        

    def get_depara(self):
        form_data = {
            'sort' :  "" ,
            'group': ""  , 
            "filter":  ""
        }
        with self.login_intactus() as s:
            r =  s.post("https://escriturador.oliveiratrust.com.br/intactus/escriturador/deparaativo/obtertodos" ,  form_data)
            depara = r.json()['Data']
            return [item['DeParaInstrumentoFinanceiroID'] for item in depara]
        


    def remover_depara(self , lista_ids):


        with self.login_intactus() as s:

            for id in lista_ids:
                print (id)
                url = f"https://escriturador.oliveiratrust.com.br/intactus/escriturador/deparaativo/remover/{id}"
                r =  s.get(url)
                soup = BeautifulSoup(r.content,'html.parser')
                token = soup.find('input',{'name' :"__RequestVerificationToken"})['value']
                form_token = {
                    "__RequestVerificationToken": token
                }
                r = s.post(url , form_token)
                
                
        
    
    def cadastrar_depara(self , depara):


        with self.login_intactus() as s:

            for id in depara:
                print (id)
                url = f"https://escriturador.oliveiratrust.com.br/intactus/escriturador/deparaativo/cadastrar"
                r =  s.get(url)
                soup = BeautifulSoup(r.content,'html.parser')
                token = soup.find('input',{'name' :"__RequestVerificationToken"})['value']
                form_token = {
                    "__RequestVerificationToken": token ,  
                    "CodigoOrigem": id['CodigoOrigem'] , 
                    "CodigoDestino": id['CodigoDestino']
                }
                r = s.post(url , form_token)
                




    def extrair_relatorio(self):
        report = "https://escriturador.oliveiratrust.com.br/intactus/escriturador/relatorio/posicaoconsolidadaativo"

        form = {
                "Formato": 'xlsx' , 
                'DataPosicao': '01/07/2022 ', 
                'DepositariaID_input': 'CETIP' , 
                'DepositariaID': '5' , 
        }
        with self.login_intactus() as s:
            r = s.post(report,form)
            df = pd.read_excel(BytesIO(r.content))
            return df

    def buscar_movimentacao(self,data,depositaria):
        movimento = "https://escriturador.oliveiratrust.com.br/intactus/escriturador/movimentacao/obtertodos"
        with self.login_intactus() as s:
            r = s.post(movimento,self.criar_form_bolsa(3,data,depositaria))
            r_trabalhada = json.loads(r.content.decode('utf-8'))['Total']
            r = s.post(movimento,self.criar_form_bolsa(r_trabalhada,data,depositaria))
            r_trabalhada2 = json.loads(r.content.decode('utf-8'))['Data']
            return r_trabalhada2




    def buscar_posicao(self,data,ativoid):
        movimento = "https://escriturador.oliveiratrust.com.br/intactus/escriturador/posicao/obtertodos"
        with self.login_intactus() as s:
            r = s.post(movimento,criar_form_bolsa(3,data,ativoid))
            r_trabalhada = json.loads(r.content.decode('utf-8'))['Total']
            r = s.post(movimento,criar_form_bolsa(r_trabalhada,data,ativoid))
            r_trabalhada2 = json.loads(r.content.decode('utf-8'))['Data']
            
            df = pd.DataFrame.from_dict(r_trabalhada2)
            if not df.empty:
                df['Depositaria'] = df['Depositaria'].apply(lambda x:x['Mnemonico'])
                df['InstrumentoFinanceiro'] = df['InstrumentoFinanceiro'].apply(lambda x : x['InstrumentoFinanceiroID'])
                df['cnpjinvestidor'] = df['Investidor'].apply(lambda x: f"{x['CPFCNPJMascara'].replace('.','').replace('-', '').replace('/', '')}" )
                df['Investidor'] = df['Investidor'].apply(lambda x: {x['Nome']} )

                campos  = ['InvestidorID',"Investidor" , "cnpjinvestidor" , "InstrumentoFinanceiroID","QuantidadeDisponivel","QuantidadeBloqueada","QuantidadeTotalDepositada"]
                return df[campos]
            else:
                return df

    def transformar_df(self, df , data):
        df['Depositaria'] = df['Depositaria'].apply(lambda x:x['Mnemonico'])
        df['InstrumentoFinanceiro'] = df['InstrumentoFinanceiro'].apply(lambda x : x['InstrumentoFinanceiroID'])
        df['cnpjinvestidor'] = df['Investidor'].apply(lambda x: f"{x['CPFCNPJMascara']}" )
        df['Investidor'] = df['Investidor'].apply(lambda x: x['Nome'] )
        df["dataconsulta"] =  datetime.strptime(data,"%d/%m/%y").strftime("%Y-%m-%d")                    
        campos  = ['InvestidorID',"Investidor" , "cnpjinvestidor" , "InstrumentoFinanceiroID","QuantidadeDisponivel","QuantidadeBloqueada","QuantidadeTotalDepositada" ,  "dataconsulta"] 
        return df[campos]

    def request_o2(self, session ,  url , ativo , data):
        r = session.post(url,self.criar_form_bolsa(3,data,ativo))
        r_trabalhada = json.loads(r.content.decode('utf-8'))['Total']
        r = session.post(url,self.criar_form_bolsa(r_trabalhada,data,ativo))
        r_trabalhada2 = json.loads(r.content.decode('utf-8'))['Data']                
        df = pd.DataFrame.from_dict(r_trabalhada2)
        return df 



    def buscar_lista_posicao(self , lista_ativos,engine , data):
            movimento = "https://escriturador.oliveiratrust.com.br/intactus/escriturador/posicao/obtertodos"
            with self.login_intactus() as s:    
                for item in lista_ativos:
                    print (item)
                    df = self.request_o2(s,movimento,item ,data)
                    if not df.empty:
                        df = self.transformar_df(df,data)    
                        df.to_sql('o2',con=engine, if_exists="append" , index=False)
                        

    def emissores_intactus(self):
        emissores = "https://escriturador.oliveiratrust.com.br/intactus/icorp/emissor/obtertodos"
        with self.login_intactus() as s:
            r = s.get(emissores)
            r_trabalhada = json.loads(r.content.decode('utf-8'))['Data']
            df = pd.DataFrame.from_dict(r_trabalhada)
            df['id_input'] = df['CNPJMascara'].apply(lambda x :  f'{x.replace(".","").replace("-","").replace("/","")}')
            df['id_input'] = df['id_input']  + " - " + df['Nome'] 
            return df

    def buscar_movimentacao_bolsa(self):
        movimento = "https://escriturador.oliveiratrust.com.br/intactus/escriturador/movimentacao/obtertodos"
        with self.login_intactus() as s:
            r = s.post(movimento,criar_form_bolsa(3))
            r_trabalhada = json.loads(r.content.decode('utf-8'))['Total']
            print (r_trabalhada)
            r = s.post(movimento,criar_form_bolsa(r_trabalhada))
            r_trabalhada2 = json.loads(r.content.decode('utf-8'))['Data']



    def buscar_movimentacao_bolsa(self):
        movimento = "http://172.16.0.26/intactus/escriturador/movimentacao/obtertodos"
        with self.login_intactus() as s:
            r = s.post(movimento,criar_form_bolsa(3))
            r_trabalhada = json.loads(r.content.decode('utf-8'))['Total']
            print (r_trabalhada)
            r = s.post(movimento,criar_form_bolsa(r_trabalhada))
            r_trabalhada2 = json.loads(r.content.decode('utf-8'))['Data']


    def criar_form_ativos(self,size):
        form_ativos = {
            'sort': '',
            'page': '1',
            'pageSize': size,
            'group':'' ,
            'filter': '',
            'descricao': "", 
            'dataInicioRelacionamento': "",
            'dataFimRelacionamento': "",
        }
        return form_ativos


    def ativos_intactus(self): 
        movimento = "https://escriturador.oliveiratrust.com.br/intactus/escriturador/ativo/ObterTodos"
        with self.login_intactus() as s:
            r = s.post(movimento,self.criar_form_ativos(3))
            print (r.content)
            r_trabalhada = json.loads(r.content.decode('utf-8'))['Total']
            r = s.post(movimento,self.criar_form_ativos(r_trabalhada))
            r_trabalhada2 = json.loads(r.content.decode('utf-8'))['Data']
            df = pd.DataFrame.from_dict(r_trabalhada2)  
            return df

    
    def relatorio_posicao_analitica(self , id , id_input , path):
 
        dados = {

        'Formato': 'xlsx' ,
        'DataInicio': '28/02/2023',
        'DataFim': '31/03/2023' ,
        'EmissorID_input': id_input,
        'EmissorID': id ,
        'TipoInstrumentoFinanceiroID_input': "",
        'TipoInstrumentoFinanceiroID': "",
        'InstrumentoFinanceiroID_input': "",
        'InstrumentoFinanceiroID': "",
        'InvestidorID_input': "",
        'InvestidorID': "",
        'AdministradorID_input': "",
        'AdministradorID': ""    }

        pos_analitica = "https://escriturador.oliveiratrust.com.br/intactus/escriturador/relatorio/posicaoanalitica"
        with self.login_intactus() as s:
            r = s.post(pos_analitica,dados)
            df = pd.read_excel(BytesIO(r.content))
            if not df.empty:
                df['CPF/CNPJ'] = df['CPF/CNPJ'].apply(str)
                df.to_excel(f"{path}/{id}.xlsx" , index=False)
            # open(f"{path}/{id}.xlsx" ,'wb').wb(r.content)

    def relatorio_posicao_analitica_sql(self , id , id_input ,cnpj ,   engine , data):
 
        dados = {

        'Formato': 'xlsx' ,
        'DataInicio': data,
        'DataFim': data ,
        'EmissorID_input': id_input,
        'EmissorID': id ,
        'TipoInstrumentoFinanceiroID_input': "",
        'TipoInstrumentoFinanceiroID': "",
        'InstrumentoFinanceiroID_input': "",
        'InstrumentoFinanceiroID': "",
        'InvestidorID_input': "",
        'InvestidorID': "",
        'AdministradorID_input': "",
        'AdministradorID': ""    }

        pos_analitica = "https://escriturador.oliveiratrust.com.br/intactus/escriturador/relatorio/posicaoanalitica"
        with self.login_intactus() as s:
            r = s.post(pos_analitica,dados)
            df = pd.read_excel(BytesIO(r.content))
            if not df.empty:
                df['CPF/CNPJ'] = df['CPF/CNPJ'].apply(str)
                df['CNPJ Fundo'] = cnpj
                df.to_sql("posicao_analitica" , con=engine , index=False , if_exists="append")
            # open(f"{path}/{id}.xlsx" ,'wb').wb(r.content)
    

def get_investidores_intactus():
    form = {
        'sort': "",
        'group': "",
        'filter': ""
    } 
    movimento = "http://172.16.0.26/intactus/escriturador/investidor/obtertodos"
    with requests.session() as s:
        r = s.post(movimento,form)
        r_trabalhada = json.loads(r.content.decode('utf-8'))['Total']
        r = s.post(movimento,form)
        r_trabalhada2 = json.loads(r.content.decode('utf-8'))['Data']
        df = pd.DataFrame.from_dict(r_trabalhada2)
    return df

def ajustar_data(data):
    dfdata = datetime.strptime(data, '%Y-%m-%d')
    data_ajustada = dfdata.strftime("%d/%m/%Y")
    return data_ajustada

def dados_ativo(data):
    result = {
        'data': ajustar_data(data['Data'][0:10]) ,
        'depositaria': data['DepositariaID'] , 
        'codigo': data['InstrumentoFinanceiro']['Codigo'], 
        'ISIN': data['InstrumentoFinanceiro']['ISIN'],
        'Investidor_id': data['Investidor']['InvestidorID'] ,
        'Investidor': data['Investidor']['Mnemonico'] , 
        'cpfcnpj': data['Investidor']['CPFCNPJ'] ,
        'TipoInstrumentoFinanceiro': data['InstrumentoFinanceiro']['TipoInstrumentoFinanceiro']['Codigo'],
        'Tipo Movimentacao': data['TipoMovimentacao']['Nome'] ,
        'Quantidade': data['Quantidade']
   }
    return result

def dados_ativo2(data):
    result = {
        'data': ajustar_data(data['Data'][0:10]) ,
        'depositaria': data['DepositariaID'] , 
        'codigo': data['InstrumentoFinanceiro']['Codigo'], 
        'ISIN': data['InstrumentoFinanceiro']['ISIN'],
        'Investidor_id': data['Investidor']['InvestidorID'] ,
        'Investidor': data['Investidor']['Mnemonico'] , 
        'cpfcnpj': data['Investidor']['CPFCNPJ'] ,
        'TipoInstrumentoFinanceiro': data['InstrumentoFinanceiro']['TipoInstrumentoFinanceiro']['Codigo'],
        'Quantidade': data['QuantidadeTotalDepositada']
   }
    return result

def baixar_movimentos_cetip():
    df = buscar_movimentacao_cetip()
    dfblank = {
        'data': [] ,
        'depositaria': [], 
        'codigo': [], 
        'ISIN': [],
        'Investidor_id': [] ,
        'Investidor': [] , 
        'cpfcnpj': [] ,
        'TipoInstrumentoFinanceiro': [],
        'Tipo Movimentacao': [] ,
        'Quantidade': []
    }
    for item in df:
        apoio = dados_ativo(item)
        dfblank['data'].append(apoio['data'])
        dfblank['depositaria'].append(apoio['depositaria'])
        dfblank['codigo'].append(apoio['codigo'])
        dfblank['ISIN'].append(apoio['ISIN'])
        dfblank['Investidor_id'].append(apoio['Investidor_id'])
        dfblank['Investidor'].append(apoio['Investidor'])
        dfblank['cpfcnpj'].append(apoio['cpfcnpj'])
        dfblank['TipoInstrumentoFinanceiro'].append(apoio['TipoInstrumentoFinanceiro'])
        dfblank['Tipo Movimentacao'].append(apoio['Tipo Movimentacao'])
        dfblank['Quantidade'].append(apoio['Quantidade'])
    consolidado = pd.DataFrame.from_dict(dfblank)
    consolidado.to_sql("movimento_cetip",con=engine ,if_exists='append',index=False)
    # consolidado.to_csv('mensal_cetip.csv',index=False)

def baixar_movimentos_bolsa():
    df = buscar_movimentacao_bolsa()
    dfblank = {
        'data': [] ,
        'depositaria': [], 
        'codigo': [], 
        'ISIN': [],
        'Investidor_id': [] ,
        'Investidor': [] , 
        'cpfcnpj': [] ,
        'TipoInstrumentoFinanceiro': [],
        'Tipo Movimentacao': [] ,
        'Quantidade': []
    }
    for item in df:
        apoio = dados_ativo(item)
        dfblank['data'].append(apoio['data'])
        dfblank['depositaria'].append(apoio['depositaria'])
        dfblank['codigo'].append(apoio['codigo'])
        dfblank['ISIN'].append(apoio['ISIN'])
        dfblank['Investidor_id'].append(apoio['Investidor_id'])
        dfblank['Investidor'].append(apoio['Investidor'])
        dfblank['cpfcnpj'].append(apoio['cpfcnpj'])
        dfblank['TipoInstrumentoFinanceiro'].append(apoio['TipoInstrumentoFinanceiro'])
        dfblank['Tipo Movimentacao'].append(apoio['Tipo Movimentacao'])
        dfblank['Quantidade'].append(apoio['Quantidade'])
    consolidado = pd.DataFrame.from_dict(dfblank)
    consolidado.to_sql("movimento_bolsa",con=engine ,if_exists='append',index=False)
    # consolidado.to_csv('mensal_bolsa.csv',index=False)

def baixar_posicao_geral():
    df = buscar_posicao_geral()
    dfblank = {
        'data': [] ,
        'depositaria': [], 
        'codigo': [], 
        'ISIN': [],
        'Investidor_id': [] ,
        'Investidor': [] , 
        'cpfcnpj': [] ,
        'TipoInstrumentoFinanceiro': [],
        'Quantidade': []
    }
    for item in df:
        apoio = dados_ativo2(item)
        dfblank['data'].append(apoio['data'])
        dfblank['depositaria'].append(apoio['depositaria'])
        dfblank['codigo'].append(apoio['codigo'])
        dfblank['ISIN'].append(apoio['ISIN'])
        dfblank['Investidor_id'].append(apoio['Investidor_id'])
        dfblank['Investidor'].append(apoio['Investidor'])
        dfblank['cpfcnpj'].append(apoio['cpfcnpj'])
        dfblank['TipoInstrumentoFinanceiro'].append(apoio['TipoInstrumentoFinanceiro'])
        dfblank['Quantidade'].append(apoio['Quantidade'])
    consolidado = pd.DataFrame.from_dict(dfblank)
    print (consolidado.head())
    consolidado.to_sql("posicoes",con=engine ,if_exists='append',index=False)
    # consolidado.to_csv('posicao_geral-3108-cetip.csv',index=False)

def to_excel():
    posicoes = pd.read_sql_table('posicoes',con=engine)
    posicoes.to_csv('p_total.csv',index=False)
    posicoes_bolsa = posicoes[posicoes.depositaria == 2][['data','ISIN','Quantidade']]
    posicoes_cetip = posicoes[posicoes.depositaria == 3][['data','codigo','Quantidade']]
    posicoes_bolsa.to_csv('posicoes_b3_agosto.csv',index=False)
    posicoes_cetip.to_csv('posicoes_cetip_agosto.csv',index=False)
    mov_bolsa = pd.read_sql_table('movimento_bolsa',con=engine).to_csv('movimentos_b3_agosto.csv',index=False)
    mov_cetip = pd.read_sql_table('movimento_cetip',con=engine).to_csv('movimentos_cetip_agosto.csv',index=False)
   
def main():
    baixar_posicao_geral()
    #baixar_movimentos_bolsa()
    #baixar_movimentos_cetip()
    to_excel()

def get_all_investidores():
    sql = 'DROP TABLE IF EXISTS investidores;'
    engine.execute(sql)
    t = get_investidores()
    df =  {
        'InvestidorID':[], 
        'CPFCNPJFormatado': [], 
        'CPFCNPJ': [],
        'Nome': [], 
        'Email': [], 
        'Mnemonico': [],
        'DescricaoSelecao':[]
    
    }   

    for item in t:
        df['InvestidorID'].append(item['InvestidorID'])
        df['CPFCNPJFormatado'].append(item['CPFCNPJFormatado'])
        df['CPFCNPJ'].append(item['CPFCNPJ'])
        df['Nome'].append(item['Nome'])
        df['Email'].append(item['Email'])
        df['Mnemonico'].append(item['Mnemonico'])
        df['DescricaoSelecao'].append(item['DescricaoSelecao'])
    base = pd.DataFrame.from_dict(df)
    base.to_sql("investidores",con=engine ,if_exists='append',index=False)

def create_base_dict(ndict):
    base = {
        'ativoId':ndict['ativoId'] ,
        'investidorId': ndict['investidorId'],
        'cpfCnpjInvestidor':  ndict['cpfCnpjInvestidor'],
        'identificadorInvestidor':  ndict['identificadorInvestidor'] ,
        'dataMovimentacaoString':  ndict['dataMovimentacaoString'],
        'descricaoTipoMovimentacao': ndict['descricaoTipoMovimentacao'], 
        'quantidadeFormatada': ndict['quantidadeFormatada'] , 
    }
    return base

def get_movimentacoes_escriturais_maps():
    maps_escritural = movimentacoes_escriturais()
    df  = []
    for item in maps_escritural:    
        df.append(create_base_dict(item))
        
    testedf = pd.DataFrame.from_dict(df)
    testedf['cpfCnpjInvestidor'] = testedf['cpfCnpjInvestidor'].apply(lambda x: x.replace(".","").replace("-","").replace("/",""))
    testedf.to_sql("movimentacoes_escriturais_maps",con=engine ,if_exists='append',index=False)
    
def investidores_a_cadastrar():
    extract = pd.read_sql_table('movimentacoes_escriturais_maps',con=engine)['cpfCnpjInvestidor'].drop_duplicates().values
    lista_cabral = pd.read_sql_table('investidores',con=engine)['CPFCNPJ'].drop_duplicates().values
    a_cadastrar = {
        'cnpj': []
    }
    for item in extract:
        if int(item) in lista_cabral:
            print (item)
            #a_cadastrar['cnpj'].append(item)
    # pd.DataFrame.from_dict(a_cadastrar).to_csv('a_cadastrar.csv',index=False)

def investidores_cadastrados():
    extract = pd.read_sql_table('movimentacoes_escriturais_maps',con=engine)['cpfCnpjInvestidor'].drop_duplicates().values
    base_cabral = pd.read_sql_table('investidores',con=engine)
    lista_cabral = pd.read_sql_table('investidores',con=engine)['CPFCNPJ'].drop_duplicates().values
    cadastrados = {
        'id' :  [],
        'InvestidorID_input':  []        
    }
    for item in extract:        
        if int(item) in lista_cabral:
            filtro = base_cabral[base_cabral['CPFCNPJ']==int(item)]
            cadastrados['id'].append(filtro['InvestidorID'].values[0])
            cadastrados['InvestidorID_input'].append(f"{filtro['CPFCNPJ'].values[0]} - {filtro['Mnemonico'].values[0]}")       
    pd.DataFrame.from_dict(cadastrados).to_csv("investidores_cadastrados.csv",index=False)   

def exportar_lista_investidores_a_cadastrar():
    df_itens = pd.read_csv('a_cadastrar.csv')['cnpj'].drop_duplicates().values

    mascara = {
        'cnpjcpf': [],
        'nome' :  [] ,
        'cpfcnpjformatado': [],
        'descricaoNaturezaInvestidor':[] ,
    }
    for investidor in df_itens:
        df = consulta_investidor(investidor)
        mascara['cnpjcpf'].append(investidor)
        mascara['nome'].append(df['nome'])
        mascara['cpfcnpjformatado'].append(df['cpfCnpj'])
        mascara['descricaoNaturezaInvestidor'].append(df['descricaoNaturezaInvestidor']) 


    pd.DataFrame.from_dict(mascara).to_csv("escriturais.csv",index=False)

def criar_form_movimentacao(investidor,ativo,valores, token ):
    try:
        form_movimentacao = {
        '__RequestVerificationToken': token ,
        'Data': '01/08/2021' , 
        'EscrituradorID_input': '36113876000191 - OLIVEIRA TRUST DISTRIBUIDORA DE TÍTULOS E VALORES MOBILIÁRIOS S.A.' , 
        'EscrituradorID': '2' , 
        'TipoMovimentacaoID_input': 'DEPÓSITO', 
        'TipoMovimentacaoID': '9' , 
        'InvestidorID_input': investidor['DescricaoSelecao'], 
        'InvestidorID': investidor['InvestidorID'], 
        'InstrumentoFinanceiroID_input': ativo['InstrumentoFinanceiroID_input'] , 
        'InstrumentoFinanceiroID': ativo['InstrumentoFinanceiroID'] , 
        'PrecoUnitario': valores['pu'] , 
        'Quantidade': valores['quantidade']  , 
        'ValorTotal': valores['valorTotal']  , 
    }
    except:
        form_movimentacao = 0
    return form_movimentacao

def replicar_movimentacao():
    # df = pd.read_sql_table('movimentacoes_escriturais_maps',con=engine)
    # df.to_csv("movimentacoes_maps.csv")
    cadastro_movimentacao = "http://172.16.0.26/intactus/escriturador/movimentacaodepositoretirada/cadastrar"
    movimentacoes = pd.read_csv('import.csv')
    investidores_intactus  = pd.read_sql("investidores",con=engine)
    ativos_intactus  = pd.read_sql("ativos_intactus",con=engine)
    ativos_maps  = pd.read_sql("ativos_maps",con=engine)
    for rows in movimentacoes.iterrows():
        try:
            print(rows[1]['cpfCnpjInvestidor'])
            investidor = investidores_intactus[investidores_intactus['CPFCNPJ'] == int(rows[1].to_dict()['cpfCnpjInvestidor'])][['InvestidorID','DescricaoSelecao']].to_dict('records')[0]
        except:
            investidor = 1
            
        data = rows[1]['dataMovimentacaoString']
        valores = {
            'pu' : 1  ,
            'quantidade':  rows[1]['quantidadeFormatada'],
            'valorTotal':  rows[1]['quantidadeFormatada']

        }
        maps_data = ativos_maps[ativos_maps['id'] == rows[1]['ativoId']]['codigoCetip'].values[0]
        intactus_data = ativos_intactus[ativos_intactus['Codigo'] == maps_data ]['InstrumentoFinanceiroID'].values
        try:
            ativo = {
                'InstrumentoFinanceiroID_input': maps_data ,      
                'InstrumentoFinanceiroID':  int([ativo for ativo in intactus_data][0])
            }
        except:
            ativo = {
                'InstrumentoFinanceiroID_input': maps_data ,      
                'InstrumentoFinanceiroID':  1
            }
        print (investidor)
        print (ativo)
        if ativo['InstrumentoFinanceiroID'] != 1 and investidor!=1:
            #print ("lançando...")
            with requests.session() as s:
                r = s.get(cadastro_movimentacao)
                soup1 = BeautifulSoup(r.content,'html.parser')
                token = soup1.find("input", {"name":'__RequestVerificationToken'})['value']
                form_movimentacao = criar_form_movimentacao(investidor,ativo,valores,token)
                r = s.post(cadastro_movimentacao,form_movimentacao)
                print(form_movimentacao)
                print (r)
                
        else:
            pass
    print ("lançamentos efetuados")

def tabelas_base_escriturador():
    #investidores = get_investidores()
    #engine.execute('drop table if exists investidores')
    engine.execute('drop table if exists ativos')
    #investidores.to_sql('investidores',con=engine,if_exists='append',index=False)
    ativos_intactus = atualizar_ativos_cadastrados()
    ativos_intactus.to_sql('ativos',con=engine,if_exists='append',index=False)

def get_multiple():    
    datainicial = datetime(2021,10,1)
    for n in range(26):
        t =  datainicial + timedelta(days=n)
        print (t)
        try:
            posicao = buscar_posicao_geral(t.strftime("%d/%m/%Y"))
            posicao.to_sql('posicao',con=engine , if_exists='append',index=False)
            posicao.to_csv(t.strftime("%d-%m-%Y"))
        except Exception as e:
            print (e)
            continue


sql = '''select posicao.Data,   ativos.Codigo , ativos.ISIN , posicao.Depositaria , investidores.CPFCNPJ ,  investidores.Mnemonico, posicao.QuantidadeDisponivel  from posicao
inner join investidores
on posicao.InvestidorID = investidores.InvestidorID
inner join ativos
on posicao.InstrumentoFinanceiroID =  ativos.InstrumentoFinanceiroID'''



def definir_tipo_pessoa(cpfcnpj):
    if len(cpfcnpj.replace(".","").replace('/',"").replace("-","")) <= 11:
        id = 1
        idinput = "PESSOA FÍSICA"
    else:
        id = 2
        idinput = "PESSOA JURÍDICA"
    return {
        'input': idinput  ,
        'id': id ,
    }




def definir_perfil_tributario(perfil):
    resultado  = {
        'PJ financeira': {"PerfilTributarioID_input":"INSTITUIÇÃO FINANCEIRA" , "PerfilTributarioID":3},
        'PJ direito privado sem fins lucrativos': {"PerfilTributarioID_input":  'PESSOA JURIDICA SEM FINS LUCRATIVOS' , "PerfilTributarioID":4},
        'PJ':  {"PerfilTributarioID_input":  "PESSOA JURIDICA" , "PerfilTributarioID": 10 },
        'PF':  {"PerfilTributarioID_input":  "PESSOA FISICA" , "PerfilTributarioID": 9  },
        'INR PJ - Paraíso fiscal': {"PerfilTributarioID_input":"PESSOA JURIDICA PARAISO FISCAL"  , "PerfilTributarioID":  6 },
        'INR PJ - Não paraíso fiscal': {"PerfilTributarioID_input":   "PESSOA JURIDICA NÃO RESIDENTE" , "PerfilTributarioID": 5 } ,
        'INR PF - Não paraíso fiscal':  {"PerfilTributarioID_input":  "PESSOA FISICA NÃO RESIDENTE" , "PerfilTributarioID":  1   } ,
        'FI isento de IR':  {"PerfilTributarioID_input": "NÃO CLASSIFICADO" , "PerfilTributarioID":  0 },
        'FI':  {"PerfilTributarioID_input": "FUNDO DE INVESTIMENTO"  , "PerfilTributarioID": 7 }
}
    return resultado[perfil]



def cadastrar_investidores(cpfcnpj,nome,natureza , email):
    tipo_pessoa = definir_tipo_pessoa(cpfcnpj)
    perfil_trib =  definir_perfil_tributario(natureza)
    url  = "http://172.16.0.26/intactus/icorp/investidor/cadastrar"
    with login_intactus() as s:
        r = s.get(url)
        soup = BeautifulSoup(r.content,'html.parser')
        print (soup)
        token = soup.find('input',{'name' :"__RequestVerificationToken"})['value']
        form  =  {  '__RequestVerificationToken': token , 
                    'PessoaID': 0,
                    'ImagemURL':"data:image/jpeg;base64" ,
                    'TipoPessoaID_input': tipo_pessoa['input'],
                    'TipoPessoaID': tipo_pessoa['id'],
                    'CPFCNPJMascara': cpfcnpj,
                    'Nome': nome,
                    'Email': email,
                    'PerfilTributarioID_input': perfil_trib['PerfilTributarioID_input'],
                    'PerfilTributarioID': perfil_trib['PerfilTributarioID'],
                    'IndicadorResideExterior': 'false',
                    'Mnemonico': nome,
                    'PerfilInvestidorID': 0,
                    'TipoInvestidorID': 0,
                    'ClassificacaoAnbimaID': 0,
                    'ClassificacaoBacenID': 0,
                    'ClassificacaoCVMID': 0,
                    'DataInicioRelacionamento': '12/07/2022',
                    'DataFimRelacionamento':'' ,
                }
        print (form)
        r =  s.post(url,form)







def enviar_arquivos():
    
    header = {'Content-Type':'multipart/form-data'}
    envio_arquivo  = "http://172.16.0.26/intactus/escriturador/importacaomanual/importar"
    base = "http://172.16.0.26/intactus/escriturador/importacaomanual"


    with login_intactus() as s:
        r = s.get(base)
        soup = BeautifulSoup(r.content,'html.parser')
        token = soup.find('input',{'name' :"__RequestVerificationToken"})['value']
        form = {
                '__RequestVerificationToken': token
            }
        keys = r.cookies.get_dict().keys()
        l = []
        for i in keys:
            l.append(i)
        cook = {l[0]: r.cookies.get_dict()[l[0]]}

        r = s.post(envio_arquivo,files={'BOLSA - ESGM': open('ESGM20211217002220.DAT', 'rb')},data=form ,headers=header,cookies=cook)
        # print (r)
        print (r.text)
