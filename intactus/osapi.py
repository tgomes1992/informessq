import  requests
import json
import logging
import pandas as pd
import pymongo
import asyncio
import pandas as pd
import json
from GERACAO_5401 import ExtratorCotas
from datetime import datetime


class o2Api():


    url = "https://escriturador.oliveiratrust.com.br/intactus/iauth/api/auth/token"

    Base_URL= "escriturador.oliveiratrust.com.br/intactus/escriturador/api"

    def __init__(self ,  client_id , client_secret):
        self.client_id = client_id
        self.client_secret = client_secret

    def _get_token_form(self):
        return {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }


    def get_token(self):
        r = requests.post(self.url , self._get_token_form())
        json_response = json.loads(r.text)
        return json_response['access_token']


    def movimentacao_entrar(self ,  cpfcnpj  , instrumento , data, quantidade,  pu):
        corpo =  {            	
            "cpfcnpjInvestidor": cpfcnpj,
            "codigoInstrumentoFinanceiro": instrumento,
            "data": data,
            "quantidade": quantidade,
            "precoUnitario": pu,
            "depositaria": "ESCRITURAL"
        }
        return corpo
    
    def movimentacao_sair(self ,  cpfcnpj  , instrumento , data, quantidade,  pu):
        corpo =  {            	
            "cpfcnpjInvestidor": cpfcnpj,
            "codigoInstrumentoFinanceiro": instrumento,
            "data": data,
            "quantidade": quantidade,
            "precoUnitario": pu,
            "depositaria": "ESCRITURAL"
        }
        return corpo
    

    def movimentacao_bloquear_body(self , cpfcnpj, instrumento, data, quantidade, pu,motivo ,  depositaria):
        corpo = {
            "cpfcnpjInvestidor": cpfcnpj,
            "codigoInstrumentoFinanceiro": instrumento,
            "data": data,
            "quantidade": quantidade,
            "precoUnitario": pu,
            "motivo": motivo,
            "depositaria": depositaria
        }
        return json.dumps(corpo)

    def movimentacao_desbloquear_body(self, cpfcnpj, instrumento, data, quantidade, pu, depositaria):
        corpo = {
            "cpfcnpjInvestidor": cpfcnpj,
            "codigoInstrumentoFinanceiro": instrumento,
            "data": data,
            "quantidade": quantidade,
            "precoUnitario": pu,
            "depositaria": depositaria
        }
        return json.dumps(corpo)


    def registrar_movimentos(self,lista):
        headers = {
                'Authorization': f'Bearer {self.get_token()}' ,
                'Content-Type': 'application/json'
        }
        logging.debug('BLOQUEIOS')

        for item in lista['bloqueios']:
                body = self.movimentacao_bloquear_body(item['CpfCnpj'],item['Ativo'],item['Data'],
                                                       item['Quantidade'],1 , item["Motivo gravame"] ,"ESCRITURAL")
                registro = requests.post("https://escriturador.oliveiratrust.com.br/intactus/escriturador/api/movimentacao/bloquear", data=body,
                                         headers=headers)
                logging.debug(registro.content) # trocar o print pelo log das execucoes
                logging.debug(body)
        logging.debug('DESBLOQUEIOS')
        for item in lista['desbloqueios']:
                body = self.movimentacao_desbloquear_body(item['CpfCnpj'],item['Ativo'],item['Data'],
                                                       item['Quantidade'],1,"ESCRITURAL")
                registro = requests.post("https://escriturador.oliveiratrust.com.br/intactus/escriturador/api/movimentacao/desbloquear", data=body,
                                         headers=headers)
                logging.debug(registro.content)  # trocar o print pelo log das execucoes
                logging.debug(body)
        logging.debug('ENTRADAS')
        for item in lista['entradas']:
                body = self.movimentacao_entrar(item['CpfCnpj'],item['Ativo'],item['Data'],
                                                       item['Quantidade'],1,"ESCRITURAL")
                registro = requests.post("https://escriturador.oliveiratrust.com.br/intactus/escriturador/api/movimentacao/entrar", data=body,
                                         headers=headers)
                logging.debug(registro.content)  # trocar o print pelo log das execucoes
                logging.debug(body)
        logging.debug('SAIDAS')
        for item in lista['saidas']:
                body = self.movimentacao_entrar(item['CpfCnpj'],item['Ativo'],item['Data'],
                                                       item['Quantidade'],1,"ESCRITURAL")
                registro = requests.post("https://escriturador.oliveiratrust.com.br/intactus/escriturador/api/movimentacao/sair", data=body,
                                         headers=headers)
                print (registro.content) # trocar o print pelo log das execucoes
                logging.debug(body)



    def get_posicao(self, data,  codigoInstrumentoFinanceiro ,  cd_jcot ,  headers , cota ):

        url = f"https://escriturador.oliveiratrust.com.br/intactus/escriturador/api/Posicao/obterpordatainvestidorinstrumentofinanceiro?codigoInstrumentoFinanceiro={codigoInstrumentoFinanceiro}&data={data}"
        
        request =  requests.get(url,headers=headers)

        retorno = json.loads(request.content)['dados']

        df = pd.DataFrame.from_dict(retorno)

        df['cd_escritural'] =  codigoInstrumentoFinanceiro
        df['cd_jcot'] = cd_jcot
        df['valor_cota'] = cota

        return df


    def get_posicao_fintools(self, data,  codigoInstrumentoFinanceiro ,  cd_jcot , cota ,  headers ):
        url = f"https://escriturador.oliveiratrust.com.br/intactus/escriturador/api/fintools/investidor/posicao/obterpordatainstrumentofinanceiro?codigoInstrumentoFinanceiro={codigoInstrumentoFinanceiro}&data={data}"        
        request =  requests.get(url,headers=headers)
        retorno = json.loads(request.content)['dados']
        posicoes = retorno['posicoes']
        cnpj_emissor = retorno['instrumentoFinanceiro']['cnpjEmissor']
        df = pd.DataFrame.from_dict(posicoes)
        df['investidor'] = df['investidor'].apply(lambda x: str(x['cpfcnpj']))
        df["cnpj_fundo"] = cnpj_emissor
        df['cd_escritural'] =  codigoInstrumentoFinanceiro
        df['cd_jcot'] = cd_jcot
        df['cota'] = cota
        df['pl'] = df['quantidadeTotalDepositada'] * df['cota']
        return df


    async def get_posicao_fintools_async(self, data, codigoInstrumentoFinanceiro, headers ,  engine):
            url = f"https://escriturador.oliveiratrust.com.br/intactus/escriturador/api/fintools/investidor/posicao/obterpordatainstrumentofinanceiro?codigoInstrumentoFinanceiro={codigoInstrumentoFinanceiro}&data={data}"

            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.get(url) as response:
                    response_data = await response.read()
                    

            retorno = json.loads(response_data)['dados']

            if not retorno:
                 pass
            else:
                posicoes = retorno['posicoes']
                cnpj_emissor = retorno['instrumentoFinanceiro']['cnpjEmissor']
                df = pd.DataFrame.from_dict(posicoes)            
                if not df.empty:
                    df['investidor'] = df['investidor'].apply(lambda x: x['cpfcnpj'])
                    df["cnpj_fundo"] = cnpj_emissor
                    df['cd_escritural'] = codigoInstrumentoFinanceiro
                    df['dataconsulta'] =  data
                    df.to_sql("posicoeso2",con=engine , if_exists="append")
    

    async def get_posicao_list_fintools_async(self, data, items, engine):
        headers = {
            'Authorization': f'Bearer {self.get_token()}',
            'Content-Type': 'application/json'
        }

        tasks = []

        for item in items:
            tasks.append(self.get_posicao_fintools_async(data, item['codigosInstrumentosFinanceiros'], headers, engine))

        await asyncio.gather(*tasks)



    def get_posicao_list_fintools(self, data  , items , mongo_engine  ,  index):
        headers = {
                'Authorization': f'Bearer {self.get_token()}' ,
                'Content-Type': 'application/json'
        }

        for item in items:
            try:
                #todo lógica para consultar a cota do item antes de realizar a busca no o2
                print (item['descricao'])
                data_datetime = datetime.strptime(data, "%Y-%m-%d")
                cota = ExtratorCotas(data_datetime).buscar_cota({"codigo": item['cd_jcot'] ,  "dataPosicao": data})
                print (cota)               
                df = self.get_posicao(data , item['descricao'] , item['cd_jcot'] , cota , headers)
                mongo_engine.insert_many(df.to_dict('records'))
            except Exception as e:
                print (e)

    


    def get_posicao_mongo(self, data,  codigoInstrumentoFinanceiro  ,  header):
        url = f"https://escriturador.oliveiratrust.com.br/intactus/escriturador/api/Posicao/obterpordatainvestidorinstrumentofinanceiro?codigoInstrumentoFinanceiro={ codigoInstrumentoFinanceiro['DescricaoSelecao'] }&data={data}"
        
        request =  requests.get(url,headers=header)

        retorno = json.loads(json.loads(request.content)['jsonRetorno'])

        if len(retorno) > 0:
            for item in retorno:
                item['dataconsulta'] = data
                item["ativoID"] = codigoInstrumentoFinanceiro['InstrumentoFinanceiroID']

        return retorno


    def get_posicao_list(self, listaAtivos,data,engine):
        headers = {
                'Authorization': f'Bearer {self.get_token()}' ,
                'Content-Type': 'application/json'
        }
        for item in listaAtivos:
            try:
                print (item['DescricaoSelecao'])
                df =  self.get_posicao(data,item,headers)
                if not df.empty:
                    df['dataconsulta'] =  data
                    df.to_sql("posicoeso2",con=engine , if_exists="append")
            except Exception as e:
                print (e)


    def get_posicao_list_mongo(self, listaAtivos,data,engine):
        headers = {
                'Authorization': f'Bearer {self.get_token()}' ,
                'Content-Type': 'application/json'
        }
        for item in listaAtivos:
            try:
                print (item['DescricaoSelecao'])
                df =  self.get_posicao_mongo(data,item,headers)
                engine['posicoeso2'].insert_many(df)      
                engine['extracao_ok'].insert_one({"ativo":item["DescricaoSelecao"]})                   
            except Exception as e:
                engine['extracao_com_erro'].insert_one({"ativo":item['DescricaoSelecao']})   
                print (e)


    def get_ativos(self):
        headers = {
                'Authorization': f'Bearer {self.get_token()}' ,
                'Content-Type': 'application/json'
        }


        url = "https://escriturador.oliveiratrust.com.br/intactus/escriturador/api/instrumentofinanceiro/obtertodos"    


        request =  requests.get(url,headers=headers)

        retorno = json.loads(request.content)['dados']

        df = pd.DataFrame.from_dict(retorno)
        
        df['cnpjEmissor'] = df['cnpjEmissor'].apply(str)


        return df
    

    
    def valor_cota_adicionar(self , df_cotas):
        headers = {
                'Authorization': f'Bearer {self.get_token()}' ,
                'Content-Type': 'application/json'
        }
        resultado = []

        url = "https://escriturador.oliveiratrust.com.br/intactus/escriturador/api/valorcota/adicionar"    

        cotas = [ { "data": datetime.strptime(item['Data'] , "%d/%m/%Y").strftime("%Y-%m-%d"),
                    "valor":item['Valor'],
                    "descricaoCodigoInstrumentoFinanceiro": item['Ativo']
                    } for item in df_cotas.to_dict("records")]

        for cota in cotas:
            ndict  = {}
            
            request =  requests.post(url,headers=headers , data=json.dumps(cota) )
            ndict['input'] =  cota
            ndict['retorno'] = request.json()['mensagensExecucao'][0]['mensagem']
            resultado.append(ndict)
            print (cota)

        return pd.DataFrame.from_dict(resultado)


