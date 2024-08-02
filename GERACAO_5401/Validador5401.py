import xml.etree.ElementTree as ET
import pandas as pd
import requests
from io import BytesIO
from .validador_cpf_cnpj import ValidadorCpfCnpj
from xml.dom import minidom
import json
from concurrent.futures import ThreadPoolExecutor

endereco = "https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv"

liquidacao = requests.get(endereco)

df = pd.read_csv(BytesIO(liquidacao.content), delimiter=";", encoding="ANSI")
df['CNPJ_FUNDO'] = df['CNPJ_FUNDO'].apply(lambda x: x.replace(".", "")
                                          .replace("/", "")
                                          .replace("-", ""))


def get_fundos_nome(cnpj):
    try:
        return df[df['CNPJ_FUNDO'] == cnpj].to_dict("records")[0]['DENOM_SOCIAL']
    except:
        return "na"


class XML_5401:

    def __init__(self, path, nome):
        self.tree = ET.parse(path)
        self.root = self.tree.getroot()
        self.filename = nome
        # self.df_cotistas = pd.read_excel("cotistas.xlsx" , dtype=str)
        # self.lista_cotistas = list(self.df_cotistas['identificacao'].values)

    def get_fundos(self):
        fundos = []
        for item in self.root.iter("fundo"):
            fundo = {
                "cnpjFundo": item.get('cnpjFundo'),
                "quantidadeCotas": item.get("quantidadeCotas"),
                "quantidadeCotistas": item.get("quantidadeCotistas"),
                "plFundo": item.get("plFundo"),
            }
            fundos.append(fundo)
        df = pd.DataFrame.from_dict(fundos)
        df['nome_fundo'] = df['cnpjFundo'].apply(get_fundos_nome)
        return df

    def get_cotistas(self):
        cotistas = []
        for item in self.root.iter("cotista"):
            cotista = {
                "tipoPessoa": item.get("tipoPessoa"),
                "identificacao": item.get("identificacao"),
                "classificacao": item.get("classificacao")
            }
            cotistas.append(cotista)
        df = pd.DataFrame.from_dict(cotistas)
        df['validacao_cpf_cnpj'] = df['identificacao'].apply(ValidadorCpfCnpj().definir_validacao)
        return df.drop_duplicates()

    def get_cotas(self):
        cotas = []
        for item in self.root.iter("fundo"):
            cota_element = item.iter("cota")
            for papel_cota in cota_element:
                cota = {
                    "cnpj_fundo": item.get("cnpjFundo"),
                    'tipoCota': papel_cota.get('tipoCota'),
                    "qtdeCotas": papel_cota.get("qtdeCotas"),
                    "valorCota": papel_cota.get("valorCota"),
                }
                cotas.append(cota)
        df = pd.DataFrame.from_dict(cotas)
        return df.drop_duplicates()

    # def change_cotistas_job(self , fundo):
    #     cnpj_fundos = fundo.attrib.get("cnpjFundo")
    #     cotistas = fundo.findall(".//cotista")
    #     for cotista in cotistas:
    #          if cotista.attrib.get("identificacao") in self.lista_cotistas:
    #               tp_pessoa = self.df_cotistas[self.df_cotistas['identificacao'] ==  str(cotista.attrib.get("identificacao"))].to_dict("records")[0]
    #               cotista.set("tipoPessoa" , str(tp_pessoa['tipoPessoa']))
    #               cotista.set("classificacao" , str(tp_pessoa['classificacao']))

    def ajuste_elemento_fundos(self, fundo):
        '''ajuste de quantidade de cotistas'''
        for item in fundo:
            cotistas = item
            qtd_cotistas = len(cotistas)
        fundo.set('quantidadeCotistas', str(qtd_cotistas))

        '''ajuste da quantidade de cotas e pl'''

    def ajuste_elemento_cotista(self):
        pass

    def ajuste_elemento_cotas(self):
        pass

    def arquivo_jcot(self, path):
        tree = ET.parse(path)
        root = tree.getroot()
        return root.findall(".//fundo")

    def ajuste_pl_fundo(self, string_base):
        if len(string_base.split(".")[1]) == 1:
            return string_base + "0"
        else:
            return string_base

    def ajuste_quantidade_pl_fundos(self, fundo):
        cnpj_fundo = fundo.attrib.get("cnpjFundo", "")
        print(cnpj_fundo)
        cotas_fundo = round(float(fundo.attrib.get("quantidadeCotas")), 2)
        total_valor_cotas = 0
        valor_total_cotistas = 0
        cotistas = fundo.findall(".//cotista")
        for cotista in cotistas:
            # print (cotista)
            cotas = cotista.findall(".//cota")
            for cota in cotas:
                qtde_cotas = float(cota.attrib.get("qtdeCotas"))
                valor_cota = float(cota.attrib.get("valorCota"))
                total_valor_cotas += qtde_cotas
                valor_total_cotistas += qtde_cotas * valor_cota

        fundo.set("plFundo", self.ajuste_pl_fundo(str(round(valor_total_cotistas, 2))))
        fundo.set("quantidadeCotas", self.ajuste_pl_fundo(str(round(total_valor_cotas, 10))))

    def ajuste_tipo_cota(self, fundo):
        cotas = fundo.findall(".//cota")
        for cota in cotas:
            cota.set("tipoCota", "1")

    def valida_pco(self, string_cotistas):
        if len(string_cotistas) == 15:
            return True
        else:
            return False

    def cotas_to_dataframe(self, cotas):
        # Parse the XML data

        data = []
        for cota in cotas:
            attributes = cota.attrib
            data.append(attributes)

        # Convert list of dictionaries to a DataFrame
        df = pd.DataFrame(data)

        # # Convert numerical columns to appropriate data types

        df['tipoCota'] = pd.to_numeric(df['tipoCota'])
        df['qtdeCotas'] = pd.to_numeric(df['qtdeCotas'])

        return df

    def ajuste_pco(self, fundo):
        pco = []
        try:

            fundo_parse = ET.fromstring(ET.tostring(fundo))

            cotistas = fundo_parse.findall(".//cotista")

            for cotista in cotistas:
                try:
                    string_cotista = cotista.attrib.get("identificacao", "")
                    tipo_pessoa = cotista.attrib.get("tipoPessoa", "")
                    classificacao = cotista.attrib.get("classificacao", "")

                    cotas = cotista.findall(".//cota")

                    cotas_df = self.cotas_to_dataframe(cotas)

                    if self.valida_pco(string_cotista):
                        corpo_pco = {
                            "identificacao": string_cotista,
                            "tipoPessoa": tipo_pessoa,
                            "classificacao": classificacao,
                            "cotas": cotas_df.to_dict('records')

                        }
                        pco.append(corpo_pco)


                    else:

                        continue
                except Exception as e:
                    print(e)

            return pco


        except Exception as e:
            print(e)

    def remover_cotistas(self, fundo, cotistas):
        try:
            investidores = fundo.find(".//cotistas")
            cotistas_tag = investidores.findall(".//cotista")

            ids = [item['identificacao'] for item in cotistas]

            for investidor in cotistas_tag:
                if investidor.attrib.get("identificacao", "") in ids:
                    try:
                        investidores.remove(investidor)
                    except Exception as e:
                        print(e)
        except Exception as e:
            print(e)

    def valida_cpf_cnpj(self, numero):
        # Remove caracteres não numéricos
        numero = ''.join(filter(str.isdigit, str(numero)))

        # Verifica se é CPF (11 dígitos)
        if len(numero) <= 11:
            cpf = [int(digito) for digito in numero.zfill(11)]
            # Calcula o primeiro dígito verificador
            dv1 = sum([(i + 1) * cpf[i] for i in range(9)]) % 11
            dv1 = 0 if dv1 < 2 else 11 - dv1
            # Calcula o segundo dígito verificador
            dv2 = sum([(i + 1) * cpf[i] for i in range(10)]) % 11
            dv2 = 0 if dv2 < 2 else 11 - dv2
            # Retorna True se os dígitos verificadores estão corretos
            # return cpf[9] == dv1 and cpf[10] == dv2
            return "CPF"

        # Verifica se é CNPJ (14 dígitos)
        elif len(numero) <= 14:
            cnpj = [int(digito) for digito in numero.zfill(14)]
            # Calcula o primeiro dígito verificador
            dv1 = (sum([(i % 8 + 2) * cnpj[i] for i in range(12)]) % 11) % 10
            # Calcula o segundo dígito verificador
            dv2 = (sum([(i % 8 + 2) * cnpj[i] for i in range(13)]) % 11) % 10
            # Retorna True se os dígitos verificadores estão corretos
            # return cnpj[12] == dv1 and cnpj[13] == dv2
            return "CNPJ"

        # Se não é CPF nem CNPJ, retorna False
        else:
            return False

    def tipo_pessoa(self, string_cotista):
        if len(string_cotista) == 14:
            return "2"
        elif len(string_cotista) == 11:
            return "1"
        else:
            return "5"

    def formatar_cpf_cnpj(self, string_documento):
        if self.valida_cpf_cnpj(str(string_documento)) == 'CPF':
            return str(string_documento).zfill(11)
        elif self.valida_cpf_cnpj(str(string_documento)) == 'CNPJ':
            return str(string_documento).zfill(14)
        else:
            return str(string_documento)

    def criar_cotistas_unico(self, cotista):
        # todo criar função para validar o tipo de cotista e a sua respectiva classificação
        try:
            dados_formatado = self.formatar_cpf_cnpj(str(cotista["cpfcnpjInvestidor"]))
            cotista_elemento = ET.Element("cotista")
            cotista_elemento.set("tipoPessoa", self.tipo_pessoa(dados_formatado))
            cotista_elemento.set("identificacao", self.formatar_cpf_cnpj(cotista["cpfcnpjInvestidor"]))

            if cotista['cpfcnpjInvestidor'] == '09358105000191':
                cotista_elemento.set('classificacao', str(3))
            else:
                cotista_elemento.set('classificacao', str(1))

            if cotista['cpfcnpjInvestidor'] == '02332886000104':
                cotista_elemento.set('classificacao', str(2))
            else:
                cotista_elemento.set('classificacao', str(1))

            return cotista_elemento
        except Exception as e:
            print("erro cotistas", e)

    def adicionar_pco_consolidado(self, lista_pco, fundo):
        main = []
        cotas = [investidor['cotas'] for investidor in lista_pco]

        cotista = self.criar_cotistas_unico({"cpfcnpjInvestidor": "02332886000104"})

        for cota in cotas:
            for item in cota:
                main.append(item)

        df = pd.DataFrame.from_dict(main)

        new_df = df.groupby(['tipoCota', "valorCota"]).sum().reset_index()

        ncotas = new_df.to_xml(attr_cols=['tipoCota', 'qtdeCotas', 'valorCota'],
                               index=False, root_name="cotas", row_name='cota', pretty_print=False)

        cotas_elemento = ET.fromstring(ncotas)

        cotista.append(cotas_elemento)

        cotistas_elemento = fundo.find(".//cotistas")

        cotistas_elemento.append(cotista)

    def job_ajuste_fundos_pco(self, fundo):
        # # dados = pd.read_excel('cota_unica.xlsx' ,  dtype=str)

        cnpj_fundo = fundo.attrib.get("cnpjFundo", "")

        lista_pco = self.ajuste_pco(fundo)

        # remover cotistas pcos

        self.remover_cotistas(fundo, lista_pco)

        self.adicionar_pco_consolidado(lista_pco, fundo)

        self.ajuste_elemento_fundos(fundo)
        self.ajuste_quantidade_pl_fundos(fundo)

    def job_ajuste_cota(self, cota):
        qtde_cotas = float(cota.attrib.get("qtdeCotas"))
        cota.set("qtdeCotas", str(round(qtde_cotas, 8)))

    def job_ajuste_cotista(self, cotista):
        identificacao = str(cotista.attrib.get("identificacao"))

        if len(identificacao) == 11:
            cotista.set("tipoPessoa", str(1))
        elif len(identificacao) == 14:
            cotista.set("tipoPessoa", str(2))
        else:
            pass

    def reescrever_xml(self, path):
        print("reescrevendo arquivo")
        xml_string = minidom.parseString(ET.tostring(self.root)).toprettyxml()
        xml_string = "\n".join(line for line in xml_string.split("\n") if line.strip())

        file_name = path

        with open(f'{file_name}', "w") as file:
            file.write(xml_string)

    def ajustar_qtd_cotista_elemento_fundos(self):

        fundos = self.root.findall(".//fundo")

        print(len(fundos))

        with ThreadPoolExecutor() as executor:
            executor.map(self.job_ajuste_fundos, fundos)

        # self.ajustar_cotas()
        # # self.ajustar_cotistas()

        file_name = f"DTVM/AJUSTADO.xml"
        self.reescrever_xml(file_name)

    def ajustar_pcos(self):

        fundos = self.root.findall(".//fundo")

        with ThreadPoolExecutor() as executor:
            executor.map(self.job_ajuste_fundos_pco, fundos)

        file_name = f"arquivos_ajustados/{self.filename}.xml"

        self.reescrever_xml(file_name)

    def ajustar_cotas(self):
        cotas = self.root.findall(".//cota")
        with ThreadPoolExecutor() as executor:
            executor.map(self.job_ajuste_cota, cotas)

    def ajustar_cotistas(self):
        cotistas = self.root.findall(".//cotista")
        with ThreadPoolExecutor() as executor:
            executor.map(self.job_ajuste_cotista, cotistas)

    def gerar_arquivo_validacao(self):
        with pd.ExcelWriter("validacao.xlsx") as writer:
            self.get_fundos().to_excel(writer, sheet_name="fundos", index=False)
            self.get_cotas().to_excel(writer, sheet_name="cotas", index=False)
            self.get_cotistas().to_excel(writer, sheet_name="cotistas", index=False)




