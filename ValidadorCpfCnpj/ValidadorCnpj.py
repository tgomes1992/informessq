


class ValidadorCnpj():


    def __init__(self , cnpj):
        self.base_cnpj = str(cnpj).replace(".", "").replace("/", "").replace("-", "")
        self.cnpj = str(self.base_cnpj).zfill(14)
        self.NOVO_CNPJ = list(self.cnpj)

    def somar_dicionario(self , dicionario):
        resultado = 0
        for item in dicionario:
            resultado += int(dicionario[item])

        return resultado

    def calcular_digito_verificador(self , somatorio):
        digito = (somatorio % 11)
        if digito < 2:
            return 0
        else:
            return 11 - digito



    def validarCnpj(self):

        validacao_digito1 = {
            "num1": int(self.NOVO_CNPJ[0]) * 5,
            "num2": int(self.NOVO_CNPJ[1]) * 4,
            "num3": int(self.NOVO_CNPJ[2]) * 3,
            "num4": int(self.NOVO_CNPJ[3]) * 2,
            "num5": int(self.NOVO_CNPJ[4]) * 9,
            "num6": int(self.NOVO_CNPJ[5]) * 8,
            "num7": int(self.NOVO_CNPJ[6]) * 7,
            "num8": int(self.NOVO_CNPJ[7]) * 6,
            "num9": int(self.NOVO_CNPJ[8]) * 5,
            "num10": int(self.NOVO_CNPJ[9]) * 4,
            "num11":  int(self.NOVO_CNPJ[10]) * 3,
            "num12":  int(self.NOVO_CNPJ[11]) * 2,
        }

        somatorio_digito_um = self.somar_dicionario(validacao_digito1)
        primeiro_digito_verificador = self.calcular_digito_verificador(somatorio_digito_um)

        validacao_digito2 = {
            "num1": int(self.NOVO_CNPJ[0]) * 6,
            "num2": int(self.NOVO_CNPJ[1]) * 5,
            "num3": int(self.NOVO_CNPJ[2]) * 4,
            "num4": int(self.NOVO_CNPJ[3]) * 3,
            "num5": int(self.NOVO_CNPJ[4]) * 2,
            "num6": int(self.NOVO_CNPJ[5]) * 9,
            "num7": int(self.NOVO_CNPJ[6]) * 8,
            "num8": int(self.NOVO_CNPJ[7]) * 7,
            "num9": int(self.NOVO_CNPJ[8]) * 6,
            "num10": int(self.NOVO_CNPJ[9]) * 5,
            "num11":  int(self.NOVO_CNPJ[10]) * 4,
            "num12":  int(self.NOVO_CNPJ[11]) * 3,
            "num13": int(primeiro_digito_verificador) * 2 ,
        }

        somatorio_digito_dois = self.somar_dicionario(validacao_digito2)
        segundo_digito_verificador = self.calcular_digito_verificador(somatorio_digito_dois)


        if (int(self.NOVO_CNPJ[12]) ==  primeiro_digito_verificador) and (int(self.NOVO_CNPJ[13]) == segundo_digito_verificador):
            return True
        else:
            return False


