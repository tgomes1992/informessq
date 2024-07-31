import re


class ValidadorCpfCnpj:
    @staticmethod
    def validar_cpf(cpf):
        # Remova todos os caracteres não numéricos
        cpf = re.sub(r'[^0-9]', '', cpf)

        # Verifique se o CPF tem 11 dígitos
        if len(cpf) != 11:
            return False

        # Calcule o primeiro dígito verificador
        total = 0
        for i in range(9):
            total += int(cpf[i]) * (10 - i)

        resto = total % 11
        if resto < 2:
            digito1 = 0
        else:
            digito1 = 11 - resto

        # Calcule o segundo dígito verificador
        total = 0
        for i in range(10):
            total += int(cpf[i]) * (11 - i)

        resto = total % 11
        if resto < 2:
            digito2 = 0
        else:
            digito2 = 11 - resto

        # Verifique se os dígitos verificadores estão corretos
        if int(cpf[9]) == digito1 and int(cpf[10]) == digito2:
            return True
        else:
            return False

    @staticmethod
    def validar_cnpj(cnpj):
        # Remova todos os caracteres não numéricos
        cnpj = re.sub(r'[^0-9]', '', cnpj)

        # Verifique se o CNPJ tem 14 dígitos
        if len(cnpj) != 14:
            return False

        # Calcule o primeiro dígito verificador
        total = 0
        pesos = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        for i in range(12):
            total += int(cnpj[i]) * pesos[i]

        resto = total % 11
        if resto < 2:
            digito1 = 0
        else:
            digito1 = 11 - resto

        # Calcule o segundo dígito verificador
        total = 0
        pesos = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        for i in range(13):
            total += int(cnpj[i]) * pesos[i]

        resto = total % 11
        if resto < 2:
            digito2 = 0
        else:
            digito2 = 11 - resto

        # Verifique se os dígitos verificadores estão corretos
        if int(cnpj[12]) == digito1 and int(cnpj[13]) == digito2:
            return True
        else:
            return False

    @staticmethod
    def definir_validacao(string):
        if len(string) > 11:
            return ValidadorCpfCnpj().validar_cnpj(string)
        elif len(string) < 14:
            return ValidadorCpfCnpj().validar_cpf(string)


