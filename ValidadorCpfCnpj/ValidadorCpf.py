



class ValidadorCpf():



    def __init__(self , cpf):
        self.base_cpf = cpf.zfill(11)
        self.cpf = str(self.base_cpf).replace("." ,"").replace("/","").replace("-","")
        self.NOVO_CPF = list(self.cpf)
        self.NOVO_CPF_STRING = " ".join(self.NOVO_CPF)


    def validarCpfConhecidos(self):
        lst = self.NOVO_CPF
        if not lst:
            return True  # An empty list is considered to have all items the same
        first_item = lst[0]
        for item in lst:
            if item != first_item:
                return False
        return True

    def validarResto(self , resto):
        if resto == 10:
            return 0
        else:
            return resto


    def validarCpf(self):
        # print (self.base_cpf)
        if len(self.NOVO_CPF) == 11 and not self.validarCpfConhecidos():
            primeiro1 = int(self.NOVO_CPF[0]) * 10
            primeiro2 = int(self.NOVO_CPF[1]) * 9
            primeiro3 = int(self.NOVO_CPF[2]) * 8
            primeiro4 = int(self.NOVO_CPF[3]) * 7
            primeiro5 = int(self.NOVO_CPF[4]) * 6
            primeiro6 = int(self.NOVO_CPF[5]) * 5
            primeiro7 = int(self.NOVO_CPF[6]) * 4
            primeiro8 = int(self.NOVO_CPF[7]) * 3
            primeiro9 = int(self.NOVO_CPF[8]) * 2

            seg_primeiro1 = int(self.NOVO_CPF[0]) * 11
            seg_primeiro2 = int(self.NOVO_CPF[1]) * 10
            seg_primeiro3 = int(self.NOVO_CPF[2]) * 9
            seg_primeiro4 = int(self.NOVO_CPF[3]) * 8
            seg_primeiro5 = int(self.NOVO_CPF[4]) * 7
            seg_primeiro6 = int(self.NOVO_CPF[5]) * 6
            seg_primeiro7 = int(self.NOVO_CPF[6]) * 5
            seg_primeiro8 = int(self.NOVO_CPF[7]) * 4
            seg_primeiro9 = int(self.NOVO_CPF[8]) * 3
            seg_primeiro10 = int(self.NOVO_CPF[9]) * 2

            soma_validacao = (
                        primeiro1 + primeiro2 + primeiro3 + primeiro4 + primeiro5 + primeiro6 + primeiro7 + primeiro8 + primeiro9)

            soma_validacao_validii = ( seg_primeiro1 + seg_primeiro2 +
                seg_primeiro3 +
                seg_primeiro4 +
                seg_primeiro5 +
                seg_primeiro6 +
                seg_primeiro7 +
                seg_primeiro8 +
                seg_primeiro9 +
                seg_primeiro10  )


            resto1 = (soma_validacao * 10) % 11
            resto2 = (soma_validacao_validii * 10) % 11

            if (self.validarResto(resto1) == int(self.NOVO_CPF[9])) and (self.validarResto(resto2) == int(self.NOVO_CPF[10])):
                return True
            else:
                return False

        else:
            return False



