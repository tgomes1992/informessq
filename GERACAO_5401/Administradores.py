from dataclasses import dataclass



@dataclass
class Adms:
    id : str
    cnpj :  str
    nome: str

    def to_dict(self):
        return {
            'cnpj': self.cnpj,
            'nome': self.nome ,
            'id': self.id
        }