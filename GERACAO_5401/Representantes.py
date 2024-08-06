from dataclasses import dataclass , asdict



@dataclass
class Representante:
    nome :  str 
    telefone: str
    administrador: str
    
    def to_dict(self):
        return asdict(Representante)