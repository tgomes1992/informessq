from flask import Blueprint , request , jsonify , render_template , redirect , url_for
from JCOTSERVICE import ListFundosService , RelPosicaoFundoCotistaService
from bson.objectid import ObjectId
from ..db import db
import pandas as pd
from GERACAO_5401.Representantes import Representante
from GERACAO_5401.Administradores import Adms
from dataclasses import asdict

cadastros = Blueprint('cadastros', __name__ , url_prefix='/cadastros')


@cadastros.route('/cadastros', methods=['GET'])
def buscarFundosJcot():
    list_fundos = ListFundosService("thiago", "tAman1993**").listFundoRequest().to_dict('records')

    consulta = db.fundos.find({})

    codigos = [item['codigo'] for item in consulta]


    for item in list_fundos:
        if item['codigo'] not in codigos:
            db.fundos.insert_one(item)


    return jsonify({"message": "Fundos cadastrados com sucesso!"})



@cadastros.route("/administradores")
def cadastro_administradores():

    fundos = db.fundos.find({})
    adms =[]
    for item in fundos:
        if item['administrador'] not in adms:
            adms.append(item['administrador'])

    admin = [Adms(nome=" " , cnpj=item).to_dict() for item in adms]

    return jsonify(admin)


@cadastros.route("/representantes")
def cadastros_representantes():
    representante = Representante(nome="Thiago", telefone="24035621", administrador="36113876000191")

    db.representantes.insert_one(asdict(representante))

    return jsonify(representante)


@cadastros.route("/listar_fundos", methods=['GET'])
def listarFundos():
    page = int(request.args.get('page', 1))
    size = int(request.args.get('size', 10))
    skip = (page - 1) * size

    fundos  = db.fundos.find({}).skip(skip).limit(size)

    retorno = [{
        "id": str(item['_id']) ,
        "codigo": item['codigo'],
        "administrador": item['administrador']
    }  for item in fundos  ]

    total_items = db.fundos.count_documents({})

    return jsonify({
        "total_items": total_items,
        "page": page,
        "size": size,
        "items": retorno
    })



@cadastros.route("/home")
def list_fundos_template():
    
    fundos = db.fundos.find({})
    
    lista_fundos = [{ "id": str(fundo['_id']) , "codigo": fundo['codigo'] , "razaoSocial": fundo['razaoSocial']} for fundo in fundos]
    
    
    return render_template("listar_fundos.html" , fundos=lista_fundos)

@cadastros.route("/editar_fundo", methods=['GET', 'POST'])
def editarFundo():
    fundo_id = request.args.get('id')

    fundo = db.fundos.find_one({'_id': ObjectId(fundo_id)})

    if request.method == 'POST':



        fundo = db.fundos.update_one(
                                    {'_id': ObjectId(request.form.get('id'))} ,
                                    { "$set": { "tipo": request.form['tipoCota'] }}
                                 )


        
        
        return redirect(url_for("cadastros.list_fundos_template"))

    return render_template("form_register.html", fundo=fundo)


@cadastros.route("/posicaoFundo", methods=['GET', 'POST'])
def posicaoJcot():
    codigo_fundo = request.args.get('codigo')
    service = RelPosicaoFundoCotistaService("thiago", "tAman1993**")
    fundo = {"codigo": codigo_fundo ,"dataPosicao": '2024-06-28'}
    df = service.get_posicoes_json(fundo)

    trava = db.posicoes_jcot.find_one({"fundo": codigo_fundo , "data": fundo['dataPosicao']})

    if not trava:
        db.posicoes_jcot.insert_many(df)

    return {"message": f"Posições do fundo  {codigo_fundo} extraidas com sucesso"}
