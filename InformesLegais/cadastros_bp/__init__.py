from flask import Blueprint , request , jsonify , render_template , redirect
from JCOTSERVICE import ListFundosService , RelPosicaoFundoCotistaService
from bson.objectid import ObjectId
from ..db import db
import pandas as pd
from GERACAO_5401.Representantes import Representante
from dataclasses import asdict

cadastros = Blueprint('cadastros', __name__ , url_prefix='/cadastros')


@cadastros.route('/cadastros', methods=['GET'])
def buscarFundosJcot():
    list_fundos = ListFundosService("thiago", "tAman1993**").listFundoRequest().to_dict('records')

    consulta = db.fundos.find({})

    codigos = [item['codigo'] for item in consulta]

    print (codigos)

    for item in list_fundos:
        if item['codigo'] not in codigos:
            db.fundos.insert_one(item)


    return jsonify({"message": "Fundos cadastrados com sucesso!"})



@cadastros.route("/representantes")
def cadastros_representantes():
    
    representante = Representante(nome="Thiago" , telefone="24035621" , administrador="36113876000191")
    
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



@cadastros.route("/list_fundos")
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
                                    { "$set": { "tipoCota": request.form['tipoCota'] }}
                                 )
        
        
        return redirect("list_fundos")

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
