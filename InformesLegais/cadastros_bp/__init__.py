from flask import Blueprint , request , jsonify , render_template
from JCOTSERVICE import ListFundosService , RelPosicaoFundoCotistaService
from bson.objectid import ObjectId
from ..db import db
import pandas as pd

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

@cadastros.route("/editar_fundo", methods=['GET', 'POST'])
def editarFundo():
    fundo_id = request.args.get('id')

    fundo = db.fundos.find_one({'_id': ObjectId(fundo_id)})

    if request.method == 'POST':

        print (request.form['tipoCota'])
        fundo = db.fundos.update_one(
                                    {'_id': ObjectId(request.form.get('id'))} ,
                                    { "$set": { "tipoCota": request.form['tipoCota'] }}
                                 )

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
