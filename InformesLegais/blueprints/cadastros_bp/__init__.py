from flask import Blueprint , request , jsonify , render_template , redirect , url_for , send_file
from JCOTSERVICE import ListFundosService , RelPosicaoFundoCotistaService
from bson.objectid import ObjectId
from InformesLegais.db import db
import pandas as pd
from GERACAO_5401.Representantes import Representante
from GERACAO_5401.Administradores import Adms
from GERACAO_5401.Validador5401 import XML_5401
from dataclasses import asdict
from io import BytesIO
from xml.etree import ElementTree as ET


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
def administradores():

    adms = db.administradores.find({})

    admin = [Adms(nome= item['nome'] , cnpj=item['cnpj'] ,  id = str(item['_id'])).to_dict() for item in adms]

    return render_template("Administradores.html" , admin = admin )

@cadastros.route("/administradores_detalhes", methods=['GET' , 'POST'])
def cadastro_administradores_detalhe():

    id = request.args.get('id')
    admin = db.administradores.find_one({'_id':ObjectId(id)})


    if request.method == 'POST':


        cnpj = request.form.get('cnpj')
        nome = request.form.get('nome')
        cpf_representante = request.form.get('cpf_representante')
        representante = request.form.get('representante')
        tel_representante = request.form.get('tel_representante')

        # Define the update query
        update_data = {
            "cnpj": cnpj,
            "nome": nome.strip(),
            "cpf_representante": cpf_representante,
            "representante": representante,
            "tel_representante": tel_representante
        }

        # Update the document in MongoDB
        result = db.administradores.update_one({"_id": ObjectId(id)}, {"$set": update_data})

    return render_template("Administradores_detalhe.html" , admin = admin )

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

@cadastros.route("/fundossemtiporelatorio" , methods=['GET'])
def relatorio_fundos_sem_cadastro():
    '''rota para gerar em excel o relatório com os fundos sem o tipo de cota cadastrado'''
    fundos = db.fundos.find({})
    df_fundos = pd.DataFrame.from_dict(fundos)

    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_fundos[df_fundos['tipo'].isnull()].to_excel(writer, index=False)

    output.seek(0)

    return send_file(output, as_attachment=True, download_name='fundos_sem_cadastro.xlsx', mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

@cadastros.route("/jobs" , methods=['GET'])
def jobs():
    '''rota gerenciar os jobs'''

    jobs = db.celery_taskmeta.find({})

    lista_jobs = [{"status": job['status'] , "result": str(job['result'])  } for job in jobs]

    return render_template("Jobs.html" , jobs = lista_jobs)


@cadastros.route('/consolidador_investidores', methods=['GET', 'POST'])
def consolidador_investidores():

    if request.method == 'POST':
        # print (request.files)
        xml = XML_5401(request.files['arquivo'] , 'validacao')

        for investidor in xml.get_cotistas().to_dict("records"):

            if not db.investidores5401.find_one({"identificacao": investidor["identificacao"]}):
                db.investidores5401.insert_one(investidor)




    return render_template("consolidador_investidores.html")

