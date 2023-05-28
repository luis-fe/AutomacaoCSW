import RecarregarBanco
from flask import Flask, render_template, jsonify, request
from apscheduler.schedulers.background import BackgroundScheduler
import os


app = Flask(__name__)
port = int(os.environ.get('PORT', 5000))
def my_task():
    try:
        # coloque o código que você deseja executar continuamente aqui
        tamnho, datahora = RecarregarBanco.FilaTags()
        print(f'tarefa execultado com sucesso: {datahora}')
    except:
        print('falha na automacao')

scheduler = BackgroundScheduler()
scheduler.add_job(func=my_task, trigger='interval', seconds=270)
scheduler.start()

@app.route('/')
def home():
    return render_template('index.html')
@app.route('/automatizar',methods=['GET'])
def Atumatizar():
    tamanho, hora = RecarregarBanco.FilaTags()
    return jsonify({'message': f'O Fila das Fases foi atualizado. Foram adicionadas novas: {tamanho} tags',
                    'Hora da Atualizacao': f'O horario de atualizacao foi: {hora}'})

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    app.run(host='0.0.0.0', port=port)