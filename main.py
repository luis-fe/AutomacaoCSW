#
#import ConexaoJava
from flask import Flask, render_template, jsonify, request
import os


app = Flask(__name__)
port = int(os.environ.get('PORT', 5000))

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.
@app.route('/')
def home():
    return render_template('index.html')
#@app.route('/automatizar',methods=['GET'])
#def Atumatizar():
 #   tamanho = ConexaoJava.check_java_version()
  #  return jsonify({'message': f'{tamanho}'})

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    app.run(host='0.0.0.0', port=port)
    try:
        #RecarregarBanco.FilaTags()
        print_hi('luis')
    except:
        print('falha na automacao')



