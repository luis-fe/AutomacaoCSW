import pyodbc
import pandas as pd

conn = pyodbc.connect(
    DRIVER='InterSystems ODBC',SERVER='187.32.10.129',port= '1972',DATABASE='SISTEMAS',
UID='root',PWD='ccscache')

def teste():
    df_tags = pd.read_sql(
        "SELECT  codBarrasTag as codbarrastag, codNaturezaAtual , codEngenharia , codReduzido,(SELECT i.nome  FROM cgi.Item i WHERE i.codigo = t.codReduzido) as descricao , numeroop as numeroop,"
        " (SELECT i2.codCor  FROM cgi.Item2  i2 WHERE i2.Empresa = 1 and  i2.codItem  = t.codReduzido) as Cor,"
        " (SELECT tam.descricao  FROM cgi.Item2  i2 join tcp.Tamanhos tam on tam.codEmpresa = i2.Empresa and tam.sequencia = i2.codSeqTamanho  WHERE i2.Empresa = 1 and  i2.codItem  = t.codReduzido) as tamanho"
        " from tcr.TagBarrasProduto t WHERE codEmpresa = 1 and codNaturezaAtual = 5 and situacao = 3", conn)
    print(df_tags)
    return 1, 2