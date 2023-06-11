import pandas as pd
import jaydebeapi
import time
from sqlalchemy import create_engine
import datetime
import numpy

import CalculoNecessidadesEndereco
import ConexaoPostgreRailway
from psycopg2 import sql


# Função para criar os agrupamentos
def criar_agrupamentos(grupo):
    return '/'.join(sorted(set(grupo)))
def obterHoraAtual():
    agora = datetime.datetime.now()
    hora_str = agora.strftime('%d/%m/%Y %H:%M')
    return hora_str

def Funcao_Inserir (df_tags, tamanho,tabela, replace):
    # Configurações de conexão ao banco de dados
    database = "railway"
    user = "postgres"
    password = "TMecLDjZX4xMBqKd3hHY"
    host = "containers-us-west-152.railway.app"
    port = "5848"

# Cria conexão ao banco de dados usando SQLAlchemy
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

    # Inserir dados em lotes
    chunksize = tamanho
    for i in range(0, len(df_tags), chunksize):
        df_tags.iloc[i:i + chunksize].to_sql(tabela, engine, if_exists=replace, index=False , schema='Reposicao')

start_time = time.perf_counter()


def FilaTags():
    conn = jaydebeapi.connect(
    'com.intersys.jdbc.CacheDriver',
    'jdbc:Cache://187.32.10.129:1972/SISTEMAS',
    {'user': 'root', 'password': 'ccscache'},
    'CacheDB.jar'
)
    conn2 = ConexaoPostgreRailway.conexao()
    df_tags = pd.read_sql(
        "SELECT  codBarrasTag as codbarrastag, codNaturezaAtual , codEngenharia , codReduzido,(SELECT i.nome  FROM cgi.Item i WHERE i.codigo = t.codReduzido) as descricao , numeroop as numeroop,"
        " (SELECT i2.codCor  FROM cgi.Item2  i2 WHERE i2.Empresa = 1 and  i2.codItem  = t.codReduzido) as Cor,"
        " (SELECT tam.descricao  FROM cgi.Item2  i2 join tcp.Tamanhos tam on tam.codEmpresa = i2.Empresa and tam.sequencia = i2.codSeqTamanho  WHERE i2.Empresa = 1 and  i2.codItem  = t.codReduzido) as tamanho"
        " from tcr.TagBarrasProduto t WHERE codEmpresa = 1 and codNaturezaAtual = 5 and situacao = 3", conn)

    df_opstotal = pd.read_sql('SELECT top 200000 numeroOP as numeroop , totPecasOPBaixadas as totalop  '
                              'from tco.MovimentacaoOPFase WHERE codEmpresa = 1 and codFase = 236  '
                              'order by numeroOP desc ',conn)

    df_tags = pd.merge(df_tags, df_opstotal, on='numeroop', how='left')
    df_tags['totalop'] = df_tags['totalop'].replace('', numpy.nan).fillna('0')
    df_tags['codNaturezaAtual'] = df_tags['codNaturezaAtual'].astype(str)
    df_tags['totalop'] = df_tags['totalop'].astype(int)
    # CRIANDO O DATAFRAME DO QUE JA FOI REPOSTO E USANDO MERGE
       # Verificando as tag's que ja foram repostas
    TagsRepostas = pd.read_sql('select "codbarrastag" as codbarrastag, "Usuario" as usuario_  from "Reposicao"."tagsreposicao" tr ',conn2)
    df_tags = pd.merge(df_tags, TagsRepostas, on='codbarrastag', how='left')
    df_tags = df_tags.loc[df_tags['usuario_'].isnull()]
    df_tags.drop('usuario_', axis=1, inplace=True)
        # Verificando as tag's que ja estam na fila
    ESTOQUE = pd.read_sql('select "Usuario", "codbarrastag" as codbarrastag, "Situacao" as sti_aterior  from "Reposicao"."filareposicaoportag" ',conn2)
    df_tags = pd.merge(df_tags,ESTOQUE,on='codbarrastag',how='left')
    df_tags['Situacao'] = df_tags.apply(lambda row: 'Reposto' if not pd.isnull(row['Usuario']) else 'Reposição não Iniciada', axis=1)
    epc = LerEPC()
    df_tags = pd.merge(df_tags, epc, on='codbarrastag', how='left')
    df_tags.rename(columns={'codbarrastag': 'codbarrastag','codEngenharia':'CodEngenharia'
                            , 'numeroop':'numeroop'}, inplace=True)
    conn2.close()
    df_tags = df_tags.loc[df_tags['sti_aterior'].isnull()]
    df_tags.drop_duplicates(subset='codbarrastag', inplace=True)
    # Excluir a coluna 'B' inplace
    df_tags.drop('sti_aterior', axis=1, inplace=True)
    df_tags.drop_duplicates(subset='codbarrastag', inplace=True)
    df_tags['epc'] = df_tags['epc'].str.extract('\|\|(.*)').squeeze()
    print(df_tags.dtypes)
    print(df_tags['codbarrastag'].size)
    tamanho = df_tags['codbarrastag'].size
    dataHora = obterHoraAtual()
    df_tags['DataHora'] = dataHora
    df_tags.to_csv('planilha.csv')
    try:
        Funcao_Inserir(df_tags, tamanho,'filareposicaoportag', 'append')
        hora = obterHoraAtual()
        return tamanho, hora
    except:
        print('falha na funçao Inserir')
        hora = obterHoraAtual()
        return tamanho, hora

    conn.close()
    conn2.close()
    return dataHora

def LerEPC():
    conn = jaydebeapi.connect(
        'com.intersys.jdbc.CacheDriver',
        'jdbc:Cache://187.32.10.129:1972/SISTEMAS',
        {'user': 'root', 'password': 'ccscache'},
        'CacheDB.jar'
    )


    consulta = pd.read_sql('select epc.id as epc, t.codBarrasTag as codbarrastag from tcr.SeqLeituraFase  t '
                           'join Tcr_Rfid.NumeroSerieTagEPC epc on epc.codTag = t.codBarrasTag '
                           'WHERE t.codEmpresa = 1 and (t.codTransacao = 3500 or t.codTransacao = 501) '
                           'and (codLote like "23%" or  codLote like "24%" or codLote like "25%" '
                           'or codLote like "22%" )',conn)
    conn.close()

    print(consulta)
    return consulta

def SeparacoPedidos():
    conn = jaydebeapi.connect(
        'com.intersys.jdbc.CacheDriver',
        'jdbc:Cache://187.32.10.129:1972/SISTEMAS',
        {'user': 'root', 'password': 'ccscache'},
        'CacheDB.jar'
    )
    SugestoesAbertos = pd.read_sql('SELECT codPedido, dataGeracao,  priorizar, vlrSugestao,situacaosugestao, dataFaturamentoPrevisto  from ped.SugestaoPed  '
                                   'WHERE codEmpresa =1 and situacaoSugestao =2',conn)
    CapaPedido = pd.read_sql('select top 100000 codPedido, codCliente, '
                             '(select c.nome from fat.Cliente c WHERE c.codEmpresa = 1 and p.codCliente = c.codCliente) as desc_cliente, '
                             '(select r.nome from fat.Representante  r WHERE r.codEmpresa = 1 and r.codRepresent = p.codRepresentante) as desc_representante, '
                             '(select c.nomeCidade from fat.Cliente  c WHERE c.codEmpresa = 1 and c.codCliente = p.codCliente) as cidade, '
                             '(select c.nomeEstado from fat.Cliente  c WHERE c.codEmpresa = 1 and c.codCliente = p.codCliente) as estado, '
                             ' codRepresentante , codTipoNota, CondicaoDeVenda as condvenda  from ped.Pedido p  '
                                   ' WHERE p.codEmpresa = 1 order by codPedido desc ',conn)
    SugestoesAbertos = pd.merge(SugestoesAbertos,CapaPedido,on= 'codPedido', how = 'left')
    SugestoesAbertos.rename(columns={'codPedido': 'codigopedido', 'vlrSugestao': 'vlrsugestao'
        , 'dataGeracao': 'datageracao','situacaoSugestao':'situacaosugestao','dataFaturamentoPrevisto':'datafaturamentoprevisto',
        'codCliente':'codcliente', 'codRepresentante':'codrepresentante','codTipoNota':'codtiponota'}, inplace=True)
    tiponota = pd.read_sql('select  p.tipoNota as desc_tiponota  from dw_ped.Pedido p WHERE p.codEmpresa = 1 '
                           'group by tipoNota',conn)
    condicaopgto = pd.read_sql('select  p.condVenda as condicaopgto  from dw_ped.Pedido p WHERE p.codEmpresa = 1 '
                           'group by condVenda ',conn)

    # Extrair caracteres à esquerda do hífen
    tiponota['codtiponota'] = tiponota['desc_tiponota'].str.split(' -').str[0]
    condicaopgto['condvenda'] = '1||'+condicaopgto['condicaopgto'].str.split(' -').str[0]
    SugestoesAbertos = pd.merge(SugestoesAbertos, tiponota, on='codtiponota', how='left')
    SugestoesAbertos = pd.merge(SugestoesAbertos, condicaopgto, on='condvenda', how='left')

    conn2 = ConexaoPostgreRailway.conexao()
    validacao = pd.read_sql('select codigopedido, '+"'ok'"+' as "validador"  from "Reposicao".filaseparacaopedidos f ', conn2)

    SugestoesAbertos = pd.merge(SugestoesAbertos, validacao, on='codigopedido', how='left')
    SugestoesAbertos = SugestoesAbertos.loc[SugestoesAbertos['validador'].isnull()]
    # Excluir a coluna 'B' inplace
    SugestoesAbertos.drop('validador', axis=1, inplace=True)
    tamanho = SugestoesAbertos['codigopedido'].size
    dataHora = obterHoraAtual()
    SugestoesAbertos['datahora'] = dataHora
    # Contar o número de ocorrências de cada valor na coluna 'coluna'
    contagem = SugestoesAbertos['codcliente'].value_counts()

    # Criar uma nova coluna 'contagem' no DataFrame com os valores contados
    SugestoesAbertos['contagem'] = SugestoesAbertos['codcliente'].map(contagem)
    # Aplicar a função de agrupamento usando o método groupby
    SugestoesAbertos['agrupamentopedido'] = SugestoesAbertos.groupby('codcliente')['codigopedido'].transform(criar_agrupamentos)

   # try:
    Funcao_Inserir(SugestoesAbertos,tamanho,'filaseparacaopedidos','append')
     #   hora = obterHoraAtual()
      #  return tamanho, hora

    #except:
     #   print('falha na funçao Inserir Separacao')
      #  hora = obterHoraAtual()
       # conn.close()
        #return tamanho, hora


def SugestaoSKU():
    conn = jaydebeapi.connect(
        'com.intersys.jdbc.CacheDriver',
        'jdbc:Cache://187.32.10.129:1972/SISTEMAS',
        {'user': 'root', 'password': 'ccscache'},
        'CacheDB.jar'
    )
    SugestoesAbertos = pd.read_sql(
        'select s.codPedido as codpedido, s.produto, s.qtdeSugerida as qtdesugerida , s.qtdePecasConf as qtdepecasconf  '
        ', '+"'-' as enderco1 "+
        'from ped.SugestaoPedItem s  '
        'left join ped.SugestaoPed p on p.codEmpresa = s.codEmpresa and p.codPedido = s.codPedido  '
        'WHERE s.codEmpresa =1 and p.situacaoSugestao =2'
        ' order by p.dataGeracao, p.codPedido ', conn)
    conn2 = ConexaoPostgreRailway.conexao()
   # validacao = pd.read_sql('select codpedido, '+"'ok'"+' as "validador"  from "Reposicao".pedidossku f ', conn2)

    #SugestoesAbertos = pd.merge(SugestoesAbertos, validacao, on='codpedido', how='left')
  #  SugestoesAbertos = SugestoesAbertos.loc[SugestoesAbertos['validador'].isnull()]
    # Excluir a coluna 'B' inplace
  #  SugestoesAbertos.drop('validador', axis=1, inplace=True)
    SugestoesAbertos['necessidade'] = SugestoesAbertos['qtdesugerida'] - SugestoesAbertos['qtdepecasconf']
    tamanho = SugestoesAbertos['codpedido'].size
    dataHora = obterHoraAtual()
    SugestoesAbertos['datahora'] = dataHora

    if not SugestoesAbertos.empty:
        SugestoesAbertos["enderco1"] = SugestoesAbertos.apply(
            lambda row: 'Concluido' if pd.isnull(row['enderco1']) else '-', axis=1)
        SugestoesAbertos['status'] = SugestoesAbertos['qtdesugerida'].astype(str) + '/' + SugestoesAbertos['qtdepecasconf'].astype(str)
        Funcao_Inserir(SugestoesAbertos, tamanho, 'pedidossku', 'replace')
        x = CalculoNecessidadesEndereco.NecessidadesPedidos()
        SugestoesAbertos = pd.merge(SugestoesAbertos, x ,on=['codpedido','produto'], how = 'left')
        SugestoesAbertos['status'] = SugestoesAbertos['qtdesugerida'].astype(str) + '/' + SugestoesAbertos[
            'qtdepecasconf'].astype(str)
        SugestoesAbertos["enderco"] = SugestoesAbertos.apply(
            lambda row: 'Ped. Conferido' if pd.isnull(row['enderco']) and row['necessidade']==0 else row['enderco'] , axis=1)
        Funcao_Inserir(SugestoesAbertos, tamanho, 'pedidossku', 'replace')
        return SugestoesAbertos
    else:
        return f'SKU NAO ATUALIADO {dataHora}'



def VerificarPedidoFeito():
    conn = jaydebeapi.connect(
        'com.intersys.jdbc.CacheDriver',
        'jdbc:Cache://187.32.10.129:1972/SISTEMAS',
        {'user': 'root', 'password': 'ccscache'},
        'CacheDB.jar'
    )
    pedidosAtual = pd.read_sql('SELECT s.codPedido as codpedido  FROM ped.SugestaoPed s '
                   'WHERE s.codEmpresa = 1 and s.situacaoSugestao in (2, 3,5) ',conn)
    conn2 = ConexaoPostgreRailway.conexao()
    validacao = pd.read_sql('select codigopedido as codpedido , ' + "'ok'" + ' as "validador"  from "Reposicao".filaseparacaopedidos f ', conn2)

    SugestoesAbertos = pd.merge(pedidosAtual, validacao, on='codpedido', how='left')
    SugestoesAbertos = SugestoesAbertos.loc[SugestoesAbertos['validador'].isnull()]
   # Excluir a coluna 'B' inplace
    SugestoesAbertos.drop('validador', axis=1, inplace=True)
    # Obter os valores para a cláusula WHERE do DataFrame
    sugestoes_abertos = SugestoesAbertos['codpedido'].tolist()

    # Construir a consulta DELETE usando a cláusula WHERE com os valores do DataFrame
    query = sql.SQL('DELETE FROM "Reposicao"."filaseparacaopedidos" WHERE codigopedido IN ({})').format(
        sql.SQL(',').join(map(sql.Literal, sugestoes_abertos))
    )

    # Executar a consulta DELETE
    with conn2.cursor() as cursor:
        cursor.execute(query)
        conn2.commit()

    return 'teste'