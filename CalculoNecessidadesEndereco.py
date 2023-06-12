import pandas as pd
import numpy
import ConexaoPostgreRailway

def CarregarSkuAtual():
    conn2 = ConexaoPostgreRailway.conexao()
    validacao = pd.read_sql('select codpedido, endereco from "Reposicao".pedidossku f '
                            'where endereco like '+"'%-%'", conn2)
    conn2.close()
    return validacao

def NecessidadesPedidos():
    # Trazer o dataframe das sugestoes em abertos a nivel de sku
    conn = ConexaoPostgreRailway.conexao()
    pedidos = pd.read_sql(
        'select codigopedido as codpedido, codtiponota , agrupamentopedido  from "Reposicao".filaseparacaopedidos f ',
        conn)
    pedidossku = pd.read_sql(
        'select codpedido, necessidade, produto as codreduzido, endereco  from "Reposicao".pedidossku p  '
        'where necessidade > 0', conn)
    pedidos = pd.merge(pedidos, pedidossku, on='codpedido')

    # Passo 3: criar uma coluna para a necessidade do sku , segundo o criterio de ordenamento
    pedidos['necessidadeSKU'] = pedidos.groupby('codreduzido').cumcount() + 1

    # passo 4: data frame do estoque por endereco
    estoque = pd.read_sql(
        'select "Endereco" as endestoque , "CodReduzido" as codreduzido , count("CodReduzido") as saldo  from "Reposicao".tagsreposicao t '
        'group by "Endereco" , "CodReduzido" ', conn)
    estoque = estoque.sort_values(by='saldo', ascending=False)  # escolher como deseja classificar
    estoque['ordemAparicao'] = estoque.groupby('codreduzido').cumcount() + 1
    # 4.1 Obtendo o ordenamento max do estoque
    maxOrdem = max(estoque['ordemAparicao'])

    # Passo 5 - criando colunas de acordo com o numero maximo de estoque encontrado:
    for i in range(maxOrdem):
        col_name = f'endereco -{i + 1}'  # Nome da coluna baseado no valor do loop
        col_name2 = f'saldo -{i + 1}'  # Nome da coluna baseado no valor do loop
        enderecoi = estoque[estoque['ordemAparicao'] == (i + 1)]
        enderecoi = enderecoi[['codreduzido', 'endestoque', 'saldo']]  # Especificar as colunas 'A' e 'C'
        enderecoi[col_name] = enderecoi['endestoque']  # Criar a coluna com o valor do loop
        enderecoi[col_name2] = enderecoi['saldo']  # Criar a coluna com o valor do loop
        enderecoi[col_name2] = enderecoi[col_name2].astype(int)
        pedidos = pd.merge(pedidos, enderecoi, on='codreduzido', how='left')
    pedidos['Necessidade Endereco'] = 0
    pedidos['endereco'] = 'a calcular'
    for i in range(maxOrdem):
        col_name = f'endereco -{i + 1}'  # Nome da coluna baseado no valor do loop
        col_name2 = f'saldo -{i + 1}'  # Nome da coluna baseado no valor do loop
        pedidos[col_name2] = pedidos[col_name2].replace('', numpy.nan).fillna(0)

        pedidos["endereco"] = pedidos.apply(
            lambda row: row[col_name] if row['Necessidade Endereco'] == 0 else row["endereco"], axis=1)

        pedidos["Necessidade Endereco"] = pedidos.apply(
            lambda row: row['necessidadeSKU'] if row['necessidadeSKU'] <= row[col_name2] and row[
                "Necessidade Endereco"] == 0 else row["Necessidade Endereco"], axis=1)

        pedidos["necessidade"] = pedidos.apply(
            lambda row: row['necessidade'] - row["Necessidade Endereco"] if row['necessidade'] > 0 else 0, axis=1)

        pedidos['necessidadeSKU'] = pedidos.groupby('codreduzido')['necessidade'].cumsum()

    pedidos.rename(columns={'codreduzido':"produto"}, inplace=True)

    # Replicando a regra para os proximos endereços.
    pedidos['endereco'] = pedidos['endereco'].replace('', numpy.nan).fillna('a calcular')
    pedidos = pedidos[pedidos['endereco'] != 'a calcular']

    return pedidos[['codpedido','produto', 'endereco', 'Necessidade Endereco']]

def AtualizarTabelaPedidosSKU(dataframe):
    conn = ConexaoPostgreRailway.conexao()
    cursor = conn.cursor()

    for index, row in dataframe.iterrows():
        codpedido = row['codpedido']
        produto = row['produto']
        endereco = row['endereco']


        # Executar a atualização na tabela "Reposicao.pedidossku"
        cursor.execute('UPDATE "Reposicao".pedidossku SET endereco = %s WHERE codpedido = %s AND produto = %s',
                       (endereco, codpedido, produto))

    # Confirmar as alterações
    conn.commit()

    # Fechar a conexão com o banco de dados
    cursor.close()
    conn.close()


