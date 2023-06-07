import ConexaoPostgreRailway
import pandas as pd
def TratandoErroTagsSemelhanteFilaxInventario():

    delete = 'delete from "Reposicao".filareposicaoportag ' \
              ' where codbarrastag in (select t.codbarrastag  from "Reposicao".filareposicaoportag t' \
              ' join "Reposicao".tagsreposicao_inventario ti  on t.codbarrastag = ti.codbarrastag) '


    conn = ConexaoPostgreRailway.conexao()
    cursor = conn.cursor()
    cursor.execute(delete
                   , ( ))

    # Obter o número de linhas afetadas
    numero_linhas_afetadas = cursor.rowcount
    conn.commit()
    cursor.close()
    conn.close()

    return pd.DataFrame({'Mensagem': [f'Limpeza Feita']})