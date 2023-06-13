import jaydebeapi
import pandas as pd
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
LerEPC()