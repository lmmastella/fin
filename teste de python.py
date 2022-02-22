
"""
# %% teste de python

from datetime import datetime
from pprint import pprint
import pandas as pd
import numpy as np
import pandas_datareader.data as web


# %% Datas

today = datetime.today().strftime("%Y-%m-%d")
data_inicio = "2021-01-01"
data_fim = "2022-01-31"

# %% Criando um dataframe com os dados do Yahoo

_data_inicio = pd.to_datetime(
        data_inicio, format="%Y/%m/%d") - pd.DateOffset(months=1)
df = web.DataReader(['^GSPC', '^BVSP'], 'yahoo', _data_inicio, data_fim)['Adj Close']

data_inicial = pd.to_datetime(data_inicio,
                              format="%Y/%m/%d") - pd.DateOffset(days=1)
df = df.loc[data_inicial:data_fim]
# df1 = df[['Adj Close']]
# df2 = df['Adj Close']
# df3 = df.loc[:, ['Adj Close']]
# df4 = df.iloc[:, [5]].plot(figsize=(16,8))
# df1.plot(figsize=(16,8))
# df1.plot(figsize=(16,8), title='GSPC')
# df1 = pd.DataFrame(df1)
# df1.groupby(pd.Grouper(freq='M')).count().plot(figsize=(16,8))
# df2 = df1.resample('BM').count()
# df3 = round(df1.pct_change(freq='BM').dropna() * 100, 2)


# %%

df1 = df / df.shift(1) - 1
df2 = df.pct_change()
df1 = df1.loc[data_inicio:data_fim]
df2 = df2.loc[data_inicio:data_fim]


df3 = df.pct_change(freq='BM').dropna()
df3 = df3.loc[data_inicio:data_fim]
df4 = df.pct_change().resample('M').agg(lambda x: (x + 1).prod() - 1)
df4 = df4.loc[data_inicio:data_fim]

pd.DataFrame.resample()

# %%
data_inicio = pd.to_datetime(
        data_inicio, format="%Y/%m/%d") - pd.DateOffset(months=1)

data_inicial = pd.to_datetime(data_inicio,
                                  format="%Y/%m/%d") - pd.DateOffset(days=1)
df_ret = df.loc[data_inicial:data_fim]

df_ret = df_ret[data_inicio:data_fim]

"""
# %%    TESTE


# %% teste de python

from datetime import datetime
import pandas as pd
import numpy as np
import pandas_datareader.data as web
import pandas_profiling as pp



# %% Datas

today = datetime.today().strftime("%Y-%m-%d")
data_inicio = "2021-01-01"
data_fim = "2022-01-31"

# %%

cadastro = pd.read_csv('cadastro.csv').set_index('CNPJ_FUNDO')
informes = pd.read_csv('informes.csv')


# %% Dados do cnpj a pesquisar


CNPJ = [
    "05.523.348/0001-87",
    "11.858.554/0001-40",
    "39.303.195/0001-84",
    "32.972.925/0001-90",
    "35.650.636/0001-63",
    "36.249.379/0001-15"
]


# %% 1 Criar um dataframe com os dados um loop

fundos = pd.DataFrame()

for cnpj in CNPJ:
    fundo = informes[informes["CNPJ_FUNDO"] == cnpj].set_index("DT_COMPTC")
    fundo = fundo["VL_QUOTA"] / fundo["VL_QUOTA"].iloc[0]
    fundos.loc[cnpj, "Retorno(%)"] = round((fundo.iloc[-1] - 1) * 100, 2)
    fundos.loc[cnpj, "Nome"] = [cadastro.loc[cnpj, "DENOM_SOCIAL"]]
    fundos.loc[cnpj, "Tipo"] = [cadastro.loc[cnpj, "TP_FUNDO"]]
    fundos.loc[cnpj, "Classe"] = [cadastro.loc[cnpj, "CLASSE"]]
    fundos.loc[cnpj, "PL"] = [cadastro.loc[cnpj, "VL_PATRIM_LIQ"]]
    fundos.loc[cnpj, "Situacao"] = [cadastro.loc[cnpj, "SIT"]]

fundos = fundos.sort_values(by=["Retorno(%)"], ascending=False)

# %% 2 Criar um dataframe com os dados e loop aninhados - Dificil interpretacao

fundos1 = pd.DataFrame()

for cnpj in CNPJ:
    fundo1 = informes[informes["CNPJ_FUNDO"] == cnpj].set_index("DT_COMPTC")
    fundo1 = fundo1["VL_QUOTA"] / fundo1["VL_QUOTA"].iloc[0]
    fundos1.loc[cnpj, "Retorno(%)"] = round((fundo1.iloc[-1] - 1) * 100, 2)
    fundos1['Nome'] = [cadastro.loc[cnpj, 'DENOM_SOCIAL']
                       for cnpj in fundos1.index]
    fundos1['Tipo'] = [cadastro.loc[cnpj, 'TP_FUNDO']
                       for cnpj in fundos1.index]
    fundos1['Classe'] = [cadastro.loc[cnpj, 'CLASSE']
                         for cnpj in fundos1.index]
    fundos1['PL'] = [cadastro.loc[cnpj, 'VL_PATRIM_LIQ']
                     for cnpj in fundos1.index]
    fundos1['Situacao'] = [cadastro.loc[cnpj, 'SIT']
                           for cnpj in fundos1.index]

fundos1 = fundos1.sort_values(by=["Retorno(%)"], ascending=False)

# %% 3 Criar um dataframe com os dados a partir de listas e loops independentes


retorno = []
for cnpj in CNPJ:
    retorno_cal = informes[informes["CNPJ_FUNDO"]
                           == cnpj].set_index("DT_COMPTC")
    retorno_cal = retorno_cal["VL_QUOTA"] / retorno_cal["VL_QUOTA"].iloc[0]
    retorno_cal = round((retorno_cal.iloc[-1] - 1) * 100, 2)
    retorno.append(retorno_cal)

nome = [cadastro.loc[cnpj, 'DENOM_SOCIAL']
        for cnpj in CNPJ]
tipo = [cadastro.loc[cnpj, 'TP_FUNDO']
        for cnpj in CNPJ]
classe = [cadastro.loc[cnpj, 'CLASSE']
          for cnpj in CNPJ]
patri = [cadastro.loc[cnpj, 'VL_PATRIM_LIQ']
         for cnpj in CNPJ]
sit = [cadastro.loc[cnpj, 'SIT']
       for cnpj in CNPJ]

fundos2 = (pd.DataFrame(

    [
        CNPJ,
        retorno,
        nome,
        tipo,
        classe,
        patri,
        sit
    ],
    [
        'CNPJ',
        'Retorno(%)',
        'Nome',
        'Tipo',
        'Classe',
        'Patrimonio',
        'Situacao'
    ],
)
    .transpose()
    .set_index('CNPJ')
)
fundos2 = fundos2.sort_values(by=["Retorno(%)"], ascending=False)


# %% Teste

