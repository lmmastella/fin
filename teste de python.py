

# %% teste de python

from datetime import datetime
from time import strftime
import pandas as pd
import pandas_datareader.data as web

pd.set_option('styler.render.repr', 'html')


# %% consulta yahoo

def consulta_yahoo(ativos, data_inicio, data_fim, col='Adj Close'):
    """

    Consulta fundos pelo CNPJ na base de dados da CVM

    Parameters
    ----------
    stocks: list
            lista com os indicadores desejados
            '^BVSP'    - Ibovespa
            '^DJI'     - Dow Jones
            'USDBRL=X' - Dolar
            '^GSPC'    - S&P500

    start:  date
            YYYY-MM-DD

    end:  date
          YYYY-MM-DD


    Returns
    -------
    Pandas DataFrame com os indicadores e colunas escolhidos
    consulta yahoo > help(yf.download)

    """

    # tratamento das datas incluido o mes anterior para calculo de retornos
    data_inicio = pd.to_datetime(
        data_inicio, format="%Y/%m/%d") - pd.DateOffset(months=1)

    data = web.DataReader(ativos,
                          data_source='yahoo',
                          start=data_inicio,
                          end=data_fim)[[col]]

    return data

# %% Teste de funcao de retorno

def retorno_mensal_df_valores(df, data_inicio, data_fim):
    """

    Mensaliza o retorno de um banco de dados baseado nas datas de inicio e fim
    dados em uma coluna com valores diarios

    Parameters
    ----------
    df: Dataframe
        Banco de dados com datetime no index e uma coluna com os valores para cada
        cnpj

    data_inicio : Date
        Data do inicio do arquivo que tera o retorno mensalizado (index - Datetime)

    data_fim : Date
        Data do final do arquivo que tera o retorno mensalizado (index - Datetime)

    Returns
    -------
    1. Pandas DataFrame com os cnpj escolhidos e retornos mensais
    2. Pandas DataFrame com os cnpj escolhidos e retornos acumulados

    """

    # tratamento das datas incluido o mes anterior para calculo de retornos
    # data_inicial = pd.to_datetime(data_inicio,
    #                               format="%Y/%m/%d") - pd.DateOffset(days=1)
    # df = df.loc[data_inicial:data_fim]

    # Calculate monthly returns
    # ret_mensal = df.pct_change().resample('M').agg(lambda x: (x + 1).prod() - 1)
    ret_mensal = df.pct_change(freq='BM').dropna()

    # Calculate monthly cumulative returns
    ret_mensal_acum = (1 + ret_mensal).cumprod() - 1


    ret_mensal = round(ret_mensal * 100, 2)[data_inicio:data_fim]
    ret_mensal_acum = round(ret_mensal_acum * 100, 2)[data_inicio:data_fim]

    # define data de retorno dos df
    ret_mensal.index = ret_mensal.index.strftime('%Y-%m')
    ret_mensal_acum.index = ret_mensal_acum.index.strftime('%Y-%m')

    return ret_mensal, ret_mensal_acum




# %% Datas

today = datetime.today().strftime("%Y-%m-%d")
data_inicio = "2021-01-01"
data_fim = "2022-02-28"

# %% Consulta de dados do Yahoo

ativos = ['^BVSP', '^DJI', '^GSPC']
df = consulta_yahoo(ativos, data_inicio, data_fim)
df.columns = ['Ibov', 'DowJones', 'S&P500']

# utilizar para verificar
df1 =  df.reset_index()


# %% Retorno mensalizado

ret_mensal, ret_mensal_acum = retorno_mensal_df_valores(df, data_inicio, data_fim)

# %% Verificar

ret_mensal1 = ret_mensal.reset_index()

# %% tests

ativo = ['^BVSP']
ibov = consulta_yahoo(ativo, data_inicio, data_fim)
ibov.columns = ['Ibov']


ret_ibov = ibov.pct_change(freq='BM') # .dropna()
ret_ibov1 = ibov.reset_index()


# ret_ibov, ret_ibov_acum = retorno_mensal_df_valores(ibov, data_inicio, data_fim)

# %% verificar

ret_ibov1 = ret_ibov.reset_index()

# %%

"""
# %% Criando um dataframe com os dados do Yahoo

_data_inicio = pd.to_datetime(
    data_inicio, format="%Y/%m/%d") - pd.DateOffset(months=1)
df = web.DataReader(['^GSPC', '^BVSP'], 'yahoo', _data_inicio, data_fim)['Adj Close']

data_inicial = pd.to_datetime(data_inicio,
                              format="%Y/%m/%d") - pd.DateOffset(days=1)
df = df.loc[data_inicial:data_fim]
df1 = df[['Adj Close']]
df2 = df['Adj Close']
df3 = df.loc[:, ['Adj Close']]
df4 = df.iloc[:, [5]].plot(figsize=(16,8))
df1.plot(figsize=(16,8))
df1.plot(figsize=(16,8), title='GSPC')
df1 = pd.DataFrame(df1)
df1.groupby(pd.Grouper(freq='M')).count().plot(figsize=(16,8))
df2 = df1.resample('BM').count()
df3 = round(df1.pct_change(freq='BM').dropna() * 100, 2)
# %% returns

df1 = df.pct_change()
df1 = df1.loc[data_inicio:data_fim]

# verificar se o retorno mensal esta correto
df_shift1 = df.shift(1)
df2 = df / df.shift(1) - 1
df2 = df2.loc[data_inicio:data_fim]

# returns acumulados
df1_acum = (1 + df1).cumprod() - 1
# returns acumulados totais
df1a_acum = (1 + df1).prod() - 1

# returns mensais
df3 = df.pct_change(freq='BM').dropna()
df3 = df3.loc[data_inicio:data_fim]
df4 = df.pct_change().resample('BM').agg(lambda x: (x + 1).prod() - 1)
df4 = df4.loc[data_inicio:data_fim]
df3a = round(df3 * 100, 2)
df4a = round(df4 * 100, 2)




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

"""

# %% Teste

