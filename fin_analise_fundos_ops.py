"""

Base de Dados CVM:
http://dados.cvm.gov.br/

DADOS DIARIOS - MES
http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/


CADASTRO CVM
http://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv'

Base de dados BANCO CENTRAL DO BRASIL
http://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados?formato=csv
https://www3.bcb.gov.br/sgspub/localizarseries/localizarSeries.do?method=prepararTelaLocalizarSeries

"""

# %% import libraries


from datetime import datetime

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pandas_datareader.data as web
import plotly.graph_objects as go
import plotly.offline as py
import seaborn as sns


pd.options.plotting.backend = 'plotly'


# funcao consulta banco central cdi codigo 12


def consulta_bcb(codigo_bcb, data_inicio, data_fim):
    """

    Consulta indicadores no Banco Central do Brasi;

    Parameters
    ----------
    codigo_bcp: int
                ipca = consulta_bc(433)
                igpm = consulta_bc(189)
                selic_meta = consulta_bc(432)
                selic = consulta_bc(11)
                cdi diaria = consulta_bc(12)
                cdi mensal = consulta(4391)
                reservas_internacionais = consulta_bc(13621)
                pnad = consulta_bc(24369)

    Returns
    -------
    Pandas DataFrame com o indicador escolhido

    """

    # tratamento das datas incluido o mes anterior para calculo de retornos
    data_inicio = pd.to_datetime(
        data_inicio, format="%Y/%m/%d") - pd.DateOffset(months=1)

    url = "http://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados?formato=csv".format(
        codigo_bcb)
    df = pd.read_csv(
        url, parse_dates=True, dayfirst=True, index_col="data", sep=";", decimal=",")
    df.index = pd.to_datetime(df.index, format="%Y/%m/%d")
    df = df[data_inicio:data_fim]
    return df


# funcao consulta acoes


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


# funcao dataset com cadastro da cvm


def consulta_cvm_cadastro():
    """

    Parameters
    ----------
    none

    Returns
    -------
    Pandas DataFrame com os dados dos fundos

    """

    url = "http://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv"

    try:

        cadastro = pd.read_csv(
            url, sep=";", encoding="ISO-8859-1", dtype="unicode")

    except:
        print("Arquivo de dados não encontrado!")
        print("Verificar URL da CVM")

    # seleciona fundos em funcionamemto # problema com fundos encerrados
    cadastro = cadastro[
        cadastro["SIT"].isin(
            ["EM FUNCIONAMENTO NORMAL", "FASE PRÉ-OPERACIONAL"])
    ]

    # seleciona as colunas nescessarias
    cadastro = cadastro.loc[
        :, ["CNPJ_FUNDO", "DENOM_SOCIAL", "SIT", "TP_FUNDO", "CLASSE", "VL_PATRIM_LIQ"]
    ].set_index("CNPJ_FUNDO", drop=True)

    # retira duplicados
    cadastro.drop_duplicates(inplace=True)

    return cadastro


# funcao  busca informes mensais na CVM


def consulta_cvm_informes(data_inicio, data_fim):
    """

    Parameters
    ----------
    data_inicio : data
                  YYYY-MM

    data_fim : data
                  YYYY-MM

    Returns
    -------
    Pandas DataFrame com as informacoes mensais de rendimento dos fundos
    no periodo solitado

    """

    # tratamento das datas incluido o mes anterior para calculo de retornos
    data_inicio = pd.to_datetime(
        data_inicio, format="%Y/%m/%d") - pd.DateOffset(months=1)

    # datas de solicitacao dos arquivos
    datas = pd.date_range(data_inicio, data_fim, freq="MS")

    informe_completo = pd.DataFrame()
    for data in datas:
        try:
            url = "http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{}{:02d}.csv".format(
                data.year, data.month)
            informe_mensal = pd.read_csv(url, sep=";")

        except:
            print("Arquivo {} não encontrado!".format(url))
            print("Forneça outra data!")

        informe_completo = pd.concat(
            [informe_completo, informe_mensal], ignore_index=True)

    return informe_completo


# funcao dataset com informes mensais da cvm


def consulta_cvm_informes_mes(ano, mes):
    """
    Parameters
    ----------
    ano : data(ano)
          YYYY

    mes : data(mes)
          MM

    Returns
    -------
    Pandas DataFrame com as informacoes mensais de rendimento dos fundos
    """
    try:
        url = 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{}{:02d}.csv'.format(
            ano, mes)
        return pd.read_csv(url, sep=';')
    except:
        print('Arquivo de dados não encontrado!')
        print('Verificar data informada')


# consulta fundos por cnpj com totalizacao de retornos


def consulta_fundos_total(cnpj):
    """

    Consulta fundos pelo CNPJ na base de dados da CVM

    Parameters
    ----------
    cnpj: list
          lista com os cnpj solicitados em forma de lista

    Returns
    -------
    Pandas DataFrame com os cnpj escolhidos

    """

    fundos = pd.DataFrame()

    for cnpj in cnpj:
        fundo = informes[informes["CNPJ_FUNDO"] == cnpj].set_index("DT_COMPTC")
        fundo = fundo["VL_QUOTA"] / fundo["VL_QUOTA"].iloc[0]
        fundos.loc[cnpj, "Retorno(%)"] = round((fundo.iloc[-1] - 1) * 100, 2)
        fundos.loc[cnpj, "Nome"] = [cadastro.loc[cnpj, "DENOM_SOCIAL"]]
        fundos.loc[cnpj, "Tipo"] = [cadastro.loc[cnpj, "TP_FUNDO"]]
        fundos.loc[cnpj, "Classe"] = [cadastro.loc[cnpj, "CLASSE"]]
        fundos.loc[cnpj, "PL"] = [cadastro.loc[cnpj, "VL_PATRIM_LIQ"]]
        fundos.loc[cnpj, "Situacao"] = [cadastro.loc[cnpj, "SIT"]]
    return fundos.sort_values(by=["Retorno(%)"], ascending=False)


# consulta fundos por cnpj com redimentos diarios


def consulta_fundos_valores_diarios(cnpj):
    """

    Consulta fundos pelo CNPJ na base de dados da CVM

    Parameters
    ----------
    cnpj: list
          lista com os cnpj solicitados em forma de lista

    Returns
    -------
    Pandas DataFrame com os cnpj escolhidos

    """

    fundos = pd.DataFrame()

    for cnpj in cnpj:
        fundo = informes[informes["CNPJ_FUNDO"] == cnpj].set_index("DT_COMPTC")
        fundo = fundo[["VL_QUOTA"]].rename(columns={"VL_QUOTA": cnpj})
        fundos = pd.concat([fundos, fundo], axis=1)
    fundos.index = pd.to_datetime(fundos.index)
    return fundos


# funcao para calcular retorno diario de um dataframe de valores diarios

def retorno_diario_df_valores(df, data_inicio, data_fim):
    """

    Mensaliza o retorno de um bando de dados baseado nas datas de inicio e fim
    dados em uma coluna com valores diarios

    Parameters
    ----------
    df: Dataframe
        Banco de dados com datetime no index e uma coluna com os valores para cada
        cnpj

    Returns
    -------
    1. Pandas DataFrame com os retornos diarios
    2. Pandas DataFrame com os retornos diarios acumulados

    """
    # tratamento das datas incluido o mes anterior para calculo de retornos
    data_inicial = pd.to_datetime(data_inicio,
                                  format="%Y/%m/%d") - pd.DateOffset(days=1)
    df = df.loc[data_inicial:data_fim]

    # Calculate daily returns
    ret_dia = df.pct_change()
    ret_dia_acum = (1 + ret_dia).cumprod() - 1
    ret_dia = round(ret_dia * 100, 2)
    ret_dia_acum = round(ret_dia_acum * 100, 2)

    # define data de retorno dos df
    ret_dia = ret_dia[data_inicio:data_fim]
    ret_dia_acum = ret_dia_acum[data_inicio:data_fim]
    return ret_dia, ret_dia_acum


# funcao para calcular retorno diario de um dataframe de porcentagens diarios

def retorno_diario_df_pct(df, data_inicio, data_fim):
    """

    Mensaliza o retorno de um bando de dados baseado nas datas de inicio e fim
    dados em uma coluna com valores em pct

    Parameters
    ----------
    df: Dataframe
        Banco de dados com datetime no index e uma coluna com os valores percentuais


    data_inicio : Date
        Data do inicio do arquivo que tera o retorno mensalizado (index - Datetime)

    data_fim : Date
        Data do final do arquivo que tera o retorno mensalizado (index - Datetime)

    Returns
    -------
    1. Pandas DataFrame com os retornos diarios
    2. Pandas DataFrame com os retornos diarios acumulados

    """
    # tratamento das datas incluido o mes anterior para calculo de retornos
    data_inicial = pd.to_datetime(data_inicio,
                                  format="%Y/%m/%d") - pd.DateOffset(days=1)
    df = df.loc[data_inicial:data_fim]

    # Calculate daily returns

    ret_dia = df
    ret_dia_acum = (1 + ret_dia / 100).cumprod() - 1
    ret_dia_acum = round(ret_dia_acum * 100, 2)

    # define data de retorno dos df
    ret_dia = ret_dia.loc[data_inicio:data_fim]
    ret_dia_acum = ret_dia_acum[data_inicio:data_fim]
    return ret_dia, ret_dia_acum

# funcao para calcular retorno mensal de um dataframe de valores diarios


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
    data_inicial = pd.to_datetime(data_inicio,
                                  format="%Y/%m/%d") - pd.DateOffset(days=1)
    df = df.loc[data_inicial:data_fim]

    # Calculate monthly returns
    ret_mensal = df.pct_change().resample('M').agg(lambda x: (x + 1).prod() - 1)

    # Calculate monthly cumulative returns
    ret_mensal_acum = (1 + ret_mensal).cumprod() - 1

    ret_mensal = round(ret_mensal * 100, 2)
    ret_mensal_acum = round(ret_mensal_acum * 100, 2)

    # define data de retorno dos df
    ret_mensal = ret_mensal[data_inicio:data_fim]
    ret_mensal_acum = ret_mensal_acum[data_inicio:data_fim]

    return ret_mensal, ret_mensal_acum


# funcao para calcular retorno mensal de um dataframe de valores diarios percentuas

def retorno_mensal_df_pct(df, data_inicio, data_fim):
    """

    Mensaliza o retorno de um banco de dados baseado nas datas de inicio e fim
    dados em uma coluna com valores em pct

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
    data_inicial = pd.to_datetime(data_inicio,
                                  format="%Y/%m/%d") - pd.DateOffset(days=1)
    df = df.loc[data_inicial:data_fim]

    # Calculate monthly returns
    ret_mensal = df.resample('M').agg(
        lambda x: (x / 100 + 1).prod() - 1)

    # Calculate monthly cumulative returns
    ret_mensal_acum = (1 + ret_mensal).cumprod() - 1

    ret_mensal = round(ret_mensal * 100, 2)
    ret_mensal_acum = round(ret_mensal_acum * 100, 2)

    # define data de retorno dos df
    ret_mensal = ret_mensal[data_inicio:data_fim]
    ret_mensal_acum = ret_mensal_acum[data_inicio:data_fim]

    return ret_mensal, ret_mensal_acum


# funcao que seleciona e classifica fundos

def classifica_fundos(classificacao='melhores', num_ranking=0, minimo_cotista=100, classe=''):
    """
    Parameters
    ----------
    classificacao   : string
                    'melhores' ou 'piores

    num_ranking     : int
                    quantidade de fundos analisados, se igual a 0 todos os fundos

    minimo_cotista  : int
                    fundos com minimo numero de cotista

    classe          : string
                    seleciona o tipo de fundo ('acoes', 'multimercado',
                                               'rendafixa', 'cambial )


    Returns
    -------
    Pandas DataFrame com os melhore ou piores fundos
    """

    # selecao de fundos conforme cotistas
    sel_fundos = informes[informes['NR_COTST'] >= minimo_cotista]

    # seleciona fundos por tipo
    if classe == 'multimercado':
        fundo_classe = cadastro[cadastro['CLASSE']
                                == 'Fundo Multimercado']
        sel_fundos = sel_fundos[sel_fundos['CNPJ_FUNDO'].isin(
            fundo_classe.index)]
    if classe == 'acoes':
        fundo_classe = cadastro[cadastro['CLASSE'] == 'Fundo de Ações']
        sel_fundos = sel_fundos[sel_fundos['CNPJ_FUNDO'].isin(
            fundo_classe.index)]
    if classe == 'rendafixa':
        fundo_classe = cadastro[cadastro['CLASSE']
                                == 'Fundo de Renda Fixa']
        sel_fundos = sel_fundos[sel_fundos['CNPJ_FUNDO'].isin(
            fundo_classe.index)]
    if classe == 'cambial':
        fundo_classe = cadastro[cadastro['CLASSE'] == 'Fundo Cambial']
        sel_fundos = sel_fundos[sel_fundos['CNPJ_FUNDO'].isin(
            fundo_classe.index)]

    # prepara dataset
    sel_fundos = sel_fundos.pivot(index='DT_COMPTC',
                                  columns='CNPJ_FUNDO')

    # normaliza as quotas
    fundos_retorno = sel_fundos['VL_QUOTA'] / sel_fundos['VL_QUOTA'].iloc[0]

    # seleciona o tipo de classificacao
    classifica = False if classificacao == 'melhores' else True

    # seleciona a quantidade de fundos analisados
    num_fundos = len(fundos_retorno.columns) \
        if num_ranking == 0 else num_ranking

    selecao_fundos = pd.DataFrame()
    rank = np.arange(1, num_fundos + 1)

    selecao_fundos['Retorno(%)'] = (
        fundos_retorno.iloc[-1].sort_values(ascending=classifica))
    selecao_fundos = round((selecao_fundos - 1) * 100, 2)[:num_fundos]
    selecao_fundos.insert(0, 'Rank', rank)

    selecao_fundos['Nome'] = [cadastro.loc[cnpj, 'DENOM_SOCIAL']
                              for cnpj in selecao_fundos.index]
    selecao_fundos['Tipo'] = [cadastro.loc[cnpj, 'TP_FUNDO']
                              for cnpj in selecao_fundos.index]
    selecao_fundos['Classe'] = [cadastro.loc[cnpj, 'CLASSE']
                                for cnpj in selecao_fundos.index]
    selecao_fundos['PL'] = [cadastro.loc[cnpj, 'VL_PATRIM_LIQ']
                            for cnpj in selecao_fundos.index]

    return selecao_fundos


# Funcao do graficos diarios do retorno


def plot_retorno_diario(df, nome):
    """

    Plota grafico do retorno diario dos fundos conforme o df

    Parameters
    ----------
    df: Dataframe
        Banco de dados com datetime no index e uma coluna com os valores para cada
        cnpj ou nome do fundo

    nome: string
          Nome que sera titulo do grafico

    Returns
    -------
    Grafico interativo em arquivo no formato .html

    """
    fig = go.Figure()

    # fundos = df.columns[:-3] TODO: remover ultimas colunas
    fundos = df.columns
    traces = [go.Scatter(x=df.index, y=df[fundo], name=fundo)
              for fundo in fundos]

    fig.add_traces(traces)
    fig.update_layout(
        title="Retornos dos fundos " + nome + " no ano de 2021",
        legend_orientation="h",
        autosize=True,
        height=700,
        hovermode="x unified",
    )

    # fig.show()
    graph = nome + ".html"
    return py.plot(fig, filename=graph)


# funcao graficos retorno mensal


def plot_retorno_mensal(df, nome):
    """

    Plota grafico do retorno mensais dos fundos conforme o df

    Parameters
    ----------
    df: Dataframe
        Banco de dados com datetime no index e uma coluna com os valores para cada
        cnpj ou nome do fundo

    nome: string
          Nome que sera titulo do grafico

    Returns
    -------
    Grafico interativo em arquivo no formato .html

    """
    fig = go.Figure()

    # fundos = df.columns[:-3] TODO: remover
    fundos = df.columns
    traces = [go.Scatter(x=df.index, y=df[fundo], name=fundo)
              for fundo in fundos]

    # fig.add_trace(go.Scatter(x=df.index, y=df['CDI'], name='CDI',
    #                          line=dict(color='black', width=3)))

    # fig.add_trace(go.Scatter(x=df.index, y=df['Ibov'], name='Ibovespa',
    #                          line=dict(color='red', width=3)))

    # fig.add_trace(go.Scatter(x=df.index, y=df["Dolar"], name="Dolar",
    #                          line=dict(color="blue", width=3)))

    fig.add_traces(traces)
    fig.update_layout(
        title="Retornos dos fundos " + nome + " no ano de 2021",
        legend_orientation="h",
        autosize=True,
        height=700,
        hovermode="x unified",
    )

    # fig.show()
    graph = nome + ".html"
    return py.plot(fig, filename=graph)


# %% variaveis
# Opcao 1
today = datetime.today().strftime("%Y-%m-%d")
data_inicio = "2021-01-01"
data_fim = "2022-01-31"

# Opcao 2
# ano = "2021"
# mes = "10"

# %% consulta cadastro de fundos cvm com valores comerciais

cadastro = consulta_cvm_cadastro()
# cadastro.to_csv('cadastro.csv')
# cadastro = pd.read_csv('cadastro.csv').set_index('CNPJ_FUNDO')

# %% Opcao 1
# # consulta informes de fundos por periodo na cvm com valores de cotas

informes = consulta_cvm_informes(data_inicio, data_fim)
# informes.to_csv('informes.csv')
# informes = pd.read_csv('informes.csv')

# %% Opcao 2
# # consulta informe de fundos em determinado mes na cvm com valores de cotas

# informes = consulta_cvm_informes_mes(2021, 12)
# informes.to_csv('informes.csv')
# informes = pd.read_csv('informes.csv')

# %% consulta dados da bolsa

ativos = ['^BVSP', '^DJI', '^GSPC']
acoes_diario = consulta_yahoo(ativos, data_inicio, data_fim)
acoes_diario.columns = ['Ibov', 'DowJones', 'S&P500']

acoes = acoes_diario.reset_index()
# acoes_diario.to_csv('acoes_diario.csv')
# acoes_diario = pd.read_csv('acoes_diario.csv').set_index('data')

# %% Calculate daily returns

acoes_ret_diario, acoes_ret_diario_acum = retorno_diario_df_valores(
    acoes_diario, data_inicio, data_fim)

# %% Calculate monthly returns

acoes_ret_mensal, acoes_ret_mensal_acum = retorno_mensal_df_valores(
    acoes_diario, data_inicio, data_fim)

# acoes_ret_mensalT = acoes_ret_mensal.T
# acoes_ret_mensal_acumT = acoes_ret_mensal_acum.T

# %% consulta dados do dolar comercial


dolar_diario = consulta_bcb(1, data_inicio, data_fim)
dolar_diario.columns = ['Dolar']

# dolar_diario.to_csv('dolar_diario.csv')
# dolar_diario = pd.read_csv('dolar_diario.csv').set_index('data')

# %% Calculate daily returns

dolar_ret_diario, dolar_ret_diario_acum = retorno_diario_df_valores(
    dolar_diario, data_inicio, data_fim)

# %% Calculate monthly returns

dolar_ret_mensal, dolar_ret_mensal_acum = retorno_mensal_df_valores(
    dolar_diario, data_inicio, data_fim)

# dolar_ret_mensalT = dolar_ret_mensal.T
# dolar_ret_mensal_acumT = dolar_ret_mensal_acum.T


# %% consulta cdi e calcula acumulado


cdi_diario = consulta_bcb(12, data_inicio, data_fim)
cdi_diario.columns = ['CDI']

# cdi_diario.to_csv('cdi_diario.csv')
# cdi_diario = pd.read_csv('cdi_diario.csv')

# %% Calculate daily returns


cdi_ret_diario, cdi_ret_diario_acum = retorno_diario_df_pct(
    cdi_diario, data_inicio, data_fim)


# %% Calculate monthly returns

cdi_ret_mensal, cdi_ret_mensal_acum = retorno_mensal_df_pct(
    cdi_diario, data_inicio, data_fim)

# cdi_ret_mensalT = cdi_ret_mensal.T
# cdi_ret_mensal_acumT = cdi_ret_mensal_acum.T


# %% consulta ipca mensal e calcula acumulado


ipca_diario = consulta_bcb(433, data_inicio, data_fim)
ipca_diario.columns = ['IPCA']
ipca_ret_mensal = ipca_diario[data_inicio:data_fim].resample('M').last()

# ipca_diario.to_csv('ipca_diario.csv')
# ipca_diario = pd.read_csv('ipca_diario.csv')


# %% Calculate monthly returns


ipca_ret_mensal_acum = round(
    ((1 + ipca_ret_mensal / 100).cumprod() - 1) * 100, 2)

# ipca_ret_mensalT = ipca_ret_mensal.T
# ipca_ret_mensal_acumT = ipca_ret_mensal_acum.T


# %% meus fundos bb e itau


ITAU = [
    "05.523.348/0001-87",
    "11.858.554/0001-40",
    "39.303.195/0001-84",
    "32.972.925/0001-90",
    "35.650.636/0001-63",
    "36.249.379/0001-15"
]
ITAU_NAMES = [
    "Itaú Seleção MM",
    "Itaú RF Mix Crédito Privado",
    "Itaú Kinea IPCA RF",
    "Itaú Global Dinâmico RF LP",
    "Itaú Carteira",
    "Itaú Index SP500 USD"
]

BB = [
    "04.061.224/0001-64",
    "05.962.491/0001-75",
    "06.015.368/0001-00",
    "13.322.192/0001-02",
    "29.224.634/0001-00"
]
BB_NAMES = [
    "BB RF DI LP VIP",
    "BB MM Macro FI",
    "BB MM Juros e Moedas",
    "BB MM LP Multiestrategia",
    "BB RF LP High"
]

bb_diario = consulta_fundos_valores_diarios(BB)
bb_diario.columns = BB_NAMES

itau_diario = consulta_fundos_valores_diarios(ITAU)
itau_diario.columns = ITAU_NAMES


# ITAU
# %% Calculate daily returns

itau_ret_diario, itau_ret_diario_acum = retorno_diario_df_valores(
    itau_diario, data_inicio, data_fim)

# %% Calculate monthly returns

itau_ret_mensal, itau_ret_mensal_acum = retorno_mensal_df_valores(
    itau_diario, data_inicio, data_fim)

# itau_ret_mensalT = itau_ret_mensal.T
# itau_ret_mensal_acumT = itau_ret_mensal_acum.T
# itau_ret_mensal.to_csv('itau_ret_mensalT.csv')
# itau_ret_mensal_acum.to_csv('itau_ret_mensal_acumT.csv')


# BB
# %% Calculate daily returns

bb_ret_diario, bb_ret_diario_acum = retorno_diario_df_valores(
    bb_diario, data_inicio, data_fim)

# %% Calculate monthly returns

bb_ret_mensal, bb_ret_mensal_acum = retorno_mensal_df_valores(
    bb_diario, data_inicio, data_fim)

# bb_ret_mensalT = bb_ret_mensal.T
# bb_ret_mensal_acumT = bb_ret_mensal_acum.T
# bb_ret_mensal.to_csv('bb_ret_mensalT.csv')
# bb_ret_mensal_acum.to_csv('bb_ret_mensal_acumT.csv')

# %% consulta fundos totais


# fundos_itau = consulta_fundos_total(ITAU)
# fundos_bb = consulta_fundos_total(BB)


# %% valores diarios NAO TEM UTILDADE


valores_diarios = pd.concat([itau_diario, bb_diario,
                            acoes_diario, dolar_diario,
                            cdi_diario], axis=1)

valores_diarios = valores_diarios.fillna(
    axis=0, method='bfill').dropna()
valores_diarios = valores_diarios[data_inicio:data_fim]

plot_retorno_diario(valores_diarios, "valores")


# %% indices de retornos diarios plot


indices_ret_diarios = pd.concat([itau_ret_diario, bb_ret_diario,
                                 acoes_ret_diario, dolar_ret_diario,
                                 cdi_ret_diario], axis=1)

indices_ret_diarios = indices_ret_diarios.fillna(
    axis=0, method='bfill').dropna()

plot_retorno_diario(indices_ret_diarios, "diarios")
# indices_ret_diarios.Ibov.describe()
# indices_ret_diarios.Dolar.describe()


# %% indices de retornos diarios acumulados plot


indices_ret_diarios_acum = pd.concat([itau_ret_diario_acum, bb_ret_diario_acum,
                                      acoes_ret_diario_acum, dolar_ret_diario_acum,
                                      cdi_ret_diario_acum], axis=1)

indices_ret_diarios_acum = indices_ret_diarios_acum.fillna(
    axis=0, method='bfill').dropna()

plot_retorno_diario(indices_ret_diarios_acum, "diarios acumulados")


# %% correlacoes valores diarios


fig = plt.subplots(figsize=(11, 11))
sns.heatmap(
    valores_diarios.corr(), annot=True, fmt=".2f", annot_kws={"fontsize": 9})


# %% correlacoes indices diarios sem correlacao


fig = plt.subplots(figsize=(11, 11))
sns.heatmap(
    indices_ret_diarios.corr(), annot=True, fmt=".2f", annot_kws={"fontsize": 9})


# %% correlacoes indices diarios acumulados


fig = plt.subplots(figsize=(11, 11))
sns.heatmap(
    indices_ret_diarios_acum.corr(), annot=True, fmt=".2f", annot_kws={"fontsize": 9})


# %%  gerar dataset com indicadores financeiros mensal


fin_retorno_mensal = pd.concat([acoes_ret_mensal,
                                dolar_ret_mensal,
                                cdi_ret_mensal,
                                ipca_ret_mensal], axis=1)


# %%  gerar dataset com indicadores financeiros mensal acumulados


fin_retorno_mensal_acum = pd.concat([acoes_ret_mensal_acum,
                                     dolar_ret_mensal_acum,
                                     cdi_ret_mensal_acum,
                                     ipca_ret_mensal_acum], axis=1)


# %% graficos do itau mensal


itau_retorno_mensal = pd.concat([itau_ret_mensal,
                                 fin_retorno_mensal], axis=1)

plot_retorno_mensal(itau_retorno_mensal, "Itau Mensal")
# itau_retorno_mensal.T.to_csv('itau_retorno_mensal.csv')


# %% graficos do itau mensal acumulado


itau_retorno_mensal_acum = pd.concat([itau_ret_mensal_acum,
                                      fin_retorno_mensal_acum], axis=1)

plot_retorno_mensal(itau_retorno_mensal_acum, "Itau Mensal Acumulado")
# itau_retorno_mensal_acum.T.to_csv('itau_retorno_mensal_acum.csv')


# %% graficos do bb mensal


bb_retorno_mensal = pd.concat([bb_ret_mensal,
                               fin_retorno_mensal], axis=1)

plot_retorno_mensal(bb_retorno_mensal, "BB Mensal")
# bb_retorno_mensal.T.to_csv('bb_retorno_mensal.csv')

# %% graficos do bb mensal acumulado


bb_retorno_mensal_acum = pd.concat([bb_ret_mensal_acum,
                                    fin_retorno_mensal_acum], axis=1)

plot_retorno_mensal(bb_retorno_mensal_acum, "BB Mensal Acumulado")
# bb_retorno_mensal_acum.T.to_csv('bb_retorno_mensal_acum.csv')


# %% analise e classiifção dos indicadores financeiros dos fundos


melhores_fundos = classifica_fundos('melhores', 1000)
piores_fundos = classifica_fundos('piores', 1000)
# melhores_acoes = classifica_fundos('melhores', 100, classe='acoes')
# piores_acoes = classifica_fundos('piores', 100, classe='acoes')
melhores_rfixa = classifica_fundos('melhores', 500, classe='rendafixa')
piores_rfixa = classifica_fundos('piores', 500, classe='rendafixa')
melhores_multi = classifica_fundos('melhores', 500, classe='multimercado')
piores_multi = classifica_fundos('piores', 500, classe='multimercado')

# melhores_fundos.to_csv('melhores_fundos.csv')
# piores_fundos.to_csv('piores_fundos.csv')
# # melhores_acoes.to_csv('melhores_acoes.csv')
# # piores_acoes.to_csv('piores_acoes.csv')
# melhores_rfixa.to_csv('melhores_rfixa.csv')
# piores_rfixa.to_csv('piores_rfixa.csv')
# melhores_multi.to_csv('melhores_multi.csv')
# piores_multi.to_csv('piores_multi.csv')


# %% analise e classifição dos indicadores financeiros dos fundos BB e ITAU


# melhores_fundos = melhores_fundos.fillna().drop(index=12)
melhores_ITAU = melhores_fundos[melhores_fundos['Nome'].str.startswith(
    'ITAÚ')]
melhores_BB = melhores_fundos[melhores_fundos['Nome'].str.startswith('BB')]

piores_ITAU = piores_fundos[piores_fundos['Nome'].str.startswith('ITAÚ')]
piores_BB = piores_fundos[piores_fundos['Nome'].str.startswith('BB')]

melhores_ITAU.to_csv('melhores_itau.csv')
melhores_BB.to_csv('melhores_bb.csv')
piores_ITAU.to_csv('piores_itau.csv')
piores_BB.to_csv('piores_bb.csv')


# %% fundos bb e itau

fundos_itau = cadastro[cadastro['DENOM_SOCIAL'].str.startswith('ITAÚ')]
fundos_bb = cadastro[cadastro['DENOM_SOCIAL'].str.startswith('BB')]

# fundos_itau.to_csv('fundos_itau.csv')
# fundos_bb.to_csv('fundos_bb.csv')

# %% meus fundos bb e itau


ITAU = [
    "05.523.348/0001-87",
    "11.858.554/0001-40",
    "39.303.195/0001-84",
    "32.972.925/0001-90",
    "35.650.636/0001-63",
    "36.249.379/0001-15"
]
BB = [
    "04.061.224/0001-64",
    "05.962.491/0001-75",
    "06.015.368/0001-00",
    "13.322.192/0001-02",
    "29.224.634/0001-00"
]

lm_fundo_bb = consulta_fundos_total(BB)
lm_fundo_itau = consulta_fundos_total(ITAU)

# lm_fundo_bb.to_csv('lm_fundo_bb.csv')
# lm_fundo_itau.to_csv('lm_fundo_itau.csv')


# %% seleciona analise de somente um fundo


fundo = ["11.858.554/0001-40"]  # 'Itaú Seleção MM'
MM = consulta_fundos_valores_diarios(fundo)
MM_ret_mensal, MM_ret_acum = retorno_mensal_df_valores(
    MM, data_inicio, data_fim)


# %% seleciona analise de um indicador financeiro


ativos = 189  # IGPM
data_inicio = '2020-03-01'
data_fim = '2021-02-28'

igpm = consulta_bcb(ativos, data_inicio, data_fim)
igpm_mes_acum = round((((1 + igpm / 100).cumprod()) - 1) * 100, 2)


# ===============================================================================
# %% TESTE APAGAR
# ===============================================================================


# %%
