"""

Base de Dados CVM:
http://dados.cvm.gov.br/

DADOS DIARIOS - MES
http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/
https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_2022MM.zip


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


def consulta_cvm_cadastro_completo():
    """

    Parameters
    ----------
    none

    Returns
    -------
    Pandas DataFrame com os dados dos fundos com todas as colunas

    """

    url = "http://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv"

    try:

        cadastro = pd.read_csv(
            url, sep=";", encoding="ISO-8859-1", dtype="unicode")

    except:
        print("Arquivo de dados não encontrado!")
        print("Verificar URL da CVM")

    cadastro.set_index("CNPJ_FUNDO", drop=True, inplace=True)

    # retira duplicados
    cadastro.drop_duplicates(inplace=True)

    return cadastro


# funcao  busca informes mensais na CVM

def consulta_cvm_informes(data_inicio, data_fim):
    """

    Parameters
    ----------
    ATENCAO - Deprecated - Arquivos na CVM estao zipados
              Separacao de dados por virgula
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
            url = "http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{}{:02d}.zip".format(
                data.year, data.month)
            informe_mensal = pd.read_csv(url, sep=",")

        except:
            print("Arquivo {} não encontrado!".format(url))
            print("Forneça outra data!")

        informe_completo = pd.concat(
            [informe_completo, informe_mensal], ignore_index=True)

    return informe_completo


# funcao  busca informes mensais na CVM acrescido dos dados de 2021

def consulta_cvm_informes_upgrade(data_inicio, data_fim):
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

    # datas de solicitacao dos arquivos
    datas = pd.date_range(data_inicio, data_fim, freq="MS")

    informe_completo = pd.DataFrame()
    for data in datas:
        try:
            url = "http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{}{:02d}.zip".format(
                data.year, data.month)
            informe_mensal = pd.read_csv(url, sep=",")

        except:
            print("Arquivo {} não encontrado!".format(url))
            print("Forneça outra data!")

        informe_completo = pd.concat(
            [informe_completo, informe_mensal], ignore_index=True)

    informes_2021 = pd.read_csv('informes_2021.csv').drop('Unnamed: 0', axis=1)
    informe_upgrade = pd.concat(
        [informes_2021, informe_completo], ignore_index=True)

    return informe_upgrade


def consulta_cvm_informes_zip():
    """

    Parameters
    ----------

    ATENCAO - Acertar datas
    data_inicio : data
                  YYYY-MM

    data_fim : data
                  YYYY-MM

    Returns
    -------
    Pandas DataFrame com as informacoes mensais de rendimento dos fundos
    no periodo solitado

    """


    informes_abr22 = pd.read_csv('informes_abr22.csv').drop('Unnamed: 0', axis=1)
    informes = pd.read_csv('informes.csv', sep=",")
    informe_upgrade = pd.concat(
        [informes_abr22, informes], ignore_index=True)

    return informe_upgrade



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
        url = 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{}{:02d}.zip'.format(
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
        fundo = fundo.loc[data_inicio:data_fim]
        fundo = fundo["VL_QUOTA"] / fundo["VL_QUOTA"].iloc[0]
        fundos.loc[cnpj, "Retorno(%)"] = round((fundo.iloc[-1] - 1) * 100, 2)
        fundos.loc[cnpj, "Nome"] = [cadastro.loc[cnpj, "DENOM_SOCIAL"]]
        fundos.loc[cnpj, "Tipo"] = [cadastro.loc[cnpj, "TP_FUNDO"]]
        fundos.loc[cnpj, "Classe"] = [cadastro.loc[cnpj, "CLASSE"]]
        fundos.loc[cnpj, "PL"] = [cadastro.loc[cnpj, "VL_PATRIM_LIQ"]]
        fundos.loc[cnpj, "Situacao"] = [cadastro.loc[cnpj, "SIT"]]
    return fundos.sort_values(by=["Retorno(%)"], ascending=False)


"""

Opcao de criacao do dataset posteriormente aos dados, isto facilita o processamento


def consulta_fundos_total(cnpj):


    retorno = []
    for cnpj in cnpj:
        retorno_cal = informes[informes["CNPJ_FUNDO"]
                            == cnpj].set_index("DT_COMPTC")
        retorno_cal = retorno_cal["VL_QUOTA"] / retorno_cal["VL_QUOTA"].iloc[0]
        retorno_cal = round((retorno_cal.iloc[-1] - 1) * 100, 2)
        retorno.append(retorno_cal)

    nome = [cadastro.loc[cnpj, 'DENOM_SOCIAL']
            for cnpj in cnpj]
    tipo = [cadastro.loc[cnpj, 'TP_FUNDO']
            for cnpj in cnpj]
    classe = [cadastro.loc[cnpj, 'CLASSE']
            for cnpj in cnpj]
    patri = [cadastro.loc[cnpj, 'VL_PATRIM_LIQ']
            for cnpj in cnpj]
    sit = [cadastro.loc[cnpj, 'SIT']
        for cnpj in cnpj]

    fundos = (pd.DataFrame(

        [
            cnpj,
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
    return fundos.sort_values(by=["Retorno(%)"], ascending=False)


"""

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
    # data_inicial = pd.to_datetime(data_inicio,
    #                               format="%Y/%m/%d") - pd.DateOffset(days=1)
    data_inicial = pd.to_datetime(data_inicio) - pd.DateOffset(days=1)
    data_inicial = data_inicial.strftime("%Y-%m-%d")
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
    # data_inicial = pd.to_datetime(data_inicio,
    #                               format="%Y/%m/%d") - pd.DateOffset(days=1)
    data_inicial = pd.to_datetime(data_inicio) - pd.DateOffset(days=1)
    data_inicial = data_inicial.strftime("%Y-%m-%d")
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
    # TODO: tratar inicio do df na funcao de origem
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
    data_inicial = pd.to_datetime(data_inicio) - pd.DateOffset(days=1)
    data_inicial = data_inicial.strftime("%Y-%m-%d")
    df = df.loc[data_inicial:data_fim]

    # Calculate monthly returns
    ret_mensal = df.pct_change().resample('M').agg(lambda x: (x + 1).prod() - 1)
    # ret_mensal = df.pct_change(freq='BM').dropna()

    # Calculate monthly cumulative returns
    ret_mensal_acum = (1 + ret_mensal).cumprod() - 1

    ret_mensal = round(ret_mensal * 100, 2)[data_inicio:data_fim]
    ret_mensal_acum = round(ret_mensal_acum * 100, 2)[data_inicio:data_fim]

    # define data de retorno dos df
    ret_mensal.index = ret_mensal.index.strftime('%Y-%m')
    ret_mensal_acum.index = ret_mensal_acum.index.strftime('%Y-%m')

    return ret_mensal, ret_mensal_acum


# funcao para calcular retorno mensal de um dataframe de valores diarios percentuas

def retorno_mensal_df_pct(df, data_inicio, data_fim):
    # TODO: tratar inicio do df na funcao de origem
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
    # data_inicial = pd.to_datetime(data_inicio,
    #                               format="%Y/%m/%d") - pd.DateOffset(days=1)
    data_inicial = pd.to_datetime(data_inicio) - pd.DateOffset(days=1)
    data_inicial = data_inicial.strftime("%Y-%m-%d")
    df = df.loc[data_inicial:data_fim]

    # Calculate monthly returns
    ret_mensal = df.resample('M').agg(
        lambda x: (x / 100 + 1).prod() - 1)
    # ret_mensal = df.pct_change(freq='BM').dropna()

    # Calculate monthly cumulative returns
    ret_mensal_acum = (1 + ret_mensal).cumprod() - 1

    ret_mensal = round(ret_mensal * 100, 2)[data_inicio:data_fim]
    ret_mensal_acum = round(ret_mensal_acum * 100, 2)[data_inicio:data_fim]

    # define data de retorno dos df
    ret_mensal.index = ret_mensal.index.strftime('%Y-%m')
    ret_mensal_acum.index = ret_mensal_acum.index.strftime('%Y-%m')

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

    # fig.update_yaxes(rangemode="tozero")
    # fig.update_xaxes(rangemode="tozero")
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


