import pandas as pd
import pyodbc
import re
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
import time
import os

"""Config dotenv"""
from dotenv import load_dotenv
from pathlib import Path
def localizar_env(diretorio_raiz="PRIVATE_BAG.ENV"):
    path = Path(__file__).resolve()
    for parent in path.parents:
        possible = parent / diretorio_raiz / ".env"
        if possible.exists():
            return possible
    raise FileNotFoundError(f"Arquivo .env não encontrado dentro de '{diretorio_raiz}'.")
env_path = localizar_env()
load_dotenv(dotenv_path=env_path)

# Variáveis configuráveis
diretorio_planilha = r"Z:\PLANEJAMENTO LOGISTICO\02 - PESSOAL\13 - Camila Neves\ACOMP NACIONAL.xlsx"
servidor = f"{os.getenv('DB_SERVER_EXCEL')},{os.getenv('DB_PORT_EXCEL')}"
banco = os.getenv('DB_DATABASE_EXCEL')
usuario = os.getenv('DB_USER_EXCEL')
senha = os.getenv('DB_PASSWORD_EXCEL')
tabela_destino = "CD_AcompNacional"

# Mapeamento das colunas do Excel para os nomes do banco
mapeamento_colunas = {
    "SKU": "SKU",
    "DESCRIÇÃO SKU": "DESCRICAO_SKU",
    "FORN": "FORN",
    "QTD EMITIDA": "QTD_EMITIDA",
    "QTDE ENTREGUE TOTAL": "QTDE_ENTREGUE_TOTAL",
    "QTDE ENTREGA 1": "QTDE_ENTREGA_1",
    "DATA ENTREGA 1": "DATA_ENTREGA_1",
    "NF 1": "NF_1",
    "VALOR NF 1": "VALOR_NF_1",
    "VENCIMENTO NF 1": "VENCIMENTO_NF_1",
    "QTDE ENTREGA 2": "QTDE_ENTREGA_2",
    "DATA ENTREGA 2": "DATA_ENTREGA_2",
    "NF 2": "NF_2",
    "VALOR NF 2": "VALOR_NF_2",
    "VENCIMENTO NF 2": "VENCIMENTO_NF_2",
    "QTDE ENTREGA 3": "QTDE_ENTREGA_3",
    "DATA ENTREGA 3": "DATA_ENTREGA_3",
    "NF 3": "NF_3",
    "VALOR NF 3": "VALOR_NF_3",
    "VENCIMENTO NF 3": "VENCIMENTO_NF_3",
    "QTDE ENTREGA 4": "QTDE_ENTREGA_4",
    "DATA ENTREGA 4": "DATA_ENTREGA_4",
    "NF 4": "NF_4",
    "VALOR NF 4": "VALOR_NF_4",
    "VENCIMENTO NF 4": "VENCIMENTO_NF_4",
    "QTDE ENTREGA 5": "QTDE_ENTREGA_5",
    "DATA ENTREGA 5": "DATA_ENTREGA_5",
    "NF 5": "NF_5",
    "VALOR NF 5": "VALOR_NF_5",
    "VENCIMENTO NF 5": "VENCIMENTO_NF_5",
    "QTDE ENTREGA 6": "QTDE_ENTREGA_6",
    "DATA ENTREGA 6": "DATA_ENTREGA_6",
    "NF 6": "NF_6",
    "VALOR NF 6": "VALOR_NF_6",
    "VENCIMENTO NF 6": "VENCIMENTO_NF_6",
    "QTDE ENTREGA 7": "QTDE_ENTREGA_7",
    "DATA ENTREGA 7": "DATA_ENTREGA_7",
    "NF 7": "NF_7",
    "VALOR NF 7": "VALOR_NF_7",
    "VENCIMENTO NF 7": "VENCIMENTO_NF_7",
    "QTDE ENTREGA 8": "QTDE_ENTREGA_8",
    "DATA ENTREGA 8": "DATA_ENTREGA_8",
    "NF 8": "NF_8",
    "VALOR NF 8": "VALOR_NF_8",
    "VENCIMENTO NF 8": "VENCIMENTO_NF_8",
    "QTDE A ENTREGAR": "QTDE_A_ENTREGAR",
    "DATA PREVISTA": "DATA_PREVISTA",
    "ETA REAL": "ETA_REAL",
    "Disponível Venda": "DISPONIVEL_VENDA",
    "STATUS PEDIDO": "STATUS_PEDIDO",
    "PEDIDO": "PEDIDO",
    "PRAZO": "PRAZO",
    "RETORNO": "RETORNO",
}

def inserir_lote(conexao_str, tabela, df_lote, lote_id):
    """Insere um lote de dados no banco."""
    try:
        with pyodbc.connect(conexao_str) as conexao:
            cursor = conexao.cursor()
            cols = ", ".join([f"[{col}]" for col in df_lote.columns])
            placeholders = ", ".join(["?"] * len(df_lote.columns))
            sql = f"INSERT INTO {tabela} ({cols}) VALUES ({placeholders})"

            data = [tuple(row) for row in df_lote.itertuples(index=False, name=None)]
            cursor.executemany(sql, data)
            conexao.commit()
            print(f"Lote {lote_id} inserido com sucesso: {len(df_lote)} registros.")
    except Exception as e:
        print(f"Erro ao inserir lote {lote_id}: {e}")

def remover_registros_nao_visualizados(conexao_str, tabela):
    """Remove registros da tabela que não são visualizados pela view."""
    try:
        with pyodbc.connect(conexao_str) as conexao:
            cursor = conexao.cursor()
            sql = f"""
            WITH CTE_Ranking AS (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY PEDIDO, SKU, FORN, NF_1 
                        ORDER BY DATA_INSERCAO DESC
                    ) AS RowNum
                FROM {tabela}
            )
            DELETE
            FROM CTE_Ranking
            WHERE RowNum > 1;
            """
            cursor.execute(sql)
            conexao.commit()
            print("Registros não visualizados pela view foram removidos com sucesso.")
    except Exception as e:
        print(f"Erro ao remover registros não visualizados: {e}")

# Configuração da conexão com o SQL Server
try:
    start_time = time.time()

    conexao_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={servidor};"
        f"DATABASE={banco};"
        f"UID={usuario};"
        f"PWD={senha};"
    )

    try:
        # Lê a planilha com as colunas de B até BB e ignora a linha 1
        df = pd.read_excel(diretorio_planilha, sheet_name="PEDIDOS", skiprows=1, usecols="B:BB")
        
        # Verifica se o cabeçalho das colunas está correto
        colunas_esperadas = [
            "SKU", "DESCRIÇÃO SKU", "FORN", "QTD EMITIDA", "QTDE ENTREGUE TOTAL", "QTDE ENTREGA 1", 
            "DATA ENTREGA 1", "NF 1", "VALOR NF 1", "VENCIMENTO NF 1", "QTDE ENTREGA 2", "DATA ENTREGA 2", 
            "NF 2", "VALOR NF 2", "VENCIMENTO NF 2", "QTDE ENTREGA 3", "DATA ENTREGA 3", "NF 3", 
            "VALOR NF 3", "VENCIMENTO NF 3", "QTDE ENTREGA 4", "DATA ENTREGA 4", "NF 4", "VALOR NF 4", 
            "VENCIMENTO NF 4", "QTDE ENTREGA 5", "DATA ENTREGA 5", "NF 5", "VALOR NF 5", "VENCIMENTO NF 5", 
            "QTDE ENTREGA 6", "DATA ENTREGA 6", "NF 6", "VALOR NF 6", "VENCIMENTO NF 6", "QTDE ENTREGA 7", 
            "DATA ENTREGA 7", "NF 7", "VALOR NF 7", "VENCIMENTO NF 7", "QTDE ENTREGA 8", "DATA ENTREGA 8", 
            "NF 8", "VALOR NF 8", "VENCIMENTO NF 8", "QTDE A ENTREGAR", "DATA PREVISTA", "ETA REAL", 
            "Disponível Venda", "PEDIDO", "STATUS PEDIDO", "PRAZO", "RETORNO"
        ]
        colunas_atuais = df.columns.tolist()
        if colunas_atuais != colunas_esperadas:
            raise ValueError(f"Erro: O cabeçalho das colunas do Excel foi alterado. Esperado: {colunas_esperadas}, Encontrado: {colunas_atuais}")

    except Exception as e:
        print(f"Erro ao ler o arquivo Excel: {e}")
        raise

    # Substitui valores inválidos de forma robusta
    df = df.replace({pd.NA: None, "nan": None, "NaT": None, "None": None, "-": None, "": None})

    # Renomeia as colunas para os nomes do banco
    df = df.rename(columns=mapeamento_colunas)

    # Conversão da coluna "DATA_ENTREGA_1" para datetime
    df["DATA_ENTREGA_1"] = pd.to_datetime(df["DATA_ENTREGA_1"], errors="coerce")

    # Filtrar dados dos últimos 12 meses
    data_limite = datetime.now() - timedelta(days=365)
    df = df[df["DATA_ENTREGA_1"] >= data_limite]

    # Função para limpar e padronizar NFs (remove pontos, traços, letras, etc.)
    def limpar_nf(x):
        if pd.notnull(x):
            x = re.sub(r"\D", "", str(x))  # Remove tudo que não for dígito
            return x.zfill(9) if x else None
        return None

    # Aplica a limpeza nas colunas de nota fiscal
    colunas_nf = ["NF_1", "NF_2", "NF_3", "NF_4", "NF_5", "NF_6", "NF_7", "NF_8"]
    for coluna in colunas_nf:
        df[coluna] = df[coluna].apply(limpar_nf)

    # Após limpar, remove linhas onde a NF_1 ainda seja nula (exclui registros sem NF válida)
    df = df[df["NF_1"].notnull()]

    # Converter colunas específicas para FLOAT
    colunas_float = [
        "QTD_EMITIDA", "QTDE_ENTREGUE_TOTAL", "QTDE_ENTREGA_1", "QTDE_ENTREGA_2",
        "QTDE_ENTREGA_3", "QTDE_ENTREGA_4", "QTDE_ENTREGA_5", "QTDE_ENTREGA_6",
        "QTDE_ENTREGA_7", "QTDE_ENTREGA_8", "QTDE_A_ENTREGAR", "VALOR_NF_1",
        "VALOR_NF_2", "VALOR_NF_3", "VALOR_NF_4", "VALOR_NF_5", "VALOR_NF_6",
        "VALOR_NF_7", "VALOR_NF_8"
    ]
    for coluna in colunas_float:
        df[coluna] = df[coluna].apply(lambda x: float(x) if str(x).replace('.', '', 1).isdigit() else None)

    # Converte a coluna SKU para string e adiciona zeros à esquerda até ter 13 caracteres
    df["SKU"] = df["SKU"].apply(lambda x: str(x).zfill(13) if pd.notnull(x) else None)

    # Converte as demais colunas para string
    df = df.astype(str)

    # Substitui valores "nan", "NaT", "None" por None novamente
    df = df.replace(["nan", "NaT", "None"], None)

    # Logs para monitorar o progresso
    print(f"Preparando para inserir {len(df)} registros no banco de dados...")

    # Dividir os dados em lotes
    num_processadores = multiprocessing.cpu_count() - 1
    tamanho_lote = len(df) // num_processadores
    lotes = [df[i:i + tamanho_lote] for i in range(0, len(df), tamanho_lote)]

    # Inserção no banco com paralelismo usando batches
    with ThreadPoolExecutor(max_workers=num_processadores) as executor:
        futures = [executor.submit(inserir_lote, conexao_str, tabela_destino, lote, idx + 1) for idx, lote in enumerate(lotes)]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Erro em um dos threads: {e}")

    # Remover registros não visualizados pela view
    remover_registros_nao_visualizados(conexao_str, tabela_destino)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Dados inseridos com sucesso na tabela '{tabela_destino}'!")
    print(f"Tempo total de execução: {elapsed_time:.2f} segundos.")

except Exception as e:
    print(f"Ocorreu um erro geral: {e}")