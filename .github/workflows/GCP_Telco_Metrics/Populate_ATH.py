import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from datetime import datetime
from dateutil.relativedelta import relativedelta
import random
import uuid

# =========================
# Variáveis do Projeto
# =========================
PROJECT_ID   = "telco-metrics-473116"
DATASET_ID   = "telco_metrics_gold"
CUSTOMERS_TB = "customers"
ATH_TB       = "ath"

# Captura de clientes ativos
bq = bigquery.Client(project=PROJECT_ID)
sql = f"""
SELECT ID_CLIENTE, DT_ENTRADA
FROM `{PROJECT_ID}.{DATASET_ID}.{CUSTOMERS_TB}`
WHERE STATUS = 'A'
"""
clientes = bq.query(sql).to_dataframe()


# =========================
# 2) Catálogos simples de motivos
# =========================
motivos_info = [
    ("informacao", "planos", "detalhes_fatura"),
    ("informacao", "tecnico", "agendamento"),
    ("informacao", "cadastro", "dados_pessoais"),
]
motivos_soli = [
    ("solicitacao", "segunda_via", "boleto"),
    ("solicitacao", "mudanca_plano", "upgrade"),
    ("solicitacao", "visita_tecnica", "reparo"),
]
motivos_recl = [
    ("reclamacao", "cobranca", "valor_divergente"),
    ("reclamacao", "qualidade", "internet_lenta"),
    ("reclamacao", "atendimento", "tempo_espera"),
]
catalogo = {
    "informacao": motivos_info,
    "solicitacao": motivos_soli,
    "reclamacao": motivos_recl,
}

# =========================
# 3) Geração aleatória por cliente
#    - volume de interações por cliente: 0..6
#    - cada cliente tem "taxa de reclamação" própria (aleatória)
#    - retenção = 10% (True)
# =========================
def sorteia_tipo(p_reclamacao: float):
    # pesos: ajusta reclamação por cliente, restante divide entre informação/solicitação
    p_rec = max(0.0, min(1.0, p_reclamacao))
    restante = 1.0 - p_rec
    p_info = restante * 0.6
    
    p_soli = restante * 0.4
    return random.choices(
        ["informacao", "solicitacao", "reclamacao"],
        weights=[p_info, p_soli, p_rec],
        k=1
    )[0]

registros = []
# Use naive UTC datetimes for compatibility with Python < 3.11
today = datetime.now()
hoje = datetime.now().date()

for _, c in clientes.iterrows():
    id_cliente = c["ID_CLIENTE"]
    dt_entrada = c["DT_ENTRADA"]
    inicio_janela = c["DT_ENTRADA"]
    fim_janela = inicio_janela

    # volume aleatório por cliente (pode ser 0)
    n_inter = random.randint(0, 6)

    # probabilidade de reclamação exclusiva desse cliente (0%..60%)
    p_reclamacao_cliente = random.choice([0.05, 0.10, 0.15, 0.25, 0.40, 0.60])

    for i in range(n_inter):
        tipo = sorteia_tipo(p_reclamacao_cliente)
        m1, m2, m3 = random.choice(catalogo[tipo])
        # data aleatória entre dt_entrada e hoje
        Days = (hoje - inicio_janela).days
        random_days = random.randint(0, Days)
        dt_inter = dt_entrada + relativedelta(days=random_days)
        #convert dt_inter to timestamp
        dt_inter = datetime(dt_inter.year, dt_inter.month, dt_inter.day)
        aht_seg = random.randint(120, 1200)  # 2 min a 20 min

        registros.append({
            "ID_INTERACAO": str(uuid.uuid4()),
            "ID_CLIENTE": id_cliente,
            "DT_INTERACAO": dt_inter,
            "MOTIVO1": m1,
            "MOTIVO2": m2,
            "MOTIVO3": m3,
            "TIPO": tipo,
            "RETENCAO": (random.random() < 0.10),  # 10%
            "AHT_SEG": aht_seg,
            "ANO": dt_inter.year,
            "MES": dt_inter.month,
            "DT_INGESTAO": hoje,
            "FONTE": "Populate_ATH"
        })


# cria DataFrame e faz upload para o BigQuery
df = pd.DataFrame(registros, columns=[
    "ID_INTERACAO", "ID_CLIENTE", "DT_INTERACAO", "MOTIVO1", "MOTIVO2", "MOTIVO3",
    "TIPO", "RETENCAO", "AHT_SEG", "ANO", "MES", "DT_INGESTAO", "FONTE"
])
if df.empty:
    print("Nenhum registro gerado para upload (df vazio).")
    raise SystemExit(0)
# Ensure datetime columns are in datetime format
df["DT_INTERACAO"] = pd.to_datetime(df["DT_INTERACAO"], errors="coerce")
df["DT_INGESTAO"] = pd.to_datetime(df["DT_INGESTAO"], errors="coerce")

# Upload para o BigQuery
pandas_gbq.to_gbq(
    df,
    destination_table=f"{DATASET_ID}.{ATH_TB}",
    project_id=PROJECT_ID,
    if_exists="append",
    progress_bar=True,
    api_method="load_csv"
)
print(f"✅ {len(df)} interações carregadas em {PROJECT_ID}.{DATASET_ID}.{ATH_TB}")
