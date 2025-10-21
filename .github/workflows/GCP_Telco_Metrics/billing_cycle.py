import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from decimal import Decimal, ROUND_HALF_UP


PROJECT_ID   = "telco-metrics-473116"
DATASET_ID   = "telco_metrics_gold"
CUSTOMERS_TB = "customers"
BILLING_TB   = "billing"


TODAY = date.today()
FIRST_DAY_TODAY = date(TODAY.year, TODAY.month, 1)


bq = bigquery.Client(project=PROJECT_ID)

sql = f"""
SELECT ID_CLIENTE, DT_ENTRADA, VENCIMENTO, VALOR_PLANO, DESCONTO
FROM `{PROJECT_ID}.{DATASET_ID}.{CUSTOMERS_TB}`
"""
clientes = bq.query(sql).to_dataframe()

# =============================
# FUNÇÕES AUXILIARES
# =============================
def prox_mes(dt):
    """Retorna o primeiro dia do mês seguinte."""
    return (date(dt.year, dt.month, 1) + relativedelta(months=1))

def gerar_vencimento(dt_comp, dia_venc):
    """Gera data de vencimento dentro do mês da competência."""
    nm_next = dt_comp + relativedelta(months=1)
    last_day = (nm_next - relativedelta(days=1)).day
    dia = min(int(dia_venc), last_day)
    return date(dt_comp.year, dt_comp.month, dia)

def quantize2(x):
    return Decimal(x).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

# =============================
# LOOP PRINCIPAL
# =============================
faturas = []

for _, row in clientes.iterrows():
    id_cliente = row["ID_CLIENTE"]
    dt_entrada = pd.to_datetime(row["DT_ENTRADA"]).date()
    dia_venc   = int(row["VENCIMENTO"]) if row["VENCIMENTO"] else 10
    valor_plano = Decimal(str(row["VALOR_PLANO"])) if row["VALOR_PLANO"] else Decimal("0.00")
    desconto    = Decimal(str(row["DESCONTO"])) if row["DESCONTO"] else Decimal("0.00")

    # Primeira fatura é no mês seguinte à entrada
    dt_comp = prox_mes(dt_entrada)

    while dt_comp < FIRST_DAY_TODAY:
        dt_emissao = dt_comp
        dt_vencimento = gerar_vencimento(dt_comp, dia_venc)
        ano, mes = dt_comp.year, dt_comp.month

        id_fatura = f"F{id_cliente}-{ano}{mes:02d}"

        valor_base = max(float("0.00"), valor_plano - desconto)

        faturas.append((
            id_fatura,
            id_cliente,
            dt_comp,
            dt_emissao,
            dt_vencimento,
            float(valor_plano),
            float(desconto),
            valor_base,
            ano,
            mes,
            datetime.now(),   # DT_INGESTAO
            "script_populate_billing"
        ))

        dt_comp = prox_mes(dt_comp)

# =============================
# DATAFRAME E UPLOAD PARA BIGQUERY
# =============================
columns = [
    "ID_FATURA",
    "ID_CLIENTE",
    "DT_COMPETENCIA",
    "DT_EMISSAO",
    "DT_VENCIMENTO",
    "VALOR_PLANO",
    "DESCONTO",
    "VALOR_BASE",
    "ANO",
    "MES",
    "DT_INGESTAO",
    "FONTE"
]

df = pd.DataFrame(faturas, columns=columns)

if len(df) == 0:
    print("⚠️ Nenhuma fatura gerada (verifique se há clientes com DT_ENTRADA anterior ao mês atual).")
else:
    pandas_gbq.to_gbq(
        df,
        destination_table=f"{DATASET_ID}.{BILLING_TB}",
        project_id=PROJECT_ID,
        if_exists="append",
        progress_bar=True
    )
    print(f"✅ {len(df)} faturas geradas e carregadas em {PROJECT_ID}.{DATASET_ID}.{BILLING_TB}")
