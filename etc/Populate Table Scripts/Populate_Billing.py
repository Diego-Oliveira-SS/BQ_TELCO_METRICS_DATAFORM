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

# Função para gerar a data da primeira fatura (mês seguinte à entrada)
def primeira_fatura(dt, dia_venc):
    M_1 = dt + relativedelta(months=1)
    first_venc = date(M_1.year, M_1.month, dia_venc)
    return first_venc

# Captura clientes (customers)
bq = bigquery.Client(project=PROJECT_ID)
sql = f"""
SELECT ID_CLIENTE, DT_ENTRADA, VENCIMENTO, VALOR_PLANO, DESCONTO
FROM `{PROJECT_ID}.{DATASET_ID}.{CUSTOMERS_TB}`
"""
clientes = bq.query(sql).to_dataframe()

# Gera faturas (billing)
billing = []
for _, row in clientes.iterrows():
    id_cliente = row['ID_CLIENTE']
    dt_entrada = row['DT_ENTRADA']
    Vencimento = primeira_fatura(dt_entrada, row['VENCIMENTO'])

    while Vencimento - relativedelta(days=8) < TODAY:
        ano = Vencimento.year
        mes = Vencimento.month
        id_fatura = f"F{id_cliente}{ano}{mes:02d}"
        dt_emissao = Vencimento - relativedelta(days=8)
        dt_vencimento = Vencimento
        valor_plano = row["VALOR_PLANO"]
        desconto = row["DESCONTO"]
        valor_base = (valor_plano - desconto)

        billing.append((
            id_fatura,
            id_cliente,
            dt_emissao,
            dt_vencimento,
            valor_plano,
            desconto,
            valor_base,
            ano,
            mes,
            TODAY,   # DT_INGESTAO
            "populate_customers"  # FONTE
        ))

        Vencimento = Vencimento + relativedelta(months=1)

    # print(len([b[3] for b in billing]))  # dt_vencimento is the 4th element in each tuple
    # print(billing[0])  # Print the last billing entry for verification
# Envia para o BigQuery

columns = [
    "ID_FATURA",
    "ID_CLIENTE",
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

df = pd.DataFrame(billing, columns=columns)

pandas_gbq.to_gbq(
    df,
    destination_table=f"{DATASET_ID}.{BILLING_TB}",
    project_id=PROJECT_ID,
    if_exists="append",
    progress_bar=True
    )

print(f"Total de faturas geradas: {len(billing)}")