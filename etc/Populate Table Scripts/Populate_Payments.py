# Populate_Payments.py

import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import random

# Variáveis (Ajustar Conforme o Projeto)
PROJECT_ID    = "telco-metrics-473116"
DATASET_ID    = "telco_metrics_gold"
CUSTOMERS_TB  = "customers"
BILLING_TB    = "billing"
PAYMENTS_TB   = "payments"

# Políticas de pagamento
STATUS_PGTO = ["pago", "atrasado", "inadimplente"]
STATUS_PESO = [0.70, 0.25, 0.05]  # soma 1.0
METODOS  = ["pix", "boleto", "cartao"]
PESOS_MP = [0.60, 0.25, 0.15]  # soma 1.0

# Captura faturas (billing)
bq = bigquery.Client(project=PROJECT_ID)
sql_billing = f"""
    SELECT ID_FATURA, ID_CLIENTE, DT_VENCIMENTO, VALOR_BASE
    FROM `{PROJECT_ID}.{DATASET_ID}.{BILLING_TB}` where dt_ingestao = '2025-10-07' """
billing = bq.query(sql_billing).to_dataframe()

# Gera pagamentos (payments)
pagamentos = []
for _, row in billing.iterrows():
    PGTO     = random.choices(STATUS_PGTO, weights=STATUS_PESO, k=1)[0]
    if PGTO != "inadimplente":
        id_cliente    = row["ID_CLIENTE"]
        id_fatura     = row["ID_FATURA"]
        vencimento    = row["DT_VENCIMENTO"]
        valor_devido  = row["VALOR_BASE"]
        ano_venc      = int(row["DT_VENCIMENTO"].year)
        mes_venc      = int(row["DT_VENCIMENTO"].month)
        valor_pago    = row["VALOR_BASE"]
        desconto      = valor_devido - valor_pago
        metodo        = random.choices(METODOS, weights=PESOS_MP, k=1)[0]
        status_pg     = PGTO
        if status_pg == "atrasado":
            dias_atraso = random.randint(1, 90)
            dt_pagto = vencimento + timedelta(days=dias_atraso)
        elif status_pg == "pago":
            dias_atraso = random.randint(-5, 0)  # pode pagar até 5 dias antes
            dt_pagto = vencimento + timedelta(days=dias_atraso)
        id_pagamento  = f"P{ano_venc}{mes_venc:02d}-{id_cliente}"

        pagamentos.append({
            "ID_PAGAMENTO":     id_pagamento,
            "ID_FATURA":        id_fatura,
            "ID_CLIENTE":       id_cliente,
            "DT_PAGAMENTO":     dt_pagto,
            "VALOR_FATURA":     valor_devido,
            "VALOR_PAGO":       valor_pago,
            "DESCONTO":         desconto,
            "METODO_PAGAMENTO": metodo,
            "STATUS_PAGAMENTO": status_pg,
            "DT_VENCIMENTO":    vencimento,
            "DIAS_ATRASO":      int(dias_atraso),
            "ANO":              int((dt_pagto or vencimento).year),
            "MES":              int((dt_pagto or vencimento).month),
            "DT_INGESTAO":      pd.Timestamp.utcnow(),
            "FONTE":            "Populate_Payments"
        })

# Cria DataFrame e faz upload para o BigQuery
df_out = pd.DataFrame(pagamentos, columns=[
    "ID_PAGAMENTO","ID_FATURA","ID_CLIENTE","DT_PAGAMENTO",
    "VALOR_FATURA","VALOR_PAGO","DESCONTO","METODO_PAGAMENTO",
    "STATUS_PAGAMENTO","DT_VENCIMENTO","DIAS_ATRASO",
    "ANO","MES","DT_INGESTAO","FONTE"])

if df_out.empty:
    print("Nenhum registro para carga.")
else:
    pandas_gbq.to_gbq(
        df_out,
        destination_table=f"{DATASET_ID}.{PAYMENTS_TB}",
        project_id=PROJECT_ID,
        if_exists="append",
        progress_bar=True,
        # api_method="load_csv"
    )
    print(f"✅ {len(df_out)} pagamentos carregados em {PROJECT_ID}.{DATASET_ID}.{PAYMENTS_TB}")
