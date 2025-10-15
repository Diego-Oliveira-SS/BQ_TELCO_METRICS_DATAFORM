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
TODAY_DAY = TODAY.day
ciclo_venc = [(1,8), (8,15), (15,21), (21, 28), (25,11)]

month_cycle = []
vencimentos = []
for ciclo in ciclo_venc:
    month_cycle.append(ciclo[0])
    vencimentos.append(ciclo[1])

print(month_cycle, vencimentos)