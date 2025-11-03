import pandas as pd
import pandas_gbq
from datetime import date
from google.cloud import bigquery, storage
import random, datetime
from decimal import Decimal, ROUND_HALF_UP


# Variáveis (Ajustar Conforme o Projeto)
PROJECT_ID = "telco-metrics-473116"         # ex.: telco-metrics-473116
BUCKET_NAME = "telco-metrics-raw"           # ex.: gs://telco-metrics-raw
DATASET_ID = "telco_metrics_gold"           # ex.: telco_metrics_gold
TABLE_NAME = "customers"                    # ex.: customers
BATCH_SIZE = 100                            # ex.: 1000 (geração diária com variação de -30 a +30)
START = "2025-07-01"                        # Início período de entrada dos clientes
END = "2025-10-06"                          # Fim período de entrada dos clientes


today = date.today()
csv_name = f"gross_{today}.csv"
start_date = datetime.datetime.strptime(START, "%Y-%m-%d").date()
end_date = datetime.datetime.strptime(END, "%Y-%m-%d").date()


# Bases de apoio 
planos = [
("P001", "Controle 20GB", Decimal("99.90")),
("P002", "Pós 50GB", Decimal("149.90")),
("P003", "Fibra 300M", Decimal("109.90")),
("P004", "Fibra 500M", Decimal("139.90")),
("P005", "UltraFibra 1G", Decimal("199.90"))]

locais = [("RJ", "21"), ("SP", "11"), ("MG", "31"), ("PR", "41"), ("BA", "71"), ("RS", "51"), ("PE", "81")]
canais = ["loja", "online", "parceiro", "porta_a_porta", "telesales", "outros"]
origens = ["organico", "midia_paga", "indicacao", "portabilidade", "upgrade_fibra"]
ciclo_venc = [(1,8), (8,15), (15,21), (21, 28), (25,11)]

base_clientes = []
DT = start_date

# Captura último ID
bq = bigquery.Client(project=PROJECT_ID)
sql_last_id = f""" 
    SELECT distinct max(cast(substr(ID_CLIENTE,2,100) as int64))
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}`
"""
last_id = next(bq.query(sql_last_id).result(), 0)[0]

last_id_new = 0
if last_id is None:
    last_id_new = int(0)
else:
    last_id_new = int(last_id)

# Cria clientes por dia
while DT < end_date:
    
    DT += datetime.timedelta(days=1)
    SIZE = BATCH_SIZE + random.randint(-30, 30)
    print(f"Gerando {SIZE} registros para {DT}...")

    for i in range(1, SIZE + 1):
        last_id_new  += 1
        num = last_id_new
        id_cliente = f"C{num:06d}"
        nome = f"Cliente_{num:06d}"
        id_plano, nome_plano, valor_plano = random.choice(planos)
        uf, ddd = random.choice(locais)
        canal = random.choice(canais)
        origem = random.choice(origens)
        ciclo_faturamento, vencimento = random.choice(ciclo_venc)
        r = random.random()
        if r < 0.75:
            tipo = "novo"
        elif r < 0.90:
            tipo = "port_in"
        elif r < 0.97:
            tipo = "migracao_interna"
        else:
            tipo = "reconquista"
        if random.random() < 0.35:
            desconto = (valor_plano * Decimal(str(random.random() * 0.3))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        else:
            desconto = Decimal("0.00")
        dt_entrada = DT

        base_clientes.append((
            id_cliente,      
            nome,            
            id_plano,        
            nome_plano,      
            valor_plano,     
            uf,              
            ddd,             
            canal,           
            origem,          
            ciclo_faturamento,
            vencimento,
            tipo,            
            desconto,
            dt_entrada,
            "A" # Status (A=Ativo, I=Inativo)
        ))

    print(f"Gerados {SIZE} registros para {DT} - Total: {len(base_clientes)}")

# Define colunas conforme schema da tabela destino (BigQuery)
columns = ["ID_CLIENTE", "NOME_CLIENTE", "ID_PLANO", "NOME_PLANO", "VALOR_PLANO",
           "UF", "DDD", "CANAL_AQUISICAO", "ORIGEM_AQUISICAO", "CICLO_FATURAMENTO",
           "VENCIMENTO", "TIPO_AQUISICAO", "DESCONTO", "DT_ENTRADA", "STATUS"]

# cria DataFrame e faz upload para o BigQuery
df = pd.DataFrame(base_clientes, columns=columns)
pandas_gbq.to_gbq(
    df,
    destination_table=f"{DATASET_ID}.{TABLE_NAME}",
    project_id=PROJECT_ID,
    if_exists="append",  # ou "replace" / "fail"
    progress_bar=True
)
print(f"✅ {len(df)} registros carregados para {PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}")
