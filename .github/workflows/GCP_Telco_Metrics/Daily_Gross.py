
import csv, io, random
from datetime import date
from google.cloud import bigquery, storage
from decimal import Decimal, ROUND_HALF_UP

# Variáveis (ajustar conforme o projeto)
PROJECT_ID = "telco-metrics-473116"         # ex.: telco-metrics-473116
BUCKET_NAME = "telco-metrics-raw"       # ex.: gs://telco-metrics-raw
DATASET_ID = "telco_metrics_curated"    # ex.: telco_metrics_curated
CLIENTES_TABLE = "customers"
BATCH_SIZE = 100


today = date.today().isoformat()
csv_name = f"gross_{today}.csv"


# Captura último ID
bq = bigquery.Client(project=PROJECT_ID)
sql_last_id = f""" 
    SELECT distinct max(cast(substr(ID_CLIENTE,2,100) as int64))
    FROM `{PROJECT_ID}.{DATASET_ID}.{CLIENTES_TABLE}`
"""
last_id = next(bq.query(sql_last_id).result(), 0)[0]

last_id_new = 0
if last_id is None:
   last_id_new = int(0)
else:
   last_id_new = int(last_id)


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

# Cria base principal
SIZE = BATCH_SIZE + random.randint(-30, 30)
base_clientes = []
for i in range(1, SIZE + 1):
    num = last_id_new + i
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
    dt_entrada = today

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



# Gera um csv e envia para GCS para cada tipo de canal de aquisição

for canal_tipo in canais:
    output = io.StringIO()
    w = csv.writer(output)
    w.writerow([
        "ID_CLIENTE", "NOME_CLIENTE", "ID_PLANO", "NOME_PLANO", "VALOR_PLANO",
        "UF", "DDD", "CANAL_AQUISICAO", "ORIGEM_AQUISICAO", "CICLO_FATURAMENTO",
        "VENCIMENTO", "TIPO_AQUISICAO", "DESCONTO", "DT_ENTRADA", "STATUS"
    ])
    # Filtra registros pelo canal de aquisição
    for registro in base_clientes:
        if registro[7] == canal_tipo:
            w.writerow(registro)

    csv_string = output.getvalue()
    output.close()

    # Upload para GCS
    gcs = storage.Client(project=PROJECT_ID)
    bucket = gcs.bucket(BUCKET_NAME)
    canal_csv_name = f"gross_{canal_tipo}_{today}.csv"
    blob = bucket.blob(f"gross/{canal_csv_name}")
    blob.upload_from_string(csv_string, content_type="text/csv")
    print(f"Arquivo {canal_csv_name} enviado para {BUCKET_NAME}/gross/")





