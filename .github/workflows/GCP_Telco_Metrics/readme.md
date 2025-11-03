# Workflows de Métricas Telco GCP

Este diretório contém workflows do GitHub Actions e scripts Python para gerar e gerenciar dados de métricas de telecomunicações no Google Cloud Platform.

## Scripts
  - `Daily_Gross.py`: Generates customer data with random attributes
  - `Daily_Billing.py`: Generates billing data
  - `Daily_Payments.py`: Generates payment data
  - `Daily_ATH.py`: Generates ATH (Average Time to Handle) data


## Workflows Diários

  **Funcionalidades**:
  - Executa automaticamente via agendamento cron
  - Pode ser acionado manualmente via `workflow_dispatch`
  - Gera dados de clientes com atributos aleatórios (planos, localizações, canais de aquisição, etc.)
  - Envia arquivos CSV para o bucket GCS: `telco-metrics-raw/gross/`

    ### DAILY GROSS
      Este workflow executa o script `Daily_Gross.py` diariamente para gerar dados de clientes.
      **Agendamento**: Diariamente às 01:00 UTC

    ### DAILY CHURN
      Este workflow executa o script `Daily_Churn.py` diariamente para gerar dados de churn.
      **Agendamento**: Diariamente às 01:10 UTC

    ### DAILY BILLING
      Este workflow executa o script `Daily_Billing.py` diariamente para gerar dados de faturamento.
      **Agendamento**: Diariamente às 01:20 UTC

    ### DAILY PAYMENTS
      Este workflow executa o script `Daily_Payments.py` diariamente para gerar dados de pagamentos.
      **Agendamento**: Diariamente às 01:30 UTC

    ### DAILY ATH
      Este workflow executa o script `Daily_ATH.py` diariamente para gerar dados de tempo médio de atendimento.
      **Agendamento**: Diariamente às 01:40 UTC


## Instruções de Configuração

  1. **Criar uma Conta de Serviço do Google Cloud**:

    gcloud iam service-accounts create github-actions --display-name="Conta de Serviço GitHub Actions"

  2. **Conceder permissões necessárias**:

    # Permissões BigQuery
    gcloud projects add-iam-policy-binding SEU_PROJECT_ID ^
      --member="serviceAccount:github-actions@SEU_PROJECT_ID.iam.gserviceaccount.com" ^
      --role="roles/bigquery.dataViewer"
    
    # Permissões Cloud Storage
    gcloud projects add-iam-policy-binding YOUR_PROJECT_ID ^
      --member="serviceAccount:github-actions@YOUR_PROJECT_ID.iam.gserviceaccount.com" ^
      --role="roles/storage.objectCreator"

  3. **Crie uma nova chave JSON**:
    gcloud iam service-accounts keys create key.json ^
      --iam-account=github-actions@YOUR_PROJECT_ID.iam.gserviceaccount.com
    
  4. **Adicione a chave ao GitHub**:
    - Go to Repository Settings → Secrets and variables → Actions
    - Click "New repository secret"
    - Name: `GCP_SERVICE_ACCOUNT_KEY`
    - Value: Paste the entire contents of the `key.json` file

  **Secrets Necessários**:
  - `GCP_SERVICE_ACCOUNT_KEY`: Chave JSON para uma conta de serviço do Google Cloud com permissões para:
    - Ler do dataset BigQuery `telco_metrics_gold.customers`
    - Escrever no bucket Cloud Storage `telco-metrics-raw`

