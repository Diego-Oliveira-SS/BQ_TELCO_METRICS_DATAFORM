# CUSTOMERS SCHEMA
CREATE TABLE IF NOT EXISTS `telco-metrics-473116.telco_metrics_gold.customers` (
ID_CLIENTE         STRING  NOT NULL,
NOME_CLIENTE       STRING,
ID_PLANO           STRING,
NOME_PLANO         STRING,
VALOR_PLANO        NUMERIC,
UF                 STRING,
DDD                STRING,
CANAL_AQUISICAO    STRING,
ORIGEM_AQUISICAO   STRING,
CICLO_FATURAMENTO  INT64,
VENCIMENTO         INT64,
TIPO_AQUISICAO     STRING,
DESCONTO           NUMERIC,
DT_ENTRADA         DATE    NOT NULL
)
PARTITION BY DT_ENTRADA
CLUSTER BY ID_CLIENTE
-- OPTIONS (require_partition_filter = TRUE);



# BILLING SCHEMA
CREATE TABLE IF NOT EXISTS `telco-metrics-473116.telco_metrics_gold.billing` (
    ID_FATURA              STRING       NOT NULL,
    ID_CLIENTE             STRING       NOT NULL,
    DT_COMPETENCIA         DATE         NOT NULL,   -- competência (mês de referência)
    DT_EMISSAO             DATE,                   -- data de emissão
    DT_VENCIMENTO          DATE,                   -- dia de vencimento da fatura
    VALOR_PLANO            NUMERIC,
    DESCONTO               NUMERIC,
    VALOR_BASE             NUMERIC,
    ANO                    INT64,
    MES                    INT64,
    DT_INGESTAO            TIMESTAMP    DEFAULT CURRENT_TIMESTAMP(),
    FONTE                  STRING
)
PARTITION BY DT_VENCIMENTO
CLUSTER BY ANO, MES;


# PAYMENTS SCHEMA
CREATE TABLE IF NOT EXISTS `telco-metrics-473116.telco_metrics_gold.payments` (
    ID_PAGAMENTO        STRING    NOT NULL,  -- identificador único do pagamento
    ID_FATURA           STRING    NOT NULL,  -- referência à fatura
    ID_CLIENTE          STRING    NOT NULL,  -- referência ao cliente
    DT_PAGAMENTO        DATE,                -- data efetiva de pagamento
    VALOR_FATURA        NUMERIC,             -- valor total da fatura
    VALOR_PAGO          NUMERIC,             -- valor efetivamente pago
    DESCONTO            NUMERIC,             -- desconto aplicado no pagamento
    METODO_PAGAMENTO    STRING,              -- boleto, cartão, pix, débito, etc.
    STATUS_PAGAMENTO    STRING,              -- pago, atrasado, inadimplente, isento
    DT_VENCIMENTO       DATE,                -- data original de vencimento
    DIAS_ATRASO         INT64,               -- diferença entre vencimento e pagamento
    ANO                 INT64,               -- ano do pagamento
    MES                 INT64,               -- mês do pagamento
    DT_INGESTAO         TIMESTAMP,           -- data/hora de ingestão no sistema
    FONTE               STRING               -- origem do processo (ex: Populate_Payments)
)
PARTITION BY DT_PAGAMENTO
CLUSTER BY ID_CLIENTE;


# ATH SCHEMA (INTERACTIONS)
CREATE TABLE IF NOT EXISTS `telco-metrics-473116.telco_metrics_gold.ath` (
  ID_INTERACAO   STRING    NOT NULL,
  ID_CLIENTE     STRING    NOT NULL,
  DT_INTERACAO   TIMESTAMP NOT NULL,
  MOTIVO1        STRING,
  MOTIVO2        STRING,
  MOTIVO3        STRING,
  TIPO           STRING,            -- informacao | solicitacao | reclamacao
  RETENCAO       BOOL,              -- 10% true
  AHT_SEG        INT64,             -- duração da interação (segundos)
  ANO            INT64,
  MES            INT64,
  DT_INGESTAO    TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  FONTE          STRING
)
PARTITION BY DATE(DT_INTERACAO)
CLUSTER BY ID_CLIENTE, TIPO;


# CHURN SCHEMA
CREATE TABLE IF NOT EXISTS `telco-metrics-473116.telco_metrics_gold.churn` (
  ID_CLIENTE          STRING    NOT NULL,
  DT_CHURN            DATE      NOT NULL,
  MOTIVO_CHURN        STRING,
  TIPO_CANCELAMENTO   STRING,
  FEEDBACK_CLIENTE    STRING,
  DT_INGESTAO         TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  FONTE               STRING
 )
PARTITION BY DT_CHURN
CLUSTER BY ID_CLIENTE;