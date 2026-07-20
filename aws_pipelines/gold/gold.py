import sys
from pyspark.context import SparkContext
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, lit, when, upper, trim, coalesce, floor,
    to_date, date_format, year, month, quarter, trunc, last_day,
    months_between, explode, expr, row_number,
    sum as f_sum, count as f_count,
)
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# --------------------------------------------------------------------------
# Parametros: JOB_NAME e obrigatorio; o resto e opcional com default.
# --------------------------------------------------------------------------
def get_optional(argv, name, default):
    key = "--" + name
    if key in argv:
        return getResolvedOptions(argv, [name])[name]
    return default


job_args = getResolvedOptions(sys.argv, ["JOB_NAME"])

S3_BUCKET_PATH = get_optional(sys.argv, "S3_BUCKET_PATH", "s3://pipo-data-case").rstrip("/")
SILVER_DB = get_optional(sys.argv, "SILVER_DB", "silver_layer_pipo")
GOLD_DB = get_optional(sys.argv, "GOLD_DB", "gold_layer_pipo")
REFERENCE_DATE = get_optional(sys.argv, "REFERENCE_DATE", "2025-06-30")

GOLD_PATH = S3_BUCKET_PATH + "/gold"
CATALOG = "glue_catalog"

# Janela de datas coberta pela dim_data (folgada para cobrir vigencias).
DIM_DATA_START = "2021-01-01"
DIM_DATA_END = "2027-12-31"


# --------------------------------------------------------------------------
# Spark / Glue init
# --------------------------------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(job_args["JOB_NAME"], job_args)

# LEGACY tolera formatos de data variados no to_date (dd-MM-yyyy etc).
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

REF = to_date(lit(REFERENCE_DATE))


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def read_silver(nome):
    # Le tabela silver_<nome> da camada Silver (Iceberg no Glue Catalog).
    return spark.table(CATALOG + "." + SILVER_DB + ".silver_" + nome)


def parse_date(c):
    # Silver grava datas como texto dd-MM-yyyy. Aceita tambem yyyy-MM-dd
    # por seguranca. O que nao casar vira NULL (sem try_to_timestamp).
    s = col(c).cast("string")
    return coalesce(to_date(s, "dd-MM-yyyy"), to_date(s, "yyyy-MM-dd"))


def sk_date(dcol):
    # Chave inteligente da dim_data: YYYYMMDD como int. NULL -> NULL.
    return date_format(dcol, "yyyyMMdd").cast("int")


def write_gold(df, table_name):
    # Grava tabela Gold como Iceberg via glue_catalog (3 partes) com
    # CREATE OR REPLACE TABLE e LOCATION explicito em s3://.../gold/.
    view = "src_" + table_name
    df.createOrReplaceTempView(view)
    full = CATALOG + "." + GOLD_DB + "." + table_name
    loc = GOLD_PATH + "/" + table_name
    spark.sql(
        "CREATE OR REPLACE TABLE " + full
        + " USING iceberg LOCATION '" + loc + "'"
        + " AS SELECT * FROM " + view
    )
    n = spark.table(full).count()
    print("[OK] " + full + " -> " + loc + " (" + str(n) + " linhas)")


def faixa_etaria(idade_col):
    # Faixas etarias ANS (RN 63), rotulos ASCII.
    return (
        when(idade_col <= 18, "0-18")
        .when(idade_col <= 23, "19-23")
        .when(idade_col <= 28, "24-28")
        .when(idade_col <= 33, "29-33")
        .when(idade_col <= 38, "34-38")
        .when(idade_col <= 43, "39-43")
        .when(idade_col <= 48, "44-48")
        .when(idade_col <= 53, "49-53")
        .when(idade_col <= 58, "54-58")
        .otherwise("59+")
    )


def grupo_evento(tipo_col):
    # Agrupa tipo_evento em macro-categorias (valores Silver: MAIUSCULO,
    # sem acento). Ajuste as regras conforme a taxonomia clinica real.
    return (
        when(tipo_col == "PRONTO-SOCORRO", "URGENCIA")
        .when(tipo_col.isin("EXAME", "EXAME DE IMAGEM"), "DIAGNOSTICO")
        .when(tipo_col.isin("INTERNACAO", "CIRURGIA"), "HOSPITALAR")
        .otherwise("ELETIVO")
    )


# Especialidades tipicamente manejaveis em ambulatorio: um PS nelas e
# candidato a uso evitavel. Heuristica - validar com time clinico.
ESPEC_AMBULATORIAL = [
    "CLINICA GERAL", "PEDIATRIA", "DERMATOLOGIA", "OFTALMOLOGIA",
    "GINECOLOGIA", "OTORRINOLARINGOLOGIA", "NUTRICAO", "ENDOCRINOLOGIA",
    "PSIQUIATRIA",
]


def flag_ps_evitavel(tipo_col, esp_col):
    return when(
        (tipo_col == "PRONTO-SOCORRO") & (esp_col.isin(*ESPEC_AMBULATORIAL)),
        lit(True),
    ).otherwise(lit(False))


# Garante o database Gold no Catalog.
spark.sql("CREATE DATABASE IF NOT EXISTS " + CATALOG + "." + GOLD_DB)


# ==========================================================================
# DIMENSOES
# ==========================================================================

# --- dim_data -------------------------------------------------------------
# Grao: 1 linha por dia. Chave inteligente YYYYMMDD. Role-playing:
# atende data_venda, vigencias, data_evento e mes de competencia.
print("Gerando dim_data...")
dim_data = (
    spark.sql(
        "SELECT explode(sequence(to_date('" + DIM_DATA_START + "'), "
        "to_date('" + DIM_DATA_END + "'), interval 1 day)) AS data"
    )
    .select(
        date_format(col("data"), "yyyyMMdd").cast("int").alias("sk_data"),
        col("data"),
        year(col("data")).alias("ano"),
        month(col("data")).alias("mes"),
        quarter(col("data")).alias("trimestre"),
        date_format(col("data"), "yyyy-MM").alias("ano_mes"),
    )
)
write_gold(dim_data, "dim_data")


# --- dim_empresa ----------------------------------------------------------
# Grao: 1 linha por empresa. Conformada entre os 3 fatos.
print("Gerando dim_empresa...")
dim_empresa = (
    read_silver("empresas")
    .withColumn("data_inicio_relacionamento", parse_date("data_inicio_relacionamento"))
    .withColumn("sk_empresa", row_number().over(Window.orderBy("empresa_id")))
    .select(
        "sk_empresa", "empresa_id", "empresa_nome",
        "setor", "porte", "uf", "data_inicio_relacionamento",
    )
)
write_gold(dim_empresa, "dim_empresa")


# --- dim_plano ------------------------------------------------------------
# Grao: 1 linha por plano. Operadora achatada (sem snowflake).
print("Gerando dim_plano...")
planos = read_silver("planos")
operadoras = read_silver("operadoras")
dim_plano = (
    planos.join(operadoras, "operadora_id", "left")
    .withColumn(
        "flag_coparticipacao",
        when(upper(trim(col("coparticipacao"))) == "SIM", lit(True)).otherwise(lit(False)),
    )
    .withColumn("sk_plano", row_number().over(Window.orderBy("plano_id")))
    .select(
        "sk_plano", "plano_id", "plano_nome",
        "operadora_id", "operadora_nome",
        "segmentacao", "acomodacao", "flag_coparticipacao",
        col("preco_vida_mes").cast("decimal(18,2)").alias("preco_vida_mes"),
    )
)
write_gold(dim_plano, "dim_plano")


# --- dim_corretor ---------------------------------------------------------
# Grao: 1 linha por corretor. meses_de_casa apoia a normalizacao "justa"
# de performance (proxima o tempo de exposicao entre corretores).
print("Gerando dim_corretor...")
dim_corretor = (
    read_silver("corretores")
    .withColumn("data_admissao", parse_date("data_admissao"))
    .withColumn(
        "meses_de_casa",
        floor(months_between(REF, col("data_admissao"))).cast("int"),
    )
    .withColumn("sk_corretor", row_number().over(Window.orderBy("corretor_id")))
    .select(
        "sk_corretor", "corretor_id", "corretor_nome",
        "regiao", "senioridade", "data_admissao", "meses_de_casa",
    )
)
write_gold(dim_corretor, "dim_corretor")


# --- dim_beneficiario -----------------------------------------------------
# Grao: 1 linha por beneficiario. idade/faixa_etaria calculadas na data de
# referencia do case. flag_ativo = sem cancelamento ate a referencia.
print("Gerando dim_beneficiario...")
dim_beneficiario = (
    read_silver("beneficiarios")
    .withColumn("data_nascimento", parse_date("data_nascimento"))
    .withColumn("data_adesao", parse_date("data_adesao"))
    .withColumn("data_cancelamento", parse_date("data_cancelamento"))
    .withColumn(
        "idade",
        floor(months_between(REF, col("data_nascimento")) / lit(12)).cast("int"),
    )
    .withColumn("faixa_etaria", faixa_etaria(col("idade")))
    .withColumn(
        "flag_ativo",
        when(col("data_cancelamento").isNull() | (col("data_cancelamento") > REF), lit(True))
        .otherwise(lit(False)),
    )
    .withColumn("sk_beneficiario", row_number().over(Window.orderBy("beneficiario_id")))
    .select(
        "sk_beneficiario", "beneficiario_id", "sexo",
        "data_nascimento", "idade", "faixa_etaria", "tipo_beneficiario",
        "data_adesao", "data_cancelamento", "flag_ativo",
    )
)
write_gold(dim_beneficiario, "dim_beneficiario")


# --- dim_evento_saude -----------------------------------------------------
# Grao: 1 linha por combinacao tipo_evento x especialidade (mini-dim /
# junk). Carrega classificacoes derivadas de regra de negocio.
print("Gerando dim_evento_saude...")
dim_evento_saude = (
    read_silver("utilizacao")
    .select("tipo_evento", "especialidade")
    .distinct()
    .withColumn("grupo_evento", grupo_evento(col("tipo_evento")))
    .withColumn("flag_ps_evitavel", flag_ps_evitavel(col("tipo_evento"), col("especialidade")))
    .withColumn(
        "sk_evento_saude",
        row_number().over(Window.orderBy("tipo_evento", "especialidade")),
    )
    .select(
        "sk_evento_saude", "tipo_evento", "especialidade",
        "grupo_evento", "flag_ps_evitavel",
    )
)
write_gold(dim_evento_saude, "dim_evento_saude")


# Projecoes leves das dims (nk -> sk) para resolver os fatos.
map_empresa = dim_empresa.select("empresa_id", "sk_empresa")
map_plano = dim_plano.select("plano_id", "sk_plano")
map_corretor = dim_corretor.select("corretor_id", "sk_corretor")
map_benef = dim_beneficiario.select("beneficiario_id", "sk_beneficiario")
map_evento = dim_evento_saude.select("tipo_evento", "especialidade", "sk_evento_saude")


# ==========================================================================
# FATOS
# ==========================================================================

# Contratos com datas ja parseadas: reutilizado por 2 fatos.
contratos = (
    read_silver("contratos")
    .withColumn("data_venda", parse_date("data_venda"))
    .withColumn("vigencia_inicio", parse_date("vigencia_inicio"))
    .withColumn("vigencia_fim", parse_date("vigencia_fim"))
    .withColumn("valor_mensal", col("valor_mensal").cast("decimal(18,2)"))
)

# --- fct_contratos --------------------------------------------------------
# Grao: 1 linha por contrato vendido. Tres datas viram role-playing FKs.
print("Gerando fct_contratos...")
fct_contratos = (
    contratos
    .join(map_empresa, "empresa_id", "left")
    .join(map_plano, "plano_id", "left")
    .join(map_corretor, "corretor_id", "left")
    .withColumn("sk_data_venda", sk_date(col("data_venda")))
    .withColumn("sk_vigencia_inicio", sk_date(col("vigencia_inicio")))
    .withColumn("sk_vigencia_fim", sk_date(col("vigencia_fim")))
    .withColumn("valor_anualizado", (col("valor_mensal") * lit(12)).cast("decimal(18,2)"))
    .withColumn(
        "ticket_por_vida",
        when(col("num_vidas") > 0, (col("valor_mensal") / col("num_vidas")).cast("decimal(18,2)"))
        .otherwise(lit(None).cast("decimal(18,2)")),
    )
    .select(
        "sk_data_venda", "sk_vigencia_inicio", "sk_vigencia_fim",
        "sk_empresa", "sk_plano", "sk_corretor",
        "contrato_id", "status",
        col("num_vidas").cast("int").alias("num_vidas"),
        "valor_mensal", "valor_anualizado", "ticket_por_vida",
    )
)
write_gold(fct_contratos, "fct_contratos")


# --- fct_utilizacao -------------------------------------------------------
# Grao: 1 linha por evento de utilizacao (sinistro). empresa/plano sao
# desnormalizados no fato: resolvidos via beneficiario -> contrato.
print("Gerando fct_utilizacao...")
benef_link = read_silver("beneficiarios").select("beneficiario_id", "contrato_id", "empresa_id")
contrato_plano = read_silver("contratos").select("contrato_id", "plano_id")

fct_utilizacao = (
    read_silver("utilizacao")
    .withColumn("data_evento", parse_date("data_evento"))
    .join(benef_link, "beneficiario_id", "left")
    .join(contrato_plano, "contrato_id", "left")
    .join(map_benef, "beneficiario_id", "left")
    .join(map_empresa, "empresa_id", "left")
    .join(map_plano, "plano_id", "left")
    .join(map_evento, ["tipo_evento", "especialidade"], "left")
    .withColumn("sk_data_evento", sk_date(col("data_evento")))
    .select(
        "sk_data_evento", "sk_beneficiario", "sk_empresa", "sk_plano", "sk_evento_saude",
        "evento_id", "contrato_id",
        col("valor_sinistro").cast("decimal(18,2)").alias("valor_sinistro"),
    )
)
write_gold(fct_utilizacao, "fct_utilizacao")


# --- fct_mensal_contrato --------------------------------------------------
# Grao: 1 linha por contrato x mes de competencia (dentro da vigencia).
# Ponte entre premio (estoque recorrente) e sinistro (fluxo por evento).
print("Gerando fct_mensal_contrato...")

# 1) Explode a vigencia de cada contrato em meses de competencia.
meses = (
    contratos
    .select(
        "contrato_id", "empresa_id", "plano_id", "corretor_id",
        "valor_mensal", "vigencia_inicio", "vigencia_fim",
    )
    .withColumn(
        "lista_meses",
        expr("sequence(trunc(vigencia_inicio, 'MM'), trunc(vigencia_fim, 'MM'), interval 1 month)"),
    )
    .withColumn("mes_ini", explode(col("lista_meses")))
    .withColumn("mes_fim", last_day(col("mes_ini")))
    .withColumn("ano_mes", date_format(col("mes_ini"), "yyyy-MM"))
    .drop("lista_meses")
)

# 2) Vidas ativas por contrato x mes (adesao <= fim do mes e sem
#    cancelamento antes do inicio do mes).
benef_vidas = (
    read_silver("beneficiarios")
    .select(
        "beneficiario_id", "contrato_id",
        parse_date("data_adesao").alias("data_adesao"),
        parse_date("data_cancelamento").alias("data_cancelamento"),
    )
)
vidas_mes = (
    meses.select("contrato_id", "ano_mes", "mes_ini", "mes_fim")
    .join(benef_vidas, "contrato_id", "left")
    .where(
        (col("data_adesao") <= col("mes_fim"))
        & (col("data_cancelamento").isNull() | (col("data_cancelamento") >= col("mes_ini")))
    )
    .groupBy("contrato_id", "ano_mes")
    .agg(f_count("beneficiario_id").alias("vidas_ativas"))
)

# 3) Sinistros por contrato x mes (utilizacao -> beneficiario -> contrato).
sinistro_mes = (
    read_silver("utilizacao")
    .withColumn("data_evento", parse_date("data_evento"))
    .join(read_silver("beneficiarios").select("beneficiario_id", "contrato_id"), "beneficiario_id", "left")
    .withColumn("ano_mes", date_format(col("data_evento"), "yyyy-MM"))
    .groupBy("contrato_id", "ano_mes")
    .agg(
        f_sum(col("valor_sinistro").cast("decimal(18,2)")).alias("valor_sinistros_mes"),
        f_count("evento_id").alias("qtd_eventos_mes"),
    )
)

# 4) Monta o fato no grao contrato x mes.
fct_mensal_contrato = (
    meses
    .join(vidas_mes, ["contrato_id", "ano_mes"], "left")
    .join(sinistro_mes, ["contrato_id", "ano_mes"], "left")
    .join(map_empresa, "empresa_id", "left")
    .join(map_plano, "plano_id", "left")
    .join(map_corretor, "corretor_id", "left")
    .withColumn("sk_mes_competencia", sk_date(col("mes_ini")))
    .withColumn("premio_competencia", col("valor_mensal").cast("decimal(18,2)"))
    .withColumn("vidas_ativas", coalesce(col("vidas_ativas"), lit(0)).cast("int"))
    .withColumn(
        "valor_sinistros_mes",
        coalesce(col("valor_sinistros_mes"), lit(0)).cast("decimal(18,2)"),
    )
    .withColumn("qtd_eventos_mes", coalesce(col("qtd_eventos_mes"), lit(0)).cast("int"))
    .select(
        "sk_mes_competencia", "sk_empresa", "sk_plano", "sk_corretor",
        "contrato_id", "ano_mes",
        "premio_competencia", "vidas_ativas",
        "valor_sinistros_mes", "qtd_eventos_mes",
    )
)
write_gold(fct_mensal_contrato, "fct_mensal_contrato")


# --------------------------------------------------------------------------
job.commit()
print("[FIM] Camada Gold criada: 6 dimensoes + 3 fatos em Iceberg.")
