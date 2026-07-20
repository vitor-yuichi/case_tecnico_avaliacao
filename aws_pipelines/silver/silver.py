# Glue Job: Bronze -> Silver
# Pipo Saude - Case Tecnico
#
# Arquivo 100% ASCII de proposito (sem acentos literais, sem aspas
# triplas, sem simbolos especiais) para evitar erros de sintaxe
# causados pelo editor web do Glue Studio corrompendo caracteres ao
# colar o codigo. Acentos que sao necessarios (para a normalizacao de
# texto) sao escritos como escapes unicode (\uXXXX), nunca como
# caractere literal.
#
# Bucket usado neste case: s3://pipo-data-case
#   - Dados raw: s3://pipo-data-case/bronze/raw/<tabela>/ (1 subpasta por
#     tabela: beneficiarios/, contratos/, corretores/, empresas/,
#     operadoras/, planos/, utilizacao/)
#   - Leitura via Glue Catalog (database BRONZE_DB abaixo). O Crawler deve
#     ser configurado com "Table prefix" = raw_ para que as tabelas geradas
#     fiquem como raw_beneficiarios, raw_contratos, etc.
#   - Escrita em Iceberg dentro do MESMO bucket, na pasta "silver/"
#     (ex: s3://pipo-data-case/silver/silver_contratos/), registrada no
#     Glue Catalog (database SILVER_DB abaixo)
#
# Convencao de nomes: cada tabela tratada e salva como
# "silver_<nome_da_tabela>" (ex: silver_contratos, silver_empresas...).
#
# Tabelas tratadas: beneficiarios, contratos, corretores, empresas,
# operadoras, planos, utilizacao.
#
# Parametro do Job (ao rodar no Glue):
#   --S3_BUCKET_PATH s3://pipo-data-case

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, upper, trim, when, coalesce, expr, date_format,
    regexp_replace, translate
)

# ---------------------------------------------------------------------------
# Setup do Job
# ---------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET_PATH"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ATENCAO: confira se estes sao os nomes reais dos seus databases no
# Glue Catalog (Databases). Ajuste se necessario.
BRONZE_DB = "bronze_layer_pipi"
SILVER_DB = "silver_layer_pipo"

S3_BUCKET_PATH = args["S3_BUCKET_PATH"].rstrip("/")
S3_SILVER_PATH = "{}/silver".format(S3_BUCKET_PATH)


# ---------------------------------------------------------------------------
# Helpers reutilizados por todas as tabelas
# ---------------------------------------------------------------------------

# Le uma tabela da camada Bronze via Glue Data Catalog.
def read_bronze(table_name):
    return glueContext.create_dynamic_frame.from_catalog(
        database=BRONZE_DB,
        table_name=table_name
    ).toDF()


# Escreve uma tabela tratada na camada Silver, em formato Iceberg,
# dentro da pasta 'silver/' do mesmo bucket dos dados raw.
# Nome final da tabela (e da pasta no S3): 'silver_<nome_tabela>'.
# Ex: write_silver(df, "contratos")
#   -> s3://SEU-BUCKET/silver/silver_contratos
#   -> catalogo: SILVER_DB.silver_contratos
def write_silver(df, nome_tabela):
    nome_final = "silver_{}".format(nome_tabela)
    path = "{}/{}".format(S3_SILVER_PATH, nome_final)
    full_table_name = "glue_catalog.{}.{}".format(SILVER_DB, nome_final)
    (
        df.writeTo(full_table_name)
          .tableProperty("location", path)
          .using("iceberg")
          .createOrReplace()
    )
    print("[OK] {} escrita em {} ({} linhas)".format(
        full_table_name, path, df.count()
    ))


# Normaliza datas que podem chegar em multiplos formatos
# (yyyy-MM-dd, dd-MM-yyyy, dd/MM/yyyy) para o formato dd-MM-yyyy.
# Datas que nao batem com nenhum formato viram NULL (try_to_timestamp).
def parse_date(col_name):
    return date_format(
        coalesce(
            expr("try_to_timestamp({}, 'yyyy-MM-dd')".format(col_name)),
            expr("try_to_timestamp({}, 'dd-MM-yyyy')".format(col_name)),
            expr("try_to_timestamp({}, 'dd/MM/yyyy')".format(col_name))
        ).cast("date"),
        "dd-MM-yyyy"
    )


# Mapeamento de acentos -> letra sem acento, escrito via escapes unicode
# (\uXXXX) para manter o arquivo 100% ASCII.
# Ordem maiusculas: A A A A A / E E E E / I I I I / O O O O O / U U U U / C
# Ordem minusculas: a a a a a / e e e e / i i i i / o o o o o / u u u u / c
ACENTOS_DE = (
    u"\u00C1\u00C0\u00C2\u00C3\u00C4"
    u"\u00C9\u00C8\u00CA\u00CB"
    u"\u00CD\u00CC\u00CE\u00CF"
    u"\u00D3\u00D2\u00D4\u00D5\u00D6"
    u"\u00DA\u00D9\u00DB\u00DC"
    u"\u00C7"
    u"\u00E1\u00E0\u00E2\u00E3\u00E4"
    u"\u00E9\u00E8\u00EA\u00EB"
    u"\u00ED\u00EC\u00EE\u00EF"
    u"\u00F3\u00F2\u00F4\u00F5\u00F6"
    u"\u00FA\u00F9\u00FB\u00FC"
    u"\u00E7"
)
ACENTOS_PARA = "AAAAAEEEEIIIIOOOOOUUUUCaaaaaeeeeiiiiooooouuuuc"


# Padroniza colunas de texto: remove acentos, espacos duplicados,
# espacos nas pontas e converte para maiusculo.
def normalizar_texto(df, colunas):
    for coluna in colunas:
        df = df.withColumn(
            coluna,
            upper(
                trim(
                    translate(col(coluna), ACENTOS_DE, ACENTOS_PARA)
                )
            )
        )
    return df


# Remove qualquer caractere nao numerico e converte para int.
def normalizar_inteiros(df, colunas):
    for coluna in colunas:
        df = df.withColumn(
            coluna,
            regexp_replace(trim(col(coluna)), r"[^0-9]", "").cast("int")
        )
    return df


# Converte valores no formato brasileiro (ex: "R$ 1.234,56") para
# decimal(18,2). Remove prefixo "R$", troca separador de milhar (.)
# e decimal (,) quando aplicavel.
def parse_valor_monetario(df, coluna):
    return (
        df
        .withColumn(
            coluna,
            trim(regexp_replace(col(coluna), r"R\$\s*", ""))
        )
        .withColumn(
            coluna,
            when(
                col(coluna).contains(","),
                regexp_replace(
                    regexp_replace(col(coluna), r"\.", ""),
                    ",",
                    "."
                )
            ).otherwise(col(coluna))
        )
        .withColumn(coluna, col(coluna).cast("decimal(18,2)"))
    )


# ---------------------------------------------------------------------------
# BENEFICIARIOS
# ---------------------------------------------------------------------------
print("Processando beneficiarios...")

beneficiarios = read_bronze("raw_beneficiarios")

beneficiarios = beneficiarios.dropDuplicates()

beneficiarios = beneficiarios.withColumn(
    "sexo",
    when(upper(trim(col("sexo"))) == "FEMININO", "F")
    .when(upper(trim(col("sexo"))) == "MASCULINO", "M")
    .otherwise(upper(trim(col("sexo"))))
)

beneficiarios = (
    beneficiarios
    .withColumn("data_nascimento", parse_date("data_nascimento"))
    .withColumn("data_adesao", parse_date("data_adesao"))
    .withColumn("data_cancelamento", parse_date("data_cancelamento"))
)

beneficiarios = beneficiarios.withColumn(
    "tipo_beneficiario",
    upper(trim(col("tipo_beneficiario")))
)

write_silver(beneficiarios, "beneficiarios")


# ---------------------------------------------------------------------------
# CONTRATOS
# ---------------------------------------------------------------------------
print("Processando contratos...")

contratos = read_bronze("raw_contratos")

contratos = contratos.dropDuplicates()

contratos = (
    contratos
    .withColumn("data_venda", parse_date("data_venda"))
    .withColumn("vigencia_inicio", parse_date("vigencia_inicio"))
    .withColumn("vigencia_fim", parse_date("vigencia_fim"))
)

contratos = parse_valor_monetario(contratos, "valor_mensal")

contratos = contratos.withColumn("status", upper(trim(col("status"))))

contratos = normalizar_inteiros(
    contratos,
    ["contrato_id", "empresa_id", "plano_id", "corretor_id"]
)

write_silver(contratos, "contratos")


# ---------------------------------------------------------------------------
# CORRETORES
# ---------------------------------------------------------------------------
print("Processando corretores...")

corretores = read_bronze("raw_corretores")

corretores = corretores.dropDuplicates()

corretores = corretores.withColumn("data_admissao", parse_date("data_admissao"))

corretores = normalizar_texto(
    corretores,
    ["corretor_nome", "regiao", "senioridade"]
)

corretores = normalizar_inteiros(corretores, ["corretor_id"])

write_silver(corretores, "corretores")


# ---------------------------------------------------------------------------
# EMPRESAS
# ---------------------------------------------------------------------------
print("Processando empresas...")

empresas = read_bronze("raw_empresas")

empresas = empresas.dropDuplicates()

empresas = normalizar_inteiros(empresas, ["empresa_id"])

empresas = empresas.withColumn(
    "data_inicio_relacionamento",
    parse_date("data_inicio_relacionamento")
)

empresas = normalizar_texto(
    empresas,
    ["empresa_nome", "setor", "porte", "uf"]
)

# Converte nome do estado por extenso para sigla (UF)
empresas = empresas.withColumn(
    "uf",
    when(col("uf") == "ACRE", "AC")
    .when(col("uf") == "ALAGOAS", "AL")
    .when(col("uf") == "AMAPA", "AP")
    .when(col("uf") == "AMAZONAS", "AM")
    .when(col("uf") == "BAHIA", "BA")
    .when(col("uf") == "CEARA", "CE")
    .when(col("uf") == "DISTRITO FEDERAL", "DF")
    .when(col("uf") == "ESPIRITO SANTO", "ES")
    .when(col("uf") == "GOIAS", "GO")
    .when(col("uf") == "MARANHAO", "MA")
    .when(col("uf") == "MATO GROSSO", "MT")
    .when(col("uf") == "MATO GROSSO DO SUL", "MS")
    .when(col("uf") == "MINAS GERAIS", "MG")
    .when(col("uf") == "PARA", "PA")
    .when(col("uf") == "PARAIBA", "PB")
    .when(col("uf") == "PARANA", "PR")
    .when(col("uf") == "PERNAMBUCO", "PE")
    .when(col("uf") == "PIAUI", "PI")
    .when(col("uf") == "RIO DE JANEIRO", "RJ")
    .when(col("uf") == "RIO GRANDE DO NORTE", "RN")
    .when(col("uf") == "RIO GRANDE DO SUL", "RS")
    .when(col("uf") == "RONDONIA", "RO")
    .when(col("uf") == "RORAIMA", "RR")
    .when(col("uf") == "SANTA CATARINA", "SC")
    .when(col("uf") == "SAO PAULO", "SP")
    .when(col("uf") == "SERGIPE", "SE")
    .when(col("uf") == "TOCANTINS", "TO")
    .otherwise(col("uf"))
)

write_silver(empresas, "empresas")


# ---------------------------------------------------------------------------
# OPERADORAS
# ---------------------------------------------------------------------------
print("Processando operadoras...")

operadoras = read_bronze("raw_operadoras")

operadoras = operadoras.dropDuplicates()

operadoras = normalizar_texto(operadoras, ["operadora_nome"])
operadoras = normalizar_inteiros(operadoras, ["operadora_id"])

write_silver(operadoras, "operadoras")


# ---------------------------------------------------------------------------
# PLANOS
# ---------------------------------------------------------------------------
print("Processando planos...")

planos = read_bronze("raw_planos")

planos = planos.dropDuplicates()

planos = normalizar_texto(
    planos,
    ["plano_nome", "segmentacao", "acomodacao", "coparticipacao"]
)
planos = normalizar_inteiros(planos, ["plano_id", "operadora_id"])

planos = parse_valor_monetario(planos, "preco_vida_mes")

write_silver(planos, "planos")


# ---------------------------------------------------------------------------
# UTILIZACAO
# ---------------------------------------------------------------------------
print("Processando utilizacao...")

utilizacao = read_bronze("raw_utilizacao")

utilizacao = utilizacao.dropDuplicates()

utilizacao = normalizar_inteiros(utilizacao, ["evento_id", "beneficiario_id"])

utilizacao = utilizacao.withColumn("data_evento", parse_date("data_evento"))

utilizacao = normalizar_texto(utilizacao, ["tipo_evento", "especialidade"])

utilizacao = parse_valor_monetario(utilizacao, "valor_sinistro")

write_silver(utilizacao, "utilizacao")


# ---------------------------------------------------------------------------
job.commit()
print("Bronze -> Silver concluido: 7 tabelas tratadas e gravadas em Iceberg.")
