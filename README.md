# Data Case — Plataforma de Dados de Saúde Suplementar

Pipeline analítico end-to-end sobre uma carteira de planos de saúde empresariais: ingestão de dados brutos, tratamento em camadas, modelagem dimensional e análise de sinistralidade e padrões de utilização.

O objetivo do case é responder perguntas de negócio — quais planos mais vendem, performance de corretores, contratos a expirar, sinistralidade por empresa e onde o time deveria atuar — sustentadas por uma camada Gold modelada para isso.

---

arquitetura.png: Contém o desenho técnica da infra de dados
diagrama: Diagrama de modelagem dos dados



## Arquitetura AWS First

A solução foi construída inteiramente sobre serviços gerenciados da AWS, seguindo a arquitetura **Lakehouse Medallion** (Bronze → Silver → Gold), com o Glue Data Catalog como camada semântica única e o Athena como interface de consumo.

```
                          ┌─────────────────────────────────────────┐
                          │        AWS Glue Data Catalog            │
                          │   (metadados + camada semântica)        │
                          └─────────────────────────────────────────┘
                               ▲            ▲            ▲
                               │            │            │
   ┌──────────┐   ┌────────────┴───┐  ┌─────┴──────┐  ┌──┴──────────┐
   │  Fontes  │──▶│    BRONZE      │─▶│   SILVER   │─▶│    GOLD     │──▶ Athena
   │   CSV    │   │  raw_* (CSV)   │  │ silver_*   │  │ dim_* fct_* │──▶ Jupyter
   └──────────┘   └────────────────┘  │ (Iceberg)  │  │  (Iceberg)  │
                          ▲           └────────────┘  └─────────────┘
                          │                  ▲               ▲
                   ┌──────┴──────┐    ┌──────┴──────┐  ┌─────┴──────┐
                   │Glue Crawler │    │ Glue ETL    │  │ Glue ETL   │
                   │(infere      │    │ silver.py   │  │ gold.py    │
                   │ esquema)    │    │ (PySpark)   │  │ (PySpark)  │
                   └─────────────┘    └─────────────┘  └────────────┘
                          └──────────────────┬──────────────────┘
                                   AWS Glue Workflows
                              (orquestração sob demanda)

              Tudo persistido no S3  ·  Acesso governado por IAM Roles
```

### Ferramentas utilizadas

| Camada | Ferramenta | Papel na solução |
|---|---|---|
| Ingestão | **AWS Glue Crawler** | Inferência automática do esquema das tabelas brutas |
| Catálogo | **AWS Glue Data Catalog** | Metadados e camada semântica |
| Processamento | **AWS Glue ETL** | Jobs de processamento de dados |
| Orquestração | **AWS Glue Workflows** | Encadeamento de crawler e jobs |
| Segurança | **AWS IAM Role** | Controle de acesso |
| Consumo | **AWS Athena** | Consulta SQL serverless sobre a camada Gold |
| Armazenamento | **AWS S3** | Data lake — todas as camadas |
| Formato | **Apache Iceberg / Parquet** | Formato de armazenamento transacional e colunar |
| Engine | **PySpark** | Engine de processamento distribuído |
| Análise | **Pandas / SciPy / scikit-learn** | Análise exploratória, estatística e segmentação |
| Visualização | **Matplotlib / Seaborn** | Data visualization |
| IDE | **Jupyter Notebook** | Ambiente de análise |
| Versionamento | **GitHub** | Code versioning |

### Por que Iceberg

O formato Iceberg foi escolhido no lugar de Parquet puro por três motivos práticos: escrita atômica via `CREATE OR REPLACE TABLE`, o que torna cada job idempotente e elimina estados parciais em caso de falha; evolução de esquema sem reescrita manual de partições; e registro automático no Data Catalog, dispensando crawler nas camadas Silver e Gold.

---

## Estrutura do repositório

```
.
├── README.md                            # este arquivo
├── src/
│   ├── silver.py                        # Glue Job: Bronze -> Silver
│   └── gold.py                          # Glue Job: Silver -> Gold (modelo dimensional)
├── docs/
│   ├── modelo_dimensional_gold.md       # star schema: dimensões, fatos, grãos e justificativas
│   ├── modelo_gold.drawio               # diagrama ER editável (draw.io)
│   ├── metodologia_sinistralidade.md    # metodologia de cálculo detalhada
│   └── glue_workflow_passo_a_passo.md   # guia de montagem da orquestração
└── notebooks/
    └── analise.ipynb                    # EDA, segmentação e recomendações
```

---

## Camadas de dados

### Bronze — dados como chegam

Sete arquivos CSV em `s3://<bucket>/bronze/raw/<tabela>/`, catalogados pelo Glue Crawler com prefixo `raw_`. Nenhuma transformação: a camada preserva a origem para permitir reprocessamento.

Entidades: operadoras, planos, corretores, empresas, contratos, beneficiários e utilização (eventos/sinistros).

### Silver — dados tratados

Processada por `src/silver.py`. Aplica limpeza e padronização preservando o grão original de cada tabela: deduplicação, normalização de texto (maiúsculas, remoção de acentos), parsing de datas em múltiplos formatos, conversão de valores monetários do padrão brasileiro (`R$ 1.234,56` → `decimal(18,2)`), mapeamento de nomes de estado para sigla UF e tipagem de inteiros.

Saída em Iceberg no database `silver_layer_pipo`, uma tabela `silver_<entidade>` por origem.

### Gold — modelo dimensional

Processada por `src/gold.py`. Implementa uma **constelação de fatos** (fact constellation): dois fatos transacionais e um snapshot periódico, compartilhando dimensões conformadas.

**Dimensões**

| Tabela | Grão | Destaque |
|---|---|---|
| `dim_data` | 1 linha por dia | Chave inteligente `YYYYMMDD`; role-playing para 5 papéis de data |
| `dim_empresa` | 1 linha por empresa | Conformada entre os três fatos |
| `dim_plano` | 1 linha por plano | Operadora achatada (sem snowflake) |
| `dim_corretor` | 1 linha por corretor | `meses_de_casa` para normalizar comparação de performance |
| `dim_beneficiario` | 1 linha por beneficiário | `faixa_etaria` em faixas ANS (RN 63) |
| `dim_evento_saude` | 1 linha por tipo × especialidade | Mini-dimensão com `grupo_evento` e `flag_ps_evitavel` |

**Fatos**

| Tabela | Grão | Sustenta |
|---|---|---|
| `fct_contratos` | 1 linha por contrato vendido | Planos mais vendidos, valor contratado, performance de corretor, contratos a expirar |
| `fct_utilizacao` | 1 linha por evento (sinistro) | Concentração de custo, perfil de high cost claimants, padrões de utilização |
| `fct_mensal_contrato` | 1 linha por contrato × mês de competência | Sinistralidade, PMPM, evolução de vidas |

**Por que vendas e utilização são fatos separados.** São processos de negócio distintos, com grãos incompatíveis (uma venda contra milhares de eventos clínicos) e dimensionalidade diferente — venda enxerga corretor e vigência, utilização enxerga beneficiário e especialidade. Unificá-los exigiria ratear prêmio por evento (número artificial) ou produzir linhas com métricas mutuamente nulas. A integração acontece pelas dimensões conformadas.

**Por que existe o snapshot mensal.** Prêmio é estoque recorrente (o contrato "ganha" valor todo mês de vigência) e sinistro é fluxo por evento. Materializar o grão contrato × mês transforma a sinistralidade numa divisão simples, em vez de exigir que cada consulta reconstrua a vigência mês a mês.

O diagrama ER completo está em `docs/modelo_gold.drawio` e o detalhamento de chaves e justificativas de grão em `docs/modelo_dimensional_gold.md`.

---

## Orquestração

Um Glue Workflow sob demanda encadeia os componentes com dependência de sucesso:

```
[Trigger ON DEMAND] → crawler-bronze-raw → [SUCCEEDED] → job-silver → [SUCCEEDED] → job-gold
```

Se o job Silver falhar, o Gold não executa — evitando sobrescrever a camada analítica com dados parciais. Como ambos os jobs fazem full refresh via `CREATE OR REPLACE TABLE`, reexecutar o workflow é seguro e converge sempre para o mesmo estado, o que dispensa job bookmarks.

Crawler apenas no Bronze: as tabelas Iceberg de Silver e Gold se registram sozinhas no Data Catalog durante a escrita.

Passo a passo de montagem (console e AWS CLI) em `docs/glue_workflow_passo_a_passo.md`.

---

## Como executar

**Pré-requisitos:** bucket S3 com os CSVs em `bronze/raw/<tabela>/`, role IAM com acesso a S3, Glue e CloudWatch, e os databases `bronze_layer_pipo`, `silver_layer_pipo` e `gold_layer_pipo` no Data Catalog.

**1. Crawler.** Aponte para `s3://<bucket>/bronze/raw/`, database `bronze_layer_pipo`, **table prefix `raw_`** e frequência *On demand*.

**2. Jobs.** Crie `job-silver` e `job-gold` no Glue Studio (Glue 4.0 ou 5.0) com os parâmetros:

| Key | Value |
|---|---|
| `--datalake-formats` | `iceberg` |
| `--S3_BUCKET_PATH` | `s3://<bucket>` |

O `--datalake-formats iceberg` registra o catálogo `glue_catalog` no Spark — sem ele os jobs falham com `REQUIRES_SINGLE_PART_NAMESPACE`.

**3. Workflow.** Monte a cadeia descrita acima e execute via *Actions → Run*.

**4. Consumo.** Consulte a camada Gold no Athena ou conecte o notebook em `notebooks/`.

---

## Metodologia de sinistralidade

Sinistralidade é a razão entre custo assistencial e **prêmio ganho** (*earned premium*) — apenas a parcela da receita correspondente ao tempo em que o contrato esteve efetivamente vigente na janela analisada.

A distinção importa: usar prêmio anualizado (`valor_mensal × 12`) assume que todo contrato esteve vigente 12 meses. Nesta base a exposição média foi de **6,0 meses**, o que inflava o denominador em **1,91×** e produzia uma sinistralidade sistematicamente otimista. O viés é unidirecional — nenhum contrato pode ficar vigente mais de 12 meses numa janela de 12 meses.

A janela é ancorada na data de referência do case (30/06/2025) e aplicada sobre o **mês de competência**, resultando em exatamente 12 competências (2024-07 a 2025-06). A exposição de cada contrato é a interseção entre seus meses de vigência e essa janela.

Detalhamento completo, com exemplo numérico e SQL de referência, em `docs/metodologia_sinistralidade.md`.

---

## Achados de qualidade de dados

O pipeline expôs duas inconsistências relevantes na origem, ambas preservadas (não descartadas silenciosamente) e documentadas:

**Sinistros posteriores à vigência (26,2% do custo na janela).** R$ 25,8 milhões de sinistros ocorrem após o `vigencia_fim` do contrato — nenhum antes do início, mediana de 131 dias depois. Dos 1.558 eventos nessa situação, 1.170 pertencem a contratos com status `RENOVADO`, e 71,7% do valor tem outro contrato ativo da mesma empresa na data do evento. O diagnóstico é que o `contrato_id` do beneficiário não é reapontado para o contrato sucessor na renovação.

Consequência: a sinistralidade no grão contrato × mês fica em 0,52; atribuindo os sinistros à **empresa** — vínculo confiável — sobe para 0,71. Para ranking e priorização de carteira, o grão empresa × mês é o recomendado.

**Registros órfãos.** 15 eventos de utilização referenciam beneficiários inexistentes e um contrato não possui `valor_mensal`. Os joins preservam essas linhas com chaves nulas em vez de descartá-las, mantendo a rastreabilidade.

---

## Uso de IA no desenvolvimento

| Ferramenta | Aplicação |
|---|---|
| **Claude Code** (Opus / Sonnet) | Modelagem dimensional, desenvolvimento dos jobs PySpark, investigação de divergências de cálculo e documentação técnica |
| **Codex** (ChatGPT) | Apoio na escrita e refatoração de código |

As decisões de arquitetura, modelagem e interpretação analítica foram validadas contra os dados — incluindo a reconstrução independente dos resultados para conferir divergências entre consultas.