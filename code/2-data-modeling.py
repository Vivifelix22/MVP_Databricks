# MODELAGEM DE DADOS - ESQUEMA ESTRELA
# =========================================

# Importar bibliotecas necessárias
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime

# Carregar os dados brutos
credit_raw = spark.table("credit_risk_raw")
print(f"Dados brutos carregados: {credit_raw.count()} registros")
display(credit_raw.limit(5))

# CRIANDO DIMENSÕES PARA O ESQUEMA ESTRELA
# =========================================

# Dimensão Cliente
print("\n=== CRIANDO DIMENSÃO CLIENTE ===")
dim_cliente = credit_raw.select(
  monotonically_increasing_id().alias("cliente_id"),
  "person_age",
  "person_income",
  "person_home_ownership",
  "person_emp_length",
  col("cb_person_default_on_file").alias("historico_inadimplencia"),
  col("cb_person_cred_hist_length").alias("tempo_historico_credito")
).distinct()

# Exibir informações da dimensão
print(f"Número de registros na dimensão cliente: {dim_cliente.count()}")
display(dim_cliente.limit(5))

# Dimensão Empréstimo
print("\n=== CRIANDO DIMENSÃO EMPRÉSTIMO ===")
dim_emprestimo = credit_raw.select(
  monotonically_increasing_id().alias("emprestimo_id"),
  "loan_intent",
  "loan_grade",
  "loan_amnt",
  "loan_int_rate",
  "loan_percent_income",
  "loan_status"
).distinct()

# Exibir informações da dimensão
print(f"Número de registros na dimensão empréstimo: {dim_emprestimo.count()}")
display(dim_emprestimo.limit(5))

# Dimensão Tempo (data de referência - usando a data atual para fins do exercício)
print("\n=== CRIANDO DIMENSÃO TEMPO ===")
# Criar uma dimensão de tempo simples
current_date = datetime.datetime.now()
data = [(1, current_date.year, current_date.month, current_date.day, 
         current_date.strftime("%Y-%m-%d"), current_date.strftime("%B"), 
         f"{current_date.year}-Q{(current_date.month-1)//3+1}")]
schema = StructType([
  StructField("tempo_id", IntegerType(), False),
  StructField("ano", IntegerType(), False),
  StructField("mes", IntegerType(), False),
  StructField("dia", IntegerType(), False),
  StructField("data_completa", StringType(), False),
  StructField("nome_mes", StringType(), False),
  StructField("trimestre", StringType(), False)
])
dim_tempo = spark.createDataFrame(data, schema)

# Exibir informações da dimensão
display(dim_tempo)

# CRIAÇÃO DE TABELA DE FATOS
# =========================================

# Para associar as dimensões à tabela de fatos, precisamos primeiro criar dataframes temporários com as chaves
print("\n=== PREPARANDO ASSOCIAÇÃO DE CHAVES PARA TABELA DE FATOS ===")

# Mapear chaves para Cliente
temp_cliente = credit_raw.select(
  "person_age",
  "person_income",
  "person_home_ownership",
  "person_emp_length",
  col("cb_person_default_on_file").alias("historico_inadimplencia"),
  col("cb_person_cred_hist_length").alias("tempo_historico_credito")
).distinct().join(
  dim_cliente,
  ["person_age", "person_income", "person_home_ownership", "person_emp_length", 
   "historico_inadimplencia", "tempo_historico_credito"]
)

# Mapear chaves para Empréstimo
temp_emprestimo = credit_raw.select(
  "loan_intent",
  "loan_grade",
  "loan_amnt",
  "loan_int_rate",
  "loan_percent_income",
  "loan_status"
).distinct().join(
  dim_emprestimo,
  ["loan_intent", "loan_grade", "loan_amnt", "loan_int_rate", "loan_percent_income", "loan_status"]
)

# CRIAÇÃO DE TABELA DE FATOS
# =========================================

# Para associar as dimensões à tabela de fatos, vamos usar uma abordagem diferente
print("\n=== PREPARANDO ASSOCIAÇÃO DE CHAVES PARA TABELA DE FATOS ===")

# Primeiro, adicionar colunas com os mesmos nomes para fazer a junção
credit_raw_temp = credit_raw.withColumn(
  "historico_inadimplencia", col("cb_person_default_on_file")
).withColumn(
  "tempo_historico_credito", col("cb_person_cred_hist_length")
)

# Criar tabela de fatos - primeiro juntar com dimensão cliente
fato_temp1 = credit_raw_temp.join(
  dim_cliente,
  ["person_age", "person_income", "person_home_ownership", "person_emp_length", 
   "historico_inadimplencia", "tempo_historico_credito"]
)

# Depois juntar com dimensão empréstimo
fato_analise_credito = fato_temp1.join(
  dim_emprestimo,
  ["loan_intent", "loan_grade", "loan_amnt", "loan_int_rate", "loan_percent_income", "loan_status"]
).select(
  "cliente_id",
  "emprestimo_id",
  lit(1).alias("tempo_id"),  # Todos os registros com a mesma data (simplificação)
  col("loan_status").alias("status_emprestimo"),
  col("loan_amnt").alias("valor_emprestimo"),
  col("loan_int_rate").alias("taxa_juros"),
  col("loan_percent_income").alias("percentual_renda"),
  when(col("loan_status") == 1, 1).otherwise(0).alias("flag_inadimplencia")
)

# Exibir informações da tabela de fatos
print(f"Número de registros na tabela de fatos: {fato_analise_credito.count()}")
display(fato_analise_credito.limit(5))

# SALVAR NO DATA WAREHOUSE
# =========================================
print("\n=== SALVANDO MODELO DIMENSIONAL COMO TABELAS DELTA ===")
try:
  dim_cliente.write.format("delta").mode("overwrite").saveAsTable("dim_cliente")
  dim_emprestimo.write.format("delta").mode("overwrite").saveAsTable("dim_emprestimo")
  dim_tempo.write.format("delta").mode("overwrite").saveAsTable("dim_tempo")
  fato_analise_credito.write.format("delta").mode("overwrite").saveAsTable("fato_analise_credito")
  
  print("Tabelas do modelo dimensional criadas com sucesso!")
  
  # Verificar tabelas criadas
  display(spark.sql("SHOW TABLES"))
  
except Exception as e:
  print(f"Erro ao criar tabelas do modelo dimensional: {e}")
