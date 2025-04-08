# ANÁLISE DE QUALIDADE DE DADOS
# =========================================

# Importar bibliotecas necessárias
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configurações para visualizações
plt.style.use('ggplot')
sns.set(style="whitegrid")

# Carregar tabelas do modelo dimensional
print("\n=== CARREGANDO TABELAS DO MODELO DIMENSIONAL ===")
dim_cliente = spark.table("dim_cliente")
dim_emprestimo = spark.table("dim_emprestimo")
dim_tempo = spark.table("dim_tempo")
fato_analise_credito = spark.table("fato_analise_credito")

print(f"Dimensão Cliente: {dim_cliente.count()} registros")
print(f"Dimensão Empréstimo: {dim_emprestimo.count()} registros")
print(f"Dimensão Tempo: {dim_tempo.count()} registros")
print(f"Fato Análise de Crédito: {fato_analise_credito.count()} registros")

# Carregar dados originais para comparação
credit_raw = spark.table("credit_risk_raw")
print(f"Dados brutos: {credit_raw.count()} registros")

# ANÁLISE DE VALORES AUSENTES
# =========================================
print("\n=== ANÁLISE DE VALORES AUSENTES ===")

# Função para analisar valores nulos em um DataFrame
def analyze_nulls(df, table_name):
  print(f"\nAnalisando valores nulos na tabela {table_name}:")
  
  # Contar valores nulos por coluna
  null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
  
  # Calcular percentual de nulos
  total_rows = df.count()
  null_percents = df.select([
    (count(when(col(c).isNull(), c)) * 100.0 / total_rows).alias(c)
    for c in df.columns
  ])
  
  # Exibir resultados
  print("Contagem de valores nulos:")
  display(null_counts)
  
  print("Percentual de valores nulos:")
  display(null_percents)

# Analisar valores nulos em cada tabela
analyze_nulls(dim_cliente, "dim_cliente")
analyze_nulls(dim_emprestimo, "dim_emprestimo")
analyze_nulls(fato_analise_credito, "fato_analise_credito")

# ANÁLISE DE DISTRIBUIÇÃO DOS DADOS
# =========================================
print("\n=== ANÁLISE DE DISTRIBUIÇÃO DOS DADOS ===")

# Estatísticas descritivas da dimensão cliente
print("\nEstatísticas descritivas - Dimensão Cliente:")
display(dim_cliente.describe())

# Estatísticas descritivas da dimensão empréstimo
print("\nEstatísticas descritivas - Dimensão Empréstimo:")
display(dim_emprestimo.describe())

# Estatísticas descritivas da tabela de fatos
print("\nEstatísticas descritivas - Tabela de Fatos:")
display(fato_analise_credito.describe())

# ANÁLISE DE OUTLIERS
# =========================================
print("\n=== ANÁLISE DE OUTLIERS ===")

# Função para detectar outliers usando o método Z-score
def detect_outliers_zscore(df, column, threshold=3.0):
  # Calcular média e desvio padrão
  stats = df.select(
    mean(column).alias("mean"),
    stddev(column).alias("stddev")
  ).collect()[0]
  
  mean_val = stats["mean"]
  stddev_val = stats["stddev"]
  
  # Detectar outliers (Z-score > threshold)
  outliers = df.select(
    col(column),
    ((col(column) - mean_val) / stddev_val).alias("z_score")
  ).filter(abs(col("z_score")) > threshold)
  
  # Calcular percentual de outliers
  outlier_count = outliers.count()
  total_count = df.count()
  outlier_percent = (outlier_count * 100.0) / total_count
  
  print(f"Outliers em {column}: {outlier_count} registros ({outlier_percent:.2f}%)")
  print(f"  Média: {mean_val:.2f}, Desvio Padrão: {stddev_val:.2f}")
  print(f"  Limite inferior: {mean_val - threshold * stddev_val:.2f}")
  print(f"  Limite superior: {mean_val + threshold * stddev_val:.2f}")
  
  # Mostrar alguns exemplos de outliers
  if outlier_count > 0:
    print("  Exemplos de outliers:")
    display(outliers.orderBy(abs(col("z_score")).desc()).limit(5))

# Detectar outliers em colunas numéricas importantes
print("\nOutliers na Dimensão Cliente:")
detect_outliers_zscore(dim_cliente, "person_age")
detect_outliers_zscore(dim_cliente, "person_income")
detect_outliers_zscore(dim_cliente, "person_emp_length")

print("\nOutliers na Dimensão Empréstimo:")
detect_outliers_zscore(dim_emprestimo, "loan_amnt")
detect_outliers_zscore(dim_emprestimo, "loan_int_rate")
detect_outliers_zscore(dim_emprestimo, "loan_percent_income")

# ANÁLISE DE DISTRIBUIÇÃO DE CATEGORIAS
# =========================================
print("\n=== ANÁLISE DE DISTRIBUIÇÃO DE CATEGORIAS ===")

# Distribuição de tipos de residência
print("\nDistribuição de tipos de residência:")
display(dim_cliente.groupBy("person_home_ownership")
  .agg(count("*").alias("count"), 
       (count("*") * 100.0 / dim_cliente.count()).alias("percentage"))
  .orderBy(desc("count")))

# Distribuição de finalidades de empréstimo
print("\nDistribuição de finalidades de empréstimo:")
display(dim_emprestimo.groupBy("loan_intent")
  .agg(count("*").alias("count"), 
       (count("*") * 100.0 / dim_emprestimo.count()).alias("percentage"))
  .orderBy(desc("count")))

# Distribuição de graus de empréstimo
print("\nDistribuição de graus de empréstimo:")
display(dim_emprestimo.groupBy("loan_grade")
  .agg(count("*").alias("count"), 
       (count("*") * 100.0 / dim_emprestimo.count()).alias("percentage"))
  .orderBy("loan_grade"))

# Distribuição de inadimplência
print("\nDistribuição de status de empréstimo (inadimplência):")
display(fato_analise_credito.groupBy("status_emprestimo")
  .agg(count("*").alias("count"), 
       (count("*") * 100.0 / fato_analise_credito.count()).alias("percentage")))

# ANÁLISE DE CONSISTÊNCIA
# =========================================
print("\n=== ANÁLISE DE CONSISTÊNCIA DOS DADOS ===")

# Verificar relações entre dimensões e fatos
print("\nVerificando integridade referencial:")
cliente_ids_dim = dim_cliente.select("cliente_id").distinct().count()
cliente_ids_fato = fato_analise_credito.select("cliente_id").distinct().count()
print(f"Clientes únicos na dimensão: {cliente_ids_dim}")
print(f"Clientes únicos na tabela de fatos: {cliente_ids_fato}")

emprestimo_ids_dim = dim_emprestimo.select("emprestimo_id").distinct().count()
emprestimo_ids_fato = fato_analise_credito.select("emprestimo_id").distinct().count()
print(f"Empréstimos únicos na dimensão: {emprestimo_ids_dim}")
print(f"Empréstimos únicos na tabela de fatos: {emprestimo_ids_fato}")

# Verificar valores ilógicos ou inconsistentes
print("\nVerificando valores ilógicos ou inconsistentes:")

# Idade abaixo de 18 ou acima de 100
invalid_age = dim_cliente.filter((col("person_age") < 18) | (col("person_age") > 100)).count()
print(f"Clientes com idade < 18 ou > 100: {invalid_age}")

# Taxa de juros negativa ou extremamente alta
invalid_rate = dim_emprestimo.filter((col("loan_int_rate") < 0) | (col("loan_int_rate") > 50)).count()
print(f"Empréstimos com taxa de juros < 0% ou > 50%: {invalid_rate}")

# Valores de empréstimo negativos
invalid_loan = dim_emprestimo.filter(col("loan_amnt") <= 0).count()
print(f"Empréstimos com valor <= 0: {invalid_loan}")

# RESUMO DOS PROBLEMAS DE QUALIDADE
# =========================================
print("\n=== RESUMO DOS PROBLEMAS DE QUALIDADE IDENTIFICADOS ===")

# Esta seção será preenchida com base nos resultados das análises acima
# Aqui você vai resumir os principais problemas encontrados e sugerir correções
