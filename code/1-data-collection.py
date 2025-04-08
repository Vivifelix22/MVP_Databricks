# EXPLORANDO O DATASET DE RISCO DE CRÉDITO
# =============================================

# 1. Estrutura dos dados
print("\n=== ESTRUTURA DOS DADOS ===")
print("Esquema do DataFrame:")
df.printSchema()

# 2. Resumo estatístico
print("\n=== RESUMO ESTATÍSTICO ===")
display(df.describe())

# 3. Verificação de valores nulos
print("\n=== VERIFICAÇÃO DE VALORES NULOS ===")
from pyspark.sql.functions import col, count, when, isnan

# Contagem de nulos em cada coluna
null_counts = df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns])
display(null_counts)

# Calcular percentual de nulos
total_rows = df.count()
null_percent = df.select([
    (count(when(col(c).isNull() | isnan(c), c)) * 100 / total_rows).alias(c)
    for c in df.columns
])
print("Percentual de valores nulos por coluna:")
display(null_percent)

# 4. Valores únicos em colunas categóricas
print("\n=== VALORES ÚNICOS EM COLUNAS CATEGÓRICAS ===")
categorical_cols = ["person_home_ownership", "loan_intent", "loan_grade", "cb_person_default_on_file"]

for col_name in categorical_cols:
    unique_values = df.select(col_name).distinct().collect()
    print(f"Valores únicos em {col_name}:")
    for value in unique_values:
        print(f"  - {value[0]}")
    print("")

# 5. Salvando os dados como tabela Delta para fácil acesso
print("\n=== SALVANDO COMO TABELA DELTA ===")
try:
    # Criar uma tabela Delta permanente
    df.write.format("delta").mode("overwrite").saveAsTable("credit_risk_raw")
    print("Tabela 'credit_risk_raw' criada com sucesso!")
    
    # Verificar se a tabela foi criada
    display(spark.sql("SHOW TABLES"))
except Exception as e:
    print(f"Erro ao criar tabela Delta: {e}")
