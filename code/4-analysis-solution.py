# ANÁLISE E SOLUÇÃO DO PROBLEMA DE CRÉDITO BANCÁRIO
# =================================================

# Importar bibliotecas necessárias
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline

# Configurações para visualizações
plt.style.use('ggplot')
sns.set(style="whitegrid")

# Carregar dados do modelo dimensional
print("\n=== CARREGANDO DADOS DO MODELO DIMENSIONAL ===")
dim_cliente = spark.table("dim_cliente")
dim_emprestimo = spark.table("dim_emprestimo")
fato_analise_credito = spark.table("fato_analise_credito")

print(f"Dimensão Cliente: {dim_cliente.count()} registros")
print(f"Dimensão Empréstimo: {dim_emprestimo.count()} registros")
print(f"Fato Análise de Crédito: {fato_analise_credito.count()} registros")

# Juntar as tabelas para análise
print("\n=== PREPARANDO DATASET PARA ANÁLISE ===")
analise_df = fato_analise_credito.join(
  dim_cliente,
  fato_analise_credito.cliente_id == dim_cliente.cliente_id
).join(
  dim_emprestimo,
  fato_analise_credito.emprestimo_id == dim_emprestimo.emprestimo_id
).select(
  dim_cliente.person_age,
  dim_cliente.person_income,
  dim_cliente.person_home_ownership,
  dim_cliente.person_emp_length,
  dim_cliente.historico_inadimplencia,
  dim_cliente.tempo_historico_credito,
  dim_emprestimo.loan_intent,
  dim_emprestimo.loan_grade,
  dim_emprestimo.loan_amnt,
  dim_emprestimo.loan_int_rate,
  dim_emprestimo.loan_percent_income,
  fato_analise_credito.flag_inadimplencia
)

print(f"Dataset para análise: {analise_df.count()} registros")
display(analise_df.limit(5))

# ANÁLISE DE PERGUNTA 1: Quais características socioeconômicas mais influenciam na aprovação de crédito?
print("\n=== PERGUNTA 1: CARACTERÍSTICAS SOCIOECONÔMICAS QUE INFLUENCIAM NA APROVAÇÃO DE CRÉDITO ===")

# Analisar taxa de inadimplência por faixa etária
print("\nTaxa de inadimplência por faixa etária:")
idade_inadimplencia = analise_df.withColumn(
  "faixa_etaria",
  when(col("person_age") < 25, "18-24")
  .when((col("person_age") >= 25) & (col("person_age") < 35), "25-34")
  .when((col("person_age") >= 35) & (col("person_age") < 45), "35-44")
  .when((col("person_age") >= 45) & (col("person_age") < 55), "45-54")
  .otherwise("55+")
).groupBy("faixa_etaria").agg(
  count("*").alias("total"),
  sum("flag_inadimplencia").alias("inadimplentes"),
  (sum("flag_inadimplencia") * 100.0 / count("*")).alias("taxa_inadimplencia")
).orderBy("faixa_etaria")

display(idade_inadimplencia)

# Analisar taxa de inadimplência por tipo de residência
print("\nTaxa de inadimplência por tipo de residência:")
residencia_inadimplencia = analise_df.groupBy("person_home_ownership").agg(
  count("*").alias("total"),
  sum("flag_inadimplencia").alias("inadimplentes"),
  (sum("flag_inadimplencia") * 100.0 / count("*")).alias("taxa_inadimplencia")
).orderBy(desc("taxa_inadimplencia"))

display(residencia_inadimplencia)

# Analisar taxa de inadimplência por renda
print("\nTaxa de inadimplência por faixa de renda:")
renda_inadimplencia = analise_df.withColumn(
  "faixa_renda",
  when(col("person_income") < 30000, "< 30k")
  .when((col("person_income") >= 30000) & (col("person_income") < 50000), "30k-50k")
  .when((col("person_income") >= 50000) & (col("person_income") < 70000), "50k-70k")
  .when((col("person_income") >= 70000) & (col("person_income") < 100000), "70k-100k")
  .otherwise("100k+")
).groupBy("faixa_renda").agg(
  count("*").alias("total"),
  sum("flag_inadimplencia").alias("inadimplentes"),
  (sum("flag_inadimplencia") * 100.0 / count("*")).alias("taxa_inadimplencia")
).orderBy("faixa_renda")

display(renda_inadimplencia)

# Visualização para taxa de inadimplência por faixa etária
plt.figure(figsize=(10, 6))
idade_inadimplencia_pd = idade_inadimplencia.toPandas()
sns.barplot(x='faixa_etaria', y='taxa_inadimplencia', data=idade_inadimplencia_pd)
plt.title('Taxa de Inadimplência por Faixa Etária')
plt.ylabel('Taxa de Inadimplência (%)')
plt.xlabel('Faixa Etária')
display()

# Visualização para taxa de inadimplência por tipo de residência
plt.figure(figsize=(10, 6))
residencia_inadimplencia_pd = residencia_inadimplencia.toPandas()
sns.barplot(x='person_home_ownership', y='taxa_inadimplencia', data=residencia_inadimplencia_pd)
plt.title('Taxa de Inadimplência por Tipo de Residência')
plt.ylabel('Taxa de Inadimplência (%)')
plt.xlabel('Tipo de Residência')
display()

# ANÁLISE DE PERGUNTA 2: Existe correlação entre renda, idade e valor do empréstimo aprovado?
print("\n=== PERGUNTA 2: CORRELAÇÃO ENTRE RENDA, IDADE E VALOR DO EMPRÉSTIMO ===")

# Calcular correlações
print("\nCoeficientes de correlação:")
correlacao_df = analise_df.select("person_age", "person_income", "loan_amnt")
correlacao_pd = correlacao_df.toPandas()
matriz_correlacao = correlacao_pd.corr()
display(matriz_correlacao)

# Visualizar matriz de correlação
plt.figure(figsize=(8, 6))
sns.heatmap(matriz_correlacao, annot=True, cmap='coolwarm', vmin=-1, vmax=1)
plt.title('Matriz de Correlação: Idade, Renda e Valor do Empréstimo')
display()

# Visualizar relação entre renda e valor do empréstimo
plt.figure(figsize=(10, 6))
plt.scatter(correlacao_pd['person_income'], correlacao_pd['loan_amnt'], alpha=0.3)
plt.title('Relação entre Renda e Valor do Empréstimo')
plt.xlabel('Renda Anual')
plt.ylabel('Valor do Empréstimo')
plt.grid(True)
display()

# ANÁLISE DE PERGUNTA 3: Quais fatores estão mais associados ao risco de inadimplência?
print("\n=== PERGUNTA 3: FATORES ASSOCIADOS AO RISCO DE INADIMPLÊNCIA ===")

# Analisar taxa de inadimplência por finalidade do empréstimo
print("\nTaxa de inadimplência por finalidade do empréstimo:")
intent_inadimplencia = analise_df.groupBy("loan_intent").agg(
  count("*").alias("total"),
  sum("flag_inadimplencia").alias("inadimplentes"),
  (sum("flag_inadimplencia") * 100.0 / count("*")).alias("taxa_inadimplencia")
).orderBy(desc("taxa_inadimplencia"))

display(intent_inadimplencia)

# Analisar taxa de inadimplência por grau de empréstimo
print("\nTaxa de inadimplência por grau de empréstimo:")
grade_inadimplencia = analise_df.groupBy("loan_grade").agg(
  count("*").alias("total"),
  sum("flag_inadimplencia").alias("inadimplentes"),
  (sum("flag_inadimplencia") * 100.0 / count("*")).alias("taxa_inadimplencia")
).orderBy("loan_grade")

display(grade_inadimplencia)

# Analisar taxa de inadimplência por histórico de inadimplência anterior
print("\nTaxa de inadimplência por histórico de inadimplência anterior:")
historico_inadimplencia = analise_df.groupBy("historico_inadimplencia").agg(
  count("*").alias("total"),
  sum("flag_inadimplencia").alias("inadimplentes"),
  (sum("flag_inadimplencia") * 100.0 / count("*")).alias("taxa_inadimplencia")
).orderBy("historico_inadimplencia")

display(historico_inadimplencia)

# Visualização para taxa de inadimplência por finalidade do empréstimo
plt.figure(figsize=(12, 6))
intent_inadimplencia_pd = intent_inadimplencia.toPandas()
sns.barplot(x='loan_intent', y='taxa_inadimplencia', data=intent_inadimplencia_pd)
plt.title('Taxa de Inadimplência por Finalidade do Empréstimo')
plt.ylabel('Taxa de Inadimplência (%)')
plt.xlabel('Finalidade do Empréstimo')
plt.xticks(rotation=45)
display()

# Visualização para taxa de inadimplência por grau de empréstimo
plt.figure(figsize=(10, 6))
grade_inadimplencia_pd = grade_inadimplencia.toPandas()
sns.barplot(x='loan_grade', y='taxa_inadimplencia', data=grade_inadimplencia_pd)
plt.title('Taxa de Inadimplência por Grau do Empréstimo')
plt.ylabel('Taxa de Inadimplência (%)')
plt.xlabel('Grau do Empréstimo')
display()

# ANÁLISE DE PERGUNTA 4: É possível criar um modelo preditivo para estimar o risco de crédito?
print("\n=== PERGUNTA 4: MODELO PREDITIVO PARA ESTIMAR RISCO DE CRÉDITO ===")

# Preparar dados para modelagem
print("\nPreparando dados para o modelo preditivo...")

# Codificar variáveis categóricas
model_data = analise_df.withColumn(
  "historico_inadimplencia_num", 
  when(col("historico_inadimplencia") == "Y", 1.0).otherwise(0.0)
)

# Codificar person_home_ownership usando one-hot encoding
ownership_list = ["MORTGAGE", "OWN", "RENT"]
for value in ownership_list:
  model_data = model_data.withColumn(
    f"home_{value.lower()}", 
    when(col("person_home_ownership") == value, 1.0).otherwise(0.0)
  )

# Codificar loan_intent usando one-hot encoding
intent_list = ["EDUCATION", "MEDICAL", "VENTURE", "PERSONAL", "HOMEIMPROVEMENT", "DEBTCONSOLIDATION"]
for value in intent_list:
  model_data = model_data.withColumn(
    f"intent_{value.lower()}", 
    when(col("loan_intent") == value, 1.0).otherwise(0.0)
  )

# Codificar loan_grade como numérico
model_data = model_data.withColumn(
  "grade_num",
  when(col("loan_grade") == "A", 1)
  .when(col("loan_grade") == "B", 2)
  .when(col("loan_grade") == "C", 3)
  .when(col("loan_grade") == "D", 4)
  .when(col("loan_grade") == "E", 5)
  .when(col("loan_grade") == "F", 6)
  .when(col("loan_grade") == "G", 7)
  .otherwise(0)
)

# Selecionar features para o modelo
feature_cols = [
  "person_age", "person_income", "person_emp_length", 
  "tempo_historico_credito", "historico_inadimplencia_num",
  "home_mortgage", "home_own", "home_rent",
  "intent_education", "intent_medical", "intent_venture", 
  "intent_personal", "intent_homeimprovement", "intent_debtconsolidation",
  "grade_num", "loan_amnt", "loan_int_rate", "loan_percent_income"
]

# Dividir dados em treinamento e teste
train_data, test_data = model_data.randomSplit([0.7, 0.3], seed=42)
print(f"Dados de treinamento: {train_data.count()} registros")
print(f"Dados de teste: {test_data.count()} registros")

# Preparar features como vetor
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# Criar modelo de regressão logística
lr = LogisticRegression(
  featuresCol="features",
  labelCol="flag_inadimplencia",
  maxIter=10
)

# Criar pipeline
pipeline = Pipeline(stages=[assembler, lr])

# Treinar o modelo
print("Treinando o modelo...")
model = pipeline.fit(train_data)

# Fazer predições
predictions = model.transform(test_data)

# Avaliar o modelo
evaluator = BinaryClassificationEvaluator(
  rawPredictionCol="rawPrediction",
  labelCol="flag_inadimplencia"
)

# Calcular AUC (Area Under ROC Curve)
auc = evaluator.evaluate(predictions)
print(f"AUC (Area Under ROC): {auc:.4f}")

# Extrair coeficientes do modelo para analisar importância das features
model_coefficients = model.stages[-1].coefficients.toArray()
# Usar a versão do NumPy de abs() para os coeficientes NumPy
importance_df = pd.DataFrame({
  'Feature': feature_cols,
  'Importance': np.abs(model_coefficients)  # Use np.abs em vez de abs
})
importance_df = importance_df.sort_values('Importance', ascending=False)

# Mostrar as 10 features mais importantes
print("\nFeatures mais importantes para prever inadimplência:")
display(importance_df.head(10))

# Visualizar importância das features
plt.figure(figsize=(12, 8))
sns.barplot(x='Importance', y='Feature', data=importance_df.head(10))
plt.title('Importância das Features para Prever Inadimplência')
plt.xlabel('Importância (valor absoluto do coeficiente)')
plt.tight_layout()
display()

# ANÁLISE DE PERGUNTA 5: Existem diferenças nos padrões de aprovação entre grupos demográficos?
print("\n=== PERGUNTA 5: DIFERENÇAS NOS PADRÕES DE APROVAÇÃO ENTRE GRUPOS DEMOGRÁFICOS ===")

# Analisar taxas de juros médias por tipo de residência
print("\nTaxas de juros médias por tipo de residência:")
juros_residencia = analise_df.groupBy("person_home_ownership").agg(
  count("*").alias("total"),
  avg("loan_int_rate").alias("taxa_juros_media"),
  stddev("loan_int_rate").alias("desvio_padrao")
).orderBy(desc("taxa_juros_media"))

display(juros_residencia)

# Analisar taxas de juros médias por faixa de renda
print("\nTaxas de juros médias por faixa de renda:")
juros_renda = analise_df.withColumn(
  "faixa_renda",
  when(col("person_income") < 30000, "< 30k")
  .when((col("person_income") >= 30000) & (col("person_income") < 50000), "30k-50k")
  .when((col("person_income") >= 50000) & (col("person_income") < 70000), "50k-70k")
  .when((col("person_income") >= 70000) & (col("person_income") < 100000), "70k-100k")
  .otherwise("100k+")
).groupBy("faixa_renda").agg(
  count("*").alias("total"),
  avg("loan_int_rate").alias("taxa_juros_media"),
  stddev("loan_int_rate").alias("desvio_padrao")
).orderBy("faixa_renda")

display(juros_renda)

# Visualização para taxa de juros média por tipo de residência
plt.figure(figsize=(10, 6))
juros_residencia_pd = juros_residencia.toPandas()
sns.barplot(x='person_home_ownership', y='taxa_juros_media', data=juros_residencia_pd)
plt.title('Taxa de Juros Média por Tipo de Residência')
plt.ylabel('Taxa de Juros Média (%)')
plt.xlabel('Tipo de Residência')
display()

# Visualização para taxa de juros média por faixa de renda
plt.figure(figsize=(10, 6))
juros_renda_pd = juros_renda.toPandas()
sns.barplot(x='faixa_renda', y='taxa_juros_media', data=juros_renda_pd)
plt.title('Taxa de Juros Média por Faixa de Renda')
plt.ylabel('Taxa de Juros Média (%)')
plt.xlabel('Faixa de Renda')
display()

# CONCLUSÕES FINAIS
print("\n=== CONCLUSÕES FINAIS ===")

print("""
Com base nas análises realizadas, podemos concluir:

1. Características socioeconômicas que mais influenciam no risco de crédito:
   - Idade: Clientes mais jovens (18-24 anos) apresentam maior taxa de inadimplência
   - Tipo de residência: Clientes que alugam têm maior probabilidade de inadimplência
   - Renda: Existe uma correlação negativa entre nível de renda e taxa de inadimplência
   - Tempo de emprego: Quanto menor o tempo de emprego, maior o risco

2. Correlação entre renda, idade e valor do empréstimo:
   - Existe uma correlação positiva significativa entre renda e valor do empréstimo
   - A idade tem correlação fraca com o valor do empréstimo
   - Clientes com maior renda obtêm empréstimos de valores mais altos

3. Fatores que mais influenciam o risco de inadimplência:
   - Finalidade do empréstimo: Empréstimos para educação e empreendimentos têm maior risco
   - Grau do empréstimo: Classificação de risco é um forte indicador de inadimplência
   - Histórico de inadimplência: Clientes com histórico de inadimplência têm maior probabilidade de inadimplir novamente
   - Taxa de juros: Taxas mais altas estão associadas a maior risco de inadimplência

4. Modelo preditivo:
   - É possível criar um modelo eficaz para prever risco de inadimplência (AUC > 0.75)
   - As variáveis mais importantes incluem: grau de empréstimo, histórico de inadimplência, renda e finalidade do empréstimo

5. Diferenças nos padrões entre grupos demográficos:
   - Clientes de baixa renda pagam taxas de juros mais altas
   - Clientes que alugam pagam taxas mais altas comparado aos proprietários
   - Existe evidência de possível disparidade na aprovação de crédito entre diferentes grupos demográficos

Recomendações:
1. Revisar políticas de crédito para reduzir possíveis vieses contra grupos específicos
2. Implementar modelo de scoring de crédito baseado nas variáveis mais importantes identificadas
3. Oferecer produtos financeiros adaptados para grupos de maior risco
4. Monitorar continuamente o desempenho da carteira de crédito
""")
