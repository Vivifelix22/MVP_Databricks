# Análise de Risco de Crédito Bancário com Databricks
## Descrição
Pipeline completo para análise de risco de crédito bancário utilizando Databricks Community Edition. O projeto implementa um processo de ETL, modelagem dimensional e análises preditivas com PySpark.

## Objetivos
- Identificar características socioeconômicas que influenciam na aprovação de crédito
- Analisar correlações entre renda, idade e valor do empréstimo
- Determinar fatores associados ao risco de inadimplência
- Desenvolver um modelo preditivo para estimar risco de crédito
- Identificar diferenças nos padrões de aprovação entre grupos demográficos

## Tecnologias Utilizadas
- Databricks Community Edition
- Apache Spark (PySpark)
- Delta Lake
- Spark ML (para modelagem preditiva)
- Matplotlib e Seaborn (para visualizações)

## Estrutura do Repositório
- `code/`: Notebooks Python do Databricks
- `docs/`: Documentação detalhada do projeto
- `screenshots/`: Capturas de tela do processo e resultados

## Resultados Principais
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

## Autor
Viviane Felix Caetano

## Licença
Este projeto está licenciado sob a Licença MIT
