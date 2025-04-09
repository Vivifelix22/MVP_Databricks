## 4. self-assessment.md

```markdown
# Autoavaliação do Projeto de Análise de Risco de Crédito

## Atingimento dos Objetivos

No início deste projeto, estabeleci como objetivo desenvolver um pipeline completo de dados para análise de risco de crédito bancário, visando responder cinco perguntas principais:

1. Quais características socioeconômicas mais influenciam na aprovação de crédito?
2. Existe correlação entre renda, idade e valor do empréstimo aprovado?
3. Quais fatores estão mais associados ao risco de inadimplência?
4. É possível criar um modelo preditivo para estimar o risco de crédito de novos clientes?
5. Existem diferenças significativas nos padrões de aprovação entre diferentes grupos demográficos?

Considero que todos os objetivos foram alcançados com sucesso. Através das análises realizadas, consegui identificar os principais fatores socioeconômicos que influenciam o risco de crédito, demonstrar as correlações entre as variáveis de interesse, criar um modelo preditivo funcional e identificar disparidades entre grupos demográficos.

Em termos específicos:

- **Objetivo 1**: Identificamos claramente que idade, tipo de residência, renda e tempo de emprego são fatores determinantes no risco de crédito, com evidências quantitativas para cada um.

- **Objetivo 2**: Demonstramos uma correlação positiva significativa entre renda e valor do empréstimo, e uma correlação fraca entre idade e valor do empréstimo.

- **Objetivo 3**: Identificamos que finalidade do empréstimo, grau de classificação, histórico de inadimplência e percentual da renda comprometido são os principais indicadores de risco.

- **Objetivo 4**: Desenvolvemos um modelo de Regressão Logística com boa performance, demonstrando que é viável prever o risco de inadimplência.

- **Objetivo 5**: Encontramos evidências estatísticas de disparidades nas taxas de juros e aprovação entre diferentes grupos de renda e tipos de residência.

<!-- Insira aqui capturas de tela que demonstrem o atingimento dos objetivos, como gráficos ou tabelas de resultados -->

## Dificuldades Encontradas

Durante a execução do projeto, enfrentei algumas dificuldades significativas:

1. **Configuração do ambiente Databricks Community Edition**: Como iniciante na plataforma, gastei um tempo considerável entendendo suas particularidades e limitações. O Databricks Community possui restrições de recursos computacionais que exigiram otimização do código para evitar problemas de memória.

2. **Transformação de dados para o modelo dimensional**: A criação do esquema estrela a partir dos dados brutos foi desafiadora, especialmente para garantir a integridade referencial entre dimensões e fato. A geração de chaves surrogate e o mapeamento correto entre as tabelas exigiram várias revisões.

3. **Codificação de variáveis categóricas**: Para o modelo preditivo, a transformação de variáveis categóricas como `loan_intent` e `person_home_ownership` em formato numérico exigiu múltiplas etapas de transformação, aumentando a complexidade do pipeline ETL.

4. **Tratamento de inconsistências nos dados**: Alguns registros apresentavam valores incoerentes (como idades negativas ou taxas de juros fora do intervalo esperado) que precisaram ser identificados e tratados individualmente.

5. **Visualização de dados no ambiente Databricks**: A integração de bibliotecas de visualização como Matplotlib e Seaborn dentro do Databricks apresentou alguns desafios, especialmente na formatação e exibição correta dos gráficos.

<!-- Insira aqui capturas de tela que demonstrem como você superou essas dificuldades -->

## Aprendizados

Este projeto proporcionou aprendizados valiosos em várias áreas:

1. **Arquitetura de Data Warehouse**: Aprendi na prática como estruturar um modelo dimensional (esquema estrela) para análise de dados, distinguindo claramente fatos e dimensões.

2. **Processos ETL no Databricks**: Desenvolvi habilidades na criação de pipelines de extração, transformação e carga utilizando PySpark, entendendo suas vantagens para processamento distribuído.

3. **Análise de qualidade de dados**: Aprimorei técnicas para identificação e tratamento de problemas de qualidade, como valores ausentes, outliers e inconsistências.

4. **Modelagem preditiva com Spark ML**: Compreendi o fluxo de trabalho para criação e avaliação de modelos de machine learning no ambiente Spark, desde a preparação dos dados até a interpretação dos resultados.

5. **Visualização e comunicação de resultados**: Melhorei minhas habilidades em criar visualizações efetivas para comunicar insights complexos de forma clara e acessível.

## Trabalhos Futuros

Para aprimorar este projeto no futuro, considero as seguintes possibilidades:

1. **Modelos mais avançados**: Implementar algoritmos mais sofisticados como Random Forest, Gradient Boosting ou redes neurais, que poderiam capturar melhor relações não-lineares nos dados.

2. **Integração de fontes externas**: Enriquecer a análise com dados socioeconômicos externos, como indicadores regionais de emprego ou custo de vida, para contextualizar melhor o risco de crédito.

3. **Análise temporal**: Expandir o modelo para incluir a dimensão temporal de forma mais robusta, permitindo análise de tendências e sazonalidade no comportamento de pagamento.

4. **Pipeline de dados automatizado**: Desenvolver um fluxo automatizado de ingestão, processamento e atualização dos dados, utilizando ferramentas como Airflow ou Databricks Jobs.

5. **Aplicação em tempo real**: Criar uma API para que o modelo preditivo possa ser consultado em tempo real por sistemas de aprovação de crédito.

6. **Análise mais profunda de equidade**: Realizar uma análise mais detalhada sobre vieses algorítmicos e disparidades entre grupos demográficos, incluindo métricas de equidade como disparate impact e equal opportunity difference.

## Conclusão

Este projeto representou uma oportunidade valiosa para aplicar conhecimentos teóricos em um contexto prático e relevante. Apesar das dificuldades encontradas, consegui desenvolver um pipeline completo e funcional, atingindo todos os objetivos inicialmente propostos.

A experiência adquirida com o Databricks, PySpark e técnicas de modelagem dimensional será extremamente útil em projetos futuros, e os insights obtidos sobre fatores de risco de crédito têm aplicação direta no mundo real das instituições financeiras.
