# 📊 Previsão de Estoque Inteligente na AWS com SageMaker Canvas
## Descrição do Projeto

Este projeto tem como objetivo desenvolver um modelo de Machine Learning para previsão de estoque, utilizando o Amazon SageMaker Canvas, uma ferramenta no-code da AWS que permite criar modelos de ML sem a necessidade de programação.

A proposta é analisar dados históricos de vendas e estoque para prever a demanda futura, auxiliando na tomada de decisão, redução de rupturas e otimização de inventário.

Este projeto faz parte do Lab da DIO – Previsão de Estoque Inteligente na AWS com SageMaker Canvas.

## Objetivos do Projeto

Aplicar conceitos de Machine Learning no-code

Utilizar o Amazon SageMaker Canvas para criação de modelos preditivos

Realizar análise de métricas e variáveis

Gerar previsões inteligentes de estoque

Documentar todo o processo de forma profissional para portfólio

## Tecnologias Utilizadas

AWS SageMaker Canvas

Amazon Web Services (AWS)

Machine Learning (AutoML)

Análise de dados

Previsão de séries temporais

## Pré-requisitos

Conta ativa na AWS

Acesso ao Amazon SageMaker Canvas

Dataset de estoque/vendas (fornecido no repositório ou personalizado)

## Dataset Utilizado

O dataset utilizado contém informações históricas relacionadas ao estoque, como:

Data

Produto

Quantidade em estoque

item promocional

**Dataset selecionado:** /datasets/dataset-1000-com-preco-promocional-e-renovacao-estoque.csv

Observação: o dataset foi carregado diretamente no SageMaker Canvas para treinamento do modelo.

## Etapas do Projeto
### 1. Seleção e Upload do Dataset

Escolha do dataset disponível na pasta datasets

Upload do arquivo no SageMaker Canvas

Verificação automática da estrutura dos dados

### 2. Construção e Treinamento do Modelo

No SageMaker Canvas:

Definição da variável alvo (ex: quantidade de estoque futura)

Seleção das variáveis de entrada

Escolha do tipo de problema:

### 3. Previsão (Forecasting)

Início do treinamento automático do modelo

O Canvas realizou:

Limpeza automática dos dados

Engenharia de features

Teste de múltimos algoritmos

Seleção do melhor modelo

### 4. Análise dos Resultados

Após o treinamento, foram analisados:

Métricas de desempenho do modelo

Importância das variáveis

Comportamento da previsão ao longo do tempo

Principais pontos observados:

Variáveis relacionadas ao histórico de vendas tiveram maior impacto

O modelo apresentou boa capacidade de generalização

Previsões consistentes para períodos futuros

(Inserir prints do Canvas, se desejar)

### 5. Geração de Previsões

Com o modelo treinado:

Foram geradas previsões de estoque futuro

Os resultados foram exportados

As previsões permitiram identificar:

Possíveis rupturas de estoque

Períodos de alta demanda

Necessidade de reposição antecipada

## Resultados Obtidos

Modelo de previsão treinado com sucesso

Identificação de padrões de consumo

Geração de previsões automáticas

Aplicação prática de ML no contexto de negócios
![](../lab-aws-sagemaker-canvas-estoque/Captura%20de%20tela%202026-01-28%20125427.png)

## Insights do Projeto

O uso de ML no-code acelera significativamente o desenvolvimento

O SageMaker Canvas é ideal para áreas de negócio e analistas

Previsão de estoque reduz desperdícios e perdas financeiras

Dados históricos bem estruturados são essenciais para bons resultados

## Conclusão

Este projeto demonstrou como é possível aplicar Machine Learning na AWS de forma prática e acessível, utilizando o SageMaker Canvas para resolver um problema real de negócio: previsão inteligente de estoque.

A experiência contribuiu para o fortalecimento de habilidades em:

Cloud Computing

Machine Learning aplicado

Análise de dados

Documentação técnica