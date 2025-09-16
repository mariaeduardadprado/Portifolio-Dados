# 🎬 Romance Movies Analytics – Pipeline de Dados na AWS

Este projeto tem como foco a análise do gênero **Romance**, um dos mais tradicionais do cinema e também o meu favorito pessoalmente.  
O objetivo foi entender como os filmes românticos são avaliados ao longo das décadas e investigar fatores culturais e criativos que influenciam no sucesso dessas produções.  

O pipeline foi desenvolvido em **AWS**, passando por todas as etapas: ingestão → processamento → modelagem dimensional → análise visual interativa.

---

## Perguntas de Negócio
Durante o desenvolvimento, buscamos responder às seguintes perguntas:

1. Quais países produzem os filmes de romance mais bem avaliados?  
2. A quantidade de filmes interfere na nota média de um país?  
3. Como evoluiu o orçamento de produção ao longo dos anos?  
4. Existe relação entre o valor do orçamento e a nota média do filme?  
5. Filmes dirigidos por mulheres têm maior popularidade?  

---

## Arquitetura do Projeto
![Arquitetura do Pipeline](./imagens/Desafio-FilmesSeries-Completo.png)



## Etapas do Projeto

### 1. Ingestão de Dados
- Upload dos datasets originais (`movies.csv`, `series.csv`) no **S3**, organizados por data.  
- Automação via **boto3** em container Docker.  
- Enriquecimento com API **TMDB** (detalhes, créditos e keywords).  
- Automação em **AWS Lambda**, salvando em `raw/tmdb/json/`.

### 2. Trusted Zone
- Correção de colunas, remoção de duplicados, conversão de tipos.  
- Padronização de texto (`lower()`, `trim()`, etc.).  
- Conversão dos arquivos para **Parquet**, organizados em partições.  

### 3. Refined Zone
- Catálogo automático com **Glue Crawler**.  
- Modelagem dimensional (**Star Schema**):  
  - Fato: `fato_movie_rating`  
  - Dimensões: `dim_movies`, `dim_genero`, `dim_diretor`, `dim_cou try`, `dim_orcamento`, `dim_data`  
   
  ![Modelo Dimensional](./imagens/modelodimensional-starschema.png)  
  - Jobs em PySpark para união de esquemas e criação de dimensões/fatos.  

### 4. Visualização
- Conexão via **Athena** no **Amazon QuickSight**.  
- Dashboard interativo com:  
  - KPIs principais (total de filmes, média de notas, orçamento médio).  
  - Gráfico de barras: países × nota média.  
  - Mapa interativo: países × quantidade de filmes.  
  - Linha temporal: evolução do orçamento.  
  - Colunas: orçamento × nota média.  
  - Gráfico de rosca: distribuição por gênero de diretores.  
  - Tabela dinâmica: top filmes filtráveis.  

---

## Dashboard  
![Exemplo Dashboard](./imagens/Dashboard.png)

---

## Tecnologias Utilizadas
- **Cloud:** AWS (S3, Lambda, Glue, Athena, QuickSight)  
- **Processamento:** PySpark  
- **Orquestração:** Lambda (serverless)  
- **Infraestrutura:** Docker (para ingestão inicial)  
- **Banco & Modelagem:** Star Schema, Parquet, Particionamento  
- **Linguagens:** Python, SQL  

---


## Conclusões
- O projeto consolidou meu aprendizado em **ETL e pipelines de dados em nuvem**.  
- Consegui conectar **questões de negócio** com análises visuais acionáveis.  
- Dominei ferramentas modernas de **engenharia de dados (AWS, PySpark, Data Lake/WH, QuickSight)**.  

Esse projeto me preparou para aplicar práticas de engenharia de dados em cenários reais, com qualidade, escalabilidade e foco no valor de negócio.  

       
---
 **Autora:** Maria Eduarda Prado  
[LinkedIn](#) | [GitHub](#)  