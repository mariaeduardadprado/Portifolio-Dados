CREATE OR REPLACE VIEW database_refined.vw_romance_base AS
SELECT
       m.movie_id,
       m.tituloprincipal,
       m.titulooriginal,
       d.ano                          AS ano_lancamento,
       f.notamedia,
       f.numerovotos,
       f.date_key,
       dr.director_id,
       dr.nome_diretor,
       dr.gender                      AS director_gender,
       p.country_code
FROM   database_refined.fact_movie_rating  f
JOIN   database_refined.dim_movie          m  ON m.movie_id   = f.movie_id
LEFT  JOIN database_refined.dim_diretor    dr ON dr.director_id = f.director_id
LEFT  JOIN database_refined.dim_pais       p  ON p.movie_id   = m.movie_id
JOIN   database_refined.dim_data           d  ON d.date_key   = f.date_key;

CREATE OR REPLACE VIEW database_refined.vw_romance_year AS
SELECT
       ano_lancamento,   
       COUNT(DISTINCT movie_id)       AS qtde_filmes,
       ROUND(AVG(notamedia), 2)       AS media_avaliacao,
       SUM(numerovotos)               AS total_votos
FROM   database_refined.vw_romance_base
GROUP  BY ano_lancamento             
ORDER  BY ano_lancamento; 

CREATE OR REPLACE VIEW database_refined.vw_romance_diretor_genero AS
SELECT
    id                               AS movie_id,
    diretor.name                     AS nome_diretor,
    CASE
        WHEN diretor.gender = 1 THEN 'Mulher'
        WHEN diretor.gender = 2 THEN 'Homem'
        ELSE 'Não informado'
    END                              AS diretor_genero,
    diretor.popularity               AS diretor_popularity
FROM database_refined."2"
CROSS JOIN UNNEST(crew) AS t(diretor)
WHERE LOWER(diretor.job) = 'director';


CREATE OR REPLACE VIEW database_refined.vw_romance_country AS
SELECT
    id AS movie_id,
    country_code
FROM database_refined."2"
CROSS JOIN UNNEST(origin_country) AS t(country_code)
WHERE country_code IS NOT NULL;

CREATE OR REPLACE VIEW database_refined.vw_romance_orcamento AS
SELECT
    id                        AS movie_id,
    tituloprincipal,
    titulooriginal,
    release_year,
    TRY(date_parse(release_date, '%Y-%m-%d')) AS release_date,
    notamedia,
    numerovotos,
    CAST(details.budget AS BIGINT)  AS budget_usd,
    CAST(details.revenue AS BIGINT) AS revenue_usd
FROM
    database_refined."2"
WHERE
    details.budget > 0
    AND release_year IS NOT NULL;

CREATE OR REPLACE VIEW database_refined.vw_romance_year AS
SELECT
    movie_id,
    CAST(SUBSTRING(CAST(date_key AS VARCHAR), 1, 4) AS INT) AS release_year
FROM database_refined.vw_romance_base
WHERE date_key IS NOT NULL;


SELECT * FROM database_refined.vw_romance_base LIMIT 10;