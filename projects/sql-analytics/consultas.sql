-- Exemplo 1: total de turistas por cidade e ano
SELECT cidade, ano, SUM(turistas_estrangeiros + turistas_nacionais) AS total_turistas
FROM turismo_table
GROUP BY cidade, ano
ORDER BY ano DESC, total_turistas DESC;

-- Exemplo 2: gasto médio por cidade (ordenado)
SELECT cidade, AVG(gasto_medio) AS gasto_medio_medio
FROM turismo_table
GROUP BY cidade
ORDER BY gasto_medio_medio DESC;

-- Exemplo 3: janela - crescimento ano a ano (percentual)
SELECT
  cidade,
  ano,
  total_turistas,
  LAG(total_turistas) OVER (PARTITION BY cidade ORDER BY ano) AS prev_year,
  ROUND(100.0*(total_turistas - LAG(total_turistas) OVER (PARTITION BY cidade ORDER BY ano)) / NULLIF(LAG(total_turistas) OVER (PARTITION BY cidade ORDER BY ano),0),2) AS pct_change
FROM (
  SELECT cidade, ano, SUM(turistas_estrangeiros + turistas_nacionais) AS total_turistas
  FROM turismo_table
  GROUP BY cidade, ano
) t;
