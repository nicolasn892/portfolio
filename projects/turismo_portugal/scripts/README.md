# Análise de Turismo em Portugal

Projeto de portfólio demonstrando ETL, análise e dashboard interativo usando dados de turismo fictícios em Portugal.

## Estrutura
- `data/` → CSV de entrada (`turismo.csv`) e saída (`turismo_limpo.csv`)
- `scripts/` → `etl_turismo.py` e `generate_dashboard.py`
- `dist/` → HTML interativo gerado (`turismo_dashboard.html`)
- `notebooks/` → notebooks opcionais de análise

## Como rodar (local)
1. Instalar dependências:

pip install pandas plotly

2. Rodar ETL:

python projects/turismo_portugal/scripts/etl_turismo.py

3. Gerar dashboard:

python projects/turismo_portugal/scripts/generate_dashboard.py
\\Isso cria `projects/turismo_portugal/dist/turismo_dashboard.html`
