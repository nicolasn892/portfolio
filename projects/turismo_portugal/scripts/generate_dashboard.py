# generate_dashboard.py
import pandas as pd
import plotly.express as px
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
DATA_IN = BASE / "data" / "turismo_limpo.csv"
OUT_HTML = BASE / "dist" / "turismo_dashboard.html"

def build_dashboard():
    df = pd.read_csv(DATA_IN)
    # gráfico 1: turistas estrangeiros por cidade (último ano)
    ultimo_ano = df['ano'].max()
    df_last = df[df['ano'] == ultimo_ano]

    fig1 = px.bar(df_last, x='cidade', y='turistas_estrangeiros',
                  title=f"Turistas estrangeiros por cidade — {ultimo_ano}",
                  labels={'turistas_estrangeiros':'Turistas estrangeiros', 'cidade':'Cidade'})

    # gráfico 2: gasto médio por cidade ao longo dos anos
    fig2 = px.line(df, x='ano', y='gasto_medio', color='cidade',
                   markers=True, title='Gasto médio por cidade ao longo dos anos')

    # salve um HTML com ambos os gráficos
    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write("<html><head><meta charset='utf-8'><title>Turismo Portugal Dashboard</title></head><body>\n")
        f.write("<h1>Turismo Portugal — Dashboard</h1>\n")
        f.write("<h2>Gráfico: Turistas estrangeiros (último ano)</h2>\n")
        f.write(fig1.to_html(full_html=False, include_plotlyjs='cdn'))
        f.write("<hr>\n")
        f.write("<h2>Gasto médio por cidade</h2>\n")
        f.write(fig2.to_html(full_html=False, include_plotlyjs=False))
        # incluir script do plotly uma vez
        f.write("<script src='https://cdn.plot.ly/plotly-latest.min.js'></script>\n")
        f.write("</body></html>")
    print(f"Dashboard gerado: {OUT_HTML}")

if __name__ == "__main__":
    build_dashboard()
