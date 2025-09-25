import streamlit as st
import pandas as pd
import sqlite3
import os
from datetime import datetime
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Oportunidades Live", layout="wide")
st.title("Oportunidades")

# 🔄 Auto refresh a cada 2 segundos (2000 ms)
st_autorefresh(interval=2000, key="datarefresh")

DB_PATH = "oportunidades.db"

# -------------------------------------------------
# FUNÇÃO PARA LER DADOS
# -------------------------------------------------
def load_preview(limit=20):
    if not os.path.exists(DB_PATH):
        st.error(f"❌ Base de dados não encontrada em {DB_PATH}")
        return pd.DataFrame(), 0

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='oportunidades'"
    )
    exists = cur.fetchone()
    if not exists:
        conn.close()
        st.error("❌ A tabela 'oportunidades' não existe nesta BD.")
        return pd.DataFrame(), 0

    cur.execute("SELECT COUNT(*) FROM oportunidades")
    total = cur.fetchone()[0]

    df = pd.read_sql_query(
        f"SELECT * FROM oportunidades ORDER BY id DESC LIMIT {limit}", conn
    )
    conn.close()
    return df, total

# -------------------------------------------------
# MAIN
# -------------------------------------------------
df, total = load_preview()

if df.empty:
    st.warning("⚠️ Nenhum registo encontrado (ou tabela vazia).")
else:
    st.caption(f"⏱️ Última atualização: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    st.success(f"📊 Total de linhas na tabela: {total}")

    st.subheader("🎴 Últimas 20 Oportunidades")

    # Criar um container com scroll
    with st.container():
        st.markdown(
            """
            <style>
            .scrollable-cards {
                max-height: 600px;
                overflow-y: auto;
                padding-right: 10px;
                border: 1px solid #ddd;
                border-radius: 8px;
            }
            .card {
                background-color: #f9f9f9;
                padding: 12px;
                margin-bottom: 10px;
                border-radius: 8px;
                box-shadow: 0 1px 3px rgba(0,0,0,0.1);
                font-family: monospace;
                white-space: pre-line;
            }
            </style>
            """,
            unsafe_allow_html=True
        )

        st.markdown('<div class="scrollable-cards">', unsafe_allow_html=True)

        for _, row in df.iterrows():
            card_text = (
                "-------------------------------------------------------------\n"
                f"{'Bookmaker: ':<12}{row['bookmaker'].upper()}\n"
                f"{'Match: ':<12}{row['match']}\n"
                f"{'Bet Type: ':<12}{row['bet_type']}\n"
                f"{'Odd: ':<12}{row['odd']:.2f}\n"
                f"{'Mean: ':<12}{row['mean_odd']:.2f}\n"
                f"{'Overvalue: ':<12}{row['overvalue']:.2f}\n"
                f"{'Link: ':<12}{row['link']}\n"
                "-------------------------------------------------------------"
            )

            st.markdown(f'<div class="card">{card_text}</div>', unsafe_allow_html=True)

        st.markdown('</div>', unsafe_allow_html=True)
