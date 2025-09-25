import streamlit as st
import pandas as pd
import sqlite3
import os
from datetime import datetime
from streamlit_autorefresh import st_autorefresh

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
st.set_page_config(page_title="Oportunidades Live", layout="wide")
st.title("Oportunidades")

# 🔄 Auto refresh a cada 2 segundos (2000 ms)
refresh_counter = st_autorefresh(interval=2000, key="datarefresh")

# Caminho da BD
DB_PATH = "oportunidades.db"

# -------------------------------------------------
# FUNÇÃO PARA LER DADOS
# -------------------------------------------------
def load_preview(limit=10):
    if not os.path.exists(DB_PATH):
        st.error(f"❌ Base de dados não encontrada em {DB_PATH}")
        return pd.DataFrame()

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Verificar se a tabela existe
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='oportunidades'"
    )
    exists = cur.fetchone()
    if not exists:
        conn.close()
        st.error("❌ A tabela 'oportunidades' não existe nesta BD.")
        return pd.DataFrame()

    # Contar linhas
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
    st.subheader("📋 Últimos registos da BD")
    # Mostrar hora da última atualização
    st.caption(f"⏱️ Última atualização: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    st.dataframe(df, use_container_width=True)  

    st.success(f"📊 Total de linhas na tabela: {total}")

