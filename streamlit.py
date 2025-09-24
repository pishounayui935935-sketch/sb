import streamlit as st
import pandas as pd
import sqlite3
import os
import time
from datetime import datetime

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
st.set_page_config(page_title="Debug Oportunidades", layout="wide")
st.title("🔍 Debug da Base de Dados de Oportunidades")

# Caminho da BD (ajusta se necessário)
DB_PATH = os.path.abspath("oportunidades.db")
st.write("📂 Caminho da BD em uso:", DB_PATH)

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
    st.success(f"📊 Total de linhas na tabela: {total}")

    df = pd.read_sql_query(
        f"SELECT * FROM oportunidades ORDER BY id DESC LIMIT {limit}", conn
    )
    conn.close()
    return df

# -------------------------------------------------
# MAIN
# -------------------------------------------------
df = load_preview()

if df.empty:
    st.warning("⚠️ Nenhum registo encontrado (ou tabela vazia).")
else:
    st.subheader("📋 Últimos registos da BD")
    st.dataframe(df, width="stretch")  # ✅ atualizado

# Mostrar hora da última atualização
st.caption(f"⏱️ Última atualização: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Espera 2 segundos e força rerun
time.sleep(2)
st.rerun()
