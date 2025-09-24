import streamlit as st
import pandas as pd
import sqlite3
import os
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Oportunidades de Apostas", layout="wide")
st.title("📊 Oportunidades de Apostas")

# 🔄 Auto refresh a cada 15 segundos
count = st_autorefresh(interval=15_000, key="datarefresh")

DB_PATH = r"C:\Users\Daniel\oportunidades.db"

def load_data(limit=50):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        f"SELECT * FROM oportunidades ORDER BY id DESC LIMIT {limit}", conn
    )
    conn.close()
    return df

# Debug → ver se realmente está a atualizar
st.caption(f"📂 A usar BD: {DB_PATH}")
st.caption(f"🔄 Atualização #{count} às {pd.Timestamp.now()}")

df = load_data()

if df.empty:
    st.warning("Ainda não há oportunidades registadas.")
else:
    st.subheader("📋 Últimos registos")
    st.dataframe(df)

    # Mostrar cards
    st.subheader("🎴 Cards de Oportunidades")
    for _, row in df.iterrows():
        with st.container():
            st.markdown(f"### {row['match']} ({row['bookmaker'].upper()})")
            st.write(f"**Bet Type:** {row['bet_type']}")
            st.write(f"**Odd:** {row['odd']:.2f} | **Mean Odd:** {row['mean_odd']:.2f}")
            st.write(f"**Overvalue:** {row['overvalue']:.2f}%")
            if row['link']:
                st.write(f"[Abrir Link]({row['link']})")
            st.caption(f"📅 {row['timestamp']}")
            st.divider()
