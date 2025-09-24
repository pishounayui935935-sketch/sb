import streamlit as st
import pandas as pd
from datetime import datetime

st.set_page_config(page_title="Teste Streamlit", layout="wide")
st.title("✅ Teste do Dashboard de Oportunidades")

# Criar DataFrame fake
data = {
    "bookmaker": ["betclic", "bwin", "placard"],
    "match": ["Team A vs Team B", "Team C vs Team D", "Team E vs Team F"],
    "bet_type": ["Home", "Draw", "Away"],
    "odd": [2.1, 3.4, 5.2],
    "mean_odd": [2.0, 3.1, 4.8],
    "overvalue": [5.0, 9.6, 8.3],
}
df = pd.DataFrame(data)

st.subheader("📋 Oportunidades (dados de teste)")
st.dataframe(df, width="stretch")

# Cards simples
st.subheader("🎴 Cards de Oportunidades")
for _, row in df.iterrows():
    with st.container():
        st.markdown(f"### {row['match']} ({row['bookmaker'].upper()})")
        st.write(f"**Bet Type:** {row['bet_type']}")
        st.write(f"**Odd:** {row['odd']} | **Mean Odd:** {row['mean_odd']}")
        st.write(f"**Overvalue:** {row['overvalue']}%")
        st.divider()

# Hora da atualização
st.caption(f"⏱️ Última atualização: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
