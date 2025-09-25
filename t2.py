import asyncio
from datetime import datetime
import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz
from playwright.async_api import async_playwright
import time
import requests
import sqlite3
import os


pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('future.no_silent_downcasting', True)

# --- Telegram Config ---
TOKEN = "7956301287:AAGZxxHpgV9-nVQuH9zUp87CuJFSbMS35Xk"
CHAT_ID = -4863205894  # ID do grupo

# --- Global start times for each site ---
start_time_bwin = start_time_betclic = start_time_solverde = start_time_placard = time.time()

# ---------------- UTILS ---------------- #
def get_public_ip():
    try:
        ip = requests.get("https://api64.ipify.org").text
        print(f"🌐 Current public IP: {ip}")
        return ip
    except Exception as e:
        print(f"⚠️ Não consegui obter o IP: {e}")
        return None

def send_telegram_message(text):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    params = {
        "chat_id": CHAT_ID,
        "text": text,
        "disable_web_page_preview": True
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        return r.json()
    except Exception as e:
        print(f"⚠️ Erro ao enviar mensagem para Telegram: {e}")
        return None



def init_db():
    db_path = os.path.abspath("oportunidades.db")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS oportunidades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bookmaker TEXT,
            match TEXT,
            bet_type TEXT,
            odd REAL,
            mean_odd REAL,
            overvalue REAL,
            link TEXT,
            timestamp TEXT
        )
    """)
    conn.commit()
    conn.close()
    print("✅ Tabela 'oportunidades' inicializada em", db_path)

def save_opportunity(row):
    db_path = os.path.abspath("oportunidades.db")
    print("📂 A gravar na BD:", db_path)   # <-- debug
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO oportunidades (bookmaker, match, bet_type, odd, mean_odd, overvalue, link, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        row['bookmaker_opportunity'],
        row['team'],
        row['bet_type'],
        float(row['odd']),
        float(row['mean_odd']),
        float(row['overvalue']),
        row['link'],
        datetime.now().isoformat()
    ))
    conn.commit()
    conn.close()

# ---------- FUZZY MERGE ----------
def fuzzy_merge_with_deviation(df_bwin, df_betclic, df_solverde, df_placard, threshold=70):
    dfs = {
        'bwin': df_bwin,
        'betclic': df_betclic,
        'solverde': df_solverde,
        'placard': df_placard
    }

    for bk, df in dfs.items():
        if not df.empty:
            df = df.copy()
            df.loc[:, 'match_teams_' + bk] = df['casa'] + ' vs ' + df['fora']
            df.loc[:, 'bookmaker'] = bk
            dfs[bk] = df

    combined_df = pd.concat([df for df in dfs.values() if not df.empty], ignore_index=True)
    used_indices = set()
    clusters = []

    for idx, row in combined_df.iterrows():
        if idx in used_indices:
            continue
        cluster_indices = {idx}
        used_indices.add(idx)
        for jdx, other in combined_df.iterrows():
            if jdx in used_indices:
                continue
            score = fuzz.ratio(
                row['match_teams_' + row['bookmaker']],
                other['match_teams_' + other['bookmaker']]
            )
            if score >= threshold:
                cluster_indices.add(jdx)
                used_indices.add(jdx)
        clusters.append(list(cluster_indices))

    merged_rows = []
    for cluster in clusters:
        cluster_df = combined_df.loc[cluster].reset_index(drop=True)
        merged_row = {}
        merged_row['mean_odd_casa'] = cluster_df['odd_casa'].astype(float).mean()
        merged_row['mean_odd_empate'] = cluster_df['odd_empate'].astype(float).mean()
        merged_row['mean_odd_fora'] = cluster_df['odd_fora'].astype(float).mean()

        for bk in dfs.keys():
            match = cluster_df[cluster_df['bookmaker'] == bk]
            if not match.empty:
                match = match.iloc[0]
                merged_row[f'{bk}_team'] = match['match_teams_' + bk]
                home = float(match['odd_casa'])
                draw = float(match['odd_empate'])
                away = float(match['odd_fora'])
            else:
                merged_row[f'{bk}_team'] = ''
                home = draw = away = np.nan

            merged_row[f'{bk}_odd_casa'] = home
            merged_row[f'{bk}_odd_empate'] = draw
            merged_row[f'{bk}_odd_fora'] = away
            merged_row[f'{bk}_dev_casa'] = abs(home - merged_row['mean_odd_casa']) if pd.notna(home) else np.nan
            merged_row[f'{bk}_dev_empate'] = abs(draw - merged_row['mean_odd_empate']) if pd.notna(draw) else np.nan
            merged_row[f'{bk}_dev_fora'] = abs(away - merged_row['mean_odd_fora']) if pd.notna(away) else np.nan

        merged_rows.append(merged_row)

    return pd.DataFrame(merged_rows)

def find_opportunities_all(result_df):
    records = []
    bookmakers = ["bwin", "betclic", "placard", "solverde"]
    bookmarker_links = {
        "bwin": "https://www.bwin.pt/pt/sports/ao-vivo/apostar",
        "betclic": "https://www.betclic.pt/live",
        "placard": "https://www.placard.pt/apostas/inplay/soccer",
        "solverde": "https://www.solverde.pt/apostas/inplay/soccer"
    }

    # Buckets: (max_odd, overvalue_threshold)
    overvalue_buckets = [
        (2, 1),
        (5, 2),
        (10, 3),
        (20, 4),
        (float("inf"), 80)
    ]

    def get_overvalue_threshold(odd, buckets):
        for max_odd, threshold in buckets:
            if odd < max_odd:
                return threshold
        return buckets[-1][1]

    for _, row in result_df.iterrows():
        for bk in bookmakers:
            team = row.get(f"{bk}_team")
            if not team:
                continue

            odd_casa = row.get(f"{bk}_odd_casa")
            odd_empate = row.get(f"{bk}_odd_empate")
            odd_fora = row.get(f"{bk}_odd_fora")
            link = bookmarker_links.get(bk, None)

            # Home
            if pd.notna(odd_casa):
                overvalue = ((odd_casa - row["mean_odd_casa"]) / row["mean_odd_casa"]) * 100
                threshold_here = get_overvalue_threshold(odd_casa, overvalue_buckets)
                if overvalue > threshold_here:
                    records.append({
                        "bookmaker_opportunity": bk,
                        "team": team,
                        "bet_type": "Home",
                        "odd": odd_casa,
                        "mean_odd": row["mean_odd_casa"],
                        "overvalue": overvalue,
                        "link": link
                    })

            # Draw
            if pd.notna(odd_empate):
                overvalue = ((odd_empate - row["mean_odd_empate"]) / row["mean_odd_empate"]) * 100
                threshold_here = get_overvalue_threshold(odd_empate, overvalue_buckets)
                if overvalue > threshold_here:
                    records.append({
                        "bookmaker_opportunity": bk,
                        "team": team,
                        "bet_type": "Draw",
                        "odd": odd_empate,
                        "mean_odd": row["mean_odd_empate"],
                        "overvalue": overvalue,
                        "link": link
                    })

            # Away
            if pd.notna(odd_fora):
                overvalue = ((odd_fora - row["mean_odd_fora"]) / row["mean_odd_fora"]) * 100
                threshold_here = get_overvalue_threshold(odd_fora, overvalue_buckets)
                if overvalue > threshold_here:
                    records.append({
                        "bookmaker_opportunity": bk,
                        "team": team,
                        "bet_type": "Away",
                        "odd": odd_fora,
                        "mean_odd": row["mean_odd_fora"],
                        "overvalue": overvalue,
                        "link": link
                    })

    return pd.DataFrame(records)


# ---------------- SCRAPERS ---------------- #

# BWIN
async def init_bwin(p):
    browser = await p.chromium.launch(headless=False)
    page = await browser.new_page()
    await page.goto("https://www.bwin.pt/pt/sports/ao-vivo/apostar")
    await asyncio.sleep(3)
    try:
        await page.locator("#onetrust-accept-btn-handler").click()
    except:
        pass
    await asyncio.sleep(2)
    return browser, page

async def scrape_bwin_once(page, reload_interval=200):
    global start_time_bwin
        
    if time.time() - start_time_bwin >= reload_interval:
        print("🔄 Reload Bwin")
        await page.reload(wait_until="domcontentloaded")
        await asyncio.sleep(2)
        start_time_bwin = time.time()  # atualiza o global

    events = await page.locator("div.grid-event-wrapper").all()
    matches = []
    for event in events:
        teams = [t.strip() for t in await event.locator("div.participants-pair-game div.participant.ng-star-inserted").all_inner_texts() if t.strip()]
        if len(teams) != 2:
            continue
        odds_elements = await event.locator("ms-option-group.grid-option-group:nth-of-type(1) .option-value").all_inner_texts()
        odds = [o.strip().replace(",", ".") for o in odds_elements if o.strip()]
        if len(odds) != 3:
            continue
        link = await event.locator("a.grid-info-wrapper").get_attribute("href")
        full_link = f"https://www.bwin.pt{link}"
        matches.append([teams[0], odds[0], "Empate", odds[1], teams[1], odds[2], full_link])
    if not matches:
        return pd.DataFrame()
    df = pd.DataFrame(matches, columns=['casa', 'odd_casa', 'empate', 'odd_empate', 'fora', 'odd_fora', 'link_bwin'])
    df[['odd_casa', 'odd_empate', 'odd_fora']] = df[['odd_casa', 'odd_empate', 'odd_fora']].astype(float)
    df['casa_de_apostas'] = 'bwin'
    df['desporto'] = 'futebol'
    return df[['casa_de_apostas','desporto','casa','odd_casa','empate','odd_empate','fora','odd_fora','link_bwin']]

# BETCLIC
async def init_betclic(p):
    browser = await p.chromium.launch(headless=False)
    page = await browser.new_page()
    await page.goto("https://www.betclic.pt/live")
    try:
        await page.locator("#popin_tc_privacy_button_2").click()
    except:
        pass
    await asyncio.sleep(2)
    await page.locator("//span[@class='filters_label' and normalize-space()='Futebol']").click()
    await asyncio.sleep(2)
    return browser, page

async def scrape_betclic_once(page, reload_interval=200):
    global start_time_betclic
    if time.time() - start_time_betclic >= reload_interval:
        print("🔄 Reload Betclic")
        await page.reload(wait_until="domcontentloaded")
        await asyncio.sleep(2)
        start_time_betclic = time.time()
    await page.evaluate("window.scrollBy(0, 1)")
    await page.evaluate("window.scrollBy(0, -1)")
    base_url = "https://www.betclic.pt"
    wrappers = await page.locator("div.btnWrapper.is-inline").all()
    matches1, links = [], []
    for w in wrappers:
        buttons = await w.locator("button.btn.is-odd.is-large span.btn_label").all_inner_texts()
        if len(buttons) < 6:
            continue
        buttons_cleaned = [x.replace("\n", "").strip() for x in buttons]
        matches1.extend(buttons_cleaned)
        link = await w.locator("xpath=ancestor::a").first.get_attribute("href")
        links.append(f"{base_url}{link}" if link else np.nan)
    matches = [matches1[i:i+6] for i in range(0, len(matches1), 6)]
    if not matches:
        return pd.DataFrame()
    df = pd.DataFrame(matches, columns=['casa','odd_casa','empate','odd_empate','fora','odd_fora'])
    df['odd_casa'] = df['odd_casa'].str.replace(",", ".").replace("-", np.nan)
    df['odd_empate'] = df['odd_empate'].str.replace(",", ".").replace("-", np.nan)
    df['odd_fora'] = df['odd_fora'].str.replace(",", ".").replace("-", np.nan)
    df['casa_de_apostas'] = 'betclic'
    df['desporto'] = 'futebol'
    df['link_betclic'] = links if len(links) == len(df) else np.nan
    return df[['casa_de_apostas','desporto','casa','odd_casa','empate','odd_empate','fora','odd_fora','link_betclic']]

# SOLVERDE
async def init_solverde(p):
    browser = await p.chromium.launch(headless=False)
    page = await browser.new_page()
    await page.goto("https://www.solverde.pt/apostas/inplay/soccer")
    await asyncio.sleep(3)
    return browser, page

async def scrape_solverde_once(page, reload_interval=200):
    global start_time_solverde
    if time.time() - start_time_solverde >= reload_interval:
        print("🔄 Reload Solverde")
        await page.reload(wait_until="domcontentloaded")
        await asyncio.sleep(2)
        start_time_solverde = time.time()

    events = await page.locator("div.ta-EventListItem").all()
    rows = []
    for ev in events:
        teams = await ev.locator("div.ta-participantName").all_inner_texts()
        if len(teams) < 2:
            continue
        home, away = [t.strip() for t in teams[:2]]
        prices = await ev.locator("div.ta-price_text").all_inner_texts()
        prices = [p.strip().replace(",", ".") for p in prices if p.strip()]
        if len(prices) < 3:
            continue
        odd_home, odd_draw, odd_away = prices[:3]
        rows.append({
            "casa_de_apostas": "solverde",
            "desporto": "futebol",
            "casa": home,
            "odd_casa": odd_home,
            "empate": "X",
            "odd_empate": odd_draw,
            "fora": away,
            "odd_fora": odd_away,
            "link_solverde": np.nan
        })
    return pd.DataFrame(rows)

# PLACARD
async def init_placard(p):
    browser = await p.chromium.launch(headless=False)
    page = await browser.new_page()
    await page.goto("https://www.placard.pt/apostas/inplay/soccer")
    await asyncio.sleep(3)
    return browser, page

async def scrape_placard_once(page, reload_interval=200):
    global start_time_placard
    if time.time() - start_time_placard >= reload_interval:
        print("🔄 Reload Placard")
        await page.reload(wait_until="domcontentloaded")
        await asyncio.sleep(2)
        start_time_placard = time.time()

    events = await page.locator("div.ta-EventListItem").all()
    rows = []
    for ev in events:
        teams = await ev.locator("div.ta-participantName").all_inner_texts()
        if len(teams) < 2:
            continue
        home, away = [t.strip() for t in teams[:2]]
        prices = await ev.locator("div.ta-price_text").all_inner_texts()
        prices = [p.strip().replace(",", ".") for p in prices if p.strip()]
        if len(prices) < 3:
            continue
        odd_home, odd_draw, odd_away = prices[:3]
        rows.append({
            "casa_de_apostas": "placard",
            "desporto": "futebol",
            "casa": home,
            "odd_casa": odd_home,
            "empate": "X",
            "odd_empate": odd_draw,
            "fora": away,
            "odd_fora": odd_away,
            "link_placard": np.nan
        })
    return pd.DataFrame(rows)


# ---------------- HELPERS ----------------
async def init_all(p):
    bwin_browser, bwin_page = await init_bwin(p)
    betclic_browser, betclic_page = await init_betclic(p)
    solverde_browser, solverde_page = await init_solverde(p)
    placard_browser, placard_page = await init_placard(p)
    return bwin_browser, bwin_page, betclic_browser, betclic_page, solverde_browser, solverde_page, placard_browser, placard_page

async def close_all(*browsers):
    for b in browsers:
        try:
            await b.close()
        except Exception as e:
            print(f"⚠️ erro ao fechar browser: {e}")


# ---------- MAIN ----------
async def main():
    

    async with async_playwright() as p:
        bwin_browser, bwin_page, betclic_browser, betclic_page, solverde_browser, solverde_page, placard_browser, placard_page = await init_all(p)

        try:
            while True:
                try:
                    df_bwin, df_betclic, df_solverde, df_placard = await asyncio.gather(
                        scrape_bwin_once(bwin_page),
                        scrape_betclic_once(betclic_page),
                        scrape_solverde_once(solverde_page),
                        scrape_placard_once(placard_page)
                    )

                    print(f"{datetime.now():%H:%M:%S} -> Bwin:{len(df_bwin)} "
                        f"Betclic:{len(df_betclic)} Solverde:{len(df_solverde)} Placard:{len(df_placard)}")
                    print(df_bwin)
                    print(df_betclic)
                    print(df_solverde)
                    print(df_placard)


                    if all(df.empty for df in (df_bwin, df_betclic, df_solverde, df_placard)):
                        print("⚠️ Nenhum mercado disponível")
                        await asyncio.sleep(5)
                        continue

                    df_matches = fuzzy_merge_with_deviation(df_bwin, df_betclic, df_solverde, df_placard, threshold=70)

                    if df_matches.empty:
                        print("⚠️ Merge vazio")
                        await asyncio.sleep(5)
                        continue

                    df_opportunities = find_opportunities_all(df_matches)

                    if not df_opportunities.empty:
                        df_opportunities = df_opportunities[df_opportunities['mean_odd'] < 15]

                    if not df_opportunities.empty:
                        for _, row in df_opportunities.iterrows():
                            mensagem = (
                                "-------------------------------------------------------------\n"
                                f"{'Bookmaker: ':<12}{row['bookmaker_opportunity'].upper()}\n"
                                f"{'Match: ':<12}{row['team']}\n"
                                f"{'Bet Type: ':<12}{row['bet_type']}\n"
                                f"{'Odd: ':<12}{row['odd']:.2f}\n"
                                f"{'Mean: ':<12}{row['mean_odd']:.2f}\n"
                                f"{'Overvalue: ':<12}{row['overvalue']:.2f}\n"
                                f"{'Link: ':<12}{row['link']}\n"
                                "-------------------------------------------------------------"
                            )
                            print(mensagem)
                            save_opportunity(row)
                    else:
                        print("❌ odds parecidas")

                    await asyncio.sleep(5)

                except Exception as e:
                    # Se qualquer scrape falhar (timeout, reload, etc) cai aqui
                    print(f"⚠️ Falha no loop principal: {e}\n🔄 Reiniciar browsers e páginas...")

                    # Fecha tudo
                    await close_all(bwin_browser, betclic_browser, solverde_browser, placard_browser)

                    # Re-inicia tudo
                    (bwin_browser, bwin_page,
                    betclic_browser, betclic_page,
                    solverde_browser, solverde_page,
                    placard_browser, placard_page) = await init_all(p)

                    # volta ao topo do while True (continua o scraping)
                    continue

        except KeyboardInterrupt:
            print("🛑 Fechar browsers")
        finally:
            await bwin_browser.close()
            await betclic_browser.close()
            await solverde_browser.close()
            await placard_browser.close()


if __name__ == "__main__":
    init_db()
    asyncio.run(main())
