import time
import json
import re
import random
import duckdb
import os
from bs4 import BeautifulSoup
from camoufox.sync_api import Camoufox
import csv
from urllib.parse import urlparse
import argparse
import asyncio
import traceback
import sys
import pandas as pd


pattern_a_n_friends = re.compile(r'\bxi81zsa\b.*\bx1s688f\b')


def has_friends_visible(friends_tab):
    try:
        selected = friends_tab.find_all("a", href=re.compile('friends'))
        if len(selected) > 1: 
            print('amigos visibles')
            return True
    except:
        print('error in has friends visible')
        traceback.print_exc()
        pass
    print('amigos NO visibles')
    return False

def load_not_visited_profiles(db_name, profile_url, auxiliary_df):
    conn = duckdb.connect(db_name)

    # Obtener amigos (name y friend) desde la tabla friendships
    friends = conn.execute("""
        SELECT DISTINCT name, friend
        FROM friendships
        WHERE profile = ?
    """, (profile_url,)).fetchall()
    conn.close()

    # Crear diccionario de name → friend
    name_to_friend = dict(friends)

    # Extraer todos los names conocidos (los que están en friendships)
    friend_names = list(name_to_friend.keys())

    # Filtrar la tabla auxiliar por los 'alter' que están en la red de amistades
    filtered_aux = auxiliary_df[auxiliary_df['alter'].isin(friend_names)].copy()

    # Asegurar que n_interactions es numérico
    filtered_aux['n_interactions'] = pd.to_numeric(filtered_aux['n_interactions'], errors='coerce').fillna(0)

    # Ordenar por interacciones y seleccionar top 50 names
    top_50 = filtered_aux.sort_values(by='n_interactions', ascending=False).head(50)
    top_50_names = top_50['alter'].tolist()

    # Mapear esos top 50 names a friends (URLs)
    top_50_friends = [(name, name_to_friend[name]) for name in top_50_names if name in name_to_friend]

    # Obtener perfiles ya visitados
    conn = duckdb.connect(db_name)
    visited = conn.execute("""
        SELECT DISTINCT profile
        FROM profile_doms
    """).fetchall()
    conn.close()
    visited = set(v[0] for v in visited)

    # Filtrar solo los friends (URLs) no visitados
    not_visited = [friend for name, friend in top_50_friends if friend not in visited]

    return not_visited

def load_profiles_from_csv(csv_path):
    profile_urls = []
    with open(csv_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if row:  # evitar filas vacías
                profile_urls.append(row[0].strip())
    return sorted(profile_urls)

def extract_profile_name(profile_url):
    parsed = urlparse(profile_url)
    full_path = f"{parsed.path.lstrip('/')}?{parsed.query}" if parsed.query else parsed.path.lstrip('/')
    return full_path

def parse_number_of_friends(text):
    s = text.split(' ')[0]
    s = s.strip().upper() 
    if s.endswith('K'):  # Handle 'K' (thousands)
        return int(float(s[:-1]) * 1000)
    return int(s)  # Default case (simple number)

def detect_graphql_error(intercepted_response):
    if 'graphql'in intercepted_response.url:
        try:
            json_text = intercepted_response.text()
            json_data = json.loads(json_text)
            if "errors" in json_data:
                sys.exit("Errors on graphql response, execution stopped from inside listener.")
        except Exception as e:
            #print(f"Failed to parse JSON: {e}")
            pass

def zoom_out_load_friends(page,n_friends):
    page.evaluate("document.body.style.zoom=0.01")
    time.sleep(n_friends/5)



def scroll_to_bottom(page, min_pause=350, max_pause=350):#2408 de 3200 con 420 segundos de espera, en otra ejecución con 720 de espera 2232, así que es variable. Esto es otra limitación
    #page.evaluate("document.body.style.zoom=0.01")
    while True:
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(random.uniform(min_pause, max_pause))
        new_height = page.evaluate("document.body.scrollHeight")
        if new_height == current_height:
            break
        current_height = new_height

def extract_friends(friends_tab):
    amigos = []
    a_els = friends_tab.find_all("a",tabindex=0)
    for element in a_els:
        try:
            href = element.get("href")
            name = element.find('span').get_text()
            if href:
                amigos.append({'url':href,'name':name})
        except:
            pass
    return amigos



def save_to_duckdb(db_name, profile_url, friends, n_friends, dom_html):
    conn = duckdb.connect(db_name)

    # Create tables if not exist
    conn.execute("""
    CREATE TABLE IF NOT EXISTS friendships (
        profile TEXT,
        friend TEXT,
        name TEXT
    )
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS profile_doms (
        profile TEXT,
        n_friends INT,
        dom TEXT
    )
    """)

    # Insert friends
    for friend in friends:
        conn.execute("INSERT INTO friendships VALUES (?, ?, ?)", (profile_url, friend['url'], friend['name'] ))

    # Insert DOM
    conn.execute("INSERT INTO profile_doms VALUES (?, ?, ?)", (profile_url, n_friends, dom_html))

    conn.close()


def visit_and_extract(profile_url, browser, db_name, session_storage_file):

    desktop_ua = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/114.0.0.0 Safari/537.36"
    )
    

    print(f"👤 Visitando perfil: {profile_url}")


    context = browser.new_context(user_agent=desktop_ua, storage_state=session_storage_file)
    page = context.new_page()

    page.on("response", detect_graphql_error)

    if "profile.php" in profile_url: page.goto(f"{profile_url}&sk=friends",wait_until="load")
    else : page.goto(f"{profile_url}/friends",wait_until="load")

    time.sleep(5)

    friends = []
    n_friends = -1

    #PRELIMINAR CHECK TO SEE IF FRIENDS ARE AVAILABLE
    soup = BeautifulSoup(page.content(), "lxml")
    friends_tab = soup.find("div", style=re.compile('--card-corner-radius'))
    dom_html = ''
    
    if has_friends_visible(friends_tab): 
        n_friends = parse_number_of_friends(soup.find("a", class_=pattern_a_n_friends).get_text())
        print(n_friends)
        zoom_out_load_friends(page,n_friends)
        soup = BeautifulSoup(page.content(), "lxml")
        friends_tab = soup.find("div", style=re.compile('--card-corner-radius'))
        friends = extract_friends(friends_tab)
        friends = [item for item in friends if profile_url not in item['url']]
        print(len(friends))
        dom_html = ''
    
    page.close()
    save_to_duckdb(db_name, profile_url, friends, n_friends, dom_html)
    return friends


def process_profiles(profile_urls,df_interactions):
    for profile_url in profile_urls:
            profile_name = extract_profile_name(profile_url)
            db_name = os.path.join(output_dir, f"{profile_name}.duckdb")
            try:
                not_visited = load_not_visited_profiles(db_name,profile_url,auxiliary_df=df_interactions)
                partial_visits = True
            except:
                not_visited = []
                partial_visits = False

            print(f"👤 Empezando perfil: {profile_name} — {len(not_visited)} por visitar")

            if partial_visits == False:
                visit_and_extract(profile_url, browser, db_name, session)
                not_visited = load_not_visited_profiles(db_name,profile_url,auxiliary_df=df_interactions)

            for friend_url in not_visited:
                visit_and_extract(friend_url, browser, db_name, session)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Facebook Friends Crawler")

    parser.add_argument("csv_path", help="Path to the CSV file with profile URLs")
    parser.add_argument("session_json", help="Path to the JSON file with Facebook session (e.g., fb_session.json)")
    parser.add_argument("interactions_csv", help="File including interactions")

    args = parser.parse_args()

    csv_path = args.csv_path
    session = args.session_json
    df_interactions = pd.read_csv(args.interactions_csv)

    # Obtener nombre base del CSV (sin extensión)
    csv_basename = os.path.splitext(os.path.basename(csv_path))[0]

    # Crear carpeta con ese nombre si no existe
    output_dir = os.path.join("outputs", csv_basename)
    os.makedirs(output_dir, exist_ok=True)

    profile_urls = load_profiles_from_csv(csv_path)

    with Camoufox(window=(850, 5000),headless=True) as browser:
        try:
            process_profiles(profile_urls,df_interactions)
        except:
            traceback.print_exc()
            time.sleep(100)

    with Camoufox(window=(850, 5000),headless=True) as browser:
        try:
            process_profiles(profile_urls,df_interactions)
        except:
            traceback.print_exc()
            time.sleep(400)

    with Camoufox(window=(850, 5000),headless=True) as browser:
        try:
            process_profiles(profile_urls,df_interactions)
        except:
            traceback.print_exc()
            time.sleep(800)

    with Camoufox(window=(850, 5000),headless=True) as browser:
        try:
            process_profiles(profile_urls,df_interactions)
        except:
            traceback.print_exc()
            sys.exit(1)

    print("✅ Crawling completado.")
