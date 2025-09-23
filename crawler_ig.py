import time
import argparse
import pandas as pd
from camoufox.sync_api import Camoufox
from bs4 import BeautifulSoup
import re

def scroll_until_end(page, delay=1.5):
    # Espera al modal de followers
    page.wait_for_selector('div[role="dialog"]', timeout=10000)
    prev_height = 0
    while True:
        # Ejecuta scroll dentro del modal
        current_height = page.evaluate("""() => {
            const modal = document.querySelector('div[role="dialog"]');
            if (!modal) return 0;
            return modal.scrollHeight || modal.offsetHeight;
        }""")

        # Ejecuta scroll dentro del modal
        page.locator('a > div > div > span[dir="auto"]').last.scroll_into_view_if_needed()
        page.wait_for_timeout(2 * 1000)
        page.locator('a > div > div > span[dir="auto"]').last.scroll_into_view_if_needed()
        page.wait_for_timeout(2 * 1000)
        page.locator('a > div > div > span[dir="auto"]').last.scroll_into_view_if_needed()
        page.wait_for_timeout(2 * 1000)
        if current_height == prev_height:
            break
        prev_height = current_height



def extract_following(page):
    # Localiza todos los enlaces a perfiles dentro de la lista de followers
    html = page.content()  # o page.inner_html("body") si quieres solo el contenido del body
    soup = BeautifulSoup(html, "html.parser")

    # Buscar el modal
    modal = soup.find("div", {"role": "dialog"})

    # Verificar que el modal existe
    if modal:
        # Buscar todos los enlaces con role="link" dentro del modal
        links = modal.find_all("a", {"role": "link"})

        # Obtener los hrefs
        hrefs = [link.get("href") for link in links if link.get("href")]
        print(hrefs)
    else:
        print("No se encontró el modal.")

    # Filtra y normaliza las URLs
    profiles = [h.strip('/') for h in hrefs if h and h.startswith('/')]
    return list(set(profiles))## Elimina duplicados

def get_following_count(page):
    html = page.content()  # o page.inner_html("body") si quieres solo el contenido del body
    soup = BeautifulSoup(html, "html.parser")

    # Buscar el modal
    following = soup.find("a", href=re.compile('/brilliantmaps/following/'))
    print(following)
    following_count = following.find("span", class_=re.compile('html-span'))
    following_count = following_count.get_text()
    return int(following_count)

def main(input_csv, output_csv, storage_state):
    df = pd.read_csv(input_csv,header=None,names=['profile_url'])
    results = []

    # Iniciamos Camoufox con sesión guardada
    with Camoufox(window=(1200, 700), headless=False) as browser:
        context = browser.new_context(
            viewport={"width": 1200, "height": 700},
            storage_state=storage_state,
        )

        print(df)

        for _, row in df.iterrows():
            profile = row['profile_url'].rstrip('/')
            following_url = f"{profile}/following/"
            page = context.new_page()
            page.goto(following_url)
            time.sleep(4)
            following_count = get_following_count(page)
            page.click('a[href="/brilliantmaps/following/"]')
            scroll_until_end(page)
            following = extract_following(page)
            results.append({
                'profile': profile,
                'following_count': following_count,
                'following_list': following
            })
            page.close()

    # Guardar resultados en CSV y JSON
    out_df = pd.DataFrame(results)
    out_df.to_csv(output_csv, index=False)
    out_df.to_json(output_csv.replace('.csv', '.json'), orient='records', force_ascii=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Instagram following crawler')
    parser.add_argument('--input-csv', required=True, help='CSV with column profile_url')
    parser.add_argument('--output-csv', required=True, help='Output CSV file path')
    parser.add_argument('--storage-state', default='ig_session.json', help='Camoufox storage state file')
    args = parser.parse_args()
    main(args.input_csv, args.output_csv, args.storage_state)
