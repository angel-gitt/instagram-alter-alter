import argparse
import csv
import os
import re
import sys
import time
import traceback
from urllib.parse import urlparse

import duckdb
import pandas as pd
from bs4 import BeautifulSoup
from camoufox.sync_api import Camoufox

BASE_URL = "https://www.instagram.com"
COMPACT_NUMBER_RE = re.compile(r'^([\d.,\s]+)([KMB]?)$', re.IGNORECASE)


def parse_compact_number(value: str) -> int:
    if not value:
        return 0
    cleaned = value.strip().replace('\u202f', ' ').replace('\u00a0', ' ')
    cleaned = cleaned.upper()
    match = COMPACT_NUMBER_RE.match(cleaned)
    if not match:
        digits = re.sub(r'\D', '', cleaned)
        return int(digits) if digits else 0
    number_part, suffix = match.groups()
    number_part = number_part.replace(' ', '')
    try:
        if suffix:
            normalized_number = number_part.replace(',', '.')
            number = float(normalized_number)
        else:
            normalized_number = number_part.replace(',', '').replace('.', '')
            number = float(normalized_number) if normalized_number else 0
    except ValueError:
        digits = re.sub(r'\D', '', number_part)
        number = float(digits) if digits else 0
    multiplier = {'': 1, 'K': 1_000, 'M': 1_000_000, 'B': 1_000_000_000}.get(suffix.upper(), 1)
    return int(number * multiplier)


def normalize_profile_url(raw: str) -> str:
    if not raw:
        return ''
    raw = raw.strip()
    if not raw:
        return ''
    if raw.startswith('http'):
        url = raw
    else:
        handle = raw.lstrip('@').lstrip('/')
        url = f"{BASE_URL}/{handle}"
    url = url.split('?')[0]
    if not url.endswith('/'):
        url += '/'
    return url


def extract_profile_name(profile_url: str) -> str:
    parsed = urlparse(profile_url)
    identifier = parsed.path.strip('/') or parsed.netloc or "profile"
    if parsed.query:
        identifier = f"{identifier}_{parsed.query}"
    safe_identifier = re.sub(r"[^A-Za-z0-9._-]", "_", identifier)
    return safe_identifier or "profile"


def extract_username(profile_url: str) -> str:
    parsed = urlparse(profile_url)
    path = parsed.path.strip('/')
    return path.split('/')[0] if path else ''


def load_profiles_from_csv(csv_path: str) -> list[str]:
    profile_urls = []
    with open(csv_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if not row:
                continue
            normalized = normalize_profile_url(row[0])
            if normalized:
                profile_urls.append(normalized)
    return sorted(set(profile_urls))


def scroll_until_end(page, delay: float = 2.5, max_idle: int = 6, max_rounds: int = 250) -> None:
    page.wait_for_selector('div[role="dialog"]', timeout=10000)
    idle_rounds = 0
    prev_height = 0
    prev_count = 0
    rounds = 0
    while idle_rounds < max_idle and rounds < max_rounds:
        rounds += 1
        anchors = page.locator('div[role="dialog"] a[role="link"]')
        try:
            count_before = anchors.count()
        except Exception:
            count_before = 0
        if count_before == 0:
            break
        try:
            anchors.last.scroll_into_view_if_needed()
        except Exception:
            pass
        page.wait_for_timeout(int(delay * 1000))
        current_height = page.evaluate(
            """() => {
                const modal = document.querySelector('div[role=\"dialog\"]');
                if (!modal) return 0;
                return modal.scrollHeight || modal.offsetHeight || 0;
            }"""
        )
        try:
            count_after = anchors.count()
        except Exception:
            count_after = count_before
        if current_height == prev_height and count_after == prev_count:
            idle_rounds += 1
        else:
            idle_rounds = 0
            prev_height = current_height
            prev_count = count_after
        if rounds % 10 == 0:
            page.wait_for_timeout(int(delay * 1000))


def extract_following(page) -> tuple[list[dict], str]:
    try:
        modal_html = page.inner_html('div[role="dialog"]')
    except Exception:
        modal_html = page.content()
    soup = BeautifulSoup(modal_html, "html.parser")
    following = []
    seen = set()
    for anchor in soup.select('a[role="link"]'):
        href = anchor.get('href')
        if not href or href.startswith('#') or href.startswith('javascript'):
            continue
        normalized = normalize_profile_url(href)
        if not normalized or normalized in seen:
            continue
        parsed = urlparse(normalized)
        segments = [segment for segment in parsed.path.strip('/').split('/') if segment]
        if len(segments) != 1:
            continue
        spans = anchor.select('span[dir="auto"]')
        handle = spans[0].get_text(strip=True) if spans else ''
        name = spans[1].get_text(strip=True) if len(spans) > 1 else ''
        if not name:
            name = handle
        following.append({'url': normalized, 'name': name})
        seen.add(normalized)
    return following, modal_html


def get_following_count(page, username: str) -> int:
    soup = BeautifulSoup(page.content(), "html.parser")
    if not username:
        return 0
    href_pattern = re.compile(rf'/{re.escape(username)}/following/?', re.IGNORECASE)
    link = soup.find('a', href=href_pattern)
    if not link:
        return 0
    candidates = []
    text = link.get_text(strip=True)
    if text:
        candidates.append(text)
    for span in link.find_all('span'):
        span_text = span.get_text(strip=True)
        if span_text:
            candidates.append(span_text)
    for candidate in candidates:
        if re.search(r'\d', candidate):
            return parse_compact_number(candidate)
    return 0


def open_following_modal(page, username: str) -> bool:
    if not username:
        return False
    selector = f'a[href="/{username}/following/"]'
    try:
        page.wait_for_selector(selector, timeout=10000)
        page.click(selector)
        page.wait_for_selector('div[role="dialog"]', timeout=10000)
        time.sleep(2)
        return True
    except Exception:
        print("⚠️ Following not visible")
        return False


def save_to_duckdb(db_name: str, profile_url: str, following: list[dict], n_following: int, dom_html: str) -> None:
    conn = duckdb.connect(db_name)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS friendships (
                profile TEXT,
                friend TEXT,
                name TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS profile_doms (
                profile TEXT,
                n_friends INT,
                dom TEXT
            )
            """
        )
        for entry in following:
            conn.execute(
                "INSERT INTO friendships VALUES (?, ?, ?)",
                (profile_url, entry['url'], entry['name'])
            )
        conn.execute(
            "INSERT INTO profile_doms VALUES (?, ?, ?)",
            (profile_url, n_following, dom_html)
        )
    finally:
        conn.close()


def load_not_visited_profiles(db_name: str, profile_url: str, auxiliary_df: pd.DataFrame | None) -> list[str]:
    conn = duckdb.connect(db_name)
    try:
        friends = conn.execute(
            """
            SELECT DISTINCT name, friend
            FROM friendships
            WHERE profile = ?
            """,
            (profile_url,)
        ).fetchall()
    finally:
        conn.close()

    name_to_friend = dict(friends)
    if not name_to_friend:
        return []

    if (
        auxiliary_df is None
        or 'alter' not in auxiliary_df.columns
        or 'n_interactions' not in auxiliary_df.columns
    ):
        return sorted({friend for friend in name_to_friend.values()})

    filtered_aux = auxiliary_df[auxiliary_df['alter'].isin(name_to_friend.keys())].copy()
    filtered_aux['n_interactions'] = pd.to_numeric(filtered_aux['n_interactions'], errors='coerce').fillna(0)
    top_50 = filtered_aux.sort_values(by='n_interactions', ascending=False).head(50)
    top_friends = [name_to_friend[name] for name in top_50['alter'] if name in name_to_friend]

    conn = duckdb.connect(db_name)
    try:
        visited = conn.execute(
            """
            SELECT DISTINCT profile
            FROM profile_doms
            """
        ).fetchall()
    finally:
        conn.close()

    visited_set = {row[0] for row in visited}
    return [friend for friend in top_friends if friend not in visited_set]


def visit_and_extract(profile_url: str, browser, db_name: str, session_storage_file: str) -> list[dict]:
    desktop_ua = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/114.0.0.0 Safari/537.36"
    )
    username = extract_username(profile_url)
    print(f"👤 Visitando perfil: {profile_url}")

    context = browser.new_context(
        user_agent=desktop_ua,
        viewport={"width": 1200, "height": 900},
        storage_state=session_storage_file,
    )
    page = context.new_page()

    following: list[dict] = []
    dom_html = ''
    following_count = 0

    try:
        page.goto(profile_url, wait_until="load")
        time.sleep(5)
        following_count = get_following_count(page, username)
        modal_open = open_following_modal(page, username)
        if modal_open:
            scroll_until_end(page)
            following, dom_html = extract_following(page)
            attempts = 0
            while (
                following_count
                and len(following) < following_count
                and attempts < 3
            ):
                extra_delay = 3.0 + attempts
                scroll_until_end(page, delay=extra_delay, max_idle=10, max_rounds=400)
                updated_following, dom_html = extract_following(page)
                if len(updated_following) <= len(following):
                    attempts += 1
                else:
                    following = updated_following
                    attempts = 0
            print(f"   Seguimientos guardados: {len(following)} / declarados {following_count}")
        else:
            following = []
            dom_html = ''
    except Exception:
        traceback.print_exc()
        try:
            dom_html = page.content()
        except Exception:
            dom_html = ''
    finally:
        page.close()
        context.close()

    save_to_duckdb(db_name, profile_url, following, following_count, dom_html)
    return following


def process_profiles(
    profile_urls: list[str],
    df_interactions: pd.DataFrame | None,
    browser,
    session_storage_file: str,
    output_dir: str,
) -> None:
    for profile_url in profile_urls:
        profile_name = extract_profile_name(profile_url)
        db_name = os.path.join(output_dir, f"{profile_name}.duckdb")
        try:
            not_visited = load_not_visited_profiles(db_name, profile_url, df_interactions)
            partial_visits = True
        except Exception:
            not_visited = []
            partial_visits = False

        print(f"👤 Empezando perfil: {profile_name} — {len(not_visited)} por visitar")

        if not partial_visits:
            visit_and_extract(profile_url, browser, db_name, session_storage_file)
            try:
                not_visited = load_not_visited_profiles(db_name, profile_url, df_interactions)
            except Exception:
                not_visited = []

        for friend_url in not_visited:
            if friend_url == profile_url:
                continue
            visit_and_extract(friend_url, browser, db_name, session_storage_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Instagram following crawler")
    parser.add_argument("csv_path", help="Path to the CSV file with profile URLs")
    parser.add_argument("session_json", help="Path to the Camoufox storage state for Instagram")
    parser.add_argument(
        "--interactions-csv",
        dest="interactions_csv",
        help="Optional CSV with columns alter,n_interactions to prioritise alters",
    )
    args = parser.parse_args()

    csv_path = args.csv_path
    session = args.session_json
    df_interactions = None
    if args.interactions_csv:
        df_interactions = pd.read_csv(args.interactions_csv)

    csv_basename = os.path.splitext(os.path.basename(csv_path))[0]
    output_dir = os.path.join("outputs", csv_basename)
    os.makedirs(output_dir, exist_ok=True)

    profile_urls = load_profiles_from_csv(csv_path)

    success = False
    retry_delays = [0, 100, 400, 800]
    for idx, delay in enumerate(retry_delays):
        with Camoufox(window=(850, 5000), headless=True) as browser:
            try:
                process_profiles(profile_urls, df_interactions, browser, session, output_dir)
                success = True
                break
            except Exception:
                traceback.print_exc()
                if idx < len(retry_delays) - 1 and delay:
                    time.sleep(delay)

    if not success:
        sys.exit(1)

    print("✅ Crawling completado.")
