import argparse
import csv
import time
from urllib.parse import urlparse

from camoufox.sync_api import Camoufox

try:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
except ImportError:  # Fallback if playwright cannot be imported directly
    PlaywrightTimeoutError = Exception  # type: ignore


DEFAULT_WINDOW = (1200, 700)
DEFAULT_VIEWPORT = {"width": 1200, "height": 700}


def read_profiles(csv_path: str) -> list[str]:
    profiles: list[str] = []
    with open(csv_path, newline="") as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if not row:
                continue
            url = row[0].strip()
            if url:
                profiles.append(url.rstrip("/"))
    print(profiles)
    return profiles


def extract_username(profile_url: str) -> str:
    parsed = urlparse(profile_url)
    username = parsed.path.strip("/").split("/")[0]
    if not username:
        raise ValueError(f"Cannot determine username from URL: {profile_url}")
    return username


def followers_are_visible(page, username: str) -> tuple[bool, str]:
    followers_selectors = [
        f'a[href="/{username}/followers/"]',
        f'a[href="/{username}/followers"]',
    ]

    target_selector = None
    for selector in followers_selectors:
        try:
            page.wait_for_selector(selector, timeout=600)
            target_selector = selector
            break
        except PlaywrightTimeoutError:
            continue

    if not target_selector:
        return False, "followers link not found"

    try:
        page.click(target_selector)
    except Exception as exc:  # pragma: no cover
        return False, f"failed to click followers link: {exc}"  # noqa: TRY401

    try:
        page.wait_for_selector('div[role="dialog"]', timeout=600)
        return True, ""
    except PlaywrightTimeoutError:
        return False, "followers dialog not visible"
    finally:
        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(250)
        except Exception:
            pass


def process_profile(context, profile_url: str) -> dict[str, object]:
    username = extract_username(profile_url)
    page = context.new_page()
    try:
        page.goto(f"https://www.instagram.com/{username}/", wait_until="domcontentloaded")
        time.sleep(2)
        visible, reason = followers_are_visible(page, username)
    except PlaywrightTimeoutError:
        visible, reason = False, "profile load timeout"
    except Exception as exc:  # pragma: no cover
        visible, reason = False, f"unexpected error: {exc}"  # noqa: TRY401
    finally:
        page.close()

    return {
        "profile": profile_url,
        "username": username,
        "followers_visible": visible,
        "details": reason,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Check if Instagram followers are visible for profiles listed in a CSV.")
    parser.add_argument("--input-csv", default="input-prueba.csv", help="Path to input CSV with profile URLs")
    parser.add_argument("--output-csv", default="output-prueba.csv", help="Path to write results CSV")
    parser.add_argument("--storage-state", default="ig_session.json", help="Camoufox storage state file")
    parser.add_argument("--headless", action="store_true", help="Run the browser in headless mode")
    args = parser.parse_args()

    profiles = read_profiles(args.input_csv)
    if not profiles:
        print("No profiles found in the input CSV.")
        return

    results: list[dict[str, object]] = []

    with Camoufox(window=DEFAULT_WINDOW, headless=args.headless) as browser:
        context = browser.new_context(
            viewport=DEFAULT_VIEWPORT,
            device_scale_factor=3,
            storage_state=args.storage_state,
            has_touch=True,
        )

        for profile_url in profiles:
            print(f"🔍 Checking followers visibility for {profile_url}")
            result = process_profile(context, profile_url)
            if result["followers_visible"]:
                print("   ✅ Followers visible")
            else:
                details = result["details"]
                extra = f" ({details})" if details else ""
                print(f"   ❌ Followers not visible{extra}")
            results.append(result)

    if args.output_csv:
        with open(args.output_csv, "w", newline="") as outfile:
            fieldnames = ["profile", "username", "followers_visible", "details"]
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)


if __name__ == "__main__":
    main()

