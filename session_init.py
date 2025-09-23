from camoufox.sync_api import Camoufox

# User-Agent de un móvil Android con Chrome
MOBILE_USER_AGENT = (
    "Mozilla/5.0 (Linux; Android 10; SM-G970F) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Mobile Safari/537.36"
)

with Camoufox() as browser:
    context = browser.new_context(
        user_agent=MOBILE_USER_AGENT,
        viewport={"width": 375, "height": 812},  # Tamaño típico de pantalla móvil
        device_scale_factor=3,
        has_touch=True,
    )

    page = context.new_page()
    page.goto("https://www.instagram.com/accounts/login/")
    input("🔐 Please log in manually, then press ENTER...")

    # Guarda el estado de sesión
    context.storage_state(path="ig_session.json")
