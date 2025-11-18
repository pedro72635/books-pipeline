import os
import time
import json
import re
from pathlib import Path
from dotenv import load_dotenv
from urllib.parse import urljoin, quote_plus
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ================================
# CARGAR VARIABLES DE ENTORNO
# ================================
load_dotenv()
SEARCH_QUERY = os.getenv('SEARCH_QUERY', 'data science')
MAX_BOOKS = int(os.getenv('MAX_BOOKS', '15'))
RATE_LIMIT = float(os.getenv('RATE_LIMIT_SECONDS', '0.8'))
USER_AGENT = os.getenv('USER_AGENT', 'books-pipeline-bot/1.0 (+https://example.com)')

# ===========================================
# DIRECTORIOS DEL PROYECTO
# ===========================================
BASE_DIR = Path(__file__).resolve().parent.parent
landing = BASE_DIR / 'landing'
landing.mkdir(exist_ok=True)
OUTPUT_FILE = landing / 'goodreads_books.json'

# ===============================
# CONFIGURACIÓN DE SELENIUM
# ===============================
chrome_options = Options()
chrome_options.add_argument(f"user-agent={USER_AGENT}")
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
driver = webdriver.Chrome(options=chrome_options)

# ===============================
# PARSE DE RATING
# ===============================
def parse_rating_and_count(minirating_text):
    if not minirating_text:
        return None, None
    text = minirating_text.strip()
    m = re.search(r'([0-9]\.?[0-9]*)\s+avg rating', text)
    rating = float(m.group(1)) if m else None
    m2 = re.search(r'—\s*([\d,\.]+)\s*ratings', text)
    ratings_count = int(m2.group(1).replace(',', '').replace('.', '')) if m2 else None
    return rating, ratings_count

# =====================================
# EXTRACCIÓN PRECISA DEL ISBN
# =====================================
def extract_isbn_from_page():
    """
    Extrae ISBN-13 e ISBN-10 de la página de Goodreads.
    Primero intenta desde los divs específicos, luego usa pattern global si falla.
    """
    isbn10, isbn13 = None, None
    source10, source13 = None, None

    # Intentar abrir el detalle si hay botón
    try:
        btn = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button[aria-label*='Book details']"))
        )
        driver.execute_script("arguments[0].click();", btn)
        time.sleep(1.5)
    except Exception as e:
        print("No se pudo activar el botón:", e)

    # Esperar que carguen los divs
    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.TruncatedContent__text"))
        )
    except Exception as e:
        print("No se cargaron los divs de ISBN:", e)

    # Buscar los divs que contienen los ISBN
    try:
        divs = driver.find_elements(By.CSS_SELECTOR, "div.TruncatedContent__text")
        for div in divs:
            # ISBN-13: primer número de 13 dígitos en el texto principal
            full_text = div.text
            match_13 = re.search(r'\b\d{13}\b', full_text)
            if match_13:
                isbn13 = match_13.group()
                source13 = "div texto principal"

            # ISBN-10: dentro del span
            try:
                span = div.find_element(By.TAG_NAME, "span")
                match_10 = re.search(r'ISBN10:\s*([\dXx]{10})', span.text)
                if match_10:
                    isbn10 = match_10.group(1)
                    source10 = "span"
            except:
                pass

            # Si ambos encontrados, salir
            if isbn10 and isbn13:
                break

    except Exception as e:
        print("Error extrayendo ISBN desde div:", e)

    # Pattern global solo si no se encontró en los divs
    src = driver.page_source
    if not isbn13:
        m13 = re.search(r'ISBN(?:-13)?:?\s*([0-9\-]{13,17})', src, re.IGNORECASE)
        if m13:
            isbn13 = m13.group(1).replace("-", "").strip()
            source13 = "pattern_global"
    if not isbn10:
        m10 = re.search(r'ISBN(?:-10)?:?\s*([0-9Xx\-]{10,17})', src, re.IGNORECASE)
        if m10:
            isbn10 = m10.group(1).replace("-", "").strip()
            source10 = "pattern_global"

    return isbn10, isbn13


# ================================
# MAIN SCRAPER
# ================================
def main():
    books = []
    page = 1
    pbar = tqdm(total=MAX_BOOKS, desc="Libros extraídos", unit="libro", miniters=1)

    while len(books) < MAX_BOOKS:
        search_url = f"https://www.goodreads.com/search?q={quote_plus(SEARCH_QUERY)}&page={page}"
        driver.get(search_url)

        try:
            WebDriverWait(driver, 12).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.bookTitle"))
            )
        except:
            print("No se cargaron resultados.")
            break

        # ============================
        # BUSCAR ENLACES DE LIBROS
        # ============================
        book_links = driver.find_elements(By.CSS_SELECTOR, "a.bookTitle")
        author_links = driver.find_elements(By.CSS_SELECTOR, "a.authorName")
        ratings = driver.find_elements(By.CSS_SELECTOR, "span.minirating")

        for i in range(min(len(book_links), len(author_links), len(ratings))):
            book_url = urljoin(
                "https://www.goodreads.com",
                book_links[i].get_attribute("href").split("?")[0]
            )
            books.append({
                "title": book_links[i].text.strip(),
                "author": author_links[i].text.strip(),
                "rating": parse_rating_and_count(ratings[i].text)[0],
                "ratings_count": parse_rating_and_count(ratings[i].text)[1],
                "book_url": book_url
            })

            if len(books) >= MAX_BOOKS:
                break

        # ============================
        # EXTRAER DETALLE DE CADA LIBRO
        # ============================
        for book in books:
            if "isbn10" in book:
                continue  # ya procesado
            driver.get(book["book_url"])
            time.sleep(RATE_LIMIT)

            try:
                title_el = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, 'h1[data-testid="bookTitle"]')
                    )
                )
                title = title_el.text.strip()
                if title:
                    book["title"] = title
            except:
                pass

            isbn10, isbn13 = extract_isbn_from_page()
            book.update({
                "isbn10": isbn10,
                "isbn13": isbn13,
                "scrape_source": "goodreads",
                "scrape_date": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
            })

            time.sleep(RATE_LIMIT)

        page += 1
        pbar.update(len(books))

    pbar.close()

    # ================================
    # GUARDAR JSON FINAL
    # ================================
    metadata = {
        "source_urls": [f"https://www.goodreads.com/search?q={SEARCH_QUERY.replace(' ', '+')}"],
        "selectors": {
            "search_title": "a.bookTitle",
            "search_author": "a.authorName",
            "search_rating": "span.minirating",
            "book_title_detail": "h1[data-testid='bookTitle']",
            "isbn_table": "div.CollapsableList td"
        },
        "user_agent": USER_AGENT,
        "query": SEARCH_QUERY,
        "scrape_date": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        "records_extracted": len(books),
        "rate_limit_seconds": RATE_LIMIT
    }

    payload = {"metadata": metadata, "data": books}

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"[OK] Guardado {OUTPUT_FILE} con {len(books)} registros.")
    driver.quit()


if __name__ == "__main__":
    main()
