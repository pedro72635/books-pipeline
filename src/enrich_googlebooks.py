"""
Enriquecimiento con Google Books API -> landing/googlebooks_books.csv

Notas:
- Lee los libros desde landing/goodreads_books.json generado por el scraper.
- Reintentos: 5 intentos por libro ante errores de conexión o respuesta, con 5s entre cada intento.
- CSV UTF-8 con los campos completos de Google Books + query_used.
"""

import json, time, requests, os, csv
from pathlib import Path
from urllib.parse import quote_plus
from tqdm import tqdm

# Directorios base para encontrar los archivos de entrada y salida
BASE_DIR = Path(__file__).resolve().parent.parent
landing = BASE_DIR / 'landing'
GOODREADS_JSON = landing / 'goodreads_books.json'
OUT_CSV = landing / 'googlebooks_books.csv'

# Configuración cargada desde las variables de entorno (.env)
API_KEY = os.getenv('GOOGLE_BOOKS_API_KEY', '').strip()
RATE_LIMIT = float(os.getenv('RATE_LIMIT_SECONDS', '0.8'))
HEADERS = {'User-Agent': os.getenv('USER_AGENT', 'books-pipeline-bot/1.0')}

# -------------------------------------------------------
# Helpers
# -------------------------------------------------------

def build_url(query):
    url = f"https://www.googleapis.com/books/v1/volumes?q={quote_plus(query)}"
    if API_KEY:
        url += f"&key={API_KEY}"
    return url

def request_google_books(url, intentos=5, espera=5):
    for intento in range(1, intentos + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"[ADVERTENCIA] Error intento {intento}/{intentos}: {e}")
            if intento < intentos:
                time.sleep(espera)
    print(f"[ERROR] No se pudo obtener información tras {intentos} intentos")
    return None

def pick_best_item(js, title=None, author=None):
    if not js or 'items' not in js or len(js['items']) == 0:
        return None
    items = js['items']
    # Match título+autor
    if title and author:
        for it in items:
            vol = it.get('volumeInfo', {})
            t = (vol.get('title') or "").lower()
            a = ";".join(vol.get('authors', [])).lower()
            if title.lower() in t and author.lower() in a:
                return it
    # Match título
    if title:
        for it in items:
            t = (it.get('volumeInfo', {}).get('title') or "").lower()
            if title.lower() in t:
                return it
    # Match autor
    if author:
        for it in items:
            a = ";".join(it.get('volumeInfo', {}).get('authors', [])).lower()
            if author.lower() in a:
                return it
    return items[0]

def parse_volume(item):
    vol = item.get('volumeInfo', {})
    sale = item.get('saleInfo', {})
    identifiers = vol.get('industryIdentifiers', [])

    isbn10 = None
    isbn13 = None
    for idd in identifiers:
        if idd.get('type') == 'ISBN_13':
            isbn13 = idd.get('identifier')
        elif idd.get('type') == 'ISBN_10':
            isbn10 = idd.get('identifier')
    if not isbn13 and not isbn10:
        isbn13 = "NO_ISBN_GOOGLE_API"

    authors = ';'.join(vol.get('authors', []) or [])
    categories = ';'.join(vol.get('categories', []) or [])

    price_amount = None
    price_currency = None
    if sale.get('listPrice'):
        pp = sale['listPrice']
        price_amount = pp.get('amount')
        price_currency = pp.get('currencyCode')
    elif sale.get('retailPrice'):
        pp = sale['retailPrice']
        price_amount = pp.get('amount')
        price_currency = pp.get('currencyCode')

    return {
        'gb_id': item.get('id'),
        'title': vol.get('title'),
        'subtitle': vol.get('subtitle'),
        'authors': authors,
        'publisher': vol.get('publisher'),
        'pub_date': vol.get('publishedDate'),
        'language': vol.get('language'),
        'categories': categories,
        'isbn13': isbn13,
        'isbn10': isbn10,
        'price_amount': price_amount,
        'price_currency': price_currency
    }

# -------------------------------------------------------
# MAIN
# -------------------------------------------------------

def main():
    if not GOODREADS_JSON.exists():
        raise SystemExit(f"[ERROR] No se encontró {GOODREADS_JSON}. Ejecuta primero el scraper de Goodreads.")

    data = json.load(open(GOODREADS_JSON, 'r', encoding='utf-8'))
    books = data.get('data', [])
    rows = []

    for b in tqdm(books, desc="Enriqueciendo con Google Books"):
        title = b.get('title', '')
        author = b.get('author', '')
        isbn_scraper = b.get('isbn13') or b.get('isbn10')

        result = None
        url_api_utilizada = None

        # Buscar por ISBN
        if isbn_scraper:
            url = build_url(f"isbn:{isbn_scraper}")
            js = request_google_books(url)
            if js:
                item = pick_best_item(js, title, author)
                if item:
                    result = parse_volume(item)
                    url_api_utilizada = url

        # Buscar por título+autor
        if not result and title and author:
            url = build_url(f'intitle:"{title}"+inauthor:"{author}"')
            js = request_google_books(url)
            if js:
                item = pick_best_item(js, title, author)
                if item:
                    result = parse_volume(item)
                    url_api_utilizada = url

        # Fallback: solo título
        if not result and title:
            url = build_url(f'intitle:"{title}"')
            js = request_google_books(url)
            if js:
                item = pick_best_item(js, title, None)
                if item:
                    result = parse_volume(item)
                    url_api_utilizada = url

        if not result:
            result = {
                'gb_id': None,
                'title': None,
                'subtitle': None,
                'authors': None,
                'publisher': None,
                'pub_date': None,
                'language': None,
                'categories': None,
                'isbn13': "NO_ISBN_GOOGLE_API",
                'isbn10': None,
                'price_amount': None,
                'price_currency': None
            }

        # Guardar solo los campos de Google Books + query utilizada
        row = result.copy()
        row['query_used'] = url_api_utilizada
        rows.append(row)

        time.sleep(RATE_LIMIT)

    # Guardar CSV
    fieldnames = [
        'gb_id','title','subtitle','authors','publisher','pub_date',
        'language','categories','isbn13','isbn10','price_amount','price_currency','query_used'
    ]

    with open(OUT_CSV, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[OK] Archivo generado: {OUT_CSV} ({len(rows)} filas).")

if __name__ == '__main__':
    main()
