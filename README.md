# 📚 Proyecto Pipeline de Libros: Scraper y Enriquecimiento

## ✨ Descripción

Este proyecto permite extraer datos de libros desde **Goodreads** mediante scraping y enriquecerlos con información de la **API de Google Books**. Posteriormente, los datos se unifican, normalizan y deduplican para generar un dataset final listo para análisis o carga en un sistema.

El flujo completo consiste en:

1. 🕸️ Scraping de Goodreads para obtener títulos, autores, rating y URLs de los libros.
2. 🔍 Extracción detallada de ISBN-10 e ISBN-13 desde las páginas de Goodreads.
3. ⚡ Enriquecimiento con la API de Google Books, incluyendo precios, categorías y otros metadatos.
4. 🛠️ Integración y normalización de datos.
5. 🧹 Deduplicación y priorización de ISBN10 de Google Books.
6. 📦 Generación de artefactos finales:

   * `dim_book.parquet`: tabla unificada de libros.
   * `book_source_detail.parquet`: detalle por fuente de cada registro.
   * `quality_metrics.json`: métricas de calidad.
   * `schema.md`: documentación de esquema.

## 📝 Requisitos

* Python >= 3.10
* Google Chrome para Selenium
* Chromedriver compatible con tu versión de Chrome
* Claves de API:

  * `GOOGLE_BOOKS_API_KEY` en un archivo `.env`
* Variables opcionales:

  * `USER_AGENT` → user agent para peticiones HTTP
  * `RATE_LIMIT_SECONDS` → tiempo de espera entre peticiones (default 0.8s)
  * `SEARCH_QUERY` → término de búsqueda en Goodreads
  * `MAX_BOOKS` → máximo número de libros a extraer

Dependencias Python:

* requests 📝
* tqdm ⏳
* pandas 🐼
* numpy 🔢
* pyarrow 📊
* python-dotenv 🌿
* selenium 🤖

## ⚙️ Instalación

```bash
# Crear entorno virtual
python -m venv .venv
# Activar entorno
# Windows
.venv\Scripts\activate
# Linux / macOS
source venv/bin/activate

# Instalar dependencias
pip install -r requirements.txt

# Copiar el archivo .env
copy .env.example .env
```

## 🗂️ Estructura del proyecto

```
project_root/
│
├─ src/
│   ├─ scraper_goodreads.py         # 🕸️ Scraper de Goodreads
│   ├─ enrich_google_books.py       # ⚡ Enriquecimiento con Google Books API
│   ├─ integrate_pipeline.py        # 🛠️ Integración, limpieza y deduplicación
│   ├─ utils_quality.py             # 📊 Cálculo de métricas de calidad
│   └─ utils_isbn.py                # 🔢 Validación de ISBN13
│
├─ landing/                         # 📥 Archivos crudos
│   ├─ goodreads_books.json
│   └─ googlebooks_books.csv
│
├─ standard/                        # ✅ Datos finales procesados
│   ├─ dim_book.parquet
│   └─ book_source_detail.parquet
│
├─ docs/                            # 📑 Documentación y métricas
│   ├─ quality_metrics.json
│   └─ schema.md
│
├─ staging/                         # 🛠️ Archivos intermedios
├─ .env                             # 🔑 Variables de entorno (API keys)
└─ requirements.txt
```

## 🚀 Uso

1. Configurar `.env` con tus claves y parámetros:

```
GOOGLE_BOOKS_API_KEY=tu_api_key
USER_AGENT=books-pipeline-bot/1.0
RATE_LIMIT_SECONDS=0.8
SEARCH_QUERY=animals
MAX_BOOKS=15
```

2. Ejecutar scraper de Goodreads:

```bash
python src/scraper_goodreads.py
```

Generará `landing/goodreads_books.json`.

3. Ejecutar enriquecimiento con Google Books API:

```bash
python src/enrich_google_books.py
```

Generará `landing/googlebooks_books.csv`.

4. Ejecutar integración y deduplicación:

```bash
python src/integrate_pipeline.py
```

Generará:

* `standard/dim_book.parquet` 📦
* `standard/book_source_detail.parquet` 📦
* `docs/quality_metrics.json` 📊
* `docs/schema.md` 📑

