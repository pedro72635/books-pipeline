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

5. Pruebas de ejecucion:
   
Muestra de un libro con sus datos de Goodreads:
Tras ejecutar el scraper, podemos observar varios libros con estas caracteristicas.

<img width="668" height="209" alt="image" src="https://github.com/user-attachments/assets/5a409ab2-ee16-48e5-9d7f-219a4566d453" />

Ejemplo de libros obtenidos por Google Books:
Resumen de los libros resultados tras la consulta de la API de Google Books.

<img width="1231" height="108" alt="image" src="https://github.com/user-attachments/assets/9b649518-4876-4c61-94b7-bf4377b06d13" />

Datos unificados y limpios:
Ejemplo del archivo dim_book.parquet, ya limpios.

<img width="1300" height="299" alt="image" src="https://github.com/user-attachments/assets/5d56c045-b7e6-40e6-b691-8d5dd5a5c500" />

Ejemplo de archivos creados:
Estructura del programa una vez ya ejecutados todos los archivos.

<img width="170" height="513" alt="image" src="https://github.com/user-attachments/assets/f4077c56-0c86-47a6-83bb-434f7953f29b" />

Metricas de datos:
Muestras de errores y aciertos de los datos pre-transformación.

<img width="335" height="751" alt="image" src="https://github.com/user-attachments/assets/9c4b7dad-41f5-4de1-9a21-0897d738be57" />


