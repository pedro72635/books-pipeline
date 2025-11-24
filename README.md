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


# 📘 Apartado Técnico del Pipeline de Libros

## 1. Extracción (Scraper Goodreads)

### ✔️ Tecnología utilizada

-   Selenium WebDriver (headless)
-   ChromeDriver compatible
-   Esperas manuales para contenido dinámico

### ✔️ Datos extraídos desde Goodreads

-   **title**
-   **author**
-   **rating**
-   **reviews_count**
-   **published_year**
-   **genres**
-   **description**
-   **isbn10**
-   **isbn13**
-   **pages**
-   **publisher**

### ✔️ Criterios de scraping

-   Iteración por páginas hasta alcanzar `MAX_BOOKS` o agotar
    resultados.
-   Apertura de la página individual para obtener ISBN y metadatos no
    presentes en la vista de lista.
-   Limpieza y normalización:
    -   Valores vacíos → `None`
    -   Géneros convertidos en lista
    -   Descripciones largas priorizadas frente a snippets

------------------------------------------------------------------------

## 2. Enriquecimiento (Google Books API)

### ✔️ Datos utilizados del volumen

-   **title**
-   **authors**
-   **publisher**
-   **publishedDate**
-   **description**
-   **industryIdentifiers** (ISBN10 / ISBN13)
-   **pageCount**
-   **categories**
-   **language**

### ✔️ Búsqueda y prioridades

1.  **Búsqueda por ISBN** (máxima prioridad)
2.  **Búsqueda por título + autor**
3.  Selección del resultado más cercano usando similitud de cadenas

### ✔️ Criterios de selección del libro dentro de Google Books

-   Coincidencia en ISBN10 o ISBN13\
-   Alta similitud en título\
-   Coincidencia en autores\
-   Se descartan resultados poco similares

------------------------------------------------------------------------

## 3. Integración del Pipeline

### Fases

-   **Landing**: datos brutos (`goodreads_books.json`,
    `googlebooks_books.csv`)
-   **Staging**: normalización de campos y tipos
-   **Standard**: dataset final (`dim_book.parquet`)
-   **Docs**: métricas e información estructural
    (`quality_metrics.json`, `schema.md`)

------------------------------------------------------------------------

## 4. Priorización entre Goodreads y Google Books

### ✔️ Regla general

> **Si Google Books aporta un dato válido y más completo, prevalece
> Google.\
> Si no, se conserva Goodreads.**

### ✔️ Prioridad por campo

  Campo                 Prioridad
  --------------------- ---------------------------
  **ISBN13**            **Google → Goodreads**
  **ISBN10**            **Google → Goodreads**
  Title                 Google
  Authors               Google → Goodreads
  Description           Goodreads (más rica)
  Publisher             Google
  Published Year        Goodreads → Google
  Genres / Categories   Combinados y deduplicados
  Pages                 Google

### ✔️ Criterios adicionales

-   Si un valor es `None` o vacío en una fuente, se usa el de la otra.
-   Descripciones:
    -   Si Google es muy corta (\<150 chars), prevalece Goodreads.
-   Autores:
    -   Se normalizan, combinan y deduplican.

------------------------------------------------------------------------

## 5. Deduplicación

### ✔️ Identificador principal

1.  **ISBN13**\
2.  **ISBN10**\
3.  **TITLE + AUTHOR normalizados**

### ✔️ Reglas de deduplicación

-   Se elige el registro con más campos completos.
-   Comparación de calidad de texto (descripción, categorías).
-   En casos de empate estricto → prevalece **Goodreads**.
-   Se genera **book_source_detail.parquet** para rastrear origen de
    cada campo.

------------------------------------------------------------------------

## 6. Métricas de Calidad (quality_metrics.json)

Incluye: - **total_landing** - **total_standard** - **dedupe_loss** -
**complete_fields** - Advertencias: - Libros sin ISBN - Libros sin
autor - Libros sin descripción - Fechas inválidas

------------------------------------------------------------------------

## 7. Artefactos finales del pipeline

-   **dim_book.parquet** → Dataset unificado y final\
-   **book_source_detail.parquet** → Trazabilidad de origen por campo\
-   **quality_metrics.json** → Métricas de calidad\
-   **schema.md** → Documentación del esquema resultante

------------------------------------------------------------------------

## 8. Resumen del flujo completo

1.  Scraping desde Goodreads\
2.  Enriquecimiento con Google Books\
3.  Integración y normalización\
4.  Priorización campo a campo\
5.  Deduplicación basada en ISBN\
6.  Exportación de artefactos finales\
7.  Cálculo de métricas

------------------------------------------------------------------------

## Pruebas de ejecucion:
   
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


