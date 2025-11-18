# src/integrate_pipeline.py

import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import hashlib
import re
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from utils_quality import calculate_quality_metrics
from utils_isbn import validate_isbn13

load_dotenv()

# =============================================================================
# RUTAS
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent.parent
LANDING_DIR = BASE_DIR / 'landing'
STANDARD_DIR = BASE_DIR / 'standard'
DOCS_DIR = BASE_DIR / 'docs'
WORK_DIR = BASE_DIR / 'staging'

for dir_path in [STANDARD_DIR, DOCS_DIR, WORK_DIR]:
    dir_path.mkdir(exist_ok=True)

goodreads_path = LANDING_DIR / 'goodreads_books.json'
googlebooks_path = LANDING_DIR / 'googlebooks_books.csv'

if not goodreads_path.exists():
    raise FileNotFoundError(f"No se encontró: {goodreads_path}")
if not googlebooks_path.exists():
    raise FileNotFoundError(f"No se encontró: {googlebooks_path}")

print(f"[INFO] Leyendo fuentes en landing/ (solo lectura)...")
print(f"   - Goodreads: {goodreads_path}")
print(f"   - GoogleBooks: {googlebooks_path}")

ingestion_ts = datetime.utcnow().isoformat()

# =============================================================================
# LEER ARCHIVOS
# =============================================================================
with open(goodreads_path, 'r', encoding='utf-8') as f:
    gr_json = json.load(f)
df_gr = pd.DataFrame(gr_json['data'])
df_gb = pd.read_csv(googlebooks_path, encoding='utf-8')

# =============================================================================
# ASEGURAR TIPOS
# =============================================================================
for df in [df_gr, df_gb]:
    for col in ['isbn10', 'isbn13']:
        if col in df.columns:
            df[col] = df[col].astype(str).where(df[col].notnull(), np.nan)
    if 'validation_flag' not in df.columns:
        df['validation_flag'] = 'valid'

# =============================================================================
# CALCULAR MÉTRICAS ANTES DE LIMPIAR
# =============================================================================
df_gr_for_metrics = df_gr.copy()
df_gb_for_metrics = df_gb.copy()
quality_metrics = calculate_quality_metrics(df_gr_for_metrics, df_gb_for_metrics)

# =============================================================================
# LIMPIEZA PREVIA: eliminar registros sin título o ISBN válido
# =============================================================================
df_gr = df_gr[df_gr['title'].notnull() & df_gr['isbn13'].notnull()]
df_gb = df_gb[df_gb['title'].notnull() & df_gb['isbn13'].notnull()]

# =============================================================================
# RENOMBRAR COLUMNAS
# =============================================================================
gr_col_map = {
    'title': 'title',
    'author': 'authors',
    'rating': 'rating',
    'ratings_count': 'ratings_count',
    'book_url': 'book_url',
    'isbn10': 'isbn10',
    'isbn13': 'isbn13',
    'scrape_source': 'scrape_source',
    'scrape_date': 'scrape_date'
}
gb_col_map = {
    'gb_id': 'gb_id',
    'title': 'title',
    'subtitle': 'subtitle',
    'authors': 'authors',
    'publisher': 'publisher',
    'pub_date': 'pub_date',
    'language': 'language',
    'categories': 'categories',
    'isbn13': 'isbn13',
    'isbn10': 'isbn10',
    'price_amount': 'price_amount',
    'price_currency': 'price_currency',
    'query_used': 'query_used'
}
df_gr = df_gr.rename(columns=gr_col_map)
df_gb = df_gb.rename(columns=gb_col_map)

# =============================================================================
# NORMALIZAR LISTAS Y AUTHOR PRINCIPAL
# =============================================================================
def to_list(x, sep=r',|;|and'):
    if pd.isnull(x):
        return []
    if isinstance(x, str):
        return [i.strip() for i in re.split(sep, x) if i.strip()]
    return list(x) if isinstance(x, list) else []

df_gr['authors'] = df_gr['authors'].apply(to_list)
if 'authors' in df_gb:
    df_gb['authors'] = df_gb['authors'].apply(lambda x: to_list(x, sep=';'))
if 'categories' in df_gb:
    df_gb['categories'] = df_gb['categories'].apply(lambda x: to_list(x, sep=';'))

for df in [df_gr, df_gb]:
    if 'authors' in df:
        df['author_principal'] = df['authors'].apply(lambda x: x[0] if len(x) > 0 else np.nan)

# =============================================================================
# FUNCIONES AUXILIARES
# =============================================================================
def choose_field(gr_val, gb_val):
    if isinstance(gr_val, list) and len(gr_val) == 0:
        gr_val = None
    if isinstance(gb_val, list) and len(gb_val) == 0:
        gb_val = None
    return gr_val if pd.notnull(gr_val) else gb_val

def normalize_text(x):
    if pd.isnull(x):
        return np.nan
    return ' '.join(str(x).strip().lower().split())

# =============================================================================
# CREAR DF UNIFICADO
# =============================================================================
merged_records = []

df_gr['_key'] = df_gr['isbn13'].combine_first(df_gr['isbn10'])
df_gb['_key'] = df_gb['isbn13'].combine_first(df_gb['isbn10'])
all_keys = set(df_gr['_key'].dropna()) | set(df_gb['_key'].dropna())

for key in all_keys:
    gr_row = df_gr[df_gr['_key'] == key].iloc[0] if key in df_gr['_key'].values else None
    gb_row = df_gb[df_gb['_key'] == key].iloc[0] if key in df_gb['_key'].values else None

    record = {}
    # Título
    record['title'] = choose_field(gr_row.get('title') if gr_row is not None else None,
                                   gb_row.get('title') if gb_row is not None else None)
    record['title_normalized'] = normalize_text(record['title'])
    # Autores
    authors = choose_field(gr_row.get('authors') if gr_row is not None else None,
                           gb_row.get('authors') if gb_row is not None else None)
    record['authors'] = authors if authors else []
    record['author_principal'] = authors[0] if authors else np.nan
    # Editorial
    record['publisher'] = choose_field(gr_row.get('publisher') if gr_row is not None else None,
                                       gb_row.get('publisher') if gb_row is not None else None)
    # Fecha ISO
    pub_date = choose_field(gr_row.get('pub_date') if gr_row is not None else None,
                            gb_row.get('pub_date') if gb_row is not None else None)
    try:
        record['pub_date_iso'] = pd.to_datetime(pub_date, errors='coerce').strftime('%Y-%m-%d') if pub_date else np.nan
        record['year_pub'] = int(record['pub_date_iso'][:4]) if pd.notnull(record['pub_date_iso']) else np.nan
    except:
        record['pub_date_iso'] = np.nan
        record['year_pub'] = np.nan
    # Idioma
    record['language_bcp'] = normalize_text(choose_field(gr_row.get('language') if gr_row is not None else None,
                                                        gb_row.get('language') if gb_row is not None else None))
    # ISBN
    record['isbn10'] = choose_field(gr_row.get('isbn10') if gr_row is not None else None,
                                    gb_row.get('isbn10') if gb_row is not None else None)
    record['isbn13'] = choose_field(gr_row.get('isbn13') if gr_row is not None else None,
                                    gb_row.get('isbn13') if gb_row is not None else None)
    # Precio
    record['price'] = choose_field(gr_row.get('price_amount') if gr_row is not None else None,
                                   gb_row.get('price_amount') if gb_row is not None else None)
    record['currency_iso'] = normalize_text(choose_field(gr_row.get('price_currency') if gr_row is not None else None,
                                                         gb_row.get('price_currency') if gb_row is not None else None))
    # Categorías
    cats = choose_field(gr_row.get('categories') if gr_row is not None else None,
                        gb_row.get('categories') if gb_row is not None else None)
    record['categories'] = cats if cats else []
    # Validación ISBN
    record['isbn13_valid'] = validate_isbn13(record['isbn13']) if record['isbn13'] else False
    record['validation_flag'] = 'invalid_isbn' if record['isbn13'] and not record['isbn13_valid'] else 'valid'
    # Fuente y timestamp
    record['fuente_ganadora'] = 'goodreads' if gr_row is not None else 'googlebooks'
    record['ts_last_update'] = ingestion_ts

    merged_records.append(record)

df_dim_book = pd.DataFrame(merged_records)

# Detalle por fuente
df_gr['_source_name'] = 'goodreads'
df_gr['_ingestion_ts'] = ingestion_ts
df_gb['_source_name'] = 'googlebooks'
df_gb['_ingestion_ts'] = ingestion_ts
df_source_detail = pd.concat([df_gr, df_gb], ignore_index=True, sort=False)

# =============================================================================
# DEDUPLICACIÓN Y PRIORIDAD ISBN10 DE GOOGLE
# =============================================================================
df_all = df_dim_book.copy()
df_all['dedup_key'] = df_all['isbn13'].combine_first(df_all['isbn10'])
df_all.sort_values(by='ts_last_update', ascending=True, inplace=True)

def pick_best_record(group):
    # Priorizar Google ISBN10
    gb_isbn10 = group[(group['fuente_ganadora']=='googlebooks') & pd.notnull(group['isbn10'])]
    if not gb_isbn10.empty:
        chosen = gb_isbn10.iloc[-1]
    else:
        chosen = group.iloc[-1]
    return chosen

df_dim_book = df_all.groupby('dedup_key', group_keys=False).apply(pick_best_record).reset_index(drop=True)

# Generar book_id_chosen priorizando ISBN10 de Google
def generate_book_id(row):
    if pd.notnull(row.get('isbn10')) and row.get('fuente_ganadora') == 'googlebooks':
        return row['isbn10']
    if pd.notnull(row.get('isbn13')):
        return row['isbn13']
    key_str = f"{row.get('title','')}_{row.get('author_principal','')}_{row.get('publisher','')}_{row.get('pub_date_iso','')}"
    return hashlib.sha256(key_str.encode()).hexdigest()[:16]

df_dim_book['book_id_chosen'] = df_dim_book.apply(generate_book_id, axis=1)

# Marcar registros elegidos en detalle de fuente
df_source_detail['_chosen'] = df_source_detail.apply(
    lambda row: row.get('isbn10') in df_dim_book['book_id_chosen'].values or
                row.get('isbn13') in df_dim_book['book_id_chosen'].values,
    axis=1
)

# =============================================================================
# CALCULAR MÉTRICAS FINALES
# =============================================================================
quality_metrics['duplicados_encontrados'] = len(df_source_detail) - len(df_dim_book)

# =============================================================================
# EMITIR ARTEFACTOS
# =============================================================================
dim_book_path = STANDARD_DIR / 'dim_book.parquet'
source_detail_path = STANDARD_DIR / 'book_source_detail.parquet'
metrics_path = DOCS_DIR / 'quality_metrics.json'
schema_path = DOCS_DIR / 'schema.md'

pq.write_table(pa.Table.from_pandas(df_dim_book), dim_book_path)
pq.write_table(pa.Table.from_pandas(df_source_detail), source_detail_path)

with open(metrics_path, 'w', encoding='utf-8') as f:
    json.dump(quality_metrics, f, indent=4, ensure_ascii=False)

schema_content = """
# Schema Documentation

## dim_book.parquet
- book_id: str, not null, ISBN13 o ID canónico (hash)
- title: str, nullable
- title_normalized: str, nullable
- author_principal: str, nullable
- authors: list[str], nullable
- publisher: str, nullable
- year_pub: int, nullable
- pub_date_iso: str, nullable
- language_bcp: str, nullable
- isbn10: str, nullable
- isbn13: str, nullable
- categories: list[str], nullable
- price: float, nullable
- currency_iso: str, nullable
- fuente_ganadora: str, not null
- ts_last_update: str, not null
- validation_flag: str, not null
"""

with open(schema_path, 'w', encoding='utf-8') as f:
    f.write(schema_content.strip())

print(f"[OK] Integración completada.")
print(f"   dim_book: {dim_book_path}")
print(f"   detail: {source_detail_path}")
print(f"   metrics: {metrics_path}")
print(f"   schema: {schema_path}")
