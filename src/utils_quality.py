import pandas as pd
import numpy as np

def validate_types_and_formats(df, source):
    """
    Valida tipos y formatos en el DataFrame y marca las filas con 'validation_flag'.
    """
    # Normalizar valores vacíos a np.nan
    df = df.replace({'': np.nan, 'nan': np.nan, None: np.nan})
    
    # Inicializar flag
    df['validation_flag'] = 'valid'
    
    # Títulos
    if 'title' in df:
        null_titles_ratio = df['title'].isnull().mean()
        if null_titles_ratio > 0.1:
            raise AssertionError(f"Too many null titles in {source}: {null_titles_ratio*100:.2f}%")
        df.loc[df['title'].isnull(), 'validation_flag'] = 'null_title'
    
    # ISBN-13
    if 'isbn13' in df:
        df.loc[~df['isbn13'].astype(str).str.match(r'^\d{13}$', na=False), 'validation_flag'] = 'invalid_isbn_format'
    
    # Fecha de publicación
    if 'pub_date' in df:
        def is_valid_date(d):
            try:
                return pd.notnull(pd.to_datetime(d, errors='coerce'))
            except:
                return False
        invalid_dates = ~df['pub_date'].apply(is_valid_date) & df['pub_date'].notnull()
        df.loc[invalid_dates, 'validation_flag'] = 'invalid_date'
    
    # Idioma
    if 'language' in df:
        df.loc[~df['language'].astype(str).str.match(r'^[a-z]{2,3}(-[A-Z]{2})?$', na=False), 'validation_flag'] = 'invalid_language'
    
    # Moneda
    if 'price_currency' in df:
        df.loc[~df['price_currency'].astype(str).str.match(r'^[A-Z]{3}$', na=False), 'validation_flag'] = 'invalid_currency'
    
    # Precio numérico
    if 'price_amount' in df:
        df['price_amount'] = pd.to_numeric(df['price_amount'], errors='coerce')
        df.loc[df['price_amount'].isnull(), 'validation_flag'] = 'invalid_price'
    
    # Precio normalizado (después de convertir)
    if 'price' in df:
        df.loc[(df['price'] <= 0) | (df['price'] > 1000), 'validation_flag'] = 'invalid_price_range'
    
    return df


def calculate_quality_metrics(df_gr, df_gb):
    """
    Calcula métricas de calidad de datos para Goodreads y Google Books.
    """
    metrics = {}
    
    for source, df in [('goodreads', df_gr), ('googlebooks', df_gb)]:
        # Normalizar valores vacíos
        df = df.replace({'': np.nan, 'nan': np.nan, None: np.nan})
        
        # Null percentage por columna
        null_percent = {col: df[col].isnull().mean() * 100 for col in df.columns}
        
        # Filas válidas
        valid_rows_percent = (df['validation_flag'] == 'valid').mean() * 100
        
        metrics[source] = {
            'row_count': len(df),
            'null_percent': null_percent,
            'valid_rows_percent': valid_rows_percent
        }
    
    # Métricas globales
    metrics['total_rows'] = len(df_gr) + len(df_gb)
    
    metrics['valid_dates_percent'] = np.mean([
        (df_gr['validation_flag'] != 'invalid_date').mean() * 100 if 'pub_date' in df_gr else 100,
        (df_gb['validation_flag'] != 'invalid_date').mean() * 100 if 'pub_date' in df_gb else 100
    ])
    
    metrics['valid_languages_percent'] = np.mean([
        (df_gr['validation_flag'] != 'invalid_language').mean() * 100 if 'language' in df_gr else 100,
        (df_gb['validation_flag'] != 'invalid_language').mean() * 100 if 'language' in df_gb else 100
    ])
    
    metrics['valid_currencies_percent'] = np.mean([
        (df_gr['validation_flag'] != 'invalid_currency').mean() * 100 if 'price_currency' in df_gr else 100,
        (df_gb['validation_flag'] != 'invalid_currency').mean() * 100 if 'price_currency' in df_gb else 100
    ])
    
    return metrics
