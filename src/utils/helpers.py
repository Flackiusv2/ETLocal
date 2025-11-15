"""
Funciones auxiliares para el ETL
"""
import pandas as pd
from datetime import datetime
import hashlib

def clean_column_names(df):
    """Limpia nombres de columnas: sin espacios, minúsculas"""
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    return df

def standardize_text(text):
    """Estandariza texto: strip, title case"""
    if pd.isna(text):
        return None
    return str(text).strip()

def parse_date(date_str, formats=['%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d']):
    """Intenta parsear una fecha con múltiples formatos"""
    if pd.isna(date_str):
        return None
    
    for fmt in formats:
        try:
            return datetime.strptime(str(date_str), fmt).date()
        except:
            continue
    return None

def generate_hash(*args):
    """Genera un hash MD5 a partir de múltiples valores"""
    combined = '|'.join(str(arg) for arg in args if arg is not None)
    return hashlib.md5(combined.encode()).hexdigest()

def safe_int(value, default=None):
    """Convierte a entero de forma segura"""
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default

def safe_float(value, default=None):
    """Convierte a float de forma segura"""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

def normalize_localidad(localidad):
    """Normaliza nombres de localidades de Bogotá"""
    if pd.isna(localidad):
        return None
    
    localidad = str(localidad).strip()
    
    # Remover códigos del formato "00 - Bogotá"
    if ' - ' in localidad:
        localidad = localidad.split(' - ')[1]
    
    return localidad

def normalize_sexo(sexo):
    """Normaliza valores de sexo"""
    if pd.isna(sexo):
        return None
    
    sexo = str(sexo).strip().lower()
    
    if sexo in ['femenino', 'f', 'mujer']:
        return 'Femenino'
    elif sexo in ['masculino', 'm', 'hombre']:
        return 'Masculino'
    else:
        return 'Sin Información'

def get_nombre_mes(mes):
    """Retorna el nombre del mes en español"""
    meses = {
        1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril',
        5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto',
        9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
    }
    return meses.get(mes, 'Desconocido')

def get_trimestre(mes):
    """Retorna el trimestre del año"""
    if mes in [1, 2, 3]:
        return 1
    elif mes in [4, 5, 6]:
        return 2
    elif mes in [7, 8, 9]:
        return 3
    else:
        return 4

def get_bimestre(mes):
    """Retorna el bimestre del año (1-6)"""
    if mes in [1, 2]:
        return 1
    elif mes in [3, 4]:
        return 2
    elif mes in [5, 6]:
        return 3
    elif mes in [7, 8]:
        return 4
    elif mes in [9, 10]:
        return 5
    else:  # mes in [11, 12]
        return 6