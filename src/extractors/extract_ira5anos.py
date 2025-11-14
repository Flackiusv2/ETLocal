"""
Extractor para archivo osb_enf_transm_ira5anos.csv
Casos de IRA en menores de 5 años
Formato: ANO;SEXO;MIGRANTE;LOCALIDAD;COD_LOCALIDAD;ENFOQUE_DIFERENCIAL;REGIMEN_SEGURIDAD_SOCIAL
"""
import pandas as pd
import os
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.logger import ETLLogger

class IRA5AnosExtractor:
    """Extractor para datos de IRA en menores de 5 años"""
    
    def __init__(self, file_path):
        self.file_path = file_path
        self.logger = ETLLogger('IRA5AnosExtractor')
        self.df = None
    
    def extract(self):
        """Extrae datos del CSV"""
        try:
            self.logger.info(f"Leyendo archivo: {self.file_path}")
            
            # Intentar diferentes encodings
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            df = None
            
            for encoding in encodings:
                try:
                    self.df = pd.read_csv(
                        self.file_path,
                        sep=';',
                        encoding=encoding,
                        dtype=str  # Leer todo como string inicialmente
                    )
                    self.logger.info(f"Archivo leído con encoding: {encoding}")
                    break
                except UnicodeDecodeError:
                    continue
            
            if self.df is None:
                raise ValueError(f"No se pudo leer el archivo con ningún encoding probado")
            
            self.logger.info(f"Columnas encontradas: {list(self.df.columns)}")
            self.logger.info(f"Total de filas leídas: {len(self.df)}")
            
            # Renombrar columnas a nombres estándar (en minúsculas)
            self.df.columns = self.df.columns.str.strip().str.lower()
            
            # Mapear nombres
            column_mapping = {
                'ano': 'anio',
                'año': 'anio',
                'cod_localidad': 'codigo_localidad',
                'enfoque_diferencial': 'enfoque_diferencial',
                'regimen_seguridad_social': 'regimen_seguridad'
            }
            self.df.rename(columns=column_mapping, inplace=True)
            
            # Limpiar datos
            self._clean_data()
            
            self.logger.success(f"Extracción completada: {len(self.df)} registros")
            return self.df
            
        except Exception as e:
            self.logger.error(f"Error en extracción: {str(e)}")
            raise
    
    def _clean_data(self):
        """Limpia y valida los datos extraídos"""
        initial_count = len(self.df)
        
        # Eliminar filas completamente vacías
        self.df = self.df.dropna(how='all')
        
        # Limpiar espacios en blanco
        for col in self.df.columns:
            if self.df[col].dtype == 'object':
                self.df[col] = self.df[col].str.strip()
        
        # Convertir año a numérico
        if 'anio' in self.df.columns:
            self.df['anio'] = pd.to_numeric(self.df['anio'], errors='coerce')
            self.df = self.df[self.df['anio'].notna()]
            self.df['anio'] = self.df['anio'].astype(int)
        
        # Normalizar valores "Sin Dato", "Sin Información", etc.
        for col in self.df.columns:
            if self.df[col].dtype == 'object':
                self.df[col] = self.df[col].replace({
                    'Sin Dato': 'Sin Información',
                    'Sin dato': 'Sin Información',
                    'SIN DATO': 'Sin Información',
                    'Sin Informacion': 'Sin Información',
                    '': 'Sin Información',
                    'nan': 'Sin Información'
                })
        
        # Normalizar nombres de localidad (quitar código si existe)
        if 'localidad' in self.df.columns:
            self.df['localidad'] = self.df['localidad'].apply(
                lambda x: x.split(' - ')[1] if ' - ' in str(x) else str(x)
            )
        
        # Agregar columna de fuente
        self.df['source_file'] = os.path.basename(self.file_path)
        
        # Agregar tipo de enfermedad y grupo etario
        self.df['tipo_enfermedad'] = 'IRA'
        self.df['grupo_etario'] = 'Menores de 5 años'
        
        removed = initial_count - len(self.df)
        self.logger.info(f"Datos limpios: {len(self.df)} registros válidos ({removed} removidos)")
    
    def get_dataframe(self):
        """Retorna el DataFrame extraído"""
        if self.df is None:
            raise ValueError("No se han extraído datos. Ejecuta extract() primero.")
        return self.df
    
    def get_summary(self):
        """Retorna un resumen estadístico de los datos"""
        if self.df is None:
            raise ValueError("No se han extraído datos. Ejecuta extract() primero.")
        
        summary = {
            'total_registros': len(self.df),
            'años': sorted(self.df['anio'].unique().tolist()) if 'anio' in self.df.columns else [],
            'sexo_distribucion': self.df['sexo'].value_counts().to_dict() if 'sexo' in self.df.columns else {},
            'localidades': self.df['localidad'].nunique() if 'localidad' in self.df.columns else 0,
            'regimen_distribucion': self.df['regimen_seguridad'].value_counts().to_dict() if 'regimen_seguridad' in self.df.columns else {},
        }
        return summary

# Función de conveniencia
def extract_ira5anos(file_path='data_raw/osb_enf_transm_ira5anos.csv'):
    """Extrae datos de IRA en menores de 5 años"""
    extractor = IRA5AnosExtractor(file_path)
    return extractor.extract()

if __name__ == "__main__":
    # Test del extractor
    print("Testing IRA5AnosExtractor...")
    df = extract_ira5anos()
    print("\n" + "="*60)
    print("RESULTADO:")
    print("="*60)
    print(df.head(10))
    print(f"\nTotal registros: {len(df)}")
    print(f"\nColumnas: {list(df.columns)}")
    
    extractor = IRA5AnosExtractor('data_raw/osb_enf_transm_ira5anos.csv')
    extractor.extract()
    summary = extractor.get_summary()
    print("\n" + "="*60)
    print("RESUMEN:")
    print("="*60)
    for key, value in summary.items():
        print(f"{key}: {value}")
