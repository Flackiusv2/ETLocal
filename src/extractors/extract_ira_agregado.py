"""
Extractor para archivo ira-2012-2016.csv
Casos agregados de IRA por año
Formato: Ano;Numero de casos de IRA notificados a semana epidemiologica 52
"""
import pandas as pd
import os
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.logger import ETLLogger

class IRAAgregadoExtractor:
    """Extractor para datos agregados de IRA por año"""
    
    def __init__(self, file_path):
        self.file_path = file_path
        self.logger = ETLLogger('IRAAgregadoExtractor')
        self.df = None
    
    def extract(self):
        """Extrae datos del CSV"""
        try:
            self.logger.info(f"Leyendo archivo: {self.file_path}")
            
            # Leer CSV con separador de punto y coma
            self.df = pd.read_csv(
                self.file_path,
                sep=';',
                encoding='utf-8',
                dtype=str  # Leer todo como string inicialmente
            )
            
            self.logger.info(f"Columnas encontradas: {list(self.df.columns)}")
            self.logger.info(f"Total de filas leídas: {len(self.df)}")
            
            # Renombrar columnas a nombres estándar
            self.df.columns = ['anio', 'numero_casos']
            
            # Limpiar datos
            self._clean_data()
            
            self.logger.success(f"Extracción completada: {len(self.df)} registros")
            return self.df
            
        except Exception as e:
            self.logger.error(f"Error en extracción: {str(e)}")
            raise
    
    def _clean_data(self):
        """Limpia y valida los datos extraídos"""
        # Eliminar filas vacías
        self.df = self.df.dropna(subset=['anio'])
        
        # Convertir año a entero
        self.df['anio'] = pd.to_numeric(self.df['anio'], errors='coerce')
        
        # Convertir casos a entero
        self.df['numero_casos'] = pd.to_numeric(self.df['numero_casos'], errors='coerce')
        
        # Eliminar filas con valores inválidos
        self.df = self.df.dropna()
        
        # Convertir a int
        self.df['anio'] = self.df['anio'].astype(int)
        self.df['numero_casos'] = self.df['numero_casos'].astype(int)
        
        # Agregar columna de fuente
        self.df['source_file'] = os.path.basename(self.file_path)
        
        self.logger.info(f"Datos limpios: {len(self.df)} registros válidos")
    
    def get_dataframe(self):
        """Retorna el DataFrame extraído"""
        if self.df is None:
            raise ValueError("No se han extraído datos. Ejecuta extract() primero.")
        return self.df

# Función de conveniencia
def extract_ira_agregado(file_path='data_raw/ira-2012-2016.csv'):
    """Extrae datos de IRA agregados"""
    extractor = IRAAgregadoExtractor(file_path)
    return extractor.extract()

if __name__ == "__main__":
    # Test del extractor
    print("Testing IRAAgregadoExtractor...")
    df = extract_ira_agregado()
    print("\n" + "="*60)
    print("RESULTADO:")
    print("="*60)
    print(df)
    print(f"\nTotal registros: {len(df)}")
    print(f"\nAños: {sorted(df['anio'].unique())}")
    print(f"\nTotal casos: {df['numero_casos'].sum():,}")
