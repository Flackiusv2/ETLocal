"""
Extractor para archivos SISAIRE-CO
Lee mediciones de monóxido de carbono (CO) de múltiples archivos CSV
"""
import pandas as pd
import glob
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.logger import ETLLogger

class SISAIRECOExtractor:
    """Extractor para archivos SISAIRE-CO (CSV)"""
    
    def __init__(self, data_raw_path='data_raw'):
        self.logger = ETLLogger('SISAIRECOExtractor')
        self.data_raw_path = Path(data_raw_path)
        self.df_co = None
    
    def extract(self):
        """
        Extrae datos de todos los archivos SISAIRE-CO-*.csv
        """
        try:
            # Buscar todos los archivos SISAIRE-CO
            pattern = str(self.data_raw_path / 'SISAIRE-CO-*.csv')
            files = glob.glob(pattern)
            
            if not files:
                self.logger.warning(f"No se encontraron archivos SISAIRE-CO en {self.data_raw_path}")
                return None
            
            self.logger.info(f"Encontrados {len(files)} archivos SISAIRE-CO")
            
            # Lista para almacenar DataFrames
            dfs = []
            
            for file_path in sorted(files):
                file_name = Path(file_path).name
                self.logger.info(f"Leyendo: {file_name}")
                
                try:
                    # Leer CSV con encoding latin-1
                    df = pd.read_csv(
                        file_path,
                        encoding='latin-1'
                    )
                    
                    # Renombrar columnas para consistencia (eliminar espacios)
                    df.columns = df.columns.str.strip()
                    column_map = {
                        'Estacion': 'Estacion',
                        'Fecha inicial': 'Fecha_Inicial',
                        'Fecha final': 'Fecha_Final',
                        'CO': 'CO'
                    }
                    df.rename(columns=column_map, inplace=True)
                    
                    # Limpiar valores con comillas dobles en Estacion
                    df['Estacion'] = df['Estacion'].astype(str).str.replace('"', '').str.strip()
                    
                    # Convertir fechas
                    df['Fecha_Inicial'] = pd.to_datetime(df['Fecha_Inicial'], errors='coerce')
                    df['Fecha_Final'] = pd.to_datetime(df['Fecha_Final'], errors='coerce')
                    
                    # CO ya viene como float
                    
                    # Agregar fuente
                    df['source_file'] = file_name
                    
                    dfs.append(df)
                    self.logger.info(f"  ✓ {len(df)} registros leídos de {file_name}")
                    
                except Exception as e:
                    self.logger.error(f"Error leyendo {file_name}: {str(e)}")
                    continue
            
            if not dfs:
                raise ValueError("No se pudieron leer datos de ningún archivo SISAIRE-CO")
            
            # Combinar todos los DataFrames
            self.df_co = pd.concat(dfs, ignore_index=True)
            
            # Limpiar datos
            self.df_co = self._clean_data()
            
            self.logger.success(f"Extracción completada: {len(self.df_co)} registros totales de CO")
            return self.df_co
            
        except Exception as e:
            self.logger.error(f"Error en extracción SISAIRE-CO: {str(e)}")
            raise
    
    def _clean_data(self):
        """Limpia y valida los datos extraídos"""
        df = self.df_co.copy()
        
        # Remover registros con valores nulos críticos
        initial_count = len(df)
        df = df.dropna(subset=['Estacion', 'Fecha_Inicial', 'CO'])
        
        if len(df) < initial_count:
            self.logger.warning(f"Removidos {initial_count - len(df)} registros con valores nulos")
        
        # Normalizar nombres de estación
        df['Estacion'] = df['Estacion'].str.upper().str.strip()
        
        # Extraer año, mes, día y hora de la fecha inicial
        df['anio'] = df['Fecha_Inicial'].dt.year
        df['mes'] = df['Fecha_Inicial'].dt.month
        df['dia'] = df['Fecha_Inicial'].dt.day
        df['hora'] = df['Fecha_Inicial'].dt.hour
        df['fecha'] = df['Fecha_Inicial'].dt.date
        
        self.logger.info(f"Datos limpios: {len(df)} registros válidos ({initial_count - len(df)} removidos)")
        return df
    
    def get_dataframe(self):
        """Retorna el DataFrame extraído"""
        if self.df_co is None:
            raise ValueError("No se han extraído datos. Ejecuta extract() primero.")
        return self.df_co

# Función de conveniencia
def extract_sisaire_co():
    """Extrae datos de SISAIRE-CO"""
    extractor = SISAIRECOExtractor()
    return extractor.extract()

if __name__ == "__main__":
    # Test del extractor
    print("Testing SISAIRECOExtractor...")
    try:
        df = extract_sisaire_co()
        if df is not None:
            print(f"\nTotal registros: {len(df)}")
            print(f"\nEstaciones únicas: {df['Estacion'].nunique()}")
            print(f"Estaciones: {sorted(df['Estacion'].unique())}")
            print(f"\nRango de años: {df['anio'].min()} - {df['anio'].max()}")
            print(f"\nPrimeras filas:")
            print(df.head())
            print(f"\nEstadísticas de CO:")
            print(df['CO'].describe())
        else:
            print("No se extrajeron datos")
    except Exception as e:
        print(f"Error: {str(e)}")
