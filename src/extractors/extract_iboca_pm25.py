"""
Extractor para archivos IBOCA-PM25
Lee mediciones de material particulado PM2.5 de múltiples archivos Excel
"""
import pandas as pd
import glob
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.logger import ETLLogger

class IBOCAPM25Extractor:
    """Extractor para archivos IBOCA-PM25 (Excel)"""
    
    def __init__(self, data_raw_path='data_raw'):
        self.logger = ETLLogger('IBOCAPM25Extractor')
        self.data_raw_path = Path(data_raw_path)
        self.df_pm25 = None
    
    def extract(self):
        """
        Extrae datos de todos los archivos IBOCA-PM25-*.xlsx
        """
        try:
            # Buscar todos los archivos IBOCA-PM25
            pattern = str(self.data_raw_path / 'IBOCA-PM25-*.xlsx')
            files = glob.glob(pattern)
            
            if not files:
                self.logger.warning(f"No se encontraron archivos IBOCA-PM25 en {self.data_raw_path}")
                return None
            
            self.logger.info(f"Encontrados {len(files)} archivos IBOCA-PM25")
            
            # Lista para almacenar DataFrames
            dfs = []
            
            for file_path in sorted(files):
                file_name = Path(file_path).name
                self.logger.info(f"Leyendo: {file_name}")
                
                try:
                    # Leer metadata primero (fila 1 tiene las estaciones)
                    df_meta = pd.read_excel(file_path, sheet_name=0, nrows=2)
                    stations_text = df_meta.iloc[1, 1]  # Fila 1, columna 1
                    station_names = [s.strip() for s in str(stations_text).split(',')]
                    
                    # Leer datos desde la fila 6 (skiprows=6)
                    df_data = pd.read_excel(file_path, sheet_name=0, skiprows=6)
                    
                    # La primera columna es la fecha/hora
                    fecha_col = df_data.columns[0]
                    
                    # Las columnas IBOCA están cada 3 columnas (índices 3, 6, 9, 12, ...)
                    # Corresponden a: Concentración, Media móvil, IBOCA (repetido para cada estación)
                    iboca_cols = [col for col in df_data.columns if 'IBOCA' in str(col)]
                    
                    # Crear lista de registros
                    records = []
                    
                    for idx, row in df_data.iterrows():
                        try:
                            fecha_hora = pd.to_datetime(row[fecha_col])
                            
                            # Para cada columna IBOCA
                            for col_idx, iboca_col in enumerate(iboca_cols):
                                pm25_value = row[iboca_col]
                                
                                # Obtener nombre de estación
                                if col_idx < len(station_names):
                                    estacion = station_names[col_idx]
                                else:
                                    estacion = f"Estacion_{col_idx + 1}"
                                
                                if pd.notna(pm25_value) and isinstance(pm25_value, (int, float)):
                                    records.append({
                                        'Estacion': estacion,
                                        'Fecha_Hora': fecha_hora,
                                        'PM25': float(pm25_value),
                                        'source_file': file_name
                                    })
                        except:
                            continue
                    
                    if records:
                        df_file = pd.DataFrame(records)
                        dfs.append(df_file)
                        self.logger.info(f"  ✓ {len(df_file)} registros leídos de {file_name}")
                    else:
                        self.logger.warning(f"No se pudieron extraer datos de {file_name}")
                    
                except Exception as e:
                    self.logger.error(f"Error leyendo {file_name}: {str(e)}")
                    continue
            
            if not dfs:
                raise ValueError("No se pudieron leer datos de ningún archivo IBOCA-PM25")
            
            # Combinar todos los DataFrames
            self.df_pm25 = pd.concat(dfs, ignore_index=True)
            
            # Limpiar datos
            self.df_pm25 = self._clean_data()
            
            self.logger.success(f"Extracción completada: {len(self.df_pm25)} registros totales de PM2.5")
            return self.df_pm25
            
        except Exception as e:
            self.logger.error(f"Error en extracción IBOCA-PM25: {str(e)}")
            raise
    
    def _clean_data(self):
        """Limpia y valida los datos extraídos"""
        df = self.df_pm25.copy()
        
        # Remover registros con valores nulos críticos
        initial_count = len(df)
        df = df.dropna(subset=['Estacion', 'Fecha_Hora', 'PM25'])
        
        if len(df) < initial_count:
            self.logger.warning(f"Removidos {initial_count - len(df)} registros con valores nulos")
        
        # Normalizar nombres de estación
        df['Estacion'] = df['Estacion'].str.upper().str.strip()
        
        # Extraer año, mes, día y hora
        df['anio'] = df['Fecha_Hora'].dt.year
        df['mes'] = df['Fecha_Hora'].dt.month
        df['dia'] = df['Fecha_Hora'].dt.day
        df['hora'] = df['Fecha_Hora'].dt.hour
        df['fecha'] = df['Fecha_Hora'].dt.date
        
        self.logger.info(f"Datos limpios: {len(df)} registros válidos ({initial_count - len(df)} removidos)")
        return df
    
    def get_dataframe(self):
        """Retorna el DataFrame extraído"""
        if self.df_pm25 is None:
            raise ValueError("No se han extraído datos. Ejecuta extract() primero.")
        return self.df_pm25

# Función de conveniencia
def extract_iboca_pm25():
    """Extrae datos de IBOCA-PM25"""
    extractor = IBOCAPM25Extractor()
    return extractor.extract()

if __name__ == "__main__":
    # Test del extractor
    print("Testing IBOCAPM25Extractor...")
    try:
        df = extract_iboca_pm25()
        if df is not None:
            print(f"\nTotal registros: {len(df)}")
            print(f"\nEstaciones únicas: {df['Estacion'].nunique()}")
            print(f"Estaciones: {sorted(df['Estacion'].unique())}")
            print(f"\nRango de años: {df['anio'].min()} - {df['anio'].max()}")
            print(f"\nPrimeras filas:")
            print(df.head())
            print(f"\nEstadísticas de PM2.5:")
            print(df['PM25'].describe())
        else:
            print("No se extrajeron datos")
    except Exception as e:
        print(f"Error: {str(e)}")
