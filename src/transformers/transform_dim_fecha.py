"""
Transformador para DimFecha
Genera la dimensión temporal a partir de los años en los datos
"""
import pandas as pd
from datetime import datetime, timedelta
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.logger import ETLLogger
from src.utils.helpers import get_nombre_mes, get_trimestre

class DimFechaTransformer:
    """Transformador para la dimensión de fechas"""
    
    def __init__(self):
        self.logger = ETLLogger('DimFechaTransformer')
        self.df_dim_fecha = None
    
    def transform(self, extracted_data):
        """
        Transforma los datos extraídos en la dimensión fecha
        Genera todas las fechas desde el año mínimo al máximo encontrado
        """
        try:
            self.logger.info("Iniciando transformación de DimFecha...")
            
            # Obtener rango de años de todas las fuentes
            years = set()
            
            for source_name, df in extracted_data.items():
                if df is not None and 'anio' in df.columns:
                    years.update(df['anio'].unique())
            
            if not years:
                raise ValueError("No se encontraron años en los datos extraídos")
            
            min_year = min(years)
            max_year = max(years)
            
            self.logger.info(f"Generando fechas desde {min_year} hasta {max_year}")
            
            # Generar todas las fechas del rango
            start_date = datetime(min_year, 1, 1)
            end_date = datetime(max_year, 12, 31)
            
            date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            
            # Crear DataFrame de fechas
            self.df_dim_fecha = pd.DataFrame({
                'Fecha': date_range,
                'Anio': date_range.year,
                'Mes': date_range.month,
                'Dia': date_range.day,
            })
            
            # Agregar columnas calculadas
            self.df_dim_fecha['NombreMes'] = self.df_dim_fecha['Mes'].apply(get_nombre_mes)
            self.df_dim_fecha['Trimestre'] = self.df_dim_fecha['Mes'].apply(get_trimestre)
            
            self.logger.success(f"DimFecha transformada: {len(self.df_dim_fecha)} fechas generadas")
            return self.df_dim_fecha
            
        except Exception as e:
            self.logger.error(f"Error en transformación de DimFecha: {str(e)}")
            raise
    
    def get_dataframe(self):
        """Retorna el DataFrame transformado"""
        if self.df_dim_fecha is None:
            raise ValueError("No se ha transformado la dimensión. Ejecuta transform() primero.")
        return self.df_dim_fecha

# Función de conveniencia
def transform_dim_fecha(extracted_data):
    """Transforma la dimensión fecha"""
    transformer = DimFechaTransformer()
    return transformer.transform(extracted_data)

if __name__ == "__main__":
    # Test del transformador
    print("Testing DimFechaTransformer...")
    
    # Datos de prueba
    test_data = {
        'ira_agregado': pd.DataFrame({'anio': [2012, 2013, 2014, 2015, 2016]}),
        'neumonia': pd.DataFrame({'anio': [2015, 2016, 2017, 2018, 2019]})
    }
    
    transformer = DimFechaTransformer()
    df_fecha = transformer.transform(test_data)
    
    print("\n" + "="*60)
    print("RESULTADO DimFecha:")
    print("="*60)
    print(df_fecha.head(10))
    print(f"\nTotal fechas: {len(df_fecha)}")
    print(f"\nAños únicos: {sorted(df_fecha['Anio'].unique())}")
    print(f"\nColumnas: {list(df_fecha.columns)}")
