"""
Loader para AnalisisCorrelacion
"""
import pandas as pd
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.loaders.base_loader import BaseLoader
from src.utils.logger import ETLLogger

class AnalisisCorrelacionLoader:
    """Loader para la tabla de análisis de correlación"""
    
    def __init__(self):
        self.logger = ETLLogger('AnalisisCorrelacionLoader')
    
    def load(self, df_analisis, truncate=True):
        """
        Carga el análisis de correlación
        
        Args:
            df_analisis: DataFrame con los datos del análisis
            truncate: Si True, limpia la tabla antes de cargar
        """
        self.logger.start_process("CARGA DE ANALISIS CORRELACION")
        
        try:
            # Preparar DataFrame final
            df_final = pd.DataFrame({
                'CodigoLocalidad': df_analisis['CodigoLocalidad'],
                'Anio': df_analisis['Anio'].astype(int),
                'Bimestre': df_analisis['Bimestre'].astype(int),
                'Concentracion_avg': df_analisis['Concentracion_avg'],
                'NumMediciones': df_analisis['NumMediciones'].astype(int),
                'Hospitalizaciones': df_analisis['Hospitalizaciones'].astype(int),
                'HospitalizacionRate': df_analisis['HospitalizacionRate']
            })
            
            # Cargar a la base de datos
            loader = BaseLoader('AnalisisCorrelacion')
            
            try:
                loader.connect()
                
                if truncate:
                    loader.truncate_table()
                
                rows = loader.load_dataframe(df_final, if_exists='append')
                
                loader.update_etl_control(
                    process_name='Load_AnalisisCorrelacion',
                    rows_loaded=rows,
                    status='Success',
                    notes=f'Carga exitosa: {rows} registros'
                )
                
                self.logger.end_process("CARGA DE ANALISIS CORRELACION", success=True)
                return rows
                
            finally:
                loader.disconnect()
            
        except Exception as e:
            self.logger.error(f"Error en carga de AnalisisCorrelacion: {str(e)}")
            self.logger.end_process("CARGA DE ANALISIS CORRELACION", success=False)
            raise

# Función de conveniencia
def load_analisis_correlacion(df_analisis, truncate=True):
    """Carga el análisis de correlación"""
    loader = AnalisisCorrelacionLoader()
    return loader.load(df_analisis, truncate)

if __name__ == "__main__":
    print("Testing AnalisisCorrelacionLoader...")

