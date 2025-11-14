"""
Transformador para DimHora
Genera todas las horas del día (0-23) con información adicional
"""
import pandas as pd
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.logger import ETLLogger

class DimHoraTransformer:
    """Transformador para la dimensión de hora"""
    
    def __init__(self):
        self.logger = ETLLogger('DimHoraTransformer')
        self.df_dim_hora = None
    
    def transform(self, extracted_data=None):
        """
        Genera la dimensión de horas (0-23)
        """
        try:
            self.logger.info("Iniciando transformación de DimHora...")
            
            # Generar todas las horas del día
            horas = list(range(24))
            
            # Crear DataFrame
            self.df_dim_hora = pd.DataFrame({
                'Hora': horas
            })
            
            # Agregar clasificaciones por rango horario
            def get_rango_horario(hora):
                if 0 <= hora < 6:
                    return 'Madrugada'
                elif 6 <= hora < 12:
                    return 'Mañana'
                elif 12 <= hora < 18:
                    return 'Tarde'
                else:
                    return 'Noche'
            
            def get_periodo(hora):
                if hora < 12:
                    return 'AM'
                else:
                    return 'PM'
            
            self.df_dim_hora['RangoHorario'] = self.df_dim_hora['Hora'].apply(get_rango_horario)
            self.df_dim_hora['Periodo'] = self.df_dim_hora['Hora'].apply(get_periodo)
            
            # Formato de hora para visualización (HH:00)
            self.df_dim_hora['HoraFormato'] = self.df_dim_hora['Hora'].apply(lambda h: f"{h:02d}:00")
            
            self.logger.success(f"DimHora transformada: {len(self.df_dim_hora)} horas generadas")
            return self.df_dim_hora
            
        except Exception as e:
            self.logger.error(f"Error en transformación de DimHora: {str(e)}")
            raise
    
    def get_dataframe(self):
        """Retorna el DataFrame transformado"""
        if self.df_dim_hora is None:
            raise ValueError("No se ha transformado la dimensión. Ejecuta transform() primero.")
        return self.df_dim_hora

# Función de conveniencia
def transform_dim_hora(extracted_data=None):
    """Transforma la dimensión hora"""
    transformer = DimHoraTransformer()
    return transformer.transform(extracted_data)

if __name__ == "__main__":
    # Test del transformador
    print("Testing DimHoraTransformer...")
    df = transform_dim_hora()
    print(f"\nTotal horas: {len(df)}")
    print(f"\nHoras del día:")
    print(df)
