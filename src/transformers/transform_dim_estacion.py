"""
Transformador para DimEstacion
Genera estaciones de monitoreo únicas
"""
import pandas as pd
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.logger import ETLLogger

class DimEstacionTransformer:
    """Transformador para la dimensión de estaciones de monitoreo"""
    
    def __init__(self):
        self.logger = ETLLogger('DimEstacionTransformer')
        self.df_dim_estacion = None
    
    def transform(self, extracted_data):
        """
        Transforma los datos extraídos en la dimensión estación
        Extrae estaciones únicas de las mediciones ambientales
        """
        try:
            self.logger.info("Iniciando transformación de DimEstacion...")
            
            # Lista para almacenar estaciones
            estaciones = []
            
            # Extraer de SISAIRE-CO
            if 'sisaire_co' in extracted_data and extracted_data['sisaire_co'] is not None:
                df_co = extracted_data['sisaire_co']
                estaciones.extend(df_co['Estacion'].unique().tolist())
            
            # Extraer de IBOCA-PM25
            if 'iboca_pm25' in extracted_data and extracted_data['iboca_pm25'] is not None:
                df_pm25 = extracted_data['iboca_pm25']
                estaciones.extend(df_pm25['Estacion'].unique().tolist())
            
            if not estaciones:
                raise ValueError("No se encontraron estaciones en los datos")
            
            # Crear DataFrame con estaciones únicas
            estaciones_unicas = sorted(set(estaciones))
            
            self.df_dim_estacion = pd.DataFrame({
                'NombreEstacion': estaciones_unicas,
                'TipoEstacion': 'Calidad del Aire',
                'Ubicacion': 'Bogotá, Colombia'
            })
            
            self.logger.success(f"DimEstacion transformada: {len(self.df_dim_estacion)} estaciones únicas")
            return self.df_dim_estacion
            
        except Exception as e:
            self.logger.error(f"Error en transformación de DimEstacion: {str(e)}")
            raise
    
    def get_dataframe(self):
        """Retorna el DataFrame transformado"""
        if self.df_dim_estacion is None:
            raise ValueError("No se ha transformado la dimensión. Ejecuta transform() primero.")
        return self.df_dim_estacion

# Función de conveniencia
def transform_dim_estacion(extracted_data):
    """Transforma la dimensión estación"""
    transformer = DimEstacionTransformer()
    return transformer.transform(extracted_data)

if __name__ == "__main__":
    # Test del transformador
    print("Testing DimEstacionTransformer...")
    from src.extractors.master_extractor import MasterExtractor
    
    extractor = MasterExtractor()
    data = extractor.extract_all()
    
    df = transform_dim_estacion(data)
    print(f"\nTotal estaciones: {len(df)}")
    print(f"\nEstaciones:")
    print(df)
