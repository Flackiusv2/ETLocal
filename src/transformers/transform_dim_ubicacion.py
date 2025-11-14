"""
Transformador para DimUbicacion
Genera ubicaciones únicas (localidades y barrios)
"""
import pandas as pd
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.logger import ETLLogger
from src.utils.helpers import normalize_localidad

class DimUbicacionTransformer:
    """Transformador para la dimensión de ubicación"""
    
    def __init__(self):
        self.logger = ETLLogger('DimUbicacionTransformer')
        self.df_dim_ubicacion = None
    
    def transform(self, extracted_data):
        """
        Transforma los datos extraídos en la dimensión ubicación
        Extrae localidades únicas
        """
        try:
            self.logger.info("Iniciando transformación de DimUbicacion...")
            
            # Lista para almacenar DataFrames de ubicaciones
            ubicaciones_list = []
            
            # Extraer de neumonía
            if 'neumonia' in extracted_data and extracted_data['neumonia'] is not None:
                df_neumonia = extracted_data['neumonia'].copy()
                df_ubicaciones = df_neumonia[[
                    'localidad', 'codigo_localidad'
                ]].copy()
                ubicaciones_list.append(df_ubicaciones)
            
            # Extraer de IRA 5 años
            if 'ira5anos' in extracted_data and extracted_data['ira5anos'] is not None:
                df_ira5 = extracted_data['ira5anos'].copy()
                df_ubicaciones = df_ira5[[
                    'localidad', 'codigo_localidad'
                ]].copy()
                ubicaciones_list.append(df_ubicaciones)
            
            if not ubicaciones_list:
                raise ValueError("No se encontraron datos de ubicación")
            
            # Combinar todas las ubicaciones
            df_all_ubicaciones = pd.concat(ubicaciones_list, ignore_index=True)
            
            # Normalizar localidad
            df_all_ubicaciones['localidad'] = df_all_ubicaciones['localidad'].apply(normalize_localidad)
            
            # Consolidar registros que solo dicen "Bogota" o sin información como "BogotaSinLocalidad"
            df_all_ubicaciones.loc[
                (df_all_ubicaciones['localidad'].str.strip().str.upper() == 'BOGOTA') | 
                (df_all_ubicaciones['localidad'].isna()) | 
                (df_all_ubicaciones['localidad'].str.strip() == ''),
                'localidad'
            ] = 'BogotaSinLocalidad'
            
            # Agregar "Bogota,Colombia" al final de la localidad para geolocalización
            df_all_ubicaciones['localidad'] = df_all_ubicaciones['localidad'] + ', Bogota, Colombia'
            
            # Obtener combinaciones únicas
            self.df_dim_ubicacion = df_all_ubicaciones.drop_duplicates(
                subset=['localidad', 'codigo_localidad']
            ).reset_index(drop=True)
            
            # Renombrar columnas (sin Barrio, Tipo, SourceFile, CreatedAt)
            self.df_dim_ubicacion.rename(columns={
                'localidad': 'Localidad',
                'codigo_localidad': 'CodigoLocalidad'
            }, inplace=True)
            
            # Reordenar columnas
            self.df_dim_ubicacion = self.df_dim_ubicacion[[
                'Localidad', 'CodigoLocalidad'
            ]]
            
            self.logger.success(f"DimUbicacion transformada: {len(self.df_dim_ubicacion)} ubicaciones únicas")
            return self.df_dim_ubicacion
            
        except Exception as e:
            self.logger.error(f"Error en transformación de DimUbicacion: {str(e)}")
            raise
    
    def get_dataframe(self):
        """Retorna el DataFrame transformado"""
        if self.df_dim_ubicacion is None:
            raise ValueError("No se ha transformado la dimensión. Ejecuta transform() primero.")
        return self.df_dim_ubicacion

# Función de conveniencia
def transform_dim_ubicacion(extracted_data):
    """Transforma la dimensión ubicación"""
    transformer = DimUbicacionTransformer()
    return transformer.transform(extracted_data)

if __name__ == "__main__":
    # Test del transformador
    print("Testing DimUbicacionTransformer...")
    
    # Simular datos extraídos
    from src.extractors.master_extractor import MasterExtractor
    
    master = MasterExtractor()
    data = master.extract_all()
    
    transformer = DimUbicacionTransformer()
    df_ubicacion = transformer.transform(data)
    
    print("\n" + "="*60)
    print("RESULTADO DimUbicacion:")
    print("="*60)
    print(df_ubicacion.head(20))
    print(f"\nTotal ubicaciones únicas: {len(df_ubicacion)}")
    print(f"\nColumnas: {list(df_ubicacion.columns)}")
    print(f"\nLocalidades:")
    print(df_ubicacion['Localidad'].value_counts())
