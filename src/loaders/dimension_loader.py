"""
Loader para todas las dimensiones
"""
import pandas as pd
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.loaders.base_loader import BaseLoader
from src.utils.logger import ETLLogger

class DimensionLoader:
    """Loader para cargar todas las dimensiones"""
    
    def __init__(self):
        self.logger = ETLLogger('DimensionLoader')
        self.results = {}
    
    def load_all_dimensions(self, transformed_data, truncate=True):
        """
        Carga todas las dimensiones en orden
        
        Args:
            transformed_data: Diccionario con datos transformados
            truncate: Si True, limpia las tablas antes de cargar
        """
        self.logger.start_process("CARGA DE DIMENSIONES")
        
        try:
            # Orden de carga (sin dependencias entre ellas)
            dimensions = [
                ('dim_fecha', 'DimFecha'),
                ('dim_hora', 'DimHora'),
                ('dim_clinica', 'DimClinica'),
                ('dim_paciente', 'DimPaciente'),
                ('dim_ubicacion', 'DimUbicacion'),
                ('dim_exposicion', 'DimExposicion')
            ]
            
            for data_key, table_name in dimensions:
                if data_key in transformed_data and transformed_data[data_key] is not None:
                    self.logger.info(f"\nCargando {table_name}...")
                    rows = self._load_dimension(
                        df=transformed_data[data_key],
                        table_name=table_name,
                        truncate=truncate
                    )
                    self.results[table_name] = rows
                else:
                    self.logger.warning(f"No hay datos para {table_name}")
            
            self.logger.end_process("CARGA DE DIMENSIONES", success=True)
            return self.results
            
        except Exception as e:
            self.logger.error(f"Error en carga de dimensiones: {str(e)}")
            self.logger.end_process("CARGA DE DIMENSIONES", success=False)
            raise
    
    def _load_dimension(self, df, table_name, truncate=True):
        """Carga una dimensión específica"""
        loader = BaseLoader(table_name)
        
        try:
            loader.connect()
            
            if truncate:
                loader.truncate_table()
            
            rows = loader.load_dataframe(df, if_exists='append')
            
            # Actualizar control ETL
            loader.update_etl_control(
                process_name=f'Load_{table_name}',
                rows_loaded=rows,
                status='Success',
                notes=f'Carga exitosa de {table_name}'
            )
            
            return rows
            
        except Exception as e:
            self.logger.error(f"Error cargando {table_name}: {str(e)}")
            raise
        finally:
            loader.disconnect()
    
    def get_dimension_ids(self, table_name):
        """
        Obtiene los IDs de una dimensión cargada (para hacer lookups)
        Retorna un DataFrame con todos los datos de la dimensión
        """
        loader = BaseLoader(table_name)
        try:
            loader.connect()
            df = loader.read_table()
            return df
        finally:
            loader.disconnect()

# Funciones de conveniencia
def load_dimensions(transformed_data, truncate=True):
    """Carga todas las dimensiones"""
    loader = DimensionLoader()
    return loader.load_all_dimensions(transformed_data, truncate)

if __name__ == "__main__":
    # Test del loader de dimensiones
    print("Testing DimensionLoader...")
    
    # Extraer y transformar primero
    from src.extractors.master_extractor import MasterExtractor
    from src.transformers.master_transformer import MasterTransformer
    
    print("\n1. Extrayendo datos...")
    extractor = MasterExtractor()
    extracted_data = extractor.extract_all()
    
    print("\n2. Transformando datos...")
    transformer = MasterTransformer()
    transformed_data = transformer.transform_all(extracted_data)
    
    print("\n3. Cargando dimensiones...")
    loader = DimensionLoader()
    results = loader.load_all_dimensions(transformed_data, truncate=True)
    
    print("\n" + "="*60)
    print("RESUMEN DE CARGA DE DIMENSIONES")
    print("="*60)
    for table, rows in results.items():
        print(f"{table}: {rows:,} registros cargados")
