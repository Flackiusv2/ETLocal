"""
Loader Maestro
Orquesta la carga completa de dimensiones y hechos
"""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.loaders.dimension_loader import DimensionLoader
from src.loaders.hecho_loader import HechoHospitalizacionesLoader
from src.loaders.hecho_medicion_loader import HechoMedicionAmbientalLoader
from src.utils.logger import ETLLogger

class MasterLoader:
    """Orquestador para cargar todas las dimensiones y hechos"""
    
    def __init__(self):
        self.logger = ETLLogger('MasterLoader')
        self.results = {}
    
    def load_all(self, transformed_data, truncate=True):
        """
        Carga todos los datos en SQL Server
        
        Args:
            transformed_data: Diccionario con datos transformados
            truncate: Si True, limpia las tablas antes de cargar
        """
        self.logger.start_process("CARGA COMPLETA DEL DATA WAREHOUSE")
        
        try:
            # 1. Cargar dimensiones primero
            self.logger.info("\n" + "="*60)
            self.logger.info("PASO 1: CARGANDO DIMENSIONES")
            self.logger.info("="*60)
            
            dim_loader = DimensionLoader()
            dim_results = dim_loader.load_all_dimensions(transformed_data, truncate)
            self.results['dimensiones'] = dim_results
            
            # 2. Cargar hechos
            self.logger.info("\n" + "="*60)
            self.logger.info("PASO 2: CARGANDO HECHOS")
            self.logger.info("="*60)
            
            # Cargar HechoHospitalizaciones
            self.logger.info("\nCargando HechoHospitalizaciones...")
            hecho_hosp_loader = HechoHospitalizacionesLoader()
            hecho_hosp_rows = hecho_hosp_loader.load(
                transformed_data['hecho_hospitalizaciones'], 
                truncate
            )
            
            # Cargar HechoMedicionAmbiental
            self.logger.info("\nCargando HechoMedicionAmbiental...")
            hecho_medicion_loader = HechoMedicionAmbientalLoader()
            hecho_medicion_rows = hecho_medicion_loader.load(
                transformed_data['hecho_medicion_ambiental'],
                transformed_data,
                truncate
            )
            
            self.results['hechos'] = {
                'HechoHospitalizaciones': hecho_hosp_rows,
                'HechoMedicionAmbiental': hecho_medicion_rows
            }
            
            self.logger.end_process("CARGA COMPLETA DEL DATA WAREHOUSE", success=True)
            return self.results
            
        except Exception as e:
            self.logger.error(f"Error en carga maestra: {str(e)}")
            self.logger.end_process("CARGA COMPLETA DEL DATA WAREHOUSE", success=False)
            raise
    
    def get_load_summary(self):
        """Retorna un resumen de la carga"""
        summary = {
            'dimensiones': {},
            'hechos': {},
            'total_registros': 0
        }
        
        if 'dimensiones' in self.results:
            for table, rows in self.results['dimensiones'].items():
                summary['dimensiones'][table] = rows
                summary['total_registros'] += rows
        
        if 'hechos' in self.results:
            for table, rows in self.results['hechos'].items():
                summary['hechos'][table] = rows
                summary['total_registros'] += rows
        
        return summary

# Función de conveniencia
def load_all_data(transformed_data, truncate=True):
    """Carga todos los datos al DW"""
    master = MasterLoader()
    return master.load_all(transformed_data, truncate)

if __name__ == "__main__":
    # Test completo del ETL
    print("="*60)
    print("TESTING ETL COMPLETO (EXTRACT -> TRANSFORM -> LOAD)")
    print("="*60)
    
    from src.extractors.master_extractor import MasterExtractor
    from src.transformers.master_transformer import MasterTransformer
    
    try:
        # EXTRACT
        print("\n" + "="*60)
        print("FASE 1: EXTRACCIÓN")
        print("="*60)
        extractor = MasterExtractor()
        extracted_data = extractor.extract_all()
        
        # TRANSFORM
        print("\n" + "="*60)
        print("FASE 2: TRANSFORMACIÓN")
        print("="*60)
        transformer = MasterTransformer()
        transformed_data = transformer.transform_all(extracted_data)
        
        # LOAD
        print("\n" + "="*60)
        print("FASE 3: CARGA")
        print("="*60)
        master_loader = MasterLoader()
        results = master_loader.load_all(transformed_data, truncate=True)
        
        # RESUMEN FINAL
        print("\n" + "="*60)
        print("RESUMEN FINAL DEL ETL")
        print("="*60)
        
        summary = master_loader.get_load_summary()
        
        print("\nDIMENSIONES CARGADAS:")
        for table, rows in summary['dimensiones'].items():
            print(f"  - {table}: {rows:,} registros")
        
        print("\nHECHOS CARGADOS:")
        for table, rows in summary['hechos'].items():
            print(f"  - {table}: {rows:,} registros")
        
        print(f"\nTOTAL REGISTROS CARGADOS: {summary['total_registros']:,}")
        print("\n✓ ETL COMPLETADO EXITOSAMENTE")
        
    except Exception as e:
        print(f"\n✗ ERROR EN ETL: {str(e)}")
        import traceback
        traceback.print_exc()
