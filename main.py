"""
MAIN ETL - Data Warehouse Salud
Script principal para ejecutar el proceso ETL completo
"""
import sys
import io
from pathlib import Path
from datetime import datetime

# Configurar encoding para Windows PowerShell
if sys.platform == 'win32':
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    else:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent))

from src.extractors.master_extractor import MasterExtractor
from src.transformers.master_transformer import MasterTransformer
from src.loaders.master_loader import MasterLoader
from src.utils.logger import ETLLogger
from config.db_config import db_config

def main():
    """Función principal del ETL"""
    
    # Logger principal
    logger = ETLLogger('MAIN_ETL')
    
    print("="*70)
    print(" " * 15 + "ETL - DATA WAREHOUSE SALUD")
    print(" " * 20 + "Universidad 2025-2")
    print("="*70)
    print(f"\nFecha de ejecución: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    try:
        # 0. Verificar conexión a SQL Server
        logger.info("\n0. Verificando conexión a SQL Server...")
        success, message = db_config.test_connection()
        if not success:
            logger.error(f"No se pudo conectar a SQL Server: {message}")
            return False
        logger.success(message)
        
        # 1. EXTRACCIÓN
        logger.start_process("EXTRACCIÓN DE DATOS")
        extractor = MasterExtractor()
        extracted_data = extractor.extract_all()
        logger.end_process("EXTRACCIÓN DE DATOS", success=True)
        
        # Resumen de extracción
        extraction_summary = extractor.get_extraction_summary()
        total_extracted = sum(info['registros'] for info in extraction_summary.values())
        logger.info(f"\nTotal registros extraídos: {total_extracted:,}")
        
        # 2. TRANSFORMACIÓN
        logger.start_process("TRANSFORMACIÓN DE DATOS")
        transformer = MasterTransformer()
        transformed_data = transformer.transform_all(extracted_data)
        logger.end_process("TRANSFORMACIÓN DE DATOS", success=True)
        
        # Resumen de transformación
        transformation_summary = transformer.get_transformation_summary()
        total_transformed = sum(info['registros'] for info in transformation_summary.values())
        logger.info(f"\nTotal registros transformados: {total_transformed:,}")
        
        # 3. CARGA
        logger.start_process("CARGA A SQL SERVER")
        loader = MasterLoader()
        results = loader.load_all(transformed_data, truncate=True)
        logger.end_process("CARGA A SQL SERVER", success=True)
        
        # 4. RESUMEN FINAL
        print("\n" + "="*70)
        print(" " * 25 + "RESUMEN FINAL")
        print("="*70)
        
        summary = loader.get_load_summary()
        
        print("\n📊 DIMENSIONES CARGADAS:")
        for table, rows in summary['dimensiones'].items():
            print(f"   ✓ {table:30} {rows:>10,} registros")
        
        print("\n📈 HECHOS CARGADOS:")
        for table, rows in summary['hechos'].items():
            print(f"   ✓ {table:30} {rows:>10,} registros")
        
        print("\n📊 ANÁLISIS CARGADOS:")
        for table, rows in summary['analisis'].items():
            print(f"   ✓ {table:30} {rows:>10,} registros")
        
        print("\n" + "-"*70)
        print(f"   TOTAL REGISTROS CARGADOS:        {summary['total_registros']:>10,}")
        print("-"*70)
        
        print("\n✅ ETL COMPLETADO EXITOSAMENTE")
        print(f"   Hora de finalización: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("\n" + "="*70)
        print("   El Data Warehouse está listo para conectarse a Power BI")
        print("="*70)
        
        return True
        
    except Exception as e:
        logger.error(f"\n❌ ERROR EN ETL: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
