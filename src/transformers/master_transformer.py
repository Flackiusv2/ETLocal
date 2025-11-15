"""
Orquestador de todos los transformadores
Coordina la transformación de todas las dimensiones y hechos
"""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.logger import ETLLogger
from src.transformers.transform_dim_fecha import DimFechaTransformer
from src.transformers.transform_dim_paciente import DimPacienteTransformer
from src.transformers.transform_dim_ubicacion import DimUbicacionTransformer
from src.transformers.transform_dim_clinica import DimClinicaTransformer
from src.transformers.transform_dim_exposicion import DimExposicionTransformer
from src.transformers.transform_dim_hora import DimHoraTransformer
from src.transformers.transform_hecho_hospitalizaciones import HechoHospitalizacionesTransformer
from src.transformers.transform_hecho_medicion import HechoMedicionAmbientalTransformer
from src.transformers.transform_analisis_correlacion import AnalisisCorrelacionTransformer

class MasterTransformer:
    """Orquestador para transformar todos los datos"""
    
    def __init__(self):
        self.logger = ETLLogger('MasterTransformer')
        self.transformed_data = {}
    
    def transform_all(self, extracted_data):
        """
        Transforma todos los datos extraídos
        
        Args:
            extracted_data: Diccionario con los datos extraídos
            
        Returns:
            Diccionario con todas las dimensiones y hechos transformados
        """
        self.logger.start_process("TRANSFORMACIÓN DE TODAS LAS DIMENSIONES Y HECHOS")
        
        try:
            # 1. Transformar DimFecha
            self.logger.info("\n1. Transformando DimFecha...")
            transformer_fecha = DimFechaTransformer()
            self.transformed_data['dim_fecha'] = transformer_fecha.transform(extracted_data)
            
            # 2. Transformar DimClinica
            self.logger.info("\n2. Transformando DimClinica...")
            transformer_clinica = DimClinicaTransformer()
            self.transformed_data['dim_clinica'] = transformer_clinica.transform(extracted_data)
            
            # 3. Transformar DimPaciente
            self.logger.info("\n3. Transformando DimPaciente...")
            transformer_paciente = DimPacienteTransformer()
            self.transformed_data['dim_paciente'] = transformer_paciente.transform(extracted_data)
            
            # 4. Transformar DimUbicacion
            self.logger.info("\n4. Transformando DimUbicacion...")
            transformer_ubicacion = DimUbicacionTransformer()
            self.transformed_data['dim_ubicacion'] = transformer_ubicacion.transform(extracted_data)
            
            # 5. Transformar DimExposicion
            self.logger.info("\n5. Transformando DimExposicion...")
            transformer_exposicion = DimExposicionTransformer()
            self.transformed_data['dim_exposicion'] = transformer_exposicion.transform(extracted_data)
            
            # 6. Transformar DimHora
            self.logger.info("\n6. Transformando DimHora...")
            transformer_hora = DimHoraTransformer()
            self.transformed_data['dim_hora'] = transformer_hora.transform(extracted_data)
            
            # 7. Transformar HechoHospitalizaciones
            self.logger.info("\n7. Transformando HechoHospitalizaciones...")
            transformer_hechos = HechoHospitalizacionesTransformer()
            self.transformed_data['hecho_hospitalizaciones'] = transformer_hechos.transform(extracted_data)
            
            # 8. Transformar HechoMedicionAmbiental
            self.logger.info("\n8. Transformando HechoMedicionAmbiental...")
            transformer_mediciones = HechoMedicionAmbientalTransformer()
            self.transformed_data['hecho_medicion_ambiental'] = transformer_mediciones.transform(extracted_data)
            
            # 9. Transformar AnalisisCorrelacion (requiere dimensiones y hechos)
            self.logger.info("\n9. Transformando AnalisisCorrelacion...")
            transformer_analisis = AnalisisCorrelacionTransformer()
            dim_data = {
                'dim_fecha': self.transformed_data['dim_fecha'],
                'dim_ubicacion': self.transformed_data['dim_ubicacion']
            }
            fact_data = {
                'hecho_medicion': self.transformed_data['hecho_medicion_ambiental'],
                'hecho_hospitalizacion': self.transformed_data['hecho_hospitalizaciones']
            }
            self.transformed_data['analisis_correlacion'] = transformer_analisis.transform(dim_data, fact_data)
            
            self.logger.end_process("TRANSFORMACIÓN DE TODAS LAS DIMENSIONES Y HECHOS", success=True)
            return self.transformed_data
            
        except Exception as e:
            self.logger.error(f"Error en transformación maestra: {str(e)}")
            self.logger.end_process("TRANSFORMACIÓN DE TODAS LAS DIMENSIONES Y HECHOS", success=False)
            raise
    
    def get_transformation_summary(self):
        """Retorna un resumen de todas las transformaciones"""
        summary = {}
        for table_name, df in self.transformed_data.items():
            if df is not None:
                summary[table_name] = {
                    'registros': len(df),
                    'columnas': list(df.columns),
                    'memoria_mb': round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2)
                }
        return summary
    
    def get_data(self, table_name):
        """Obtiene los datos transformados de una tabla específica"""
        return self.transformed_data.get(table_name)

# Función de conveniencia
def transform_all_data(extracted_data):
    """Transforma todos los datos"""
    master = MasterTransformer()
    return master.transform_all(extracted_data)

if __name__ == "__main__":
    # Test del transformador maestro
    print("="*60)
    print("TESTING MASTER TRANSFORMER")
    print("="*60)
    
    # Primero extraer
    from src.extractors.master_extractor import MasterExtractor
    
    extractor = MasterExtractor()
    extracted_data = extractor.extract_all()
    
    # Luego transformar
    master = MasterTransformer()
    transformed_data = master.transform_all(extracted_data)
    
    print("\n" + "="*60)
    print("RESUMEN DE TRANSFORMACIÓN")
    print("="*60)
    
    summary = master.get_transformation_summary()
    for table, info in summary.items():
        print(f"\n{table.upper()}:")
        print(f"  - Registros: {info['registros']:,}")
        print(f"  - Columnas: {len(info['columnas'])}")
        print(f"  - Memoria: {info['memoria_mb']} MB")
    
    total_records = sum(info['registros'] for info in summary.values())
    print(f"\n{'='*60}")
    print(f"TOTAL DE REGISTROS TRANSFORMADOS: {total_records:,}")
    print(f"{'='*60}")
