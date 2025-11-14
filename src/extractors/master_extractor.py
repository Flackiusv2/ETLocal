"""
Orquestador de todos los extractores
Maneja la extracción de todas las fuentes de datos
"""
import pandas as pd
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.logger import ETLLogger
from src.extractors.extract_ira_agregado import IRAAgregadoExtractor
from src.extractors.extract_neumonia import NeumoniaExtractor
from src.extractors.extract_ira5anos import IRA5AnosExtractor
from src.extractors.extract_sisaire_co import SISAIRECOExtractor
from src.extractors.extract_iboca_pm25 import IBOCAPM25Extractor

class MasterExtractor:
    """Orquestador para extraer todos los archivos CSV"""
    
    def __init__(self, data_raw_path='data_raw'):
        self.data_raw_path = Path(data_raw_path)
        self.logger = ETLLogger('MasterExtractor')
        self.extracted_data = {}
    
    def extract_all(self):
        """Extrae todos los archivos CSV"""
        self.logger.start_process("EXTRACCIÓN DE TODAS LAS FUENTES")
        
        try:
            # 1. IRA Agregado (2012-2016)
            self.logger.info("Extrayendo IRA agregado...")
            ira_path = self.data_raw_path / 'ira-2012-2016.csv'
            if ira_path.exists():
                extractor_ira = IRAAgregadoExtractor(str(ira_path))
                self.extracted_data['ira_agregado'] = extractor_ira.extract()
            else:
                self.logger.warning(f"Archivo no encontrado: {ira_path}")
            
            # 2. Neumonía
            self.logger.info("\nExtrayendo datos de neumonía...")
            neumonia_path = self.data_raw_path / 'osb_enf_trans_neumonia.csv'
            if neumonia_path.exists():
                extractor_neumonia = NeumoniaExtractor(str(neumonia_path))
                self.extracted_data['neumonia'] = extractor_neumonia.extract()
            else:
                self.logger.warning(f"Archivo no encontrado: {neumonia_path}")
            
            # 3. IRA menores de 5 años
            self.logger.info("\nExtrayendo IRA menores de 5 años...")
            ira5_path = self.data_raw_path / 'osb_enf_transm_ira5anos.csv'
            if ira5_path.exists():
                extractor_ira5 = IRA5AnosExtractor(str(ira5_path))
                self.extracted_data['ira5anos'] = extractor_ira5.extract()
            else:
                self.logger.warning(f"Archivo no encontrado: {ira5_path}")
            
            # 4. SISAIRE-CO (Mediciones de monóxido de carbono)
            self.logger.info("\nExtrayendo mediciones SISAIRE-CO...")
            extractor_co = SISAIRECOExtractor(str(self.data_raw_path))
            self.extracted_data['sisaire_co'] = extractor_co.extract()
            
            # 5. IBOCA-PM25 (Mediciones de material particulado)
            self.logger.info("\nExtrayendo mediciones IBOCA-PM25...")
            extractor_pm25 = IBOCAPM25Extractor(str(self.data_raw_path))
            self.extracted_data['iboca_pm25'] = extractor_pm25.extract()
            
            self.logger.end_process("EXTRACCIÓN DE TODAS LAS FUENTES", success=True)
            return self.extracted_data
            
        except Exception as e:
            self.logger.error(f"Error en extracción maestra: {str(e)}")
            self.logger.end_process("EXTRACCIÓN DE TODAS LAS FUENTES", success=False)
            raise
    
    def get_extraction_summary(self):
        """Retorna un resumen de todas las extracciones"""
        summary = {}
        for source, df in self.extracted_data.items():
            if df is not None and isinstance(df, pd.DataFrame):
                summary[source] = {
                    'registros': len(df),
                    'columnas': list(df.columns),
                    'memoria_mb': round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2)
                }
        return summary
    
    def get_data(self, source_name):
        """Obtiene los datos de una fuente específica"""
        return self.extracted_data.get(source_name)

# Función de conveniencia
def extract_all_sources(data_raw_path='data_raw'):
    """Extrae todas las fuentes de datos"""
    master = MasterExtractor(data_raw_path)
    return master.extract_all()

if __name__ == "__main__":
    # Test del extractor maestro
    print("="*60)
    print("TESTING MASTER EXTRACTOR")
    print("="*60)
    
    master = MasterExtractor()
    data = master.extract_all()
    
    print("\n" + "="*60)
    print("RESUMEN DE EXTRACCIÓN")
    print("="*60)
    
    summary = master.get_extraction_summary()
    for source, info in summary.items():
        print(f"\n{source.upper()}:")
        print(f"  - Registros: {info['registros']:,}")
        print(f"  - Columnas: {len(info['columnas'])}")
        print(f"  - Memoria: {info['memoria_mb']} MB")
    
    total_records = sum(info['registros'] for info in summary.values())
    print(f"\n{'='*60}")
    print(f"TOTAL DE REGISTROS EXTRAÍDOS: {total_records:,}")
    print(f"{'='*60}")
