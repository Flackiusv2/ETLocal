"""
Transformer para DimExposicion
Define los indicadores de exposición ambiental (tipos de contaminantes)
"""
import pandas as pd
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.logger import ETLLogger

class DimExposicionTransformer:
    """
    Transforma datos para la dimensión DimExposicion
    Define los tipos de indicadores ambientales medidos
    """
    
    def __init__(self):
        self.logger = ETLLogger('DimExposicionTransformer')
    
    def transform(self, extracted_data: dict) -> pd.DataFrame:
        """
        Crea las dimensiones de exposición basadas en los tipos de mediciones disponibles
        
        Args:
            extracted_data: Diccionario con los datos extraídos (no se usa, son datos estáticos)
            
        Returns:
            DataFrame con las exposiciones (indicadores ambientales)
        """
        self.logger.info("Iniciando transformación de DimExposicion...")
        
        # Definir los indicadores de exposición ambiental
        exposiciones = [
            {
                'Indicador': 'Monóxido de Carbono (CO)',
                'TipoIndicador': 'Contaminante Gaseoso'
            },
            {
                'Indicador': 'Material Particulado PM2.5',
                'TipoIndicador': 'Contaminante Particulado'
            }
        ]
        
        df_exposicion = pd.DataFrame(exposiciones)
        
        self.logger.info(f"✓ DimExposicion transformada: {len(df_exposicion)} indicadores")
        
        return df_exposicion
