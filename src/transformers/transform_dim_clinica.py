"""
Transformador para DimClinica
Genera tipos de enfermedades respiratorias como "clínicas"
"""
import pandas as pd
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.logger import ETLLogger

class DimClinicaTransformer:
    """Transformador para la dimensión de clínica/enfermedad"""
    
    def __init__(self):
        self.logger = ETLLogger('DimClinicaTransformer')
        self.df_dim_clinica = None
    
    def transform(self, extracted_data):
        """
        Transforma los datos en la dimensión clínica
        En este caso, usamos los tipos de enfermedad como "clínicas"
        """
        try:
            self.logger.info("Iniciando transformación de DimClinica...")
            
            # Definir enfermedades respiratorias con códigos CIE-10
            enfermedades = [
                {
                    'CodigoCIE': 'J12-J18',
                    'NombreClinica': 'Neumonía',
                    'TipoHospitalizacion': 'Enfermedad Respiratoria Aguda'
                },
                {
                    'CodigoCIE': 'J00-J06',
                    'NombreClinica': 'IRA (Infección Respiratoria Aguda)',
                    'TipoHospitalizacion': 'Enfermedad Respiratoria Aguda'
                },
                {
                    'CodigoCIE': 'J00-J22',
                    'NombreClinica': 'IRA General',
                    'TipoHospitalizacion': 'Enfermedad Respiratoria Aguda'
                },
                {
                    'CodigoCIE': 'Sin Especificar',
                    'NombreClinica': 'Enfermedad Respiratoria Sin Especificar',
                    'TipoHospitalizacion': 'Sin Especificar'
                }
            ]
            
            self.df_dim_clinica = pd.DataFrame(enfermedades)
            
            self.logger.success(f"DimClinica transformada: {len(self.df_dim_clinica)} tipos de enfermedad")
            return self.df_dim_clinica
            
        except Exception as e:
            self.logger.error(f"Error en transformación de DimClinica: {str(e)}")
            raise
    
    def get_dataframe(self):
        """Retorna el DataFrame transformado"""
        if self.df_dim_clinica is None:
            raise ValueError("No se ha transformado la dimensión. Ejecuta transform() primero.")
        return self.df_dim_clinica

# Función de conveniencia
def transform_dim_clinica(extracted_data=None):
    """Transforma la dimensión clínica"""
    transformer = DimClinicaTransformer()
    return transformer.transform(extracted_data or {})

if __name__ == "__main__":
    # Test del transformador
    print("Testing DimClinicaTransformer...")
    
    transformer = DimClinicaTransformer()
    df_clinica = transformer.transform({})
    
    print("\n" + "="*60)
    print("RESULTADO DimClinica:")
    print("="*60)
    print(df_clinica)
    print(f"\nTotal tipos de enfermedad: {len(df_clinica)}")
    print(f"\nColumnas: {list(df_clinica.columns)}")
