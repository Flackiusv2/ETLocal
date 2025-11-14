"""
Transformador para DimPaciente
Genera combinaciones únicas de características de pacientes
"""
import pandas as pd
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.logger import ETLLogger
from src.utils.helpers import normalize_sexo

class DimPacienteTransformer:
    """Transformador para la dimensión de pacientes"""
    
    def __init__(self):
        self.logger = ETLLogger('DimPacienteTransformer')
        self.df_dim_paciente = None
    
    def transform(self, extracted_data):
        """
        Transforma los datos extraídos en la dimensión paciente
        Extrae combinaciones únicas de características de pacientes
        """
        try:
            self.logger.info("Iniciando transformación de DimPaciente...")
            
            # Lista para almacenar DataFrames de pacientes
            pacientes_list = []
            
            # Extraer de neumonía
            if 'neumonia' in extracted_data and extracted_data['neumonia'] is not None:
                df_neumonia = extracted_data['neumonia'].copy()
                df_pacientes = df_neumonia[[
                    'sexo', 'migrante', 'enfoque_diferencial', 
                    'regimen_seguridad'
                ]].copy()
                df_pacientes['grupo_etario'] = 'Menores de 5 años'  # Asumido si no está especificado
                pacientes_list.append(df_pacientes)
            
            # Extraer de IRA 5 años
            if 'ira5anos' in extracted_data and extracted_data['ira5anos'] is not None:
                df_ira5 = extracted_data['ira5anos'].copy()
                df_pacientes = df_ira5[[
                    'sexo', 'migrante', 'enfoque_diferencial', 
                    'regimen_seguridad', 'grupo_etario'
                ]].copy()
                pacientes_list.append(df_pacientes)
            
            if not pacientes_list:
                raise ValueError("No se encontraron datos de pacientes")
            
            # Combinar todos los pacientes
            df_all_pacientes = pd.concat(pacientes_list, ignore_index=True)
            
            # Normalizar sexo
            df_all_pacientes['sexo'] = df_all_pacientes['sexo'].apply(normalize_sexo)
            
            # Asignar valores por defecto
            # Si grupo_etario está vacío o es NaN, asignar 'Menores de 5 años'
            df_all_pacientes['grupo_etario'] = df_all_pacientes['grupo_etario'].fillna('Menores de 5 años')
            df_all_pacientes.loc[df_all_pacientes['grupo_etario'].str.strip() == '', 'grupo_etario'] = 'Menores de 5 años'
            
            # Si regimen_seguridad está vacío o es NaN, asignar 'Contributivo'
            df_all_pacientes['regimen_seguridad'] = df_all_pacientes['regimen_seguridad'].fillna('Contributivo')
            df_all_pacientes.loc[df_all_pacientes['regimen_seguridad'].str.strip() == '', 'regimen_seguridad'] = 'Contributivo'
            
            # Obtener combinaciones únicas
            self.df_dim_paciente = df_all_pacientes.drop_duplicates(
                subset=['sexo', 'migrante', 'grupo_etario', 
                       'enfoque_diferencial', 'regimen_seguridad']
            ).reset_index(drop=True)
            
            # Renombrar columnas para que coincidan con la BD (sin Ciudad, Estrato, SourceFile, CreatedAt)
            self.df_dim_paciente.rename(columns={
                'sexo': 'Sexo',
                'grupo_etario': 'GrupoEtario',
                'migrante': 'Migrante',
                'enfoque_diferencial': 'EnfoqueDiferencial',
                'regimen_seguridad': 'RegimenSeguridadSocial'
            }, inplace=True)
            
            self.logger.success(f"DimPaciente transformada: {len(self.df_dim_paciente)} combinaciones únicas")
            return self.df_dim_paciente
            
        except Exception as e:
            self.logger.error(f"Error en transformación de DimPaciente: {str(e)}")
            raise
    
    def get_dataframe(self):
        """Retorna el DataFrame transformado"""
        if self.df_dim_paciente is None:
            raise ValueError("No se ha transformado la dimensión. Ejecuta transform() primero.")
        return self.df_dim_paciente

# Función de conveniencia
def transform_dim_paciente(extracted_data):
    """Transforma la dimensión paciente"""
    transformer = DimPacienteTransformer()
    return transformer.transform(extracted_data)

if __name__ == "__main__":
    # Test del transformador
    print("Testing DimPacienteTransformer...")
    
    # Simular datos extraídos
    from src.extractors.master_extractor import MasterExtractor
    
    master = MasterExtractor()
    data = master.extract_all()
    
    transformer = DimPacienteTransformer()
    df_paciente = transformer.transform(data)
    
    print("\n" + "="*60)
    print("RESULTADO DimPaciente:")
    print("="*60)
    print(df_paciente.head(15))
    print(f"\nTotal combinaciones únicas: {len(df_paciente)}")
    print(f"\nColumnas: {list(df_paciente.columns)}")
    print(f"\nDistribución por sexo:")
    print(df_paciente['Sexo'].value_counts())
