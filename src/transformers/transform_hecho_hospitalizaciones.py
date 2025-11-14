"""
Transformador para HechoHospitalizaciones
Prepara los datos de hechos para carga
"""
import pandas as pd
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.logger import ETLLogger
from src.utils.helpers import normalize_sexo, normalize_localidad

class HechoHospitalizacionesTransformer:
    """Transformador para la tabla de hechos de hospitalizaciones"""
    
    def __init__(self):
        self.logger = ETLLogger('HechoHospitalizacionesTransformer')
        self.df_hechos = None
    
    def transform(self, extracted_data):
        """
        Transforma los datos extraídos en hechos de hospitalización
        """
        try:
            self.logger.info("Iniciando transformación de HechoHospitalizaciones...")
            
            hechos_list = []
            
            # 1. Procesar IRA Agregado (casos por año)
            if 'ira_agregado' in extracted_data and extracted_data['ira_agregado'] is not None:
                df_ira = extracted_data['ira_agregado'].copy()
                
                # Para datos agregados, crear un registro por año
                for _, row in df_ira.iterrows():
                    hecho = {
                        'Fecha': pd.Timestamp(year=row['anio'], month=1, day=1).date(),
                        'Anio': row['anio'],
                        'Sexo': None,
                        'Migrante': None,
                        'Localidad': None,
                        'CodigoLocalidad': None,
                        'EnfoqueDiferencial': None,
                        'RegimenSeguridadSocial': None,
                        'GrupoEtario': None,
                        'TipoEnfermedad': 'IRA General',
                        'NumeroCasos': row['numero_casos']
                    }
                    hechos_list.append(hecho)
            
            # 2. Procesar Neumonía (casos individuales)
            if 'neumonia' in extracted_data and extracted_data['neumonia'] is not None:
                df_neumonia = extracted_data['neumonia'].copy()
                
                for _, row in df_neumonia.iterrows():
                    localidad = normalize_localidad(row['localidad'])
                    # Agregar sufijo si no es None
                    if localidad:
                        localidad = f"{localidad}, Bogota, Colombia"
                    
                    hecho = {
                        'Fecha': pd.Timestamp(year=row['anio'], month=1, day=1).date(),
                        'Anio': row['anio'],
                        'Sexo': normalize_sexo(row['sexo']),
                        'Migrante': row['migrante'],
                        'Localidad': localidad,
                        'CodigoLocalidad': row['codigo_localidad'],
                        'EnfoqueDiferencial': row['enfoque_diferencial'],
                        'RegimenSeguridadSocial': row['regimen_seguridad'],  # Nombre correcto de la columna
                        'GrupoEtario': 'Menores de 5 años',  # Default igual que en DimPaciente
                        'TipoEnfermedad': 'Neumonía',
                        'NumeroCasos': 1  # Cada fila es un caso
                    }
                    hechos_list.append(hecho)
            
            # 3. Procesar IRA menores de 5 años
            if 'ira5anos' in extracted_data and extracted_data['ira5anos'] is not None:
                df_ira5 = extracted_data['ira5anos'].copy()
                
                for _, row in df_ira5.iterrows():
                    localidad = normalize_localidad(row['localidad'])
                    # Agregar sufijo si no es None
                    if localidad:
                        localidad = f"{localidad}, Bogota, Colombia"
                    
                    hecho = {
                        'Fecha': pd.Timestamp(year=row['anio'], month=1, day=1).date(),
                        'Anio': row['anio'],
                        'Sexo': normalize_sexo(row['sexo']),
                        'Migrante': row['migrante'],
                        'Localidad': localidad,
                        'CodigoLocalidad': row['codigo_localidad'],
                        'EnfoqueDiferencial': row['enfoque_diferencial'],
                        'RegimenSeguridadSocial': row['regimen_seguridad'],  # Nombre correcto de la columna
                        'GrupoEtario': row['grupo_etario'],
                        'TipoEnfermedad': 'IRA',
                        'NumeroCasos': 1  # Cada fila es un caso
                    }
                    hechos_list.append(hecho)
            
            if not hechos_list:
                raise ValueError("No se generaron hechos de hospitalización")
            
            # Crear DataFrame de hechos
            self.df_hechos = pd.DataFrame(hechos_list)
            
            self.logger.success(f"HechoHospitalizaciones transformado: {len(self.df_hechos)} registros")
            self.logger.info(f"Total casos: {self.df_hechos['NumeroCasos'].sum():,}")
            
            return self.df_hechos
            
        except Exception as e:
            self.logger.error(f"Error en transformación de HechoHospitalizaciones: {str(e)}")
            raise
    
    def get_dataframe(self):
        """Retorna el DataFrame transformado"""
        if self.df_hechos is None:
            raise ValueError("No se ha transformado el hecho. Ejecuta transform() primero.")
        return self.df_hechos
    
    def get_summary(self):
        """Retorna un resumen de los hechos transformados"""
        if self.df_hechos is None:
            raise ValueError("No se ha transformado el hecho. Ejecuta transform() primero.")
        
        summary = {
            'total_registros': len(self.df_hechos),
            'total_casos': self.df_hechos['NumeroCasos'].sum(),
            'por_enfermedad': self.df_hechos.groupby('TipoEnfermedad')['NumeroCasos'].sum().to_dict(),
            'por_anio': self.df_hechos.groupby('Anio')['NumeroCasos'].sum().to_dict(),
            'años_cubiertos': sorted(self.df_hechos['Anio'].unique().tolist())
        }
        return summary

# Función de conveniencia
def transform_hecho_hospitalizaciones(extracted_data):
    """Transforma el hecho de hospitalizaciones"""
    transformer = HechoHospitalizacionesTransformer()
    return transformer.transform(extracted_data)

if __name__ == "__main__":
    # Test del transformador
    print("Testing HechoHospitalizacionesTransformer...")
    
    # Extraer datos
    from src.extractors.master_extractor import MasterExtractor
    
    master = MasterExtractor()
    data = master.extract_all()
    
    # Transformar
    transformer = HechoHospitalizacionesTransformer()
    df_hechos = transformer.transform(data)
    
    print("\n" + "="*60)
    print("RESULTADO HechoHospitalizaciones:")
    print("="*60)
    print(df_hechos.head(15))
    print(f"\nTotal registros: {len(df_hechos)}")
    print(f"\nColumnas: {list(df_hechos.columns)}")
    
    summary = transformer.get_summary()
    print("\n" + "="*60)
    print("RESUMEN:")
    print("="*60)
    for key, value in summary.items():
        print(f"{key}: {value}")
