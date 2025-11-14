"""
Loader para HechoHospitalizaciones
Realiza lookups a las dimensiones para obtener las llaves foráneas
"""
import pandas as pd
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.loaders.base_loader import BaseLoader
from src.loaders.dimension_loader import DimensionLoader
from src.utils.logger import ETLLogger

class HechoHospitalizacionesLoader:
    """Loader para la tabla de hechos de hospitalizaciones"""
    
    def __init__(self):
        self.logger = ETLLogger('HechoHospitalizacionesLoader')
        self.dim_loader = DimensionLoader()
    
    def load(self, df_hechos, truncate=True):
        """
        Carga los hechos de hospitalización
        
        Args:
            df_hechos: DataFrame con los hechos transformados
            truncate: Si True, limpia la tabla antes de cargar
        """
        self.logger.start_process("CARGA DE HECHO HOSPITALIZACIONES")
        
        try:
            # 1. Obtener dimensiones para lookups
            self.logger.info("Obteniendo dimensiones para lookups...")
            dim_fecha = self.dim_loader.get_dimension_ids('DimFecha')
            dim_clinica = self.dim_loader.get_dimension_ids('DimClinica')
            dim_paciente = self.dim_loader.get_dimension_ids('DimPaciente')
            dim_ubicacion = self.dim_loader.get_dimension_ids('DimUbicacion')
            
            self.logger.info(f"Dimensiones cargadas: Fecha={len(dim_fecha)}, Clinica={len(dim_clinica)}, Paciente={len(dim_paciente)}, Ubicacion={len(dim_ubicacion)}")
            
            # 2. Hacer lookups y preparar datos para carga
            self.logger.info("Realizando lookups a dimensiones...")
            df_final = self._prepare_fact_data(
                df_hechos, dim_fecha, dim_clinica, dim_paciente, dim_ubicacion
            )
            
            # 3. Cargar a SQL Server
            self.logger.info(f"Preparados {len(df_final)} registros para carga...")
            loader = BaseLoader('HechoHospitalizaciones')
            
            try:
                loader.connect()
                
                if truncate:
                    loader.truncate_table()
                
                rows = loader.load_dataframe(df_final, if_exists='append')
                
                # Actualizar control ETL
                loader.update_etl_control(
                    process_name='Load_HechoHospitalizaciones',
                    rows_loaded=rows,
                    status='Success',
                    notes=f'Carga exitosa de {rows} hechos'
                )
                
                self.logger.end_process("CARGA DE HECHO HOSPITALIZACIONES", success=True)
                return rows
                
            finally:
                loader.disconnect()
            
        except Exception as e:
            self.logger.error(f"Error en carga de hechos: {str(e)}")
            self.logger.end_process("CARGA DE HECHO HOSPITALIZACIONES", success=False)
            raise
    
    def _prepare_fact_data(self, df_hechos, dim_fecha, dim_clinica, dim_paciente, dim_ubicacion):
        """Prepara los datos de hechos con lookups a dimensiones"""
        
        df = df_hechos.copy()
        
        # Lookup IDFecha
        self.logger.info("Lookup IDFecha...")
        df['Fecha'] = pd.to_datetime(df['Fecha'])
        dim_fecha['Fecha'] = pd.to_datetime(dim_fecha['Fecha'])
        df = df.merge(
            dim_fecha[['IDFecha', 'Fecha']],
            on='Fecha',
            how='left'
        )
        
        # Lookup IDClinica (por TipoEnfermedad -> NombreClinica)
        self.logger.info("Lookup IDClinica...")
        df = df.merge(
            dim_clinica[['IDClinica', 'NombreClinica']],
            left_on='TipoEnfermedad',
            right_on='NombreClinica',
            how='left'
        )
        
        # Lookup IDPaciente
        self.logger.info("Lookup IDPaciente...")
        df = df.merge(
            dim_paciente[['IDPaciente', 'Sexo', 'Migrante', 'GrupoEtario', 
                         'EnfoqueDiferencial', 'RegimenSeguridadSocial']],
            left_on=['Sexo', 'Migrante', 'GrupoEtario', 'EnfoqueDiferencial', 'RegimenSeguridadSocial'],
            right_on=['Sexo', 'Migrante', 'GrupoEtario', 'EnfoqueDiferencial', 'RegimenSeguridadSocial'],
            how='left'
        )
        
        # Lookup IDUbicacion
        self.logger.info("Lookup IDUbicacion...")
        df = df.merge(
            dim_ubicacion[['IDUbicacion', 'Localidad', 'CodigoLocalidad']],
            left_on=['Localidad', 'CodigoLocalidad'],
            right_on=['Localidad', 'CodigoLocalidad'],
            how='left'
        )
        
        # Seleccionar solo las columnas necesarias para la tabla de hechos
        df_final = df[['IDClinica', 'IDFecha', 'IDPaciente', 'IDUbicacion', 'NumeroCasos']].copy()
        
        # Agregar columnas que no tenemos datos (como NULL)
        df_final['IDHora'] = None
        df_final['TipoIngreso'] = None
        df_final['DuracionDias'] = None
        
        # Log de registros sin match (para debugging)
        null_clinica = df_final['IDClinica'].isna().sum()
        null_fecha = df_final['IDFecha'].isna().sum()
        null_paciente = df_final['IDPaciente'].isna().sum()
        null_ubicacion = df_final['IDUbicacion'].isna().sum()
        
        if null_clinica > 0:
            self.logger.warning(f"{null_clinica} registros sin IDClinica")
        if null_fecha > 0:
            self.logger.warning(f"{null_fecha} registros sin IDFecha")
        if null_paciente > 0:
            self.logger.warning(f"{null_paciente} registros sin IDPaciente")
        if null_ubicacion > 0:
            self.logger.warning(f"{null_ubicacion} registros sin IDUbicacion")
        
        # Eliminar registros sin IDFecha o IDClinica (requeridos)
        initial_count = len(df_final)
        df_final = df_final[df_final['IDFecha'].notna() & df_final['IDClinica'].notna()]
        removed = initial_count - len(df_final)
        
        if removed > 0:
            self.logger.warning(f"{removed} registros removidos por falta de llaves requeridas")
        
        return df_final

# Función de conveniencia
def load_hecho_hospitalizaciones(df_hechos, truncate=True):
    """Carga el hecho de hospitalizaciones"""
    loader = HechoHospitalizacionesLoader()
    return loader.load(df_hechos, truncate)

if __name__ == "__main__":
    # Test del loader de hechos
    print("Testing HechoHospitalizacionesLoader...")
    
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
    from src.loaders.dimension_loader import load_dimensions
    load_dimensions(transformed_data, truncate=True)
    
    print("\n4. Cargando hechos...")
    loader = HechoHospitalizacionesLoader()
    rows = loader.load(transformed_data['hecho_hospitalizaciones'], truncate=True)
    
    print("\n" + "="*60)
    print(f"HECHOS CARGADOS: {rows:,} registros")
    print("="*60)
