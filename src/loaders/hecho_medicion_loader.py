"""
Loader para HechoMedicionAmbiental
"""
import pandas as pd
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.loaders.base_loader import BaseLoader
from src.utils.logger import ETLLogger

class HechoMedicionAmbientalLoader:
    """Loader para la tabla de hechos de mediciones ambientales"""
    
    def __init__(self):
        self.logger = ETLLogger('HechoMedicionAmbientalLoader')
    
    def load(self, df_hecho, transformed_data, truncate=True):
        """
        Carga el hecho de mediciones ambientales
        Realiza lookups a las dimensiones para obtener los IDs
        
        Args:
            df_hecho: DataFrame con los datos del hecho
            transformed_data: Diccionario con todas las dimensiones transformadas
            truncate: Si True, limpia la tabla antes de cargar
        """
        self.logger.start_process("CARGA DE HECHO MEDICION AMBIENTAL")
        
        try:
            # 1. Obtener las dimensiones para hacer lookups
            self.logger.info("Obteniendo dimensiones para lookups...")
            
            # Cargar dimensiones desde la BD
            dim_fecha = self._get_dimension_data('DimFecha')
            dim_hora = self._get_dimension_data('DimHora')
            dim_exposicion = self._get_dimension_data('DimExposicion')
            dim_ubicacion = self._get_dimension_data('DimUbicacion')
            
            self.logger.info(f"Dimensiones cargadas: Fecha={len(dim_fecha)}, Hora={len(dim_hora)}, Exposicion={len(dim_exposicion)}, Ubicacion={len(dim_ubicacion)}")
            
            # 2. Hacer los lookups
            self.logger.info("Realizando lookups a dimensiones...")
            
            df_fact = df_hecho.copy()
            
            # Lookup IDFecha
            self.logger.info("Lookup IDFecha...")
            df_fact['fecha_pd'] = pd.to_datetime(df_fact['fecha'])
            dim_fecha['Fecha_pd'] = pd.to_datetime(dim_fecha['Fecha'])
            df_fact = df_fact.merge(
                dim_fecha[['IDFecha', 'Fecha_pd']],
                left_on='fecha_pd',
                right_on='Fecha_pd',
                how='left'
            )
            df_fact.drop(['fecha_pd', 'Fecha_pd'], axis=1, inplace=True)
            
            # Lookup IDHora
            self.logger.info("Lookup IDHora...")
            df_fact = df_fact.merge(
                dim_hora[['IDHora', 'Hora']],
                left_on='hora',
                right_on='Hora',
                how='left'
            )
            df_fact.drop(['Hora'], axis=1, inplace=True)
            
            # Lookup IDExposicion
            self.logger.info("Lookup IDExposicion...")
            df_fact = df_fact.merge(
                dim_exposicion[['IDExposicion', 'Indicador']],
                left_on='indicador',
                right_on='Indicador',
                how='left'
            )
            df_fact.drop(['Indicador'], axis=1, inplace=True)
            
            # Lookup IDUbicacion
            self.logger.info("Lookup IDUbicacion...")
            df_fact = df_fact.merge(
                dim_ubicacion[['IDUbicacion', 'Localidad']],
                left_on='localidad',
                right_on='Localidad',
                how='left'
            )
            df_fact.drop(['Localidad'], axis=1, inplace=True)
            
            # 3. Verificar lookups
            missing_fecha = df_fact['IDFecha'].isna().sum()
            missing_hora = df_fact['IDHora'].isna().sum()
            missing_exposicion = df_fact['IDExposicion'].isna().sum()
            missing_ubicacion = df_fact['IDUbicacion'].isna().sum()
            
            if missing_fecha > 0:
                self.logger.warning(f"{missing_fecha} registros sin IDFecha")
            if missing_hora > 0:
                self.logger.warning(f"{missing_hora} registros sin IDHora")
            if missing_exposicion > 0:
                self.logger.warning(f"{missing_exposicion} registros sin IDExposicion")
            if missing_ubicacion > 0:
                self.logger.warning(f"{missing_ubicacion} registros sin IDUbicacion")
            
            # Remover registros sin llaves requeridas
            initial_count = len(df_fact)
            df_fact = df_fact.dropna(subset=['IDFecha', 'IDHora', 'IDExposicion', 'IDUbicacion'])
            removed = initial_count - len(df_fact)
            
            if removed > 0:
                self.logger.warning(f"{removed} registros removidos por falta de llaves requeridas")
            
            # 4. Preparar DataFrame final
            self.logger.info(f"Preparados {len(df_fact)} registros para carga...")
            
            df_final = pd.DataFrame({
                'IDFecha': df_fact['IDFecha'].astype(int),
                'IDHora': df_fact['IDHora'].astype(int),
                'IDExposicion': df_fact['IDExposicion'].astype(int),
                'IDUbicacion': df_fact['IDUbicacion'].astype(int),
                'ValorCO': df_fact['CO'],
                'ValorPM25': df_fact['PM25']
            })
            
            # 5. Cargar a la base de datos
            loader = BaseLoader('HechoMedicionAmbiental')
            
            try:
                loader.connect()
                
                if truncate:
                    loader.truncate_table()
                
                rows = loader.load_dataframe(df_final, if_exists='append')
                
                loader.update_etl_control(
                    process_name='Load_HechoMedicionAmbiental',
                    rows_loaded=rows,
                    status='Success',
                    notes=f'Carga exitosa: {rows} registros'
                )
                
                self.logger.end_process("CARGA DE HECHO MEDICION AMBIENTAL", success=True)
                return rows
                
            finally:
                loader.disconnect()
            
        except Exception as e:
            self.logger.error(f"Error en carga de HechoMedicionAmbiental: {str(e)}")
            self.logger.end_process("CARGA DE HECHO MEDICION AMBIENTAL", success=False)
            raise
    
    def _get_dimension_data(self, table_name):
        """Obtiene todos los datos de una dimensión"""
        loader = BaseLoader(table_name)
        
        try:
            loader.connect()
            query = f"SELECT * FROM dbo.{table_name}"
            df = pd.read_sql(query, loader.engine)
            return df
        finally:
            loader.disconnect()

# Función de conveniencia
def load_hecho_medicion_ambiental(df_hecho, transformed_data, truncate=True):
    """Carga el hecho de mediciones ambientales"""
    loader = HechoMedicionAmbientalLoader()
    return loader.load(df_hecho, transformed_data, truncate)

if __name__ == "__main__":
    print("Testing HechoMedicionAmbientalLoader...")
    # Este test requiere que las dimensiones ya estén cargadas
