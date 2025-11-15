"""
Transformador para AnalisisCorrelacion
Genera datos agregados por localidad, año y bimestre
"""
import pandas as pd
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.logger import ETLLogger

class AnalisisCorrelacionTransformer:
    """Transformador para la tabla de análisis de correlación"""
    
    def __init__(self):
        self.logger = ETLLogger('AnalisisCorrelacionTransformer')
        self.df_analisis = None
    
    def transform(self, dim_data, fact_data):
        """
        Transforma los datos para generar análisis de correlación
        
        Args:
            dim_data: Diccionario con las dimensiones transformadas
            fact_data: Diccionario con los hechos transformados
        """
        try:
            self.logger.info("Iniciando transformación de AnalisisCorrelacion...")
            
            # Obtener los DataFrames necesarios
            df_fecha = dim_data['dim_fecha']
            df_ubicacion = dim_data['dim_ubicacion']
            df_medicion = fact_data['hecho_medicion']
            df_hospitalizacion = fact_data['hecho_hospitalizacion']
            
            # Agregar datos de mediciones ambientales
            # Convertir fecha a datetime si es necesario
            df_medicion_copy = df_medicion.copy()
            df_medicion_copy['fecha'] = pd.to_datetime(df_medicion_copy['fecha'])
            
            # Necesitamos hacer merge con fecha y ubicación para obtener Bimestre y Localidad
            df_med_enriquecido = df_medicion_copy.merge(
                df_fecha[['Fecha', 'Anio', 'Bimestre']],
                left_on='fecha',
                right_on='Fecha',
                how='left'
            ).merge(
                df_ubicacion[['Localidad', 'CodigoLocalidad']],
                left_on='localidad',
                right_on='Localidad',
                how='left'
            )
            
            # Agregar por Localidad, Año, Bimestre
            med_agg = df_med_enriquecido.groupby(['Localidad', 'Anio', 'Bimestre']).agg({
                'concentracion': 'mean',  # Promedio de concentración
                'fecha': 'count'  # Número de mediciones
            }).reset_index()
            
            med_agg.columns = ['Localidad', 'Anio', 'Bimestre', 'Concentracion_avg', 'NumMediciones']
            
            # Agregar datos de hospitalizaciones
            # HechoHospitalizaciones ya incluye Localidad y CodigoLocalidad
            df_hospitalizacion_copy = df_hospitalizacion.copy()
            
            # HechoHospitalizaciones ya tiene las columnas 'Localidad' y 'Anio', solo necesitamos agregar Bimestre
            # Primero, convertir Fecha a datetime si es necesario
            df_hospitalizacion_copy['Fecha'] = pd.to_datetime(df_hospitalizacion_copy['Fecha'])
            
            # Hacer merge con fecha solo para obtener Bimestre
            df_hosp_enriquecido = df_hospitalizacion_copy.merge(
                df_fecha[['Fecha', 'Bimestre']],
                left_on='Fecha',
                right_on='Fecha',
                how='left'
            )
            
            # Agregar por Localidad (ya existe en df_hospitalizacion), Año, Bimestre
            hosp_agg = df_hosp_enriquecido.groupby(['Localidad', 'Anio', 'Bimestre']).agg({
                'NumeroCasos': 'sum'  # Suma de hospitalizaciones
            }).reset_index()
            
            hosp_agg.columns = ['Localidad', 'Anio', 'Bimestre', 'Hospitalizaciones']
            
            # Combinar ambos agregados
            self.df_analisis = med_agg.merge(
                hosp_agg,
                on=['Localidad', 'Anio', 'Bimestre'],
                how='left'
            )
            
            # Llenar hospitalizaciones faltantes con 0
            self.df_analisis['Hospitalizaciones'] = self.df_analisis['Hospitalizaciones'].fillna(0).astype(int)
            
            # Calcular total de hospitalizaciones por Año y Bimestre (para toda Bogotá)
            total_bogota = self.df_analisis.groupby(['Anio', 'Bimestre'])['Hospitalizaciones'].sum().reset_index()
            total_bogota.columns = ['Anio', 'Bimestre', 'TotalBogota']
            
            # Hacer merge para obtener el total
            self.df_analisis = self.df_analisis.merge(
                total_bogota,
                on=['Anio', 'Bimestre'],
                how='left'
            )
            
            # Calcular tasa de hospitalización (proporción respecto al total de Bogotá)
            self.df_analisis['HospitalizacionRate'] = (
                self.df_analisis['Hospitalizaciones'].astype(float) / 
                self.df_analisis['TotalBogota']
            )
            
            # Llenar con 0 donde no hay total
            self.df_analisis['HospitalizacionRate'] = self.df_analisis['HospitalizacionRate'].fillna(0)
            
            # Eliminar la columna temporal TotalBogota
            self.df_analisis = self.df_analisis.drop(columns=['TotalBogota'])
            
            # Remover filas sin Localidad
            self.df_analisis = self.df_analisis.dropna(subset=['Localidad'])
            
            self.logger.success(f"AnalisisCorrelacion transformado: {len(self.df_analisis)} registros")
            self.logger.info(f"Rango de años: {self.df_analisis['Anio'].min()} - {self.df_analisis['Anio'].max()}")
            self.logger.info(f"Localidades únicas: {self.df_analisis['Localidad'].nunique()}")
            
            return self.df_analisis
            
        except Exception as e:
            self.logger.error(f"Error en transformación de AnalisisCorrelacion: {str(e)}")
            raise
    
    def get_dataframe(self):
        """Retorna el DataFrame transformado"""
        if self.df_analisis is None:
            raise ValueError("No se ha transformado el análisis. Ejecuta transform() primero.")
        return self.df_analisis

# Función de conveniencia
def transform_analisis_correlacion(dim_data, fact_data):
    """Transforma el análisis de correlación"""
    transformer = AnalisisCorrelacionTransformer()
    return transformer.transform(dim_data, fact_data)

if __name__ == "__main__":
    print("Testing AnalisisCorrelacionTransformer...")

