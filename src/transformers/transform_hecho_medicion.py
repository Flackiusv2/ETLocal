"""
Transformador para HechoMedicionAmbiental
Prepara datos de mediciones de CO y PM2.5
"""
import pandas as pd
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.logger import ETLLogger

class HechoMedicionAmbientalTransformer:
    """Transformador para el hecho de mediciones ambientales"""
    
    def __init__(self):
        self.logger = ETLLogger('HechoMedicionAmbientalTransformer')
        self.df_hecho = None
    
    def transform(self, extracted_data):
        """
        Transforma los datos de mediciones ambientales en el hecho
        """
        try:
            self.logger.info("Iniciando transformación de HechoMedicionAmbiental...")
            
            records = []
            
            # Mapeo de estaciones a localidades de Bogotá
            # Las estaciones se mapean a localidades aproximadas según su ubicación
            estacion_a_localidad = {
                'USME': 'Usme, Bogota, Colombia',
                'TUNAL': 'Tunjuelito, Bogota, Colombia',
                'KENNEDY': 'Kennedy, Bogota, Colombia',
                'SUBA': 'Suba, Bogota, Colombia',
                'FONTIBON': 'Fontibón, Bogota, Colombia',
                'FONTIBÓN': 'Fontibón, Bogota, Colombia',
                'PUENTE ARANDA': 'Puente Aranda, Bogota, Colombia',
                'CARVAJAL': 'Kennedy, Bogota, Colombia',
                'LAS FERIAS': 'Engativá, Bogota, Colombia',
                'GUAYMARAL': 'Suba, Bogota, Colombia',
                'USAQUEN': 'Usaquén, Bogota, Colombia',
                'USAQUÉN': 'Usaquén, Bogota, Colombia',
                'SAN CRISTOBAL': 'San Cristóbal, Bogota, Colombia',
                'SAN CRISTÓBAL': 'San Cristóbal, Bogota, Colombia',
                'ENGATIVA': 'Engativá, Bogota, Colombia',
                'ENGATIVÁ': 'Engativá, Bogota, Colombia',
                'CIUDAD BOLIVAR': 'Ciudad Bolívar, Bogota, Colombia',
                'CIUDAD BOLÍVAR': 'Ciudad Bolívar, Bogota, Colombia',
                'BOLIVIA': 'Ciudad Bolívar, Bogota, Colombia',
                'CDAR': 'Fontibón, Bogota, Colombia',
                'COLINA': 'Suba, Bogota, Colombia',
                'SEVILLANA': 'Kennedy, Bogota, Colombia',
                'MOVIL 7MA': 'Sin Información, Bogota, Colombia',
                'MÓVIL 7MA': 'Sin Información, Bogota, Colombia',
                'MINAMBIENTE': 'Santa Fe, Bogota, Colombia',
                'JAZMIN': 'Puente Aranda, Bogota, Colombia',
                'JAZMÍN': 'Puente Aranda, Bogota, Colombia'
            }
            
            # Procesar SISAIRE-CO
            if 'sisaire_co' in extracted_data and extracted_data['sisaire_co'] is not None:
                self.logger.info("Procesando mediciones de CO...")
                df_co = extracted_data['sisaire_co'].copy()
                
                for _, row in df_co.iterrows():
                    estacion = str(row['Estacion']).strip().upper()
                    # Buscar la localidad correspondiente
                    localidad = estacion_a_localidad.get(estacion, 'Sin Información, Bogota, Colombia')
                    
                    records.append({
                        'fecha': row['fecha'],
                        'hora': row['hora'],
                        'indicador': 'Monóxido de Carbono (CO)',
                        'localidad': localidad,
                        'CO': row['CO'],
                        'PM25': None,
                        'tipo_medicion': 'CO'
                    })
            
            # Procesar IBOCA-PM25
            if 'iboca_pm25' in extracted_data and extracted_data['iboca_pm25'] is not None:
                self.logger.info("Procesando mediciones de PM2.5...")
                df_pm25 = extracted_data['iboca_pm25'].copy()
                
                for _, row in df_pm25.iterrows():
                    estacion = str(row['Estacion']).strip().upper()
                    # Buscar la localidad correspondiente
                    localidad = estacion_a_localidad.get(estacion, 'Sin Información, Bogota, Colombia')
                    
                    records.append({
                        'fecha': row['fecha'],
                        'hora': row['hora'],
                        'indicador': 'Material Particulado PM2.5',
                        'localidad': localidad,
                        'CO': None,
                        'PM25': row['PM25'],
                        'tipo_medicion': 'PM25'
                    })
            
            if not records:
                raise ValueError("No se generaron registros de mediciones")
            
            # Crear DataFrame del hecho
            self.df_hecho = pd.DataFrame(records)
            
            # Calcular totales
            total_co = self.df_hecho[self.df_hecho['tipo_medicion'] == 'CO']['CO'].sum()
            total_pm25 = self.df_hecho[self.df_hecho['tipo_medicion'] == 'PM25']['PM25'].sum()
            
            self.logger.success(f"HechoMedicionAmbiental transformado: {len(self.df_hecho)} registros")
            self.logger.info(f"Total mediciones CO: {self.df_hecho[self.df_hecho['tipo_medicion'] == 'CO'].shape[0]:,}")
            self.logger.info(f"Total mediciones PM2.5: {self.df_hecho[self.df_hecho['tipo_medicion'] == 'PM25'].shape[0]:,}")
            
            return self.df_hecho
            
        except Exception as e:
            self.logger.error(f"Error en transformación de HechoMedicionAmbiental: {str(e)}")
            raise
    
    def get_dataframe(self):
        """Retorna el DataFrame transformado"""
        if self.df_hecho is None:
            raise ValueError("No se ha transformado el hecho. Ejecuta transform() primero.")
        return self.df_hecho

# Función de conveniencia
def transform_hecho_medicion_ambiental(extracted_data):
    """Transforma el hecho de mediciones ambientales"""
    transformer = HechoMedicionAmbientalTransformer()
    return transformer.transform(extracted_data)

if __name__ == "__main__":
    # Test del transformador
    print("Testing HechoMedicionAmbientalTransformer...")
    from src.extractors.master_extractor import MasterExtractor
    
    extractor = MasterExtractor()
    data = extractor.extract_all()
    
    df = transform_hecho_medicion_ambiental(data)
    print(f"\nTotal registros: {len(df):,}")
    print(f"\nPrimeras filas:")
    print(df.head(10))
    print(f"\nEstadísticas:")
    print(df.describe())
