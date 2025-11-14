"""
Loader Base
Funcionalidades comunes para todos los loaders
"""
import pandas as pd
from datetime import datetime
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from config.db_config import db_config
from src.utils.logger import ETLLogger

class BaseLoader:
    """Clase base para todos los loaders"""
    
    def __init__(self, table_name):
        self.table_name = table_name
        self.logger = ETLLogger(f'Loader_{table_name}')
        self.engine = None
        self.connection = None
    
    def connect(self):
        """Establece conexión con SQL Server"""
        try:
            self.engine = db_config.get_sqlalchemy_engine()
            self.connection = self.engine.connect()
            self.logger.info(f"Conexión establecida para cargar {self.table_name}")
        except Exception as e:
            self.logger.error(f"Error al conectar: {str(e)}")
            raise
    
    def disconnect(self):
        """Cierra la conexión"""
        try:
            if self.connection:
                self.connection.close()
            if self.engine:
                self.engine.dispose()
            self.logger.info(f"Conexión cerrada para {self.table_name}")
        except Exception as e:
            self.logger.error(f"Error al cerrar conexión: {str(e)}")
    
    def truncate_table(self):
        """Limpia la tabla antes de cargar"""
        try:
            self.logger.info(f"Limpiando tabla {self.table_name}...")
            query = f"TRUNCATE TABLE dbo.{self.table_name}"
            self.connection.execute(query)
            self.logger.success(f"Tabla {self.table_name} limpiada")
        except Exception as e:
            self.logger.warning(f"No se pudo truncar {self.table_name}: {str(e)}")
    
    def load_dataframe(self, df, if_exists='append', chunksize=100):
        """
        Carga un DataFrame a SQL Server
        
        Args:
            df: DataFrame a cargar
            if_exists: 'append', 'replace', 'fail'
            chunksize: Tamaño del lote (reducido para SQL Server)
        """
        try:
            rows_before = self.get_row_count()
            
            self.logger.info(f"Cargando {len(df)} registros a {self.table_name}...")
            
            df.to_sql(
                name=self.table_name,
                con=self.engine,
                schema='dbo',
                if_exists=if_exists,
                index=False,
                chunksize=chunksize,
                method='multi'
            )
            
            rows_after = self.get_row_count()
            rows_inserted = rows_after - rows_before
            
            self.logger.success(f"Cargados {rows_inserted} registros a {self.table_name}")
            return rows_inserted
            
        except Exception as e:
            self.logger.error(f"Error al cargar datos a {self.table_name}: {str(e)}")
            raise
    
    def get_row_count(self):
        """Obtiene el número de filas en la tabla"""
        try:
            query = f"SELECT COUNT(*) as count FROM dbo.{self.table_name}"
            result = pd.read_sql(query, self.connection)
            return result['count'].iloc[0]
        except:
            return 0
    
    def update_etl_control(self, process_name, rows_loaded, status='Success', notes=''):
        """Actualiza la tabla de control ETL"""
        try:
            # Verificar si ya existe el proceso
            check_query = f"""
            SELECT COUNT(*) as count 
            FROM dbo.ETL_Control 
            WHERE ProcessName = '{process_name}'
            """
            result = pd.read_sql(check_query, self.connection)
            exists = result['count'].iloc[0] > 0
            
            if exists:
                # Update
                update_query = f"""
                UPDATE dbo.ETL_Control
                SET LastRun = GETDATE(),
                    LastFile = '{self.table_name}',
                    RowsLoaded = {rows_loaded},
                    Status = '{status}',
                    Notes = '{notes}'
                WHERE ProcessName = '{process_name}'
                """
                self.connection.execute(update_query)
            else:
                # Insert
                insert_query = f"""
                INSERT INTO dbo.ETL_Control 
                (ProcessName, LastRun, LastFile, RowsLoaded, Status, Notes)
                VALUES ('{process_name}', GETDATE(), '{self.table_name}', 
                        {rows_loaded}, '{status}', '{notes}')
                """
                self.connection.execute(update_query)
            
            self.logger.info(f"ETL_Control actualizado para {process_name}")
            
        except Exception as e:
            self.logger.warning(f"No se pudo actualizar ETL_Control: {str(e)}")
    
    def execute_query(self, query):
        """Ejecuta una query SQL"""
        try:
            result = self.connection.execute(query)
            return result
        except Exception as e:
            self.logger.error(f"Error al ejecutar query: {str(e)}")
            raise
    
    def read_table(self, query=None):
        """Lee datos de la tabla"""
        try:
            if query is None:
                query = f"SELECT * FROM dbo.{self.table_name}"
            return pd.read_sql(query, self.connection)
        except Exception as e:
            self.logger.error(f"Error al leer tabla: {str(e)}")
            raise

if __name__ == "__main__":
    # Test del loader base
    print("Testing BaseLoader...")
    
    loader = BaseLoader('ETL_Control')
    try:
        loader.connect()
        count = loader.get_row_count()
        print(f"Registros en ETL_Control: {count}")
        loader.disconnect()
        print("✓ BaseLoader funcionando correctamente")
    except Exception as e:
        print(f"✗ Error: {str(e)}")
