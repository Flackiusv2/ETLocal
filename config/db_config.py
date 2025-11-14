"""
Configuración de conexión a SQL Server
"""
import os
from dotenv import load_dotenv
import pyodbc
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# Cargar variables de entorno
load_dotenv()

class DatabaseConfig:
    """Clase para manejar la configuración de la base de datos"""
    
    def __init__(self):
        self.server = os.getenv('DB_SERVER', 'localhost')
        self.database = os.getenv('DB_DATABASE', 'DW_Salud')
        self.driver = os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server')
        self.username = os.getenv('DB_USER', '')
        self.password = os.getenv('DB_PASSWORD', '')
        self.trusted_connection = os.getenv('DB_TRUSTED_CONNECTION', 'yes')
    
    def get_connection_string(self):
        """Retorna la cadena de conexión para pyodbc"""
        if self.trusted_connection.lower() == 'yes':
            conn_str = (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"Trusted_Connection=yes;"
            )
        else:
            conn_str = (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
            )
        return conn_str
    
    def get_sqlalchemy_engine(self):
        """Retorna un engine de SQLAlchemy para pandas"""
        conn_str = self.get_connection_string()
        connection_url = f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}"
        engine = create_engine(connection_url, fast_executemany=True)
        return engine
    
    def test_connection(self):
        """Prueba la conexión a la base de datos"""
        try:
            conn = pyodbc.connect(self.get_connection_string())
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()
            conn.close()
            return True, f"Conexión exitosa: {version[0][:50]}..."
        except Exception as e:
            return False, f"Error de conexión: {str(e)}"

# Instancia global
db_config = DatabaseConfig()

if __name__ == "__main__":
    # Test de conexión
    print("Probando conexión a SQL Server...")
    success, message = db_config.test_connection()
    if success:
        print(f"✓ {message}")
    else:
        print(f"✗ {message}")
