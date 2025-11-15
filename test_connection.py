"""
Script de prueba para verificar la conexión a SQL Server
"""
import sys
import io

# Configurar encoding para Windows PowerShell
if sys.platform == 'win32':
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    else:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

sys.path.append('.')

from config.db_config import db_config

def test_connection():
    print("=" * 60)
    print("PROBANDO CONEXIÓN A SQL SERVER")
    print("=" * 60)
    
    success, message = db_config.test_connection()
    
    if success:
        print(f"\n✓ {message}")
        print("\n¡La conexión a SQL Server fue exitosa!")
        return True
    else:
        print(f"\n✗ {message}")
        print("\nPor favor verifica:")
        print("1. Que SQL Server esté ejecutándose")
        print("2. La configuración en el archivo .env")
        print("3. Los drivers ODBC instalados")
        return False

if __name__ == "__main__":
    test_connection()
