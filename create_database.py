"""
Script para crear la base de datos DW_Salud
Este script se conecta a SQL Server sin especificar una base de datos
y luego crea DW_Salud ejecutando el script SQL
"""
import pyodbc
import os
import sys
from dotenv import load_dotenv

# Configurar encoding para Windows PowerShell
import io
if sys.platform == 'win32':
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    else:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Cargar variables de entorno
load_dotenv()

def create_database():
    """Crea la base de datos y ejecuta el script de setup"""
    
    server = os.getenv('DB_SERVER', 'localhost')
    driver = os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server')
    
    print("="*70)
    print(" " * 15 + "CREACIÓN DE BASE DE DATOS")
    print("="*70)
    print(f"\nServidor: {server}")
    print(f"Base de datos: DW_Salud")
    print("="*70)
    
    # Conexión inicial al servidor (master database)
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE=master;"
        f"Trusted_Connection=yes;"
    )
    
    try:
        print("\n1. Conectando a SQL Server (master)...")
        conn = pyodbc.connect(conn_str, autocommit=True)
        cursor = conn.cursor()
        print("   ✓ Conexión exitosa")
        
        # Verificar si la base de datos ya existe
        print("\n2. Verificando si DW_Salud existe...")
        cursor.execute("SELECT database_id FROM sys.databases WHERE name = 'DW_Salud'")
        exists = cursor.fetchone()
        
        if exists:
            print("   ⚠ La base de datos DW_Salud ya existe")
            respuesta = input("\n   ¿Deseas eliminarla y recrearla? (s/n): ")
            if respuesta.lower() == 's':
                print("\n   Eliminando base de datos existente...")
                cursor.execute("ALTER DATABASE DW_Salud SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
                cursor.execute("DROP DATABASE DW_Salud")
                print("   ✓ Base de datos eliminada")
            else:
                print("\n   Cancelando operación...")
                conn.close()
                return False
        
        # Crear la base de datos
        print("\n3. Creando base de datos DW_Salud...")
        cursor.execute("CREATE DATABASE DW_Salud")
        print("   ✓ Base de datos creada exitosamente")
        
        # Cambiar a la nueva base de datos
        cursor.close()
        conn.close()
        
        # Conectar a la nueva base de datos
        conn_str_new = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE=DW_Salud;"
            f"Trusted_Connection=yes;"
        )
        
        print("\n4. Conectando a DW_Salud...")
        conn = pyodbc.connect(conn_str_new, autocommit=False)
        cursor = conn.cursor()
        print("   ✓ Conexión exitosa")
        
        # Leer y ejecutar el script SQL
        print("\n5. Ejecutando script de setup (setup_database.sql)...")
        with open('setup_database.sql', 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        # Dividir el script en comandos individuales (separados por GO)
        commands = sql_script.split('GO')
        
        total_commands = len([cmd for cmd in commands if cmd.strip()])
        print(f"   Total de comandos a ejecutar: {total_commands}")
        
        for i, command in enumerate(commands, 1):
            command = command.strip()
            if command:
                try:
                    cursor.execute(command)
                    conn.commit()
                    print(f"   ✓ Comando {i}/{total_commands} ejecutado")
                except Exception as e:
                    # Algunos comandos pueden fallar si ya existen, continuamos
                    if "already exists" not in str(e).lower():
                        print(f"   ⚠ Advertencia en comando {i}: {str(e)[:100]}")
        
        print("\n   ✓ Script ejecutado completamente")
        
        # Verificar tablas creadas
        print("\n6. Verificando tablas creadas...")
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """)
        
        tables = cursor.fetchall()
        print(f"\n   Tablas creadas ({len(tables)}):")
        for table in tables:
            print(f"   ✓ {table[0]}")
        
        cursor.close()
        conn.close()
        
        print("\n" + "="*70)
        print("✅ BASE DE DATOS CREADA EXITOSAMENTE")
        print("="*70)
        print("\nAhora puedes ejecutar:")
        print("   python test_connection.py  (para probar la conexión)")
        print("   python main.py             (para ejecutar el ETL)")
        print("="*70)
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        print("\nVerifica:")
        print("1. SQL Server está ejecutándose")
        print("2. Tienes permisos para crear bases de datos")
        print("3. La configuración en .env es correcta")
        return False

if __name__ == "__main__":
    success = create_database()
    if not success:
        exit(1)

