"""
Script para actualizar la estructura de la tabla DimHora
"""
import pyodbc
import os
import sys
import io
from dotenv import load_dotenv

# Configurar encoding para Windows PowerShell
if sys.platform == 'win32':
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    else:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Cargar variables de entorno
load_dotenv()

def fix_dimhora_table():
    """Actualiza la estructura de DimHora"""
    
    server = os.getenv('DB_SERVER', 'localhost')
    driver = os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server')
    database = 'DW_Salud'
    
    print("="*70)
    print(" " * 15 + "ACTUALIZACIÓN DE TABLA DimHora")
    print("="*70)
    
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"Trusted_Connection=yes;"
    )
    
    try:
        print("\n1. Conectando a DW_Salud...")
        conn = pyodbc.connect(conn_str, autocommit=True)
        cursor = conn.cursor()
        print("   ✓ Conexión exitosa")
        
        # Verificar si hay FK que referencien a DimHora
        print("\n2. Verificando dependencias...")
        cursor.execute("""
            SELECT OBJECT_NAME(parent_object_id) as Tabla,
                   name as ConstraintName
            FROM sys.foreign_keys
            WHERE referenced_object_id = OBJECT_ID('dbo.DimHora')
        """)
        fks = cursor.fetchall()
        
        if fks:
            print(f"   ⚠ Encontradas {len(fks)} foreign keys que referencian DimHora:")
            for fk in fks:
                print(f"     - {fk.Tabla}.{fk.ConstraintName}")
                print(f"\n   Eliminando foreign key: {fk.ConstraintName}")
                cursor.execute(f"ALTER TABLE dbo.{fk.Tabla} DROP CONSTRAINT {fk.ConstraintName}")
                print(f"   ✓ {fk.ConstraintName} eliminada")
        else:
            print("   ✓ No hay foreign keys que referencien DimHora")
        
        # Eliminar la tabla existente
        print("\n3. Eliminando tabla DimHora existente...")
        cursor.execute("DROP TABLE IF EXISTS dbo.DimHora")
        print("   ✓ Tabla eliminada")
        
        # Crear la nueva tabla con estructura actualizada
        print("\n4. Creando tabla DimHora con nueva estructura...")
        create_table_sql = """
        CREATE TABLE dbo.DimHora (
            IDHora INT IDENTITY(1,1) PRIMARY KEY,
            Hora INT,
            RangoHorario NVARCHAR(20),
            Periodo NVARCHAR(5),
            HoraFormato NVARCHAR(10)
        )
        """
        cursor.execute(create_table_sql)
        print("   ✓ Tabla creada con nueva estructura")
        
        # Verificar estructura
        print("\n5. Verificando columnas...")
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'DimHora'
            ORDER BY ORDINAL_POSITION
        """)
        columnas = cursor.fetchall()
        print("   Columnas de DimHora:")
        for col in columnas:
            tipo = f"{col.DATA_TYPE}"
            if col.CHARACTER_MAXIMUM_LENGTH:
                tipo += f"({col.CHARACTER_MAXIMUM_LENGTH})"
            print(f"   ✓ {col.COLUMN_NAME:20} {tipo}")
        
        cursor.close()
        conn.close()
        
        print("\n" + "="*70)
        print("✅ TABLA DimHora ACTUALIZADA EXITOSAMENTE")
        print("="*70)
        print("\nAhora puedes ejecutar:")
        print("   python main.py  (para ejecutar el ETL completo)")
        print("="*70)
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        return False

if __name__ == "__main__":
    success = fix_dimhora_table()
    if not success:
        exit(1)

