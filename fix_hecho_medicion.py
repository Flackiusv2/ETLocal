"""
Script para modificar HechoMedicionAmbiental según el modelo correcto
"""
import pyodbc
import sys
import io

# Configurar encoding para Windows PowerShell
if sys.platform == 'win32':
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    else:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

conn_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=DW_Salud;Trusted_Connection=yes;'

try:
    conn = pyodbc.connect(conn_string)
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("MODIFICANDO HechoMedicionAmbiental")
    print("="*60 + "\n")
    
    # 1. Eliminar FK a DimEstacion
    print("1. Eliminando FK a DimEstacion...")
    cursor.execute("""
        SELECT f.name 
        FROM sys.foreign_keys AS f
        WHERE OBJECT_NAME(f.parent_object_id) = 'HechoMedicionAmbiental'
          AND OBJECT_NAME(f.referenced_object_id) = 'DimEstacion'
    """)
    fk = cursor.fetchone()
    if fk:
        cursor.execute(f"ALTER TABLE dbo.HechoMedicionAmbiental DROP CONSTRAINT {fk[0]}")
        conn.commit()
        print(f"   ✓ FK eliminada: {fk[0]}")
    
    # 2. Eliminar columna IDEstacion
    print("\n2. Eliminando columna IDEstacion...")
    cursor.execute("ALTER TABLE dbo.HechoMedicionAmbiental DROP COLUMN IDEstacion")
    conn.commit()
    print("   ✓ Columna eliminada")
    
    # 3. Agregar columna IDExposicion
    print("\n3. Agregando columna IDExposicion...")
    cursor.execute("ALTER TABLE dbo.HechoMedicionAmbiental ADD IDExposicion INT NULL")
    conn.commit()
    print("   ✓ Columna agregada")
    
    # 4. Agregar columna IDUbicacion
    print("\n4. Agregando columna IDUbicacion...")
    cursor.execute("ALTER TABLE dbo.HechoMedicionAmbiental ADD IDUbicacion INT NULL")
    conn.commit()
    print("   ✓ Columna agregada")
    
    # 5. Crear FK a DimExposicion
    print("\n5. Creando FK a DimExposicion...")
    cursor.execute("""
        ALTER TABLE dbo.HechoMedicionAmbiental
        ADD CONSTRAINT FK_Med_Exposicion
        FOREIGN KEY (IDExposicion) REFERENCES dbo.DimExposicion(IDExposicion)
    """)
    conn.commit()
    print("   ✓ FK creada")
    
    # 6. Crear FK a DimUbicacion
    print("\n6. Creando FK a DimUbicacion...")
    cursor.execute("""
        ALTER TABLE dbo.HechoMedicionAmbiental
        ADD CONSTRAINT FK_Med_Ubicacion
        FOREIGN KEY (IDUbicacion) REFERENCES dbo.DimUbicacion(IDUbicacion)
    """)
    conn.commit()
    print("   ✓ FK creada")
    
    # 7. Verificar nueva estructura
    print("\n" + "="*60)
    print("NUEVA ESTRUCTURA")
    print("="*60 + "\n")
    
    cursor.execute("""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'HechoMedicionAmbiental' AND TABLE_SCHEMA = 'dbo'
        ORDER BY ORDINAL_POSITION
    """)
    cols = cursor.fetchall()
    for col_name, data_type in cols:
        print(f"  {col_name}: {data_type}")
    
    print("\n" + "="*60)
    print("✅ HechoMedicionAmbiental actualizada exitosamente")
    print("="*60 + "\n")
    
    conn.close()
    
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    conn.rollback()
