"""
Script para actualizar la tabla DimHora con las columnas correctas
"""
import pyodbc

conn_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=DW_Salud;Trusted_Connection=yes;'

try:
    conn = pyodbc.connect(conn_string)
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("ACTUALIZANDO TABLA DimHora")
    print("="*60 + "\n")
    
    # Verificar si hay datos
    cursor.execute("SELECT COUNT(*) FROM DimHora")
    count = cursor.fetchone()[0]
    print(f"Registros actuales en DimHora: {count}")
    
    # Verificar columnas actuales
    cursor.execute("""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'DimHora' AND TABLE_SCHEMA = 'dbo'
        ORDER BY ORDINAL_POSITION
    """)
    current_cols = [row[0] for row in cursor.fetchall()]
    print(f"Columnas actuales: {current_cols}")
    
    # Eliminar constraints de foreign key primero
    print("\nBuscando foreign keys que referencian DimHora...")
    cursor.execute("""
        SELECT 
            OBJECT_NAME(f.parent_object_id) AS TableName,
            f.name AS ForeignKeyName
        FROM sys.foreign_keys AS f
        WHERE OBJECT_NAME(f.referenced_object_id) = 'DimHora'
    """)
    fks = cursor.fetchall()
    
    for table_name, fk_name in fks:
        print(f"  Eliminando FK: {fk_name} de tabla {table_name}")
        cursor.execute(f"ALTER TABLE dbo.{table_name} DROP CONSTRAINT {fk_name}")
        conn.commit()
    
    # Eliminar la tabla vieja y crear la nueva
    print("\nEliminando tabla DimHora vieja...")
    cursor.execute("DROP TABLE IF EXISTS dbo.DimHora")
    conn.commit()
    print("✓ Tabla eliminada")
    
    print("\nCreando tabla DimHora con nuevo esquema...")
    cursor.execute("""
        CREATE TABLE dbo.DimHora (
            IDHora INT IDENTITY(1,1) PRIMARY KEY,
            Hora INT NOT NULL,
            RangoHorario NVARCHAR(20),
            Periodo NVARCHAR(5),
            HoraFormato NVARCHAR(10)
        )
    """)
    conn.commit()
    print("✓ Tabla creada con nuevo esquema")
    
    # Recrear foreign keys
    print("\nRecreando foreign keys...")
    for table_name, fk_name in fks:
        print(f"  Recreando FK: {fk_name} en tabla {table_name}")
        cursor.execute(f"""
            ALTER TABLE dbo.{table_name}
            ADD CONSTRAINT {fk_name}
            FOREIGN KEY (IDHora) REFERENCES dbo.DimHora(IDHora)
        """)
        conn.commit()
    
    # Verificar nuevas columnas
    cursor.execute("""
        SELECT COLUMN_NAME, DATA_TYPE 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'DimHora' AND TABLE_SCHEMA = 'dbo'
        ORDER BY ORDINAL_POSITION
    """)
    new_cols = cursor.fetchall()
    print("\nNuevas columnas:")
    for col_name, col_type in new_cols:
        print(f"  - {col_name}: {col_type}")
    
    print("\n" + "="*60)
    print("✅ DimHora actualizada exitosamente")
    print("="*60 + "\n")
    
except Exception as e:
    print(f"\n❌ ERROR: {e}")
finally:
    cursor.close()
    conn.close()
