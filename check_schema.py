"""
Script para verificar la estructura de las tablas
"""
import pyodbc

conn_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=DW_Salud;Trusted_Connection=yes;'

try:
    conn = pyodbc.connect(conn_string)
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("ESTRUCTURA DE HechoMedicionAmbiental")
    print("="*60 + "\n")
    
    cursor.execute("""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'HechoMedicionAmbiental' AND TABLE_SCHEMA = 'dbo'
        ORDER BY ORDINAL_POSITION
    """)
    cols = cursor.fetchall()
    for col_name, data_type, max_len in cols:
        if max_len:
            print(f"  {col_name}: {data_type}({max_len})")
        else:
            print(f"  {col_name}: {data_type}")
    
    print("\n" + "="*60)
    print("FOREIGN KEYS DE HechoMedicionAmbiental")
    print("="*60 + "\n")
    
    cursor.execute("""
        SELECT 
            fk.name AS FK_Name,
            OBJECT_NAME(fk.parent_object_id) AS TableName,
            COL_NAME(fc.parent_object_id, fc.parent_column_id) AS ColumnName,
            OBJECT_NAME(fk.referenced_object_id) AS ReferencedTable
        FROM sys.foreign_keys AS fk
        INNER JOIN sys.foreign_key_columns AS fc 
            ON fk.object_id = fc.constraint_object_id
        WHERE OBJECT_NAME(fk.parent_object_id) = 'HechoMedicionAmbiental'
    """)
    fks = cursor.fetchall()
    for fk_name, table, col, ref_table in fks:
        print(f"  {col} -> {ref_table}")
    
    print("\n" + "="*60)
    print("ESTRUCTURA DE DimExposicion")
    print("="*60 + "\n")
    
    cursor.execute("""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'DimExposicion' AND TABLE_SCHEMA = 'dbo'
        ORDER BY ORDINAL_POSITION
    """)
    cols = cursor.fetchall()
    for col_name, data_type, max_len in cols:
        if max_len:
            print(f"  {col_name}: {data_type}({max_len})")
        else:
            print(f"  {col_name}: {data_type}")
    
    conn.close()
    
except Exception as e:
    print(f"\n❌ ERROR: {e}")
