"""
Script para actualizar el esquema de HechoHospitalizaciones
Elimina las columnas SourceFile y CreatedAt que ya no son necesarias
"""
import pyodbc

def fix_hecho_hospitalizaciones_schema():
    """Actualiza el esquema de HechoHospitalizaciones"""
    
    conn_str = (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=localhost;'
        'DATABASE=DW_Salud;'
        'Trusted_Connection=yes;'
    )
    
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        print("\n" + "="*60)
        print("ACTUALIZANDO ESQUEMA DE HechoHospitalizaciones")
        print("="*60 + "\n")
        
        # 1. Verificar columnas actuales
        cursor.execute("""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'HechoHospitalizaciones'
            ORDER BY ORDINAL_POSITION
        """)
        print("Columnas actuales:")
        for row in cursor.fetchall():
            print(f"  - {row[0]}")
        
        # 2. Eliminar columna SourceFile si existe
        print("\n1. Eliminando columna SourceFile...")
        try:
            cursor.execute("ALTER TABLE HechoHospitalizaciones DROP COLUMN SourceFile")
            print("   ✓ Columna SourceFile eliminada")
        except Exception as e:
            if "does not exist" in str(e) or "no existe" in str(e):
                print("   ℹ Columna SourceFile no existe (ya fue eliminada)")
            else:
                print(f"   ⚠ Error: {e}")
        
        # 3. Eliminar constraint DEFAULT de CreatedAt primero
        print("\n2. Eliminando constraint DEFAULT de CreatedAt...")
        try:
            # Buscar el nombre de la constraint
            cursor.execute("""
                SELECT dc.name
                FROM sys.default_constraints dc
                JOIN sys.columns c ON dc.parent_column_id = c.column_id
                JOIN sys.tables t ON dc.parent_object_id = t.object_id
                WHERE t.name = 'HechoHospitalizaciones' AND c.name = 'CreatedAt'
            """)
            constraint = cursor.fetchone()
            if constraint:
                constraint_name = constraint[0]
                cursor.execute(f"ALTER TABLE HechoHospitalizaciones DROP CONSTRAINT {constraint_name}")
                print(f"   ✓ Constraint {constraint_name} eliminada")
            else:
                print("   ℹ No se encontró constraint DEFAULT")
        except Exception as e:
            print(f"   ⚠ Error al eliminar constraint: {e}")
        
        # 4. Eliminar columna CreatedAt si existe
        print("\n3. Eliminando columna CreatedAt...")
        try:
            cursor.execute("ALTER TABLE HechoHospitalizaciones DROP COLUMN CreatedAt")
            print("   ✓ Columna CreatedAt eliminada")
        except Exception as e:
            if "does not exist" in str(e) or "no existe" in str(e):
                print("   ℹ Columna CreatedAt no existe (ya fue eliminada)")
            else:
                print(f"   ⚠ Error: {e}")
        
        conn.commit()
        
        # 5. Verificar columnas finales
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'HechoHospitalizaciones'
            ORDER BY ORDINAL_POSITION
        """)
        print("\n4. Columnas finales:")
        for row in cursor.fetchall():
            print(f"  - {row[0]:25} {row[1]:15} {'NULL' if row[2] == 'YES' else 'NOT NULL'}")
        
        conn.close()
        print("\n" + "="*60)
        print("✓ Esquema actualizado correctamente")
        print("="*60 + "\n")
        
    except Exception as e:
        print(f"\n❌ Error: {e}\n")
        raise

if __name__ == "__main__":
    fix_hecho_hospitalizaciones_schema()
