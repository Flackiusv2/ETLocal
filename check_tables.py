"""
Script para verificar el estado de las tablas en DW_Salud
"""
import pyodbc

def check_tables():
    """Verifica el conteo de registros en todas las tablas"""
    
    conn_str = (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=localhost;'
        'DATABASE=DW_Salud;'
        'Trusted_Connection=yes;'
    )
    
    tables = [
        'DimFecha',
        'DimHora', 
        'DimClinica',
        'DimPaciente',
        'DimUbicacion',
        'DimExposicion',
        'HechoHospitalizaciones',
        'HechoMedicionAmbiental'
    ]
    
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        print("\n" + "="*60)
        print("ESTADO DE TABLAS EN DW_Salud")
        print("="*60 + "\n")
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            status = "❌ CON DATOS" if count > 0 else "✓ VACÍA"
            print(f"{table:30} {count:>10,} registros  {status}")
        
        conn.close()
        print("\n" + "="*60)
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    check_tables()
