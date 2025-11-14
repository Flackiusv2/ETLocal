"""
Script para actualizar la base de datos con las nuevas tablas
"""
import pyodbc

def execute_sql():
    # Conectar a SQL Server
    conn_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=DW_Salud;Trusted_Connection=yes;'
    conn = pyodbc.connect(conn_string, autocommit=True)
    cursor = conn.cursor()
    
    try:
        # 1. Crear DimEstacion si no existe
        print("Creando DimEstacion...")
        sql_dim_estacion = """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='DimEstacion')
        BEGIN
            CREATE TABLE dbo.DimEstacion (
                IDEstacion INT IDENTITY(1,1) PRIMARY KEY,
                NombreEstacion NVARCHAR(200) NOT NULL,
                TipoEstacion NVARCHAR(100),
                Ubicacion NVARCHAR(200)
            )
            PRINT 'Tabla DimEstacion creada'
        END
        ELSE
            PRINT 'Tabla DimEstacion ya existe'
        """
        cursor.execute(sql_dim_estacion)
        print("✓ DimEstacion OK")
        
        # 2. Verificar si HechoMedicionAmbiental necesita actualizarse
        print("\nVerificando HechoMedicionAmbiental...")
        cursor.execute("""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'HechoMedicionAmbiental' 
            AND COLUMN_NAME IN ('IDEstacion', 'ValorCO', 'ValorPM25')
        """)
        columns = [row[0] for row in cursor.fetchall()]
        
        if 'IDEstacion' not in columns:
            print("Necesita actualización de HechoMedicionAmbiental...")
            print("⚠ ATENCIÓN: La tabla HechoMedicionAmbiental necesita ser recreada")
            print("   Por favor ejecuta setup_database.sql manualmente desde SQL Server Management Studio")
        else:
            print("✓ HechoMedicionAmbiental ya tiene las columnas correctas")
        
        print("\n✅ Actualización completada")
        
    except Exception as e:
        print(f"✗ Error: {str(e)}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    execute_sql()
