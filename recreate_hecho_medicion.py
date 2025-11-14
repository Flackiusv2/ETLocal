"""
Script para recrear HechoMedicionAmbiental
"""
import pyodbc

conn_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=DW_Salud;Trusted_Connection=yes;'
conn = pyodbc.connect(conn_string, autocommit=True)
cursor = conn.cursor()

try:
    # Eliminar tabla vieja
    print("Eliminando HechoMedicionAmbiental vieja...")
    cursor.execute("DROP TABLE IF EXISTS dbo.HechoMedicionAmbiental")
    print("✓ Tabla eliminada")
    
    # Crear tabla nueva
    print("\nCreando HechoMedicionAmbiental nueva...")
    sql = """
    CREATE TABLE dbo.HechoMedicionAmbiental (
        IDMedicion BIGINT IDENTITY(1,1) PRIMARY KEY,
        IDFecha INT NOT NULL,
        IDHora INT NOT NULL,
        IDEstacion INT NOT NULL,
        ValorCO FLOAT NULL,
        ValorPM25 FLOAT NULL,
        CONSTRAINT FK_Med_Fecha FOREIGN KEY (IDFecha) REFERENCES dbo.DimFecha(IDFecha),
        CONSTRAINT FK_Med_Hora FOREIGN KEY (IDHora) REFERENCES dbo.DimHora(IDHora),
        CONSTRAINT FK_Med_Estacion FOREIGN KEY (IDEstacion) REFERENCES dbo.DimEstacion(IDEstacion)
    )
    """
    cursor.execute(sql)
    print("✓ Tabla creada con éxito")
    
    print("\n✅ HechoMedicionAmbiental lista para usar")
    
except Exception as e:
    print(f"✗ Error: {str(e)}")
finally:
    cursor.close()
    conn.close()
