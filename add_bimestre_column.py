"""
Script para agregar la columna Bimestre a DimFecha
y actualizar sus valores basados en el mes
"""
import pyodbc
import sys
import io
from dotenv import load_dotenv
import os

# Configurar encoding para Windows PowerShell
if sys.platform == 'win32':
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    else:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Cargar variables de entorno
load_dotenv()

def add_bimestre_column():
    """Agrega la columna Bimestre a DimFecha y calcula sus valores"""
    
    server = os.getenv('DB_SERVER', 'localhost')
    driver = os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server')
    database = 'DW_Salud'
    
    print("="*70)
    print(" " * 15 + "AGREGANDO COLUMNA BIMESTRE A DimFecha")
    print("="*70)
    
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"Trusted_Connection=yes;"
    )
    
    try:
        print("\n1. Conectando a DW_Salud...")
        conn = pyodbc.connect(conn_str, autocommit=False)
        cursor = conn.cursor()
        print("   ✓ Conexión exitosa")
        
        # Verificar si la columna ya existe
        print("\n2. Verificando si la columna Bimestre existe...")
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'DimFecha' 
            AND COLUMN_NAME = 'Bimestre'
        """)
        exists = cursor.fetchone()[0]
        
        if exists:
            print("   ⚠ La columna Bimestre ya existe")
            respuesta = input("\n   ¿Deseas recalcular los valores? (s/n): ")
            if respuesta.lower() != 's':
                print("\n   Cancelando operación...")
                conn.close()
                return False
        else:
            # Agregar la columna
            print("\n3. Agregando columna Bimestre...")
            cursor.execute("ALTER TABLE dbo.DimFecha ADD Bimestre INT NULL")
            conn.commit()
            print("   ✓ Columna agregada")
        
        # Actualizar valores de Bimestre basados en el Mes
        print("\n4. Calculando valores de Bimestre...")
        
        # Bimestre 1: Enero-Febrero
        cursor.execute("UPDATE dbo.DimFecha SET Bimestre = 1 WHERE Mes IN (1, 2)")
        print("   ✓ Bimestre 1 (Ene-Feb) calculado")
        
        # Bimestre 2: Marzo-Abril
        cursor.execute("UPDATE dbo.DimFecha SET Bimestre = 2 WHERE Mes IN (3, 4)")
        print("   ✓ Bimestre 2 (Mar-Abr) calculado")
        
        # Bimestre 3: Mayo-Junio
        cursor.execute("UPDATE dbo.DimFecha SET Bimestre = 3 WHERE Mes IN (5, 6)")
        print("   ✓ Bimestre 3 (May-Jun) calculado")
        
        # Bimestre 4: Julio-Agosto
        cursor.execute("UPDATE dbo.DimFecha SET Bimestre = 4 WHERE Mes IN (7, 8)")
        print("   ✓ Bimestre 4 (Jul-Ago) calculado")
        
        # Bimestre 5: Septiembre-Octubre
        cursor.execute("UPDATE dbo.DimFecha SET Bimestre = 5 WHERE Mes IN (9, 10)")
        print("   ✓ Bimestre 5 (Sep-Oct) calculado")
        
        # Bimestre 6: Noviembre-Diciembre
        cursor.execute("UPDATE dbo.DimFecha SET Bimestre = 6 WHERE Mes IN (11, 12)")
        print("   ✓ Bimestre 6 (Nov-Dic) calculado")
        
        conn.commit()
        
        # Verificar resultados
        print("\n5. Verificando actualización...")
        cursor.execute("""
            SELECT Bimestre, COUNT(*) as Total 
            FROM dbo.DimFecha 
            GROUP BY Bimestre 
            ORDER BY Bimestre
        """)
        
        resultados = cursor.fetchall()
        print("   Distribución por Bimestre:")
        for bimestre, total in resultados:
            print(f"   ✓ Bimestre {bimestre}: {total:,} registros")
        
        # Mostrar ejemplo
        print("\n6. Ejemplo de registros:")
        cursor.execute("""
            SELECT TOP 5 Fecha, Mes, NombreMes, Bimestre, Trimestre 
            FROM dbo.DimFecha 
            ORDER BY Fecha
        """)
        
        ejemplos = cursor.fetchall()
        print(f"\n   {'Fecha':<12} {'Mes':<4} {'NombreMes':<12} {'Bim':<4} {'Trim':<4}")
        print("   " + "-"*40)
        for fecha, mes, nombre_mes, bimestre, trimestre in ejemplos:
            print(f"   {str(fecha):<12} {mes:<4} {nombre_mes:<12} {bimestre:<4} {trimestre:<4}")
        
        cursor.close()
        conn.close()
        
        print("\n" + "="*70)
        print("✅ COLUMNA BIMESTRE AGREGADA Y CALCULADA EXITOSAMENTE")
        print("="*70)
        print("\nResumen:")
        print("  • Se agregó la columna Bimestre a DimFecha")
        print("  • Se calcularon los valores para todas las fechas")
        print("  • Bimestre 1: Enero-Febrero")
        print("  • Bimestre 2: Marzo-Abril")
        print("  • Bimestre 3: Mayo-Junio")
        print("  • Bimestre 4: Julio-Agosto")
        print("  • Bimestre 5: Septiembre-Octubre")
        print("  • Bimestre 6: Noviembre-Diciembre")
        print("="*70)
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        conn.rollback()
        return False

if __name__ == "__main__":
    success = add_bimestre_column()
    if not success:
        exit(1)

