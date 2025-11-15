"""
Script para limpiar todas las tablas del DW
VERSIÓN MEJORADA: Usa transacciones y verifica el resultado
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
    # Conectar SIN autocommit para usar transacciones
    conn = pyodbc.connect(conn_string)
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("LIMPIANDO TODAS LAS TABLAS DEL DATA WAREHOUSE")
    print("="*60 + "\n")
    
    # PASO 1: Limpiar tablas de hechos primero (tienen FKs a dimensiones)
    print("PASO 1: Limpiando tablas de HECHOS...")
    tables_hechos = ['HechoMedicionAmbiental', 'HechoHospitalizaciones']
    
    for table in tables_hechos:
        try:
            # Verificar conteo antes
            cursor.execute(f"SELECT COUNT(*) FROM dbo.{table}")
            count_before = cursor.fetchone()[0]
            
            # Eliminar datos
            cursor.execute(f"DELETE FROM dbo.{table}")
            conn.commit()
            
            # Verificar conteo después
            cursor.execute(f"SELECT COUNT(*) FROM dbo.{table}")
            count_after = cursor.fetchone()[0]
            
            if count_after == 0:
                print(f"✓ {table:30} limpiada ({count_before:,} → {count_after} registros)")
            else:
                print(f"⚠ {table:30} NO se limpió completamente ({count_before:,} → {count_after:,} registros)")
                
        except Exception as e:
            print(f"❌ {table:30} ERROR: {str(e)}")
            conn.rollback()
    
    # PASO 2: Limpiar dimensiones
    print("\nPASO 2: Limpiando tablas de DIMENSIONES...")
    tables_dims = ['DimFecha', 'DimHora', 'DimClinica', 'DimPaciente', 'DimUbicacion', 'DimExposicion']
    
    for table in tables_dims:
        try:
            # Verificar conteo antes
            cursor.execute(f"SELECT COUNT(*) FROM dbo.{table}")
            count_before = cursor.fetchone()[0]
            
            # Eliminar datos
            cursor.execute(f"DELETE FROM dbo.{table}")
            conn.commit()
            
            # Verificar conteo después
            cursor.execute(f"SELECT COUNT(*) FROM dbo.{table}")
            count_after = cursor.fetchone()[0]
            
            if count_after == 0:
                print(f"✓ {table:30} limpiada ({count_before:,} → {count_after} registros)")
            else:
                print(f"⚠ {table:30} NO se limpió completamente ({count_before:,} → {count_after:,} registros)")
                
        except Exception as e:
            print(f"❌ {table:30} ERROR: {str(e)}")
            conn.rollback()
    
    print("\n" + "="*60)
    print("✅ PROCESO DE LIMPIEZA COMPLETADO")
    print("="*60 + "\n")
    
except Exception as e:
    print(f"\n❌ ERROR CRÍTICO: {str(e)}")
finally:
    cursor.close()
    conn.close()
