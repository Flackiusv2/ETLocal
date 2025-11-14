import sqlite3
import pandas as pd
import numpy as np
from scipy import stats
import statsmodels.api as sm
from statsmodels.formula.api import ols
from statsmodels.stats.multicomp import pairwise_tukeyhsd

# =======================
# Conexión a la base de datos
# =======================
conn = sqlite3.connect("C:/Users/maria/Downloads/sample.db")

# =======================
# Función para ANOVA
# =======================
def run_anova(df, dependent, factor):
    """
    Ejecuta ANOVA unidireccional y retorna tabla con SS, df, MS, F, p, Eta2 y decisión H0
    """
    model = ols(f'{dependent} ~ C({factor})', data=df).fit()
    anova_table = sm.stats.anova_lm(model, typ=2)
    anova_table['Eta2'] = anova_table['sum_sq'] / anova_table['sum_sq'].sum()
    anova_table['Decision_H0'] = np.where(anova_table['PR(>F)'] < 0.05, 'Rechaza H0', 'No Rechaza H0')
    return anova_table.reset_index().rename(columns={'index':'Factor'})

# =======================
# Extraer datos
# =======================
# Hospitalizaciones
hospitalizaciones = pd.read_sql("""
SELECT h.IDHospitalizacion,
       h.IDPaciente,
       h.IDUbicacion,
       f.Año,
       f.Mes,
       f.Fecha
FROM HechoHospitalizacionesAsma h
JOIN DimFecha f ON h.IDFecha = f.IDFecha
""", conn)

# Mediciones de contaminación
exposicion = pd.read_sql("""
SELECT m.IDMedicion,
       m.IDUbicacion,
       f.Año,
       f.Mes,
       e.Indicador,
       m.Concentracion
FROM HechoMedicionAmbiental m
JOIN DimExposicion e ON m.IDExposicion = e.IDExposicion
JOIN DimFecha f ON m.IDFecha = f.IDFecha
""", conn)

# Ubicaciones
ubicaciones = pd.read_sql("SELECT IDUbicacion, Localidad FROM DimUbicacion", conn)

# =======================
# Calcular población por localidad (aproximación)
# =======================
pacientes = hospitalizaciones[['IDPaciente', 'IDUbicacion']].drop_duplicates()
pacientes = pacientes.merge(ubicaciones, on='IDUbicacion', how='left')
df_pob = pacientes.groupby('Localidad')['IDPaciente'].nunique().reset_index().rename(columns={'IDPaciente':'Poblacion'})

# =======================
# Agregaciones
# =======================
# Hospitalizaciones por localidad, año y mes
hosp_agg = hospitalizaciones.merge(ubicaciones, on='IDUbicacion', how='left')
hosp_agg2 = hosp_agg.groupby(['Localidad','Año','Mes'])['IDHospitalizacion'].count().reset_index().rename(columns={'IDHospitalizacion':'Conteo'})

# PM2.5 promedio por localidad, año y mes
pm_agg = exposicion.merge(ubicaciones, on='IDUbicacion', how='left')
pm_agg2 = pm_agg[pm_agg['Indicador']=='PM2.5'].groupby(['Localidad','Año','Mes'])['Concentracion'].mean().reset_index()

# Merge final
data_agg = pd.merge(hosp_agg2, pm_agg2, on=['Localidad','Año','Mes'], how='inner')
data_agg = pd.merge(data_agg, df_pob, on='Localidad', how='left')

# Filtrar para evitar división por cero
data_agg = data_agg[data_agg['Poblacion'] > 0]

# Calcular tasa por 100k
data_agg['Tasa'] = data_agg['Conteo'] / data_agg['Poblacion'] * 100000

# =======================
# Crear cuartiles de PM2.5
# =======================
try:
    data_agg['PM2_5_Cuartil'] = pd.qcut(data_agg['Concentracion'], 4, labels=False) + 1
except ValueError:
    print("No se pudieron crear 4 cuartiles, asignando todo a 1")
    data_agg['PM2_5_Cuartil'] = 1

# =======================
# ANOVA y Post-hoc
# =======================
if data_agg['PM2_5_Cuartil'].nunique() < 2:
    print("No hay suficientes grupos para ANOVA.")
else:
    # ANOVA para Conteo
    anova_conteo = run_anova(data_agg, dependent='Conteo', factor='PM2_5_Cuartil')
    anova_conteo.to_sql('Tabla_ANOVA_PM2_5_Conteo', conn, if_exists='replace', index=False)
    
    # ANOVA para Tasa
    anova_tasa = run_anova(data_agg, dependent='Tasa', factor='PM2_5_Cuartil')
    anova_tasa.to_sql('Tabla_ANOVA_PM2_5_Tasa', conn, if_exists='replace', index=False)

    # Post-hoc Tukey HSD para Conteo
    tukey_conteo = pairwise_tukeyhsd(endog=data_agg['Conteo'], groups=data_agg['PM2_5_Cuartil'], alpha=0.05)
    tukey_df_conteo = pd.DataFrame(data=tukey_conteo._results_table.data[1:], columns=tukey_conteo._results_table.data[0])
    tukey_df_conteo.to_sql('Tabla_PostHoc_PM2_5_Conteo', conn, if_exists='replace', index=False)

    # Post-hoc Tukey HSD para Tasa
    tukey_tasa = pairwise_tukeyhsd(endog=data_agg['Tasa'], groups=data_agg['PM2_5_Cuartil'], alpha=0.05)
    tukey_df_tasa = pd.DataFrame(data=tukey_tasa._results_table.data[1:], columns=tukey_tasa._results_table.data[0])
    tukey_df_tasa.to_sql('Tabla_PostHoc_PM2_5_Tasa', conn, if_exists='replace', index=False)

# =======================
# Cerrar conexión
# =======================
conn.close()
