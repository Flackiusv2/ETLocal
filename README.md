# ETL - Data Warehouse de Salud (DW_Salud)

Proyecto de ETL para cargar datos de enfermedades respiratorias en un modelo dimensional para análisis de BI.

## 📁 Estructura del Proyecto

```
ETL/
├── config/                     # Configuraciones
│   ├── db_config.py           # Conexión a SQL Server
│   └── __init__.py
├── src/                       # Código fuente
│   ├── extractors/            # Módulos de extracción por fuente
│   ├── transformers/          # Transformaciones por dimensión/hecho
│   ├── loaders/               # Carga a SQL Server
│   └── utils/                 # Utilidades (logging, helpers)
│       ├── logger.py
│       └── helpers.py
├── logs/                      # Logs de ejecución
├── data_raw/                  # Archivos CSV fuente
│   ├── ira-2012-2016.csv
│   ├── osb_enf_trans_neumonia.csv
│   ├── osb_enf_transm_ira5anos.csv
│   └── osb_metadatoenftransm-enferrespiratorias.csv
├── .env                       # Variables de entorno (NO SUBIR A GIT)
├── .gitignore                 
├── requirements.txt           # Dependencias Python
├── setup_database.sql         # Script SQL para crear BD
├── test_connection.py         # Script de prueba de conexión
└── main.py                    # Orquestador principal del ETL
```

## 🎯 Modelo Dimensional

### Dimensiones:
- **DimFecha**: Dimensión temporal (año, mes, día, trimestre)
- **DimHora**: Dimensión horaria
- **DimClinica**: Información clínica (CIE, tipo hospitalización)
- **DimPaciente**: Características del paciente (sexo, edad, estrato, régimen)
- **DimUbicacion**: Ubicación geográfica (barrio, localidad)
- **DimExposicion**: Indicadores de exposición ambiental

### Tablas de Hechos:
- **HechoHospitalizaciones**: Casos de hospitalizaciones por enfermedades respiratorias
- **HechoMedicionAmbiental**: Mediciones ambientales relacionadas

## 🚀 Instalación

### Prerrequisitos
- Python 3.13.5
- SQL Server 2022 Developer
- ODBC Driver 17 para SQL Server

### Paso 1: Instalar dependencias Python
```powershell
pip install -r requirements.txt
```

### Paso 2: Configurar variables de entorno
Edita el archivo `.env` con tu configuración:
```
DB_SERVER=localhost
DB_DATABASE=DW_Salud
DB_TRUSTED_CONNECTION=yes
```

### Paso 3: Crear la base de datos
```powershell
sqlcmd -S localhost -E -i setup_database.sql
```

### Paso 4: Probar conexión
```powershell
python test_connection.py
```

## 📊 Fuentes de Datos

| Archivo | Descripción | Formato |
|---------|-------------|---------|
| `ira-2012-2016.csv` | Casos agregados de IRA por año | Año;Casos |
| `osb_enf_trans_neumonia.csv` | Casos de neumonía con detalle | ANO;SEXO;MIGRANTE;... |
| `osb_enf_transm_ira5anos.csv` | IRA en menores de 5 años | ANO;SEXO;MIGRANTE;... |
| `osb_metadatoenftransm...csv` | Metadatos (referencia) | Descriptivo |

## 🔄 Flujo del ETL

1. **Extracción**: Lectura de CSV desde `data_raw/`
2. **Transformación**: 
   - Limpieza de datos
   - Estandarización de columnas
   - Cálculo de valores derivados
3. **Carga**:
   - Primero dimensiones (orden de dependencias)
   - Luego tablas de hechos

## 📝 Uso

### Ejecutar ETL completo
```powershell
python main.py
```

### Ejecutar por módulos
```powershell
# Solo extracción
python -m src.extractors.extract_ira

# Solo transformación
python -m src.transformers.transform_paciente

# Solo carga
python -m src.loaders.load_dimensions
```

## 📈 Conectar a Power BI

1. Abrir Power BI Desktop
2. Obtener datos > SQL Server
3. Servidor: `localhost`
4. Base de datos: `DW_Salud`
5. Seleccionar tablas de dimensiones y hechos

## 🛠️ Desarrollo

### Agregar nueva fuente de datos
1. Crear extractor en `src/extractors/`
2. Crear transformer en `src/transformers/`
3. Actualizar loader correspondiente
4. Registrar en `main.py`

### Logging
Todos los logs se guardan en `logs/` con timestamp.

## 📚 Documentación Técnica

- **Conexión DB**: `config/db_config.py`
- **Logging**: `src/utils/logger.py`
- **Helpers**: `src/utils/helpers.py`

## ⚠️ Troubleshooting

### Error: "ODBC Driver not found"
Instalar: https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

### Error: "Login failed"
Verificar autenticación en `.env`

### Datos no cargan
Revisar logs en `logs/` para detalles del error

## 👨‍💻 Autor

Proyecto de BI - Universidad 2025-2

## 📄 Licencia

Uso académico
