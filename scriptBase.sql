-- 0. Crear DB (ejecutar en el server)
CREATE DATABASE DW_Salud;
GO
USE DW_Salud;
GO

-- 1. Tabla de control de ETL
CREATE TABLE dbo.ETL_Control (
    ProcessName NVARCHAR(100) PRIMARY KEY,
    LastRun DATETIME2,
    LastFile NVARCHAR(200),
    RowsLoaded INT,
    Status NVARCHAR(20),
    Notes NVARCHAR(MAX)
);
GO

-- 2. Dimensiones
CREATE TABLE dbo.DimFecha (
    IDFecha INT IDENTITY(1,1) PRIMARY KEY,
    Fecha DATE NOT NULL,
    Año INT,
    Mes INT,
    Dia INT
);

CREATE TABLE dbo.DimHora (
    IDHora INT IDENTITY(1,1) PRIMARY KEY,
    HoraSmall TIME,
    HoraTexto NVARCHAR(20)
);

CREATE TABLE dbo.DimClinica (
    IDClinica INT IDENTITY(1,1) PRIMARY KEY,
    CodigoCIE NVARCHAR(50),
    NombreClinica NVARCHAR(200),
    TipoHospitalizacion NVARCHAR(100),
    -- Columnas adicionales...
    SourceFile NVARCHAR(200),
    CreatedAt DATETIME2 DEFAULT SYSUTCDATETIME()
);

CREATE TABLE dbo.DimPaciente (
    IDPaciente INT IDENTITY(1,1) PRIMARY KEY,
    Sexo NVARCHAR(10),
    GrupoEtario NVARCHAR(50),
    Ciudad NVARCHAR(100),
    Estrato INT,
    SourceFile NVARCHAR(200),
    CreatedAt DATETIME2 DEFAULT SYSUTCDATETIME()
);

CREATE TABLE dbo.DimUbicacion (
    IDUbicacion INT IDENTITY(1,1) PRIMARY KEY,
    Barrio NVARCHAR(150),
    Localidad NVARCHAR(150),
    Tipo NVARCHAR(100),
    SourceFile NVARCHAR(200)
);

CREATE TABLE dbo.DimExposicion (
    IDExposicion INT IDENTITY(1,1) PRIMARY KEY,
    Indicador NVARCHAR(200),
    SourceFile NVARCHAR(200)
);
GO

-- 3. Tablas de Hecho
CREATE TABLE dbo.HechoHospitalizaciones (
    IDHospitalizacion BIGINT IDENTITY(1,1) PRIMARY KEY,
    IDClinica INT NOT NULL,
    IDFecha INT NOT NULL,
    IDHora INT NULL,
    IDPaciente INT NULL,
    IDUbicacion INT NULL,
    -- métricas/flags
    TipoIngreso NVARCHAR(50),
    DuracionDias INT,
    SourceFile NVARCHAR(200),
    CreatedAt DATETIME2 DEFAULT SYSUTCDATETIME(),
    CONSTRAINT FK_Hosp_Clinica FOREIGN KEY (IDClinica) REFERENCES dbo.DimClinica(IDClinica),
    CONSTRAINT FK_Hosp_Fecha FOREIGN KEY (IDFecha) REFERENCES dbo.DimFecha(IDFecha),
    CONSTRAINT FK_Hosp_Hora FOREIGN KEY (IDHora) REFERENCES dbo.DimHora(IDHora),
    CONSTRAINT FK_Hosp_Paciente FOREIGN KEY (IDPaciente) REFERENCES dbo.DimPaciente(IDPaciente),
    CONSTRAINT FK_Hosp_Ubicacion FOREIGN KEY (IDUbicacion) REFERENCES dbo.DimUbicacion(IDUbicacion)
);

CREATE TABLE dbo.HechoMedicionAmbiental (
    IDMedicion BIGINT IDENTITY(1,1) PRIMARY KEY,
    IDExposicion INT,
    IDFecha INT,
    IDHora INT,
    IDUbicacion INT,
    Concentracion FLOAT,
    IDMedicionOrigen NVARCHAR(100),
    SourceFile NVARCHAR(200),
    CreatedAt DATETIME2 DEFAULT SYSUTCDATETIME(),
    CONSTRAINT FK_Med_Exposicion FOREIGN KEY (IDExposicion) REFERENCES dbo.DimExposicion(IDExposicion),
    CONSTRAINT FK_Med_Fecha FOREIGN KEY (IDFecha) REFERENCES dbo.DimFecha(IDFecha),
    CONSTRAINT FK_Med_Hora FOREIGN KEY (IDHora) REFERENCES dbo.DimHora(IDHora),
    CONSTRAINT FK_Med_Ubicacion FOREIGN KEY (IDUbicacion) REFERENCES dbo.DimUbicacion(IDUbicacion)
);
GO

-- 4. Ejemplo tabla staging (dinámica: por proceso podrías crear tablas staging temporales)
CREATE TABLE dbo.Stg_Hospitalizaciones (
    raw_ID NVARCHAR(200),
    ClinicaCodigo NVARCHAR(200),
    FechaSalida DATE,
    HoraSalida NVARCHAR(20),
    PacienteID NVARCHAR(200),
    Barrio NVARCHAR(200),
    TipoIngreso NVARCHAR(100),
    DuracionDias INT,
    SourceFile NVARCHAR(200)
);
GO
