-- Script para crear la Base de Datos DW_Salud
-- Modelo Dimensional para BI de Salud

-- 0. Limpiar y crear DB
USE master;
GO

IF EXISTS (SELECT name FROM sys.databases WHERE name = 'DW_Salud')
BEGIN
    ALTER DATABASE DW_Salud SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DW_Salud;
END
GO

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
    Fecha DATE NOT NULL UNIQUE,
    Anio INT,
    Mes INT,
    Dia INT,
    NombreMes NVARCHAR(20),
    Trimestre INT,
    Bimestre INT
);
GO

CREATE TABLE dbo.DimHora (
    IDHora INT IDENTITY(1,1) PRIMARY KEY,
    Hora INT,
    RangoHorario NVARCHAR(20),
    Periodo NVARCHAR(5),
    HoraFormato NVARCHAR(10)
);
GO

CREATE TABLE dbo.DimClinica (
    IDClinica INT IDENTITY(1,1) PRIMARY KEY,
    CodigoCIE NVARCHAR(50),
    NombreClinica NVARCHAR(200),
    TipoHospitalizacion NVARCHAR(100),
    SourceFile NVARCHAR(200),
    CreatedAt DATETIME2 DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE dbo.DimPaciente (
    IDPaciente INT IDENTITY(1,1) PRIMARY KEY,
    Sexo NVARCHAR(20),
    GrupoEtario NVARCHAR(50),
    Ciudad NVARCHAR(100),
    Estrato INT,
    Migrante NVARCHAR(50),
    EnfoqueDiferencial NVARCHAR(100),
    RegimenSeguridadSocial NVARCHAR(100),
    SourceFile NVARCHAR(200),
    CreatedAt DATETIME2 DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE dbo.DimUbicacion (
    IDUbicacion INT IDENTITY(1,1) PRIMARY KEY,
    Barrio NVARCHAR(150),
    Localidad NVARCHAR(150),
    CodigoLocalidad NVARCHAR(50),
    Tipo NVARCHAR(100),
    SourceFile NVARCHAR(200)
);
GO

CREATE TABLE dbo.DimExposicion (
    IDExposicion INT IDENTITY(1,1) PRIMARY KEY,
    Indicador NVARCHAR(200),
    TipoIndicador NVARCHAR(100),
    SourceFile NVARCHAR(200)
);
GO

CREATE TABLE dbo.DimEstacion (
    IDEstacion INT IDENTITY(1,1) PRIMARY KEY,
    NombreEstacion NVARCHAR(200) NOT NULL,
    TipoEstacion NVARCHAR(100),
    Ubicacion NVARCHAR(200)
);
GO

-- 3. Tablas de Hecho
CREATE TABLE dbo.HechoHospitalizaciones (
    IDHospitalizacion BIGINT IDENTITY(1,1) PRIMARY KEY,
    IDClinica INT NULL,
    IDFecha INT NOT NULL,
    IDHora INT NULL,
    IDPaciente INT NULL,
    IDUbicacion INT NULL,
    NumeroCasos INT DEFAULT 1,
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
GO

CREATE TABLE dbo.HechoMedicionAmbiental (
    IDMedicion BIGINT IDENTITY(1,1) PRIMARY KEY,
    IDFecha INT NOT NULL,
    IDHora INT NOT NULL,
    Concentracion FLOAT NOT NULL,
    IDExposicion INT NOT NULL,
    IDUbicacion INT NOT NULL,
    CONSTRAINT FK_Med_Fecha FOREIGN KEY (IDFecha) REFERENCES dbo.DimFecha(IDFecha),
    CONSTRAINT FK_Med_Hora FOREIGN KEY (IDHora) REFERENCES dbo.DimHora(IDHora),
    CONSTRAINT FK_Med_Exposicion FOREIGN KEY (IDExposicion) REFERENCES dbo.DimExposicion(IDExposicion),
    CONSTRAINT FK_Med_Ubicacion FOREIGN KEY (IDUbicacion) REFERENCES dbo.DimUbicacion(IDUbicacion)
);
GO

-- Tabla de Análisis: Correlación por Localidad-Bimestre
CREATE TABLE dbo.AnalisisCorrelacion (
    IDAnalisis INT IDENTITY(1,1) PRIMARY KEY,
    CodigoLocalidad NVARCHAR(50) NOT NULL,
    Anio INT NOT NULL,
    Bimestre INT NOT NULL,
    Concentracion_avg FLOAT,
    NumMediciones INT,
    Hospitalizaciones INT,
    HospitalizacionRate FLOAT,
    UNIQUE (CodigoLocalidad, Anio, Bimestre)
);
GO

-- 4. Tablas de Staging
CREATE TABLE dbo.Stg_IRA_Agregado (
    Anio INT,
    NumeroCasos INT,
    SourceFile NVARCHAR(200)
);
GO

CREATE TABLE dbo.Stg_Neumonia (
    Anio INT,
    Sexo NVARCHAR(20),
    Migrante NVARCHAR(50),
    Localidad NVARCHAR(150),
    CodLocalidad NVARCHAR(50),
    EnfoqueDiferencial NVARCHAR(100),
    RegimenSeguridadSocial NVARCHAR(100),
    SourceFile NVARCHAR(200)
);
GO

CREATE TABLE dbo.Stg_IRA5Anios (
    Anio INT,
    Sexo NVARCHAR(20),
    Migrante NVARCHAR(50),
    Localidad NVARCHAR(150),
    CodLocalidad NVARCHAR(50),
    EnfoqueDiferencial NVARCHAR(100),
    RegimenSeguridadSocial NVARCHAR(100),
    SourceFile NVARCHAR(200)
);
GO

-- 5. Indices para mejorar performance
CREATE NONCLUSTERED INDEX IX_HechoHospitalizaciones_Fecha 
ON dbo.HechoHospitalizaciones(IDFecha);
GO

CREATE NONCLUSTERED INDEX IX_HechoHospitalizaciones_Paciente 
ON dbo.HechoHospitalizaciones(IDPaciente);
GO

CREATE NONCLUSTERED INDEX IX_HechoMedicionAmbiental_Fecha 
ON dbo.HechoMedicionAmbiental(IDFecha);
GO

PRINT 'Base de datos DW_Salud creada exitosamente!';
GO
