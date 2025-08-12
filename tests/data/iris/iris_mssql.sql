IF DB_ID(N'app_db') IS NULL
BEGIN
  CREATE DATABASE [app_db];
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N'app')
BEGIN
  CREATE LOGIN [app] WITH PASSWORD = N'app', CHECK_POLICY = OFF, CHECK_EXPIRATION = OFF;
END;
GO

USE [app_db];
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'app')
BEGIN
  CREATE USER [app] FOR LOGIN [app];
END;
GO

IF NOT EXISTS (
  SELECT 1
  FROM sys.database_role_members drm
  JOIN sys.database_principals r ON drm.role_principal_id = r.principal_id
  JOIN sys.database_principals m ON drm.member_principal_id = m.principal_id
  WHERE r.name = N'db_owner' AND m.name = N'app'
)
BEGIN
  ALTER ROLE db_owner ADD MEMBER [app];
END;
GO

IF OBJECT_ID(N'dbo.iris', N'U') IS NOT NULL DROP TABLE dbo.iris;
CREATE TABLE dbo.iris (
  sepal_length FLOAT NULL,
  sepal_width  FLOAT NULL,
  petal_length FLOAT NULL,
  petal_width  FLOAT NULL,
  species NVARCHAR(50) NULL
);
GO

BULK INSERT dbo.iris
FROM '/data/iris.csv'
WITH (
  FIRSTROW = 2,
  FIELDTERMINATOR = ',',
  ROWTERMINATOR = '0x0a',
  TABLOCK
);
GO
