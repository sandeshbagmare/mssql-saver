-- Run as sa to create langgraph databases
-- Connect: sqlcmd -S localhost -U sa -P SqlPass123!

CREATE DATABASE langgraph;
GO
CREATE DATABASE langgraph_test;
GO

-- Verify TCP is enabled (run in SQL Server Configuration Manager if needed)
-- Or via T-SQL:
EXEC sys.sp_configure 'show advanced options', 1;
RECONFIGURE;
EXEC sys.sp_configure 'remote access', 1;
RECONFIGURE;
GO

-- Verify connection
SELECT @@VERSION;
GO
