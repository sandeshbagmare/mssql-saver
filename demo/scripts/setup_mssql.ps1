# PowerShell script to configure SQL Server after installation
# Run as Administrator

param(
    [string]$SA_Password = "SqlPass123!"
)

$sqlcmd = "sqlcmd"

Write-Host "Creating langgraph databases..."

$sql = @"
CREATE DATABASE langgraph;
GO
CREATE DATABASE langgraph_test;
GO
SELECT name FROM sys.databases WHERE name LIKE 'langgraph%';
GO
"@

$sql | & $sqlcmd -S "localhost" -U "sa" -P $SA_Password

Write-Host "Enabling TCP/IP via registry (requires restart)..."
$tcpKey = "HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQLServer\SuperSocketNetLib\Tcp"
if (Test-Path $tcpKey) {
    Set-ItemProperty -Path $tcpKey -Name "Enabled" -Value 1
    Write-Host "TCP enabled. Restart SQL Server service to apply."
} else {
    Write-Host "TCP registry key not found — enable manually via SQL Server Configuration Manager."
}

Write-Host "Restarting SQL Server service..."
Restart-Service -Name "MSSQLSERVER" -Force -ErrorAction SilentlyContinue

Write-Host "Done. Test connection: sqlcmd -S localhost,1433 -U sa -P $SA_Password"
