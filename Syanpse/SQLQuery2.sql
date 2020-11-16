CREATE USER [adlssvc] FROM EXTERNAL PROVIDER
GO

EXEC sp_addrolemember 'db_owner', [adlssvc]
GO

SELECT * FROM sys.sql_logins WHERE name = '[adlssvc]';
GO


DROP USer [adlssvc];

Set-AzSqlServer -ResourceGroupName accenture -ServerName accsqldwsvr.database.windows.net -AssignIdentity

Set-AzSqlServer -ResourceGroupName accenture -ServerName accsqldwsvr -AssignIdentity

$xyz = Get-AzSqlServer  -ResourceGroupName accenture -ServerName accsqldwsvr
$xyz.identity

ResourceId               : /subscriptions/c46a9435-c957-4e6c-a0f4-b9a597984773/resourceGroups/accenture/providers/Micro
                           soft.Sql/servers/accsqldwsvr
ResourceGroupName        : accenture
ServerName               : accsqldwsvr
Location                 : eastus2
SqlAdministratorLogin    : sqladmin
SqlAdministratorPassword :
ServerVersion            : 12.0
Tags                     : {}
Identity                 : Microsoft.Azure.Management.Sql.Models.ResourceIdentity
FullyQualifiedDomainName : accsqldwsvr.database.windows.net
MinimalTlsVersion        :
PublicNetworkAccess      : Enabled


PS C:\Users\babal> $xyz = Get-AzSqlServer  -ResourceGroupName accenture -ServerName accsqldwsvr
>> $xyz.identity

PrincipalId                          Type           TenantId
-----------                          ----           --------
2f7ca793-4e18-46ab-8ea8-aac057e375d2 SystemAssigned 72f988bf-86f1-41af-91ab-2d7cd011db47

use master;
CREATE LOGIN adbingest WITH PASSWORD = 'Azure!2345678';

select * from dbo.test1;

select top 200 * from dbo.factdata1;

select top 200 * from dbo.factdata;

select count(*) from dbo.factdata;

select count(*) from dbo.factdata1;

truncate table dbo.factdata1;
