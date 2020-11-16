# Spark processing dataframe and insert data to azure synapse

## Use Case

- Process data from parquet
- Create sql join
- Insert processed data into Syanpse DW using ADB connector
- Insert processed data into Synapse DW using jdbc
- For Jdbc use service principal and sql user

## Architecture

![alt text](https://github.com/balakreshnan/ADBsynapseDW/blob/main/images/Dataengineering.jpg "Architecture")

## Setup

- Azure databricks version 7.3
- SPark 3.0
- Azure Synapse Analytics - DW100c
- Azure Datalake Store Gen 2
- Azure Keyvaults

## Steps and Code

- First load all the configs from key vault
- Secrets are stored in key vault
- follow the documentation to add to keyvault as secret scopes.

```
val accbbstorekey = dbutils.secrets.get(scope = "secretstore", key = "accbbstore")
val svcpid = dbutils.secrets.get(scope = "secretstore", key = "svcpid")
val scvpsecret = dbutils.secrets.get(scope = "secretstore", key = "scvpsecret")
val tenantid = dbutils.secrets.get(scope = "secretstore", key = "tenantid")
val sqlsapassword = dbutils.secrets.get(scope = "secretstore", key = "sqlsapassword")
val sqluserpass = dbutils.secrets.get(scope = "secretstore", key = "sqluserpass")
val svcname = dbutils.secrets.get(scope = "allsecrects", key = "svcname")
```

- Now configure the underlying storage

```
spark.conf.set(
  "fs.azure.account.key.storageaccountname.blob.core.windows.net",
  accbbstorekey)
```

- Create the fact table

```
%sql
CREATE TEMPORARY VIEW factdata
USING org.apache.spark.sql.parquet
OPTIONS (
  path "wasbs://containername@storagename.blob.core.windows.net/fact/*.parquet"
)
```

- Now time to create dimensions

- Start with State

```
%sql
CREATE TEMPORARY VIEW dimstate
USING org.apache.spark.sql.parquet
OPTIONS (
  path "wasbs://containername@storagename.blob.core.windows.net/dimstate/*.parquet"
)
```

- Create table county

```
%sql
CREATE TEMPORARY VIEW dimcounty
USING org.apache.spark.sql.parquet
OPTIONS (
  path "wasbs://containername@storagename.blob.core.windows.net/dimcountryname/*.parquet"
)
```

- Create Table for Race

```
%sql
CREATE TEMPORARY VIEW dimrace
USING org.apache.spark.sql.parquet
OPTIONS (
  path "wasbs://containername@storagename.blob.core.windows.net/dimRace/*.parquet"
)
```

- Create Table for Sex

```
%sql
CREATE TEMPORARY VIEW dimsex
USING org.apache.spark.sql.parquet
OPTIONS (
  path "wasbs://containername@storagename.blob.core.windows.net/dimsex/*.parquet"
)
```

- Now create some joins to test the query 

```
%sql
Select factdata.*, dimstate.StateName as dimStateName, dimcounty.CountyName as dimCountyName, 
dimrace.Race as dimRace, dimsex.Sex as dimSex
from factdata inner join dimstate on factdata.StateName = dimstate.StateName 
join dimcounty on factdata.countyName = dimcounty.CountyName
join dimrace on factdata.race = dimrace.Race
join dimsex on factdata.sex = dimsex.Sex
```

- Now let's test the Azure synapse DW read 
- I have create a simple one column dummy table
- test1 only to validate read is succesful

```
val jdbcconnstr = "jdbc:sqlserver://servername.database.windows.net:1433;database=dbname;user=sqluser@accsqldwsvr;password=" + sqlsapassword + ";encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
```

```
val dfread = spark.read
  .format("com.databricks.spark.sqldw")
  .option("url", jdbcconnstr)
  .option("tempDir", "wasbs://containername@storagename.blob.core.windows.net/temp")
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("query", "select * from dbo.test1")
  .load()
```

- display data

```
display(dfread)
```

- imports for sql activities

```
import sqlContext.implicits._
```

- Bring the SQL data into dataframe to write back

```
val dffact = sqlContext.sql("Select factdata.*, dimstate.StateName as dimStateName, dimcounty.CountyName as dimCountyName, dimrace.Race as dimRace, dimsex.Sex as dimSex from factdata inner join dimstate on factdata.StateName = dimstate.StateName join dimcounty on factdata.countyName = dimcounty.CountyName join dimrace on factdata.race = dimrace.Race join dimsex on factdata.sex = dimsex.Sex")
```

## Using databricks bulk connector 

```
display(dffact)
```

- To write faster using sql username and azure databricks sql dw connector. 
- Write to table called dbo.factdata
- bulk write uses polybase to stage and bulk inserts to sql dw
- No service principal service support

```
dffact.write
  .format("com.databricks.spark.sqldw")
  .option("url", jdbcconnstr)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", "dbo.factdata")
  .option("tempDir", "wasbs://containenamer@storagename.blob.core.windows.net/tempfact")
  .save()
```

- Note the time taken

```
Command took 33.46 seconds -- by xxxx at 11/14/2020, 9:14:09 AM on devcluster
```

- Now change parition

```
dffact.repartition(10).write
  .format("com.databricks.spark.sqldw")
  .option("url", jdbcconnstr)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", "dbo.factdata")
  .option("tempDir", "wasbs://container@storageacct.blob.core.windows.net/tempfact")
  .mode("overwrite")
  .save()
```

- Now the time

```
Command took 26.57 seconds -- by xxxx at 11/14/2020, 9:18:49 AM on devcluster
```

- There are close 2 millions rows of data (2137632).
- close to 74,000 rows loaded per second
- Math is 2137632 / 26.57 seconds = 80,453

## Use JDBC driver using sql and service principal authentication

```
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
```

- now jbdc settings

```
var connection:java.sql.Connection = _
var statement:java.sql.Statement = _

val jdbcUsername = "sqluser"
val jdbcPassword = sqlsapassword
val jdbcHostname = "servername.database.windows.net" //typically, this is in the form or servername.database.windows.net
val jdbcPort = 1433
val jdbcDatabase ="dbname"
val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
val jdbc_url = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
```

- now imports

```
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import java.sql.{Connection,DriverManager,ResultSet}
```

- Now take the same dataframe and insert using JDBC

```
connection = DriverManager.getConnection(jdbc_url, jdbcUsername, jdbcPassword)

dffact.take(100).foreach { row =>
  //println(row.mkString(",").split(",")(0) + "-" + row.mkString(",").split(",")(1)) 
    
    statement = connection.createStatement
    //true   
  
    val valueStr = "'" + row.mkString(",").split(",")(0) + "'," + "'" + row.mkString(",").split(",")(1) + "'," + "'" + row.mkString(",").split(",")(2) + "'," + "'" + row.mkString(",").split(",")(3) + "'," + "'" + row.mkString(",").split(",")(4) + "'," + "'" + row.mkString(",").split(",")(5) + "'," + "'" + row.mkString(",").split(",")(6) + "'," + "'" + row.mkString(",").split(",")(7) + "'"

    println(valueStr)    
    statement.execute("INSERT INTO " + "dbo.factdata1" + " VALUES (" + valueStr + ")")
   
}
connection.close
```

- Note the time taken

```
Command took 1.60 minutes -- by  at 11/14/2020, 9:24:41 AM on devcluster
```

- Now run with parition

```
connection = DriverManager.getConnection(jdbc_url, jdbcUsername, jdbcPassword)

dffact.repartition(10).take(1000).foreach { row =>
  //println(row.mkString(",").split(",")(0) + "-" + row.mkString(",").split(",")(1)) 
    
    statement = connection.createStatement
    //true   
  
    val valueStr = "'" + row.mkString(",").split(",")(0) + "'," + "'" + row.mkString(",").split(",")(1) + "'," + "'" + row.mkString(",").split(",")(2) + "'," + "'" + row.mkString(",").split(",")(3) + "'," + "'" + row.mkString(",").split(",")(4) + "'," + "'" + row.mkString(",").split(",")(5) + "'," + "'" + row.mkString(",").split(",")(6) + "'," + "'" + row.mkString(",").split(",")(7) + "'"

    println(valueStr)    
    statement.execute("INSERT INTO " + "dbo.factdata1" + " VALUES (" + valueStr + ")")
   
}
connection.close
```

```
Command took 1.70 minutes -- by xxxx at 11/14/2020, 9:29:35 AM on devcluster
```

- JDBC averages about 8 or so records with DW100c

- Now configure service principal account
- Follow this instruction for adding service principal to Azure SQL DW
- https://docs.microsoft.com/en-us/azure/azure-sql/database/authentication-aad-service-principal-tutorial#create-the-service-principal-user-in-azure-sql-database
- Wait for 20 minutes or so to propagate the changes
- Store the service principal account name and secret in Azure Keyvault
- Configure JDBC now
- If you look below the username is service principal name
- jdbc password is the service principal secret.

```
var connection:java.sql.Connection = _
var statement:java.sql.Statement = _

val jdbcUsername = svcname
val jdbcPassword = scvpsecret
val jdbcHostname = "servername.database.windows.net" //typically, this is in the form or servername.database.windows.net
val jdbcPort = 1433
val jdbcDatabase ="dbname"
val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
val jdbc_url = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
```

- Write to SQL DW using Service principal
- Still have to test

```
connection = DriverManager.getConnection(jdbc_url, jdbcUsername, jdbcPassword)

dffact.take(1000).foreach { row =>
  //println(row.mkString(",").split(",")(0) + "-" + row.mkString(",").split(",")(1)) 
    
    statement = connection.createStatement
    //true   
  
    val valueStr = "'" + row.mkString(",").split(",")(0) + "'," + "'" + row.mkString(",").split(",")(1) + "'," + "'" + row.mkString(",").split(",")(2) + "'," + "'" + row.mkString(",").split(",")(3) + "'," + "'" + row.mkString(",").split(",")(4) + "'," + "'" + row.mkString(",").split(",")(5) + "'," + "'" + row.mkString(",").split(",")(6) + "'," + "'" + row.mkString(",").split(",")(7) + "'"

    println(valueStr)    
    statement.execute("INSERT INTO " + "dbo.factdata1" + " VALUES (" + valueStr + ")")
   
}
connection.close
```

## Scale Azure Synapse Analytics

- Scale to DW200c
- Run inserting Bulk using databricks connector

## Using databricks bulk connector 

```
display(dffact)
```

- To write faster using sql username and azure databricks sql dw connector. 
- Write to table called dbo.factdata
- bulk write uses polybase to stage and bulk inserts to sql dw
- No service principal service support

```
dffact.write
  .format("com.databricks.spark.sqldw")
  .option("url", jdbcconnstr)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", "dbo.factdata")
  .option("tempDir", "wasbs://containenamer@storagename.blob.core.windows.net/tempfact")
  .save()
```

- Note the time taken

```
Command took 23.60 seconds -- by xxxx at 11/14/2020, 9:14:09 AM on devcluster
```

- Now change parition

```
dffact.repartition(10).write
  .format("com.databricks.spark.sqldw")
  .option("url", jdbcconnstr)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", "dbo.factdata")
  .option("tempDir", "wasbs://container@storageacct.blob.core.windows.net/tempfact")
  .mode("overwrite")
  .save()
```

- Now the time

```
Command took 19.76 seconds -- by xxxx at 11/14/2020, 9:18:49 AM on devcluster
```

- There are close 2 millions rows of data (2137632).
- close to 74,000 rows loaded per second
- Math is 2137632 / 23.60 seconds = 90,578

## Use JDBC driver using sql and service principal authentication

```
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
```

- now jbdc settings

```
var connection:java.sql.Connection = _
var statement:java.sql.Statement = _

val jdbcUsername = "sqluser"
val jdbcPassword = sqlsapassword
val jdbcHostname = "servername.database.windows.net" //typically, this is in the form or servername.database.windows.net
val jdbcPort = 1433
val jdbcDatabase ="dbname"
val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
val jdbc_url = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
```

- now imports

```
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import java.sql.{Connection,DriverManager,ResultSet}
```

- Now take the same dataframe and insert using JDBC

```
connection = DriverManager.getConnection(jdbc_url, jdbcUsername, jdbcPassword)

dffact.take(100).foreach { row =>
  //println(row.mkString(",").split(",")(0) + "-" + row.mkString(",").split(",")(1)) 
    
    statement = connection.createStatement
    //true   
  
    val valueStr = "'" + row.mkString(",").split(",")(0) + "'," + "'" + row.mkString(",").split(",")(1) + "'," + "'" + row.mkString(",").split(",")(2) + "'," + "'" + row.mkString(",").split(",")(3) + "'," + "'" + row.mkString(",").split(",")(4) + "'," + "'" + row.mkString(",").split(",")(5) + "'," + "'" + row.mkString(",").split(",")(6) + "'," + "'" + row.mkString(",").split(",")(7) + "'"

    println(valueStr)    
    statement.execute("INSERT INTO " + "dbo.factdata1" + " VALUES (" + valueStr + ")")
   
}
connection.close
```

- Note the time taken

```
Command took 1.31 minutes -- by  at 11/14/2020, 9:24:41 AM on devcluster
```

- Now run with parition

```
connection = DriverManager.getConnection(jdbc_url, jdbcUsername, jdbcPassword)

dffact.repartition(1).take(1000).foreach { row =>
  //println(row.mkString(",").split(",")(0) + "-" + row.mkString(",").split(",")(1)) 
    
    statement = connection.createStatement
    //true   
  
    val valueStr = "'" + row.mkString(",").split(",")(0) + "'," + "'" + row.mkString(",").split(",")(1) + "'," + "'" + row.mkString(",").split(",")(2) + "'," + "'" + row.mkString(",").split(",")(3) + "'," + "'" + row.mkString(",").split(",")(4) + "'," + "'" + row.mkString(",").split(",")(5) + "'," + "'" + row.mkString(",").split(",")(6) + "'," + "'" + row.mkString(",").split(",")(7) + "'"

    println(valueStr)    
    statement.execute("INSERT INTO " + "dbo.factdata1" + " VALUES (" + valueStr + ")")
   
}
connection.close
```

```
Command took 1.39 minutes -- by xxxx at 11/14/2020, 9:29:35 AM on devcluster
```

- JDBC averages about 9 or so records with DW200c

- Now configure service principal account
- Follow this instruction for adding service principal to Azure SQL DW
- https://docs.microsoft.com/en-us/azure/azure-sql/database/authentication-aad-service-principal-tutorial#create-the-service-principal-user-in-azure-sql-database
- Wait for 20 minutes or so to propagate the changes
- Store the service principal account name and secret in Azure Keyvault
- Configure JDBC now
- If you look below the username is service principal name
- jdbc password is the service principal secret.

```

## Create table to hold the data for below

```
Drop table dbo.factdata2;

create table dbo.factdata2
(
id bigint identity(1,1),
decennialTime varchar(500),
stateName varchar(500),
countyName varchar(500),
population int,
race varchar(500),
sex varchar(100)
)
WITH ( CLUSTERED COLUMNSTORE INDEX )  


select * from dbo.factdata2;

select count(*) from dbo.factdata2;

truncate table dbo.factdata2;
```

## Now Time to try with Service principal

- Create a Service Principal account
- Store their token in Azure keyvault
- Install ADSAL library - com.microsoft.azure:adal4j:1.6.6
- Install Spark SQL library - com.microsoft.azure:azure-sqldb-spark:1.0.2
- Lets create the code

- import libraries

```
import com.microsoft.aad.adal4j.ClientCredential
import com.microsoft.aad.adal4j.AuthenticationContext
import java.util.concurrent.Executors
```

- now configure the service principal information

```
val TenantId = tenantid
val authority = "https://login.windows.net/" + TenantId
val resourceAppIdURI = "https://database.windows.net/"
val ServicePrincipalId = "svc application id"
val ServicePrincipalPwd = scvpsecret
```

- Initiate authenticator

```
val service = Executors.newFixedThreadPool(1)
val context = new AuthenticationContext(authority, true, service);
```

- Now time to get Access Token

```
//Get access token
val ClientCred = new ClientCredential(ServicePrincipalId, ServicePrincipalPwd)
val authResult = context.acquireToken(resourceAppIdURI, ClientCred, null)
val accessToken = authResult.get().getAccessToken
```

- now time to create connection properties

```
val jdbcHostname = "servername.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "dbname"

// Create the JDBC URL without passing in the user and password parameters.
val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"

// Create a Properties() object to hold the parameters.
import java.util.Properties
val connectionProperties = new Properties()


connectionProperties.put("accessToken", accessToken)
```

- initiate the driver

```
val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
connectionProperties.setProperty("Driver", driverClass)
```

- Validate the authentication

```
val df_table = spark.read.jdbc(jdbcUrl, "dbo.test1", connectionProperties)
display(df_table)
```

- import save options

```
import org.apache.spark.sql.SaveMode
```

- Now only take 1000 rows from 2 million rows table

```
val dffact1000 = dffact.select(dffact("decennialTime").alias("decennialTime"),dffact("stateName"),dffact("countyName"),dffact("population"),dffact("race")).limit(1000)
```

- Now write to table

```
dffact1000.write.mode("append").jdbc(jdbcUrl, "dbo.factdata2", connectionProperties)
```

- Time taken to process 1000 records in DW100c

```
Command took 1.38 minutes -- by xxxx at 11/16/2020, 2:17:08 PM on devcluster
```

- so 1000/100 (seconds) = 10 records per second.

- Try to read and validate

```
val dffact1 = spark.read.jdbc(jdbcUrl, "dbo.factdata2", connectionProperties)
display(dffact1)
```

