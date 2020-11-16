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

```
display(dffact)
```

- To write faster using sql username and azure databricks sql dw connector. 
- Write to table called dbo.factdata
- bulk write uses polybase to stage and bulk inserts to sql dw

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

- JDBC averages about 625 or so records with DW100c

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