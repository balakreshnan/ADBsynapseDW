decennialTime
stateName
countyName
population
race
sec
minAge
maxAge


create table dbo.factdata1
(
decennialTime varchar(50),
stateName varchar(50),
countyName varchar(50),
population varchar(50),
race varchar(50),
sec varchar(50),
minAge varchar(50),
maxAge varchar(50)
)


CREATE MASTER KEY ENCRYPTION BY PASSWORD = '23987hxJ#KL95234nl0zBe';
GO

select top 200 * from dbo.factdata;

select count(*) from dbo.factdata;

create table dbo.factdata1
(
decennialTime varchar(50),
stateName varchar(50),
countyName varchar(50),
population varchar(50),
race varchar(50),
sec varchar(50),
minAge varchar(50),
maxAge varchar(50)
)

select count(*) from dbo.factdata1;


