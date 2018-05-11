/*
MS SQL Script for Creating dimTime dimension table
Time range	: 1996 -1998
For OLTP	: northwind
Output table: dimTime

Note.
It works in SQL Server 2008.
Insert statements has to be modified for older versions.
*/
-- drop table Numbers_Small;
-- drop table Numbers_Big;



if exists (select * from sys.tables where name='dimTime')
	drop table dimTime;
go

CREATE TABLE Numbers_Small (Number INT);
INSERT INTO Numbers_Small VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);

CREATE TABLE Numbers_Big (Number_Big BIGINT);
INSERT INTO Numbers_Big ( Number_Big )
SELECT thousands.number * 1000 + hundreds.number * 100 + tens.number * 10 + ones.number as number_big
FROM numbers_small thousands, numbers_small hundreds, numbers_small tens, numbers_small ones;

CREATE TABLE dimTime(
[TimeKey] [int] NOT NULL PRIMARY KEY,
[Date] [datetime] NULL,
[Day] [char](10) NULL,
[DayOfWeek] [smallint] NULL,
[DayOfMonth] [smallint] NULL,
[DayOfYear] [smallint] NULL,
[WeekOfYear] [smallint] NULL,
[Month] [char](10) NULL,
[MonthOfYear] [smallint] NULL,
[QuarterOfYear] [smallint] NULL,
[Year] [int] NULL
);
INSERT INTO dimTime(TimeKey, Date) values(-1,'9999-12-31'); -- Create dummy for a "null" date/time
INSERT INTO dimTime (TimeKey, Date)
SELECT number_big, DATEADD(day, number_big,  '1996-01-01') as Date
FROM numbers_big
WHERE DATEADD(day, number_big,  '1996-01-01') BETWEEN '1996-01-01' AND '1998-12-31'
ORDER BY number_big;

/*
INSERT INTO dimTime (TimeKey, Date)
SELECT CONVERT(INT, CONVERT(CHAR(10),DATEADD(day, number_big,  '1996-01-01'), 112)) as DateKey,
CONVERT(DATE,DATEADD(day, number_big,  '1996-01-01')) as Date
FROM numbers_big
WHERE DATEADD(day, number_big,  '1996-01-01') BETWEEN '1996-01-01' AND '1998-12-31'
ORDER BY 1;
*/

UPDATE dimTime
SET Day = DATENAME(DW, Date),
DayOfWeek = DATEPART(WEEKDAY, Date),
DayOfMonth = DAY(Date),
DayOfYear = DATEPART(DY,Date),
WeekOfYear = DATEPART(WK,Date),
Month = DATENAME(MONTH,Date),
MonthOfYear = MONTH(Date),
QuarterOfYear = DATEPART(Q, Date),
Year = YEAR(Date);
drop table Numbers_Small;
drop table Numbers_Big;

Go