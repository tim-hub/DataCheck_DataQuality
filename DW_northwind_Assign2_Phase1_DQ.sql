-------------------------------------------------------
--Data Warehousing Assignment 2 - Phase 1
--Data Quality Checking and Logging for Data Warehouse
--Check For DW: 	My Northwind DW
--Student ID:   	#######
--Student Name: 	MyName MySurname
--Student ID:   	#######
--Student Name: 	MyName MySurname
-------------------------------------------------------

print '***************************************************************'
print '****** Section 1: DQLog table'
print '***************************************************************'
-- Write your SQL statements below to DROP and CREATE a DQLog table

USE dbAssign2;  
GO

if exists (select * from sys.tables where name='DQLog')
	drop table DQLog;
go

CREATE TABLE DQLog(
	LogID int Primary KEY IDENTITY(1,1),
	RowID BINARY(8),
	DBName nvarchar(20) NOT NULL,
	TableName nvarchar(20) NOT NULL,
	RuleNo tinyint not null check (RuleNo >= 1 and RuleNo <=12),
	Action nvarchar(6) not null  check (Action in ('Allow', 'Fix', 'Reject'))
)

print '***************************************************************'
print '****** Section 2: DQ Checking and Logging based on DQ Rules'
print '***************************************************************'
-- Please use the following template for the checking of each DQ rule
-- by filling in the ##### for each rule checking

print '================ BEGIN RULE ## CHECKING =================='
print 'DQ Rule ##: 	######################'
print 'Action: 		#####'
print 'Database: 	#####'
print '------------------------'
print 'Table: 		#####'
print '------------------------'
-- Write your SQL statement below...

print '================ BEGIN RULE ## CHECKING =================='
print 'DQ Rule 1: 	Reject if price is less or equal to 0'
print 'Action: 		Reject'
print 'Database: 	northwind7'
print '------------------------'
print 'Table: 		products'
print '------------------------'

--SELECT p.%%physloc%%, 'northwind7', 'Products', 1, 'Reject'
--	From northwind7.dbo.Products p
--	WHERE p.UnitPrice <=0

Insert DQLog
	SELECT p.%%physloc%%, 'northwind7', 'Products', 1, 'Reject'
	From northwind7.dbo.Products p
	WHERE p.UnitPrice <=0

print '================ BEGIN RULE ## CHECKING =================='
print 'DQ Rule 2: 	Quantity Check'
print 'Action: 		Allow'
print 'Database: 	northwind7'
print '------------------------'
print 'Table: 		Order Details'
print '------------------------'

Insert DQLog
	SELECT od.%%physloc%%, 'northwind7', 'Order Details', 2, 'Reject'
	From northwind7.dbo.[Order Details] od
	WHERE od.Quantity <0 or od.Quantity = null

print '================ BEGIN RULE ## CHECKING =================='
print 'DQ Rule 3: 	Discount Check'
print 'Action: 		Allow'
print 'Database: 	northwind7'
print '------------------------'
print 'Table: 		Order Details'
print '------------------------'

Insert DQLog
	SELECT od.%%physloc%%, 'northwind7', 'Order Details', 3, 'Allow'
	From northwind7.dbo.[Order Details] od, northwind7.dbo.Products p
	WHERE od.Discount >0.2 and od.ProductID=p.ProductID and p.UnitPrice>200

print '================ BEGIN RULE ## CHECKING =================='
print 'DQ Rule 4: 	Country Check'
print 'Action: 		Fix'
print 'Database: 	northwind7'
print '------------------------'
print 'Table: 		Customers, Suppliers'
print '------------------------'

-- declare this variable table to store irregaular country name
DECLARE @irregularCountryList TABLE (name_wrong_format nvarchar(32),  name_right_format nvarchar(32))
INSERT INTO @irregularCountryList
SELECT name_int, name_out FROM (
	VALUES ('US', 'USA'), ('United States', 'USA'), ('UNITED STATES', 'USA'),
		 ('United Kingdom', 'UK'), ('UNITED KINGDOM', 'UK'), ('Britain', 'UK'), ('BRITAIN', 'UK')
	
	) AS tbl(name_int, name_out)
-- SELECT * from @irregularCountryList

Insert DQLog
	SELECT c.%%physloc%%, 'northwind7', 'Customers', 4, 'Fix'
	From northwind7.dbo.Customers c
	WHERE c.Country in (Select name_wrong_format from @irregularCountryList)

Insert DQLog
	SELECT s.%%physloc%%, 'northwind7', 'Suppliers', 4, 'Fix'
	From northwind7.dbo.Suppliers s
	WHERE s.Country in (Select name_wrong_format from @irregularCountryList)


print '================ BEGIN RULE ## CHECKING =================='
print 'DQ Rule 5: 	Postcode Check'
print 'Action: 		Fix'
print 'Database: 	northwind7'
print '------------------------'
print 'Table: 		Customers'
print '------------------------'

Insert DQLog
	SELECT c.%%physloc%%, 'northwind7', 'Customers', 5, 'Fix'
	From northwind7.dbo.Customers c
	WHERE LEN(c.PostalCode ) < 6

print '================ BEGIN RULE ## CHECKING =================='
print 'DQ Rule 6: 	Reject if categoryid or supplier id not exisits'
print 'Action: 		Reject'
print 'Database: 	northwind7'
print '------------------------'
print 'Table: 		products'
print '------------------------'
Insert DQLog
	SELECT p.%%physloc%%, 'northwind7', 'Products', 6, 'Reject'
	From northwind7.dbo.Products p
	WHERE p.CategoryID = null or
		p.SupplierID = null or
		p.CategoryID not in (
			Select CategoryID From northwind7.dbo.Categories
		) or
		p.SupplierID not in (
			Select SupplierID From northwind7.dbo.Suppliers
		) 


print '================ BEGIN RULE ## CHECKING =================='
print 'DQ Rule 7: 	Check product id in Order Details'
print 'Action: 		Reject'
print 'Database: 	northwind7'
print '------------------------'
print 'Table: 		Order Details'
print '------------------------'
Insert DQLog
	SELECT od.%%physloc%%, 'northwind7', 'Order Details', 7, 'Reject'
	From northwind7.dbo.[Order Details] od
	WHERE od.ProductID = null or
		od.ProductID not in (
			Select ProductID From northwind7.dbo.Products
		) 

print '================ BEGIN RULE ## CHECKING =================='
print 'DQ Rule 8: 	Check Customer Id, ship Address, ship city'
print 'Action: 		Reject'
print 'Database: 	northwind7'
print '------------------------'
print 'Table: 		Orders'
print '------------------------'
Insert DQLog
	SELECT o.%%physloc%%, 'northwind7', 'Orders', 8, 'Reject'
	From northwind7.dbo.[Orders] o
	WHERE (o.ShipAddress = null and o.ShipCity=null) or
		o.CustomerID= null or
		o.CustomerID not in (
			Select CustomerID From northwind7.dbo.Customers
		) 


print '================ BEGIN RULE ## CHECKING =================='
print 'DQ Rule 9: 	Check ShipVia'
print 'Action: 		Allow'
print 'Database: 	northwind7'
print '------------------------'
print 'Table: 		Orders'
print '------------------------'
Insert DQLog
	SELECT o.%%physloc%%, 'northwind7', 'Orders', 9, 'Allow'
	From northwind7.dbo.[Orders] o
	WHERE o.ShipVia = null or
		o.ShipVia not in (
			Select ShipperID from northwind7.dbo.Shippers
		)

print '================ BEGIN RULE ## CHECKING =================='
print 'DQ Rule 10: 	Check Freight'
print 'Action: 		Reject'
print 'Database: 	northwind7'
print '------------------------'
print 'Table: 		Orders'
print '------------------------'
Insert DQLog
	SELECT o.%%physloc%%, 'northwind7', 'Orders', 10, 'Reject'
	From northwind7.dbo.[Orders] o, northwind7.dbo.[Order Details] od
	WHERE od.OrderID = o.OrderID and o.Freight > 0.15 * (1-od.Discount)* od.Quantity * od.UnitPrice


print '================ BEGIN RULE ## CHECKING =================='
print 'DQ Rule 11: 	Check ShipDate'
print 'Action: 		Fix'
print 'Database: 	northwind7'
print '------------------------'
print 'Table: 		Orders'
print '------------------------'
Insert DQLog
	SELECT o.%%physloc%%, 'northwind7', 'Orders', 11, 'Fix'
	From northwind7.dbo.[Orders] o
	WHERE o.ShippedDate = null

print '=============== END RULE ## CHECKING ===================='


-- **************************************************************
-- PLEASE FILL IN NUMBERS IN THE ##### BELOW
-- **************************************************************
-- Rule no	| 	Total Logged Rows
-- **************************************************************
-- 1			####
-- 2			####
-- 3			####
-- 4			####
-- 5			####
-- 6			####
-- 7			####
-- 8			####
-- 9			####
-- 10			####
-- **************************************************************
-- Total Allow 	####
-- Total Fix 	####
-- Total Reject	####
-- **************************************************************
