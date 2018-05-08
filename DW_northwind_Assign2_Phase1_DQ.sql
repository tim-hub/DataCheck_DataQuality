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
print 'Database: 	noethwind7'
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
print 'Database: 	noethwind7'
print '------------------------'
print 'Table: 		Order Details'
print '------------------------'

Insert DQLog
	SELECT od.%%physloc%%, 'northwind7', 'Order Details', 3, 'Allow'
	From northwind7.dbo.[Order Details] od, northwind7.dbo.Products p
	WHERE od.Discount >0.2 and od.ProductID=p.ProductID and p.UnitPrice>200
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
print 'DQ Rule ##: 	######################'
print 'Action: 		#####'
print 'Database: 	#####'
print '------------------------'
print 'Table: 		#####'
print '------------------------'


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
