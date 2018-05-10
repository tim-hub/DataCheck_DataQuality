---------------------------------------
--Data Warehousing Assignment 2 - Phase 2
--My Northwind's DW ETL+DQ Script File
--Student ID:   	#######
--Student Name: 	MyName MySurname
--Student ID:   	#######
--Student Name: 	MyName MySurname
------------------------------------

print '***************************************************************'
print '****** Section 1: Creating DW Tables'
print '***************************************************************'

print 'Drop all DW tables (except dimTime)'
--Add drop statements below...
--DO NOT DROP dimTime table as you must have used Script provided on the Moodle to create it


if db_id('dbAssign2') is not null
   use dbAssign2;
go

if exists (select * from sys.tables where name='factOrders')
	drop table factOrders;
go

if exists (select * from sys.tables where name='dimProducts')
	drop table dimProducts;
go
if exists (select * from sys.tables where name='dimCustomers')
	drop table dimCustomers;
go
if exists (select * from sys.tables where name='dimSuppliers')
	drop table Suppliers;
go

print 'Creating all dimension tables required'
--Add statements below...
CREATE TABLE dimCustomers
(
	CustomerKey			int				IDENTITY(1,1) PRIMARY KEY,
	CustomerID			nchar(5),
	CompanyName			nvarchar(40),
	ContactName			nvarchar(30),
	ContactTitle		nvarchar(30),
	Address				nvarchar(60),
	City				nvarchar(15),
	Region				nvarchar(15),
	PostalCode			nvarchar(10),
	Country				nvarchar(15),
	Phone				nvarchar(24),
	Fax					nvarchar(24)
);

--dimProducts table
CREATE TABLE dimProducts
(
	ProductKey			int				IDENTITY(1,1) PRIMARY KEY,
	ProductID			int,
	ProductName			nvarchar(40),
	QuantityPerUnit		nvarchar(20),
	UnitPrice			money,
	UnitsInStock		smallint,
	UnitsOnOrder		smallint,
	ReorderLevel		smallint,
	Discontinued		bit,
	CategoryName		nvarchar(15),
	Description			ntext,
	Picture				image
);

--dimSuppliers table
CREATE TABLE dimSuppliers
(
	SupplierKey			int				IDENTITY(1,1) PRIMARY KEY,
	SupplierID			int,
	CompanyName			nvarchar(40),
	ContactName			nvarchar(30),
	ContactTitle		nvarchar(30),
	Address				nvarchar(60),
	City				nvarchar(15),
	Region				nvarchar(15),
	PostalCode			nvarchar(10),
	Country				nvarchar(15),
	Phone				nvarchar(24),
	Fax					nvarchar(24),
	HomePage			ntext
);


print 'Creating a fact table required'
--Add statements below...
CREATE TABLE 		factOrders
(
	ProductKey			int				FOREIGN KEY REFERENCES dimProducts(ProductKey),
	CustomerKey			int				FOREIGN KEY REFERENCES dimCustomers(CustomerKey),
	SupplierKey			int				FOREIGN KEY REFERENCES dimSuppliers(SupplierKey),
	OrderDateKey		int				FOREIGN KEY REFERENCES dimTime(TimeKey),
	RequiredDateKey		int				FOREIGN KEY REFERENCES dimTime(TimeKey),
	ShippedDateKey		int				FOREIGN KEY REFERENCES dimTime(TimeKey),
	OrderID				int,
	UnitPrice			money,
	Qty			smallint,
	Discount			real,
	TotalPrice			real,
	ShipperCompanyName	nvarchar(40),
	ShipperPhone		nvarchar(24),
	PRIMARY KEY (ProductKey, CustomerKey, SupplierKey, OrderDateKey)
);


print '***************************************************************'
print '****** Section 2: Populate DW Dimension Tables (except dimTime)'
print '***************************************************************'

print 'Populating all dimension tables from northwind7 and northwind8'
--Add statements below...
--IMPORTANT! All Data in dimension tables MUST satisfy all the defined DQ Rules

-- declare this variable table to store irregaular country name
-- this is same with phase 1
DECLARE @irregularCountryList TABLE (name_wrong_format nvarchar(32),  name_right_format nvarchar(32))
INSERT INTO @irregularCountryList
SELECT name_int, name_out FROM (
	VALUES ('US', 'USA'), ('United States', 'USA'), ('UNITED STATES', 'USA'),
		 ('United Kingdom', 'UK'), ('UNITED KINGDOM', 'UK'), ('Britain', 'UK'), ('BRITAIN', 'UK')

	) AS tbl(name_int, name_out)


--Populating dimProducts from northwind7

MERGE INTO dimProducts dp
USING
(
	SELECT	ProductID, ProductName, QuantityPerUnit, UnitPrice,
			UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued,
			CategoryName, Description, Picture
	FROM	northwind7.dbo.Products p, northwind7.dbo.Categories c
	WHERE	p.CategoryID = c.CategoryID
    AND p.%%physloc%% NOT IN (
      SELECT RowID FROM DQLog
      WHERE DBName = 'northwind7'
        AND TableName = 'products'
        AND (RuleNo = 1 OR RuleNo =6)
        AND Action = 'Reject'
    )
) pc ON (dp.ProductID = pc.ProductID)
WHEN MATCHED THEN
	UPDATE SET dp.ProductName = pc.ProductName
WHEN NOT MATCHED THEN
	INSERT (ProductID, ProductName, QuantityPerUnit, UnitPrice,
	UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued,
	CategoryName, Description, Picture)
	VALUES (pc.ProductID, pc.ProductName, pc.QuantityPerUnit, pc.UnitPrice,
	pc.UnitsInStock, pc.UnitsOnOrder, pc.ReorderLevel, pc.Discontinued,
	pc.CategoryName, pc.Description, pc.Picture);


print '***************************************************************'
print '****** Section 3: Populate DW Fact Tables'
print '***************************************************************'

print 'Populating the fact table from northwind7 and northwind8'
--Add statements below...
--IMPORTANT! All Data in the fact table MUST satisfy all the defined DQ Rules




print '***************************************************************'
print '****** Section 4: Counting rows of OLTP and DW Tables'
print '***************************************************************'
print 'Checking Number of Rows of each table in the source databases and the DW Database'
-- Write SQL queries to get answers to fill in the information below
-- ****************************************************************************
-- FILL IN THE #####
-- ****************************************************************************
-- Source table					Northwind7	Northwind8	Target table 	DW
-- ****************************************************************************
-- Table1						#####		#####		dim....			#####
-- Table2						#####		#####		dim....			#####
-- Table3						#####		#####		dim....			#####
-- Table..						#####		#####		dim....			#####
-- Table.. 						#####		#####		fact...			#####
-- ****************************************************************************
--Add statements below


print '***************************************************************'
print '****** Section 5: Validating DW Data'
print '***************************************************************'
print 'B: Validating Data in the fact table'
--Add statements below...


print '***************************************************************'
print '***************************************************************'
print 'My Northwind DW creation with data quality assurance is now completed'
print '***************************************************************'