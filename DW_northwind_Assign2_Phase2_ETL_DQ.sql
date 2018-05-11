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
	drop table dimSuppliers;
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
	Quantity			smallint,
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
SELECT name_in, name_out FROM (
	VALUES ('US', 'USA'), ('United States', 'USA'), ('UNITED STATES', 'USA'),
		 ('United Kingdom', 'UK'), ('UNITED KINGDOM', 'UK'), ('Britain', 'UK'), ('BRITAIN', 'UK')

	) AS tbl(name_in, name_out)


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
        AND TableName = 'Products'
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

-- populating dim products from northwind8
MERGE INTO dimProducts dp
USING
(
  SELECT  ProductID, ProductName, QuantityPerUnit, UnitPrice,
      UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued,
      CategoryName, Description, Picture
  FROM  northwind8.dbo.Products p, northwind8.dbo.Categories c
  WHERE p.CategoryID = c.CategoryID
    AND p.%%physloc%% NOT IN (
      SELECT RowID FROM DQLog
      WHERE DBName = 'northwind8'
        AND TableName = 'Products'
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


-- populating Customers from northwind7
-- pre populating

DECLARE @TempCustomers TABLE
(
  RowID           BINARY(8),
	CustomerKey			int		  IDENTITY(1,1) PRIMARY KEY,
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

MERGE INTO @TempCustomers tc
USING
(
  SELECT %%physloc%% as RowID, CustomerID, CompanyName, ContactName, ContactTitle, Address,
  City, Region, PostalCode, Country, Phone, Fax
  FROM northwind7.dbo.Customers
) c ON (
  tc.CustomerID = c.CustomerID
)
WHEN MATCHED THEN
  UPDATE SET tc.CompanyName = c.CompanyName
WHEN NOT MATCHED THEN
  INSERT (RowID, CustomerID, CompanyName, ContactName, ContactTitle, Address,
  City, Region, PostalCode, Country, Phone, Fax)
  VALUES (c.RowID, c.CustomerID, c.CompanyName, c.ContactName, c.ContactTitle, c.Address,
  c.City, c.Region, c.PostalCode, c.Country, c.Phone, c.Fax);

MERGE INTO @TempCustomers tc
USING
(
  SELECT %%physloc%% as RowID, CustomerID, CompanyName, ContactName, ContactTitle, Address,
  City, Region, PostalCode, Country, Phone, Fax
  FROM northwind8.dbo.Customers
) c ON (
  tc.CustomerID = c.CustomerID
)
WHEN MATCHED THEN
  UPDATE SET tc.CompanyName = c.CompanyName
WHEN NOT MATCHED THEN
  INSERT (RowID, CustomerID, CompanyName, ContactName, ContactTitle, Address,
  City, Region, PostalCode, Country, Phone, Fax)
  VALUES (c.RowID, c.CustomerID, c.CompanyName, c.ContactName, c.ContactTitle, c.Address,
  c.City, c.Region, c.PostalCode, c.Country, c.Phone, c.Fax);

-- update temporary table
-- update northwind7 and northwin8
UPDATE @TempCustomers
SET     Country = 'USA'
WHERE Country IN (
    SELECT name_wrong_format FROM @irregularCountryList
    WHERE name_right_format = 'USA'
  ) AND RowID IN (
    SELECT RowID FROM DQLog
    WHERE TableName = 'Customers'
    AND RuleNo = 4 AND Action='Fix'
  )
UPDATE @TempCustomers
SET     Country = 'UK'
WHERE Country IN (
    SELECT name_wrong_format FROM @irregularCountryList
    WHERE name_right_format = 'UK'
  ) AND RowID IN (
    SELECT RowID FROM DQLog
    WHERE TableName = 'Customers'
    AND RuleNo = 4 AND Action='Fix'
  )

-- update postalcode in customers
UPDATE @TempCustomers
Set PostalCode = RIGHT(('000000'+ PostalCode), 6)
WHERE RowID IN (
  SELECT RowID FROM DQLog
  WHERE DBName ='northwind7' AND TableName ='Customers'
  AND RuleNo =5 AND Action ='Fix'
)

-- merge into dimCustomers
MERGE INTO dimCustomers dc
USING
(
	SELECT CustomerID, CompanyName, ContactName, ContactTitle, Address,
	City, Region, PostalCode,
  Country,
  Phone, Fax
  FROM @TempCustomers
) c ON ( dc.CustomerID = c.CustomerID)
WHEN MATCHED THEN
	UPDATE SET dc.CompanyName = c.CompanyName
WHEN NOT MATCHED THEN
	INSERT (CustomerID, CompanyName, ContactName, ContactTitle, Address,
	City, Region, PostalCode, Country, Phone, Fax)
	VALUES (c.CustomerID, c.CompanyName, c.ContactName, c.ContactTitle, c.Address,
	c.City, c.Region, c.PostalCode, c.Country, c.Phone, c.Fax
  );

-- populating dim suppliers
DECLARE @TempSuppliers TABLE
(
  RowID           BINARY(8),
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

-- preparing merging
MERGE INTO @TempSuppliers ds
USING
(
	SELECT  %%physloc%% as RowID, SupplierID, CompanyName, ContactName, ContactTitle,
	Address, City, Region, PostalCode, Country, Phone, Fax, HomePage
  FROM northwind7.dbo.Suppliers
) s ON (ds.SupplierID = s.SupplierID)
WHEN MATCHED THEN
	UPDATE SET ds.CompanyName = s.CompanyName
WHEN NOT MATCHED THEN
	INSERT (RowID, SupplierID, CompanyName, ContactName, ContactTitle,
	Address, City, Region, PostalCode, Country, Phone, Fax, HomePage)
	VALUES (s.RowID, s.SupplierID, s.CompanyName, s.ContactName, s.ContactTitle,
	s.Address, s.City, s.Region, s.PostalCode, s.Country, s.Phone, s.Fax, s.HomePage
  );

MERGE INTO @TempSuppliers ds
USING
(
	SELECT %%physloc%% as RowID, SupplierID, CompanyName, ContactName, ContactTitle,
	Address, City, Region, PostalCode, Country, Phone, Fax, HomePage
  FROM northwind8.dbo.Suppliers
) s ON (ds.SupplierID = s.SupplierID)
WHEN MATCHED THEN
	UPDATE SET ds.CompanyName = s.CompanyName
WHEN NOT MATCHED THEN
	INSERT (RowID, SupplierID, CompanyName, ContactName, ContactTitle,
	Address, City, Region, PostalCode, Country, Phone, Fax, HomePage)
	VALUES (s.RowID, s.SupplierID, s.CompanyName, s.ContactName, s.ContactTitle,
	s.Address, s.City, s.Region, s.PostalCode, s.Country, s.Phone, s.Fax, s.HomePage
  );

-- update temporary suppliers table
-- update northwind7 and northwin8
UPDATE @TempSuppliers
SET     Country = 'USA'
WHERE Country IN (
    SELECT name_wrong_format FROM @irregularCountryList
    WHERE name_right_format = 'USA'
  ) AND RowID IN (
    SELECT RowID FROM DQLog
    WHERE TableName = 'Customers'
    AND RuleNo = 4 AND Action='Fix'
  )
UPDATE @TempSuppliers
SET     Country = 'UK'
WHERE Country IN (
    SELECT name_wrong_format FROM @irregularCountryList
    WHERE name_right_format = 'UK'
  ) AND RowID IN (
    SELECT RowID FROM DQLog
    WHERE TableName = 'Customers'
    AND RuleNo = 4 AND Action='Fix'
  )

-- merge into dim suppliers
MERGE INTO dimSuppliers ds
USING
(
	SELECT * FROM @TempSuppliers
) s ON (ds.SupplierID = s.SupplierID)
WHEN MATCHED THEN
	UPDATE SET ds.CompanyName = s.CompanyName
WHEN NOT MATCHED THEN
	INSERT (SupplierID, CompanyName, ContactName, ContactTitle,
	Address, City, Region, PostalCode, Country, Phone, Fax, HomePage)
	VALUES (s.SupplierID, s.CompanyName, s.ContactName, s.ContactTitle,
	s.Address, s.City, s.Region, s.PostalCode, s.Country, s.Phone, s.Fax, s.HomePage
  );

print '***************************************************************'
print '****** Section 3: Populate DW Fact Tables'
print '***************************************************************'

print 'Populating the fact table from northwind7 and northwind8'
--Add statements below...
--IMPORTANT! All Data in the fact table MUST satisfy all the defined DQ Rules

--Populating factOrders from northwind7
MERGE INTO  factOrders fo
USING
(
  SELECT
      dp.ProductKey as ProductKey,
      dc.CustomerKey as CustomerKey,
      ds.SupplierKey as SupplierKey,
      dt1.TimeKey as [OrderDateKey],
      dt2.TimeKey as [RequiredDateKey],
      -- o.ShippedDate as ShippedDate,
      -- use this to fix the shipped date
      CASE
        WHEN o.%%physloc%% IN (
          SELECT RowID FROM DQLog
          WHERE DBName = 'northwind7'
          AND TableName = 'Orders'
          AND (RuleNo = 11)
          AND Action = 'Fix'
        )
        THEN -1
        ELSE (
          SELECT TimeKey
          From dimTime
          WHERE dimTime.Date = o.ShippedDate
        )
        END
        AS [ShippedDateKey],
      od.OrderID as OrderID,
      od.UnitPrice as UnitPrice,
      od.Quantity as Quantity,
      od.Discount as Discount,
      od.Quantity*od.UnitPrice *(1-od.Discount) as [TotalPrice],
      sh.CompanyName as ShipperCompanyName,
      sh.Phone as ShipperPhone
  FROM  northwind7.dbo.Shippers sh,
      northwind7.dbo.Orders o,
      northwind7.dbo.[Order Details] od,
      northwind7.dbo.Products p,
      northwind7.dbo.Suppliers su,
      dimProducts dp,
      dimCustomers dc,
      dimSuppliers ds,
      dimTime dt1, dimTime dt2, dimTime dt3
  WHERE o.ShipVia = sh.ShipperID
      AND od.OrderID = o.OrderID
      AND o.CustomerID = dc.CustomerID
      AND od.ProductID = dp.ProductID
      AND od.ProductID = p.ProductID
      AND p.SupplierID = su.SupplierID
      AND su.SupplierID = ds.SupplierID
      AND dt1.Date = o.OrderDate
      AND dt2.Date = o.RequiredDate
      -- AND dt3.Date = o.ShippedDate
      -- AND p.%%physloc%% NOT IN (
      --   SELECT RowID FROM DQLog
      --   WHERE DBName = 'northwind7'
      --   AND TableName = 'Products'
      --   AND (RuleNo = 1 OR RuleNo =6)
      --   AND Action = 'Reject'
      -- )
      -- because we already have dimProducts, suppliers, customers
      -- rules about about customers, products and suppliers are not required either
      AND od.%%physloc%% NOT IN (
        SELECT RowID FROM DQLog
        WHERE DBName = 'northwind7'
        AND TableName = 'Order Details'
        AND (RuleNo = 2 or RuleNo = 7)
        AND Action = 'Reject'
      )
      AND o.%%physloc%% NOT IN (
        SELECT RowID FROM DQLog
        WHERE DBName = 'northwind7'
        AND TableName = 'Order'
        AND (RuleNo = 8 or RuleNo = 10)
        AND Action = 'Reject'
      )
) o ON (o.CustomerKey = fo.CustomerKey
    AND o.ProductKey = fo.ProductKey
    AND o.SupplierKey = fo.SupplierKey
    AND o.OrderDateKey = fo.OrderDateKey)
WHEN MATCHED THEN
  UPDATE SET fo.OrderID = o.OrderID
WHEN NOT MATCHED THEN
  INSERT (ProductKey, CustomerKey, SupplierKey, OrderDateKey,
      RequiredDateKey, ShippedDateKey, OrderID,
      UnitPrice, Quantity, Discount, TotalPrice,
      ShipperCompanyName, ShipperPhone)
  VALUES  (o.ProductKey, o.CustomerKey, o.SupplierKey, o.OrderDateKey,
      o.RequiredDateKey, o.ShippedDateKey, o.OrderID,
      o.UnitPrice, o.Quantity, o.Discount, o.TotalPrice,
      o.ShipperCompanyName, o.ShipperPhone
      );


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

print 'Checking Number of Rows of each table in the source databases and the DW Database'
--Add statements below for checking if each dimension table contains all data

--Checking number of rows in the dimCustomers table with
--the number of rows in the Customers table of northwind7 & northwind8
SELECT COUNT(*) AS [Total Rows in DW dimCustomers Table]
FROM dimCustomers;

SELECT COUNT(*) AS [Total Rows in northwind7 Customers Table]
FROM northwind7.dbo.Customers;

SELECT COUNT(*) AS [Total Rows in northwind8 Customers Table]
FROM northwind8.dbo.Customers;

--Checking number of rows in the dimProducts table with
--the number of rows in the Categories JOIN Products table of northwind7 & northwind8
SELECT COUNT(*) [Total Rows in DW dimProducts Table]
FROM dimProducts;

SELECT COUNT(*) AS [Total Rows in northwind7 Categories JOIN Products Table]
FROM northwind7.dbo.Categories c, northwind7.dbo.Products p
WHERE c.CategoryID = p.CategoryID;

SELECT COUNT(*) AS [Total Rows in northwind8 Categories JOIN Products Table]
FROM northwind8.dbo.Categories c, northwind8.dbo.Products p
WHERE c.CategoryID = p.CategoryID;

--Checking number of rows in the dimSuppliers table with
--the number of rows in the Suppliers table of northwind7 & northwind8
SELECT COUNT(*) AS [Total Rows in DW dimSuppliers Table]
FROM dimSuppliers;

SELECT COUNT(*) AS [Total Rows in northwind7 Suppliers Table]
FROM northwind7.dbo.Suppliers;

SELECT COUNT(*) AS [Total Rows in northwind8 Suppliers Table]
FROM northwind8.dbo.Suppliers;

print 'Checking factOrder data'
--Add statements below for checking if the factOrders table contains all data
--This can be done by joining Orders with [Order Details]

--Checking number of rows in the factOrders table with
--the number of rows in the joined tables of Orders & Order Details of northwind7 & northwind8
SELECT COUNT(*) as [Total Rows in DW factOrders Table]
FROM factOrders;

SELECT COUNT(*) as [Total Rows in Orders-Order Details-Shippers Table in northwind7]
FROM  northwind7.dbo.Shippers sh,
    northwind7.dbo.Orders o,
    northwind7.dbo.[Order Details] od
WHERE o.ShipVia = sh.ShipperID
    AND od.OrderID = o.OrderID;

SELECT COUNT(*) as [Total Rows in Orders-Order Details-Shippers Table in northwind8]
FROM  northwind8.dbo.Shippers sh,
    northwind8.dbo.Orders o,
    northwind8.dbo.[Order Details] od
WHERE o.ShipVia = sh.ShipperID
    AND od.OrderID = o.OrderID;

print '***************************************************************'
print '****** Section 5: Validating DW Data'
print '***************************************************************'
print 'B: Validating Data in the fact table'
--Add statements below...


print '***************************************************************'
print '***************************************************************'
print 'My Northwind DW creation with data quality assurance is now completed'
print '***************************************************************'