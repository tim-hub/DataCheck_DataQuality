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


print 'Creating all dimension tables required'
--Add statements below... 



print 'Creating a fact table required'
--Add statements below... 



print '***************************************************************'
print '****** Section 2: Populate DW Dimension Tables (except dimTime)'
print '***************************************************************'

print 'Populating all dimension tables from northwind7 and northwind8'
--Add statements below... 
--IMPORTANT! All Data in dimension tables MUST satisfy all the defined DQ Rules 



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


print
print '***************************************************************'
print 'My Northwind DW creation with data quality assurance is now completed'
print '***************************************************************'