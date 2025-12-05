/*
Script purpose: 
This Script creates a new database named 'Datawarehouse' after checking whether there is a database named 'Datawarehouse', 
If the database exsits, then drop and recreate it. In addition, this scripts creates three schema named 'Bronze', 'Silver', 
and 'Gold'

Warning:
Execute this script will drop the database, proceed with caution. Ensure data has backups. 

/*

------------------------------------------------------------------------
-- Drop and recreate the 'DataWarehouse' database

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = "DataWarehouse") I
BEGIN
   ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
   DROP DATABASE DataWarehouse;
END;
GO


-- Create Datawarehouse
Create DataWarehouse;
GO

Use DataWarehouse;
GO

------------------------------------------------------------------------
-- Create Schema
Create Schema Bronze;
GO

Create Schema Silver;
GO

Create Schema Gold;
GO 
