
-- LIMPIEZA PREVIA
DROP TABLE IF EXISTS FactSupplyChain; 
DROP TABLE IF EXISTS DimVendorLocation;
DROP TABLE IF EXISTS DimLogistics; 
DROP TABLE IF EXISTS DimVendor; 
DROP TABLE IF EXISTS DimMaterial;


-- 1. DimMaterial 
CREATE TABLE DimMaterial ( 
	SKU NVARCHAR(50) PRIMARY KEY, 
	Product_type NVARCHAR(100), 
	Manufacturing_costs DECIMAL(18,2), 
	Price DECIMAL(18,2), 
	Defect_rate_percentage FLOAT 
);

-- 2. DimVendor 
CREATE TABLE DimVendor ( 
	Vendor_ID INT IDENTITY(1,1) PRIMARY KEY, 
	Supplier_Name NVARCHAR(255) UNIQUE
);

CREATE TABLE DimVendorLocation ( 
	Vendor_Location_ID INT IDENTITY(1,1) PRIMARY KEY, 
	Vendor_ID INT NOT NULL, 
	Location NVARCHAR(255) NOT NULL, 
	Average_Lead_Time_Days INT,

	CONSTRAINT FK_VendorLocation_Vendor 
		FOREIGN KEY (Vendor_ID) REFERENCES DimVendor(Vendor_ID),

	CONSTRAINT UQ_Vendor_Location 
		UNIQUE (Vendor_ID, Location)
);

-- 3. DimLogistics 
CREATE TABLE DimLogistics ( 
	Shipping_ID INT IDENTITY(1,1) PRIMARY KEY, 
	Shipping_carrier NVARCHAR(100), 
	Transportation_mode NVARCHAR(100), 
	Route NVARCHAR(100) 
);


-- 4. Fact 
CREATE TABLE FactSupplyChain ( 
	Transaction_ID INT IDENTITY(1,1) PRIMARY KEY, 

	SKU NVARCHAR(50) NOT NULL, 
	Vendor_Location_ID INT NOT NULL, 
	Shipping_ID INT NOT NULL, 

	Stock_levels INT, 
	Order_quantities INT, 
	Number_of_products_sold INT, 
	Revenue_generated DECIMAL(18,2), 
	Shipping_costs DECIMAL(18,2), 
	Actual_Lead_Time_Days INT, 
	Reorder_Point FLOAT, 

	CONSTRAINT FK_Fact_Material 
		FOREIGN KEY (SKU) REFERENCES DimMaterial(SKU), 

	CONSTRAINT FK_Fact_VendorLocation 
		FOREIGN KEY (Vendor_Location_ID) 
		REFERENCES DimVendorLocation(Vendor_Location_ID), 

	CONSTRAINT FK_Fact_Logistics 
		FOREIGN KEY (Shipping_ID) 
		REFERENCES DimLogistics(Shipping_ID) 
);

