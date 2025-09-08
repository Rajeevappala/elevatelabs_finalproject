CREATE DATABASE OnlineRetailDatabase

USE OnlineRetailDatabase

CREATE TABLE Raw_OnlineRetail (
    InvoiceNo NVARCHAR(50) NULL,
    StockCode NVARCHAR(50) NULL,
    Description NVARCHAR(255) NULL,  
    Quantity NVARCHAR(50) NULL,
    InvoiceDate NVARCHAR(50) NULL,
    UnitPrice NVARCHAR(50) NULL,
    CustomerID NVARCHAR(50) NULL,
    Country NVARCHAR(100) NULL
);

--- INSERT DATA INTO Raw_OnlineRetail FROM CSV FILE 
BULK INSERT Raw_OnlineRetail
FROM 'C:\Users\Rajeev\OneDrive\Documents\Online Retail.csv'
WITH (
    FIRSTROW = 2,                  
    FIELDTERMINATOR = ',',      
    ROWTERMINATOR = '\n',
    CODEPAGE = '65001',           
    TABLOCK
);


--- CREATE A TABLE FOR STAGING

CREATE TABLE Staging_OnlineRetail (
    InvoiceNo NVARCHAR(50) NULL,
    StockCode NVARCHAR(50) NULL,
    Description NVARCHAR(255) NULL,
    Quantity INT NULL,
    InvoiceDate DATETIME NULL,
    UnitPrice DECIMAL(10,2) NULL,
    CustomerID NVARCHAR(50) NULL,
    Country NVARCHAR(100) NULL
);


--- INSERT DATA INTO CREATED TABLE

INSERT INTO Staging_OnlineRetail (InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country)
SELECT
    InvoiceNo,
    StockCode,
    Description,
    TRY_CAST(Quantity AS INT),
    TRY_CAST(InvoiceDate AS DATETIME),
    TRY_CAST(UnitPrice AS DECIMAL(10,2)),
    CustomerID,
    Country
FROM Raw_OnlineRetail
WHERE TRY_CAST(Quantity AS INT) IS NOT NULL;

SELECT * FROM Staging_OnlineRetail;

-- Remove duplicates from staging table
;WITH CTE AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY InvoiceNo, StockCode, CustomerID, InvoiceDate
               ORDER BY (SELECT NULL)) AS rn
    FROM Staging_OnlineRetail
)
DELETE FROM CTE WHERE rn > 1;

-- Remove rows with nulls in critical fields (InvoiceNo, StockCode, Quantity, UnitPrice)
DELETE FROM Staging_OnlineRetail
WHERE InvoiceNo IS NULL
   OR StockCode IS NULL
   OR Quantity IS NULL
   OR UnitPrice IS NULL;

--- CREATING MULTIPLE TABLES 

--- CREATE Customer Dimension TABLE 

CREATE TABLE DimCustomer (
    CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID NVARCHAR(50) UNIQUE,
    Country NVARCHAR(100)
);


--- INSERT VALUES INTO DimCustomer

;WITH LatestCountry AS (
    SELECT CustomerID, Country,
           ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY MAX(InvoiceDate) DESC) AS rn
    FROM Staging_OnlineRetail
    WHERE CustomerID IS NOT NULL
    GROUP BY CustomerID, Country
)
INSERT INTO DimCustomer (CustomerID, Country)
SELECT CustomerID, Country
FROM LatestCountry
WHERE rn = 1
  AND NOT EXISTS (
      SELECT 1 FROM DimCustomer c WHERE c.CustomerID = LatestCountry.CustomerID
  );


SELECT * FROM DimCustomer;



--- CREATE Product Dimension TABLE 

CREATE TABLE DimProduct (
    ProductKey INT IDENTITY(1,1) PRIMARY KEY,
    StockCode NVARCHAR(50) UNIQUE,
    Description NVARCHAR(255)
);


;WITH ProductDesc AS (
    SELECT StockCode, Description,
           ROW_NUMBER() OVER (
               PARTITION BY StockCode
               ORDER BY COUNT(*) DESC
           ) AS rn
    FROM Staging_OnlineRetail
    WHERE StockCode IS NOT NULL
    GROUP BY StockCode, Description
)
INSERT INTO DimProduct (StockCode, Description)
SELECT StockCode, Description
FROM ProductDesc
WHERE rn = 1
  AND NOT EXISTS (
      SELECT 1 FROM DimProduct p WHERE p.StockCode = ProductDesc.StockCode
  );


SELECT * FROM DimProduct

CREATE TABLE DimDate (
    DateKey INT PRIMARY KEY,   -- format YYYYMMDD
    FullDate DATE,
    Year INT,
    Month INT,
    Day INT,
    DayOfWeek NVARCHAR(20)
);

DECLARE @StartDate DATE = (SELECT MIN(InvoiceDate) FROM Staging_OnlineRetail);
DECLARE @EndDate DATE = (SELECT MAX(InvoiceDate) FROM Staging_OnlineRetail);

;WITH DateSeries AS (
    SELECT @StartDate AS FullDate
    UNION ALL
    SELECT DATEADD(DAY, 1, FullDate)
    FROM DateSeries
    WHERE DATEADD(DAY, 1, FullDate) <= @EndDate
)
INSERT INTO DimDate (DateKey, FullDate, Year, Month, Day, DayOfWeek)
SELECT 
    CONVERT(INT, FORMAT(FullDate, 'yyyyMMdd')) AS DateKey,
    FullDate,
    YEAR(FullDate),
    MONTH(FullDate),
    DAY(FullDate),
    DATENAME(WEEKDAY, FullDate)
FROM DateSeries
OPTION (MAXRECURSION 0);



SELECT * FROM DimDate;

CREATE TABLE FactSales (
    SalesKey INT IDENTITY(1,1) PRIMARY KEY,
    InvoiceNo NVARCHAR(50),
    CustomerKey INT NOT NULL,
    ProductKey INT NOT NULL,
    DateKey INT NOT NULL,
    Quantity INT,
    UnitPrice DECIMAL(10,2),
    TotalAmount AS (Quantity * UnitPrice) PERSISTED,

    -- Relationships
    CONSTRAINT FK_FactSales_Customer FOREIGN KEY (CustomerKey) REFERENCES DimCustomer(CustomerKey),
    CONSTRAINT FK_FactSales_Product FOREIGN KEY (ProductKey) REFERENCES DimProduct(ProductKey),
    CONSTRAINT FK_FactSales_Date FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey)
);

INSERT INTO FactSales (InvoiceNo, CustomerKey, ProductKey, DateKey, Quantity, UnitPrice)
SELECT 
    s.InvoiceNo,
    c.CustomerKey,
    p.ProductKey,
    d.DateKey,
    s.Quantity,
    s.UnitPrice
FROM Staging_OnlineRetail s
JOIN DimCustomer c ON s.CustomerID = c.CustomerID
JOIN DimProduct p ON s.StockCode = p.StockCode
JOIN DimDate d ON CONVERT(INT, FORMAT(s.InvoiceDate, 'yyyyMMdd')) = d.DateKey;


SELECT * FROM FactSales;


---- Create an Audit Table

CREATE TABLE AuditLog (
    AuditID INT IDENTITY(1,1) PRIMARY KEY,
    TableName NVARCHAR(50),
    InsertedRows INT,
    LoadDate DATETIME DEFAULT GETDATE()
);

INSERT INTO AuditLog (TableName, InsertedRows)
SELECT 'FactSales', COUNT(*) FROM FactSales;



SELECT * FROM AuditLog;


--- Automate Cleanup Using Triggers / Stored Procedures

CREATE TRIGGER trg_Audit_FactSales
ON FactSales
AFTER INSERT
AS
BEGIN
    INSERT INTO AuditLog (TableName, InsertedRows)
    VALUES ('FactSales', (SELECT COUNT(*) FROM inserted));
END;
