CREATE DATABASE Pandemic_Mortality_Trends
USE Pandemic_Mortality_Trends

---Creating A Table to insert data
CREATE TABLE COVID_Data (
Record_Date VARCHAR(MAX),
Jurisdiction_Residence VARCHAR(MAX),
Group_ VARCHAR(MAX),
Data_Period_Start VARCHAR(MAX),
Data_Period_End VARCHAR(MAX),
COVID_Deaths VARCHAR(MAX),
COVID_Pct_Of_Total VARCHAR(MAX),
Pct_Change_wk VARCHAR(MAX),
Pct_Diff_wk VARCHAR(MAX),
Crude_COVID_Rate VARCHAR(MAX),
African_American_COVID_Rate VARCHAR(MAX)
)

--Inserting Data using BULK INSERT
BULK INSERT COVID_Data
FROM 'File location and Name'
WITH(FieldTerminator = ',', RowTerminator = '\n', MaxErrors = 20, FirstRow = 2)

SELECT * FROM COVID_Data
SELECT Column_name, Data_type
FROM INFORMATION_SCHEMA.COLUMNS

--Cleaning Data
UPDATE COVID_Data
SET African_American_COVID_Rate = NULL
WHERE African_American_COVID_Rate IN
(',Death counts between 1-9 are suppressed. Rates for deaths counts <20 are unreliable.', ',Rates for deaths counts <20 are unreliable.')

--Changing Data-types
SELECT CONVERT(DATE, Record_Date)
FROM COVID_Data

ALTER TABLE COVID_Data
ALTER COLUMN Record_Date DATE

ALTER TABLE COVID_Data
ALTER COLUMN Jurisdiction_Residence VARCHAR(30)

ALTER TABLE COVID_Data
ALTER COLUMN Group_ VARCHAR(20)

SELECT CONVERT(DATE, Data_Period_Start)
FROM COVID_Data

ALTER TABLE COVID_Data
ALTER COLUMN Data_Period_Start DATE


SELECT CONVERT(DATE, Data_Period_End)
FROM COVID_Data

ALTER TABLE COVID_Data
ALTER COLUMN Data_Period_End DATE

ALTER TABLE COVID_Data
ALTER COLUMN COVID_Deaths INT

ALTER TABLE COVID_Data
ALTER COLUMN COVID_Pct_Of_Total FLOAT

ALTER TABLE COVID_Data
ALTER COLUMN Pct_Change_wk FLOAT

ALTER TABLE COVID_Data
ALTER COLUMN Pct_Diff_wk FLOAT

ALTER TABLE COVID_Data
ALTER COLUMN Crude_COVID_Rate FLOAT

SELECT TRY_CONVERT(FLOAT,African_American_COVID_Rate) FROM COVID_Data

SELECT LEFT(African_American_COVID_Rate,LEN(African_American_COVID_Rate)-1) FROM COVID_Data

UPDATE COVID_Data
SET African_American_COVID_Rate = LEFT(African_American_COVID_Rate,LEN(African_American_COVID_Rate)-1)

ALTER TABLE COVID_Data
ALTER COLUMN African_American_COVID_Rate FLOAT


--SQL Analysis
-- 1} Jurisdiction residence with the highest number of COVID deaths for the latest data period end date.

SELECT TOP 1 
Jurisdiction_Residence, Data_Period_End 'Latest_Data_Period_End_Date', COVID_Deaths
FROM COVID_Data
WHERE Data_Period_End = (SELECT MAX(Data_Period_End) FROM COVID_Data) AND Group_ = 'weekly'
ORDER BY COVID_Deaths DESC

-- 2} The top 5 jurisdictions with the highest percentage difference in aa_COVID_rate 
--    compared to the overall crude COVID rate for the latest data period end date.

WITH cte AS (
    SELECT *
    FROM COVID_Data
    WHERE Data_Period_End = (SELECT MAX(Data_Period_End) FROM COVID_Data) AND Group_ = 'weekly'
)
SELECT TOP 5
Jurisdiction_Residence, Crude_COVID_Rate, African_American_COVID_Rate,
CASE WHEN Crude_COVID_Rate = 0 THEN NULL
ELSE ABS(African_American_COVID_Rate - Crude_COVID_Rate) / Crude_COVID_Rate * 100 
END 'Percentage_Difference'
FROM cte
ORDER BY Percentage_Difference DESC

-- 3} Average COVID deaths per week for each jurisdiction residence and group, for the latest 4 data period end dates.

SELECT Jurisdiction_Residence, AVG(COVID_Deaths) 'Average_Deaths_In_Last_4_Weeks' 
FROM COVID_Data
WHERE Data_Period_End IN (SELECT DISTINCT TOP 4 Data_Period_End FROM COVID_Data ORDER BY Data_Period_End DESC)
GROUP BY Jurisdiction_Residence

-- 4} The data for the latest data period end date,
--    Excluding any jurisdictions that had zero COVID deaths and have missing values in any other column.

SELECT * FROM COVID_Data
WHERE Data_Period_End = (SELECT MAX(Data_Period_End) FROM COVID_Data)
AND COVID_Deaths > 0
AND Pct_Change_wk IS NOT NULL
AND Pct_Diff_wk IS NOT NULL
AND Crude_COVID_Rate IS NOT NULL
AND African_American_COVID_Rate IS NOT NULL

-- 5} Week-over-week percentage change in COVID_pct_of_total for all jurisdictions and groups, 
--    but only for the data period start dates after March 1, 2020.

SELECT Jurisdiction_Residence, Group_, Data_Period_Start, Data_Period_End, COVID_Pct_Of_Total, Pct_Change_wk, Pct_Diff_wk
FROM COVID_Data
WHERE Data_Period_Start > '2020-03-01'

-- 6} Grouping the data by jurisdiction residence and calculate the cumulative COVID deaths for each jurisdiction,
--    but only up to the latest data period end date.

SELECT Jurisdiction_Residence, Group_, Data_Period_Start, Data_Period_End, COVID_Deaths, 
SUM(COVID_Deaths) OVER(PARTITION BY Jurisdiction_Residence ORDER BY Data_Period_End) 'Cumulative_COVID_Deaths'
FROM COVID_Data
WHERE Group_ = 'Weekly'

SELECT * FROM COVID_Data
WHERE Group_ = 'total' AND Jurisdiction_Residence = 'Alabama'

--Procedure
/*A stored procedure that takes in a date range and calculates the average weekly percentage change in COVID deaths for each jurisdiction.
The procedure returns the average weekly percentage change along with the jurisdiction and date range as output*/
SELECT * FROM COVID_Data

CREATE PROCEDURE avg_wk_pct @Start_Date AS DATE, @End_Date AS DATE
AS
BEGIN
       WITH cte AS(
SELECT Jurisdiction_Residence, Group_, Data_Period_Start, Data_Period_End, COVID_Deaths,
LAG(COVID_Deaths) OVER(PARTITION BY Jurisdiction_Residence ORDER BY Data_Period_End) 'Previous_wk',
COVID_Deaths - LAG(COVID_Deaths) OVER(PARTITION BY Jurisdiction_Residence ORDER BY Data_Period_End) 'Difference',
CASE WHEN LAG(COVID_Deaths) OVER(PARTITION BY Jurisdiction_Residence ORDER BY Data_Period_End) = 0
THEN 0
ELSE
ROUND(  ((COVID_Deaths - LAG(COVID_Deaths) OVER(PARTITION BY Jurisdiction_Residence ORDER BY Data_Period_End) *1.0)/ 
LAG(COVID_Deaths) OVER(PARTITION BY Jurisdiction_Residence ORDER BY Data_Period_End))*100   ,2)
END 'Percent_Change'
FROM COVID_Data 
WHERE Group_ = 'weekly'
)
SELECT Jurisdiction_Residence, @Start_Date 'Date_Range_Start', @End_Date 'Date_Range_End', Percent_Change
FROM cte 
WHERE 
Data_Period_Start >= @Start_Date AND Data_Period_End <= @End_Date

END

EXECUTE avg_wk_pct '2021-09-18' , '2021-10-16'

-- A user-defined function that takes in a jurisdiction as input 
--  and returns the average crude COVID rate for that jurisdiction over the entire dataset

CREATE FUNCTION avg_cc_rate(@Jurisdiction_Residence AS VARCHAR(30))
RETURNS TABLE
AS 
RETURN
			SELECT Jurisdiction_Residence, AVG(Crude_COVID_Rate) 'Average_Crude_COVID_Rate'
			FROM COVID_Data
			WHERE Jurisdiction_Residence = @Jurisdiction_Residence
			GROUP BY Jurisdiction_Residence

SELECT * FROM
avg_cc_rate('Region 1')

--Using both the stored procedure and the user-defined function to compare the average weekly percentage change in COVID deaths for each jurisdiction 
--to the average crude COVID rate for that jurisdiction.

CREATE FUNCTION avg_cc_rate2()
RETURNS TABLE
AS
RETURN
			SELECT Jurisdiction_Residence, AVG(Crude_COVID_Rate) 'Average_Crude_COVID_Rate'
			FROM COVID_Data
			GROUP BY Jurisdiction_Residence


CREATE PROCEDURE compare @Start_Date AS DATE, @End_Date AS DATE
AS
BEGIN 
           CREATE TABLE #avg_wk_pct(
		   Jurisdiction_Residence VARCHAR(30),
		   Date_Range_Start DATE,
		   Date_Range_End DATE,
		   Percent_Change FLOAT
		   )

		   INSERT INTO #avg_wk_pct
           EXEC avg_wk_pct @Start_Date, @End_Date

		   SELECT A.Jurisdiction_Residence, A.Date_Range_Start, A.Date_Range_End, A.Percent_Change, B.Average_Crude_COVID_Rate
		   FROM #avg_wk_pct A
		   JOIN dbo.avg_cc_rate2() B
		   ON A.Jurisdiction_Residence = B.Jurisdiction_Residence

		   DROP TABLE #avg_wk_pct
END

EXEC compare '2021-09-18' , '2021-10-16'
