CREATE SCHEMA IF NOT EXISTS TEST1;

CREATE TABLE TEST1.VINES (
    Num varchar,
    Country varchar(255),
    Description varchar(255),
    Designation varchar(255),
    Points varchar,
    Price varchar,
    Province varchar(255),
    Region_1 varchar(255),
    Region_2 varchar(255),
    Variety varchar(255),
    Winery varchar(255)
) 
UNSEGMENTED ALL NODES;


COPY TEST1.VINES (
    Num,
    Country,
    Description,
    Designation,
    Points,
    Price,
    Province,
    Region_1,
    Region_2,
    Variety,
    Winery 
)
FROM LOCAL '/tmp/dataset/winemag.csv'  
DELIMITER ',' 
NULL '' 
SKIP 1 
TRAILING NULLCOLS;


select * from TEST1.VINES limit 100;


#metadata table 
CREATE SCHEMA IF NOT EXISTS SBX;

CREATE TABLE IF NOT EXISTS SBX.ETL_FILE_LOAD (
	FILE_ID AUTO_INCREMENT(1, 1, 1) NOT NULL,
	SOURCE VARCHAR(128) NOT NULL,
	FILE_NAME VARCHAR(64) NOT NULL,
	LOAD_TS TIMESTAMP NOT NULL,
	PRIMARY KEY (FILE_ID, SOURCE, FILE_NAME) ENABLED
)
ORDER BY SOURCE, FILE_NAME
UNSEGMENTED ALL NODES;


##stage table

CREATE TABLE IF NOT EXISTS SBX.STG_VINES (
    Num varchar,
    Country varchar(255),
    Description varchar(255),
    Designation varchar(255),
    Points varchar,
    Price varchar,
    Province varchar(255),
    Region_1 varchar(255),
    Region_2 varchar(255),
    Variety varchar(255),
    Winery varchar(255),
 PRIMARY KEY (Winery)
)
ORDER BY
    Winery
SEGMENTED BY HASH(Winery) ALL NODES;


# upload to stage

INSERT INTO SBX.STG_VINES(
    Num,
    Country,
    Description,
    Designation,
    Points,
    Price,
    Province,
    Region_1,
    Region_2,
    Variety,
    Winery 
)SELECT
    Num,
    Country,
    Description,
    Designation,
    Points,
    Price,
    Province,
    Region_1,
    Region_2,
    Variety,
    Winery 
FROM TEST1.VINES where Winery is not NULL;


SELECT COUNT(*) FROM SBX.STG_VINES

# upload to metadata

INSERT INTO SBX.ETL_FILE_LOAD (
	SOURCE ,
	FILE_NAME ,
	LOAD_TS
)
SELECT	DISTINCT
	'ERP' AS SOURCE ,
	'STG_OPER' AS FILE_NAME ,
	GETDATE() as LOAD_TS
FROM SBX.STG_VINES stg
	LEFT JOIN SBX.ETL_FILE_LOAD fl
		ON fl.SOURCE = 'ERP'
			AND fl.FILE_NAME = 'STG_OPER'
WHERE fl.FILE_ID IS NULL;

SELECT * FROM SBX.ETL_FILE_LOAD;


# ODS Table

CREATE TABLE IF NOT EXISTS SBX.ODS_VINES (
	FILE_ID INTEGER NOT NULL,
	LOAD_TS TIMESTAMP NOT NULL,
    Num varchar,
    Country varchar(255),
    Description varchar(255),
    Designation varchar(255),
    Points varchar,
    Price varchar,
    Province varchar(255),
    Region_1 varchar(255),
    Region_2 varchar(255),
    Variety varchar(255),
    Winery varchar(255),
 PRIMARY KEY (Num) ENABLED
)
ORDER BY
	FILE_ID,
	Winery
SEGMENTED BY HASH(Winery) ALL NODES;


# create view table for ODS


CREATE OR REPLACE VIEW SBX.V_STG_VINES_ODS_VINES AS
SELECT
	fl.FILE_ID ,
	fl.LOAD_TS ,
	 src.Num,
    src.Country,
    src.Description,
    src.Designation,
    src.Points,
    src.Price,
    src.Province,
    src.Region_1,
    src.Region_2,
    src.Variety,
    src.Winery 
FROM SBX.STG_VINES src
INNER JOIN (
		SELECT
			FILE_ID ,
			LOAD_TS ,
			SOURCE ,
			FILE_NAME
		FROM SBX.ETL_FILE_LOAD
		LIMIT 1 OVER (PARTITION BY SOURCE, FILE_NAME ORDER BY LOAD_TS DESC)
		) fl
ON fl.SOURCE = 'ERP' AND
	fl.FILE_NAME = 'STG_OPER'
LEFT JOIN SBX.ODS_VINES trg
ON fl.FILE_ID = trg.FILE_ID AND
	src.Num = trg.Num
WHERE trg.FILE_ID IS NULL;


# upload to ODS table

INSERT INTO SBX.ODS_VINES (
    FILE_ID ,
    LOAD_TS ,
    Num,
    Country,
    Description,
    Designation,
    Points,
    Price,
    Province,
    Region_1,
    Region_2,
    Variety,
    Winery 
)
SELECT
    src.FILE_ID ,
    src.LOAD_TS , 
    src.Num,
    src.Country,
    src.Description,
    src.Designation,
    src.Points,
    src.Price,
    src.Province,
    src.Region_1,
    src.Region_2,
    src.Variety,
    src.Winery 
FROM SBX.V_STG_VINES_ODS_VINES src;

SELECT * FROM SBX.ODS_VINES;

# create DDS HUB table

CREATE TABLE IF NOT EXISTS SBX.DDS_HUB_Num (
	HK_Num VARCHAR(32) NOT NULL,
	FILE_ID INTEGER NOT NULL,
	LOAD_TS TIMESTAMP NOT NULL,
	Num VARCHAR(255) NOT NULL,
	PRIMARY KEY (HK_Num) ENABLED
)
ORDER BY HK_Num
SEGMENTED BY HASH(HK_Num) ALL NODES;


# create view for Hub table

CREATE OR REPLACE VIEW SBX.V_STG_OPER_DDS_HUB_Num AS
SELECT DISTINCT
	MD5(src.Num) AS HK_Num ,
	fl.FILE_ID ,
	fl.LOAD_TS ,
	src.Num
FROM SBX.STG_VINES src
INNER JOIN (
		SELECT
			FILE_ID ,
			LOAD_TS ,
			SOURCE ,
			FILE_NAME
		FROM SBX.ETL_FILE_LOAD
		LIMIT 1 OVER (PARTITION BY SOURCE, FILE_NAME ORDER BY LOAD_TS DESC)
		) fl
ON fl.SOURCE = 'ERP' AND fl.FILE_NAME = 'STG_OPER'
LEFT JOIN SBX.DDS_HUB_Num trg
ON MD5(src.Num) = trg.HK_Num
WHERE trg.HK_Num IS NULL;


# Insert into HUB table

INSERT INTO SBX.DDS_HUB_Num (
	HK_Num ,
	FILE_ID ,
	LOAD_TS ,
	Num
)
SELECT
	src.HK_Num ,
	src.FILE_ID ,
	src.LOAD_TS ,
	src.Num
FROM SBX.V_STG_OPER_DDS_HUB_Num src;

SELECT * FROM SBX.DDS_HUB_Num;

# create DDS Satellite table

CREATE TABLE IF NOT EXISTS SBX.DDS_ST_Num_METRICS (
	HK_Num VARCHAR(32) NOT NULL,
	FILE_ID INTEGER NOT NULL,
	LOAD_TS TIMESTAMP NOT NULL,
    Num varchar,
    Country varchar(255),
    Description varchar(255),
    Designation varchar(255),
    Points varchar,
    Price varchar,
    Province varchar(255),
    Region_1 varchar(255),
    Region_2 varchar(255),
    Variety varchar(255),
    Winery varchar(255),
    HASHDIFF VARCHAR(32) NOT NULL,
	PRIMARY KEY (HK_Num, HASHDIFF) ENABLED
)
ORDER BY
	HK_Num ,
	LOAD_TS
SEGMENTED BY HASH(HK_Num) ALL NODES;


# view for DDS Satellite table

CREATE OR REPLACE VIEW SBX.V_STG_OPER_DDS_ST_Num_METRICS AS
SELECT
	MD5(src.Num) AS HK_Num ,
	fl.FILE_ID ,
	fl.LOAD_TS ,
	MD5(isnull(src.Country::VARCHAR(255),'NULL')||
	    isnull(src.Description::VARCHAR(255),'NULL')||
            isnull(src.Designation::VARCHAR(255),'NULL'||
            isnull(src.Points::VARCHAR,'NULL')||
            isnull(src.Price::VARCHAR,'NULL')||
            isnull(src.Province::VARCHAR(255),'NULL')||
            isnull(src.Region_1::VARCHAR(255),'NULL')||
            isnull(src.Region_2::VARCHAR(255),'NULL')||
            isnull(src.Variety::VARCHAR(255),'NULL')||
	    isnull(src.Winery::VARCHAR(255),'NULL')) AS HASHDIFF, 
	  src.Country,
    src.Description,
    src.Designation,
    src.Points,
    src.Price,
    src.Province,
    src.Region_1,
    src.Region_2,
    src.Variety,
    src.Winery 
FROM SBX.STG_VINES src
	INNER JOIN (
		SELECT
			FILE_ID ,
			LOAD_TS ,
			SOURCE ,
			FILE_NAME
		FROM SBX.ETL_FILE_LOAD
		LIMIT 1 OVER (PARTITION BY SOURCE, FILE_NAME ORDER BY LOAD_TS DESC)
		) fl
			ON fl.SOURCE = 'ERP'
				AND fl.FILE_NAME = 'STG_OPER'
	LEFT JOIN SBX.DDS_ST_VINES_METRICS trg
		ON MD5(src.Num) = trg.HK_Num
			AND MD5(isnull(src.Country::VARCHAR(255),'NULL')||
	    isnull(src.Description::VARCHAR(255),'NULL')||
            isnull(src.Designation::VARCHAR(255),'NULL'||
            isnull(src.Points::VARCHAR,'NULL')||
            isnull(src.Price::VARCHAR,'NULL')||
            isnull(src.Province::VARCHAR(255),'NULL')||
            isnull(src.Region_1::VARCHAR(255),'NULL')||
            isnull(src.Region_2::VARCHAR(255),'NULL')||
            isnull(src.Variety::VARCHAR(255),'NULL')||
	    isnull(src.Winery::VARCHAR(255),'NULL')) = trg.HASHDIFF
WHERE trg.HK_Num IS NULL;


# insert into sattelite

INSERT INTO SBX.DDS_ST_VINES_METRICS (
	HK_Num ,
	FILE_ID ,
	LOAD_TS ,
	HASHDIFF ,
        Country,
        Description,
        Designation,
        Points,
        Price,
        Province,
        Region_1,
        Region_2,
        Variety,
        Winery 
)
SELECT
	src.HK_Num ,
	src.FILE_ID ,
	src.LOAD_TS ,
	src.HASHDIFF ,
	src.Country,
        src.Description,
        src.Designation,
        src.Points,
        src.Price,
        src.Province,
        src.Region_1,
        src.Region_2,
        src.Variety,
        src.Winery 
FROM SBX.V_STG_OPER_DDS_ST_Num_METRICS src;

SELECT * FROM SBX.DDS_ST_DISTRICT_METRICS;


# view to get data


CREATE OR REPLACE VIEW SBX.V_DDS_OPER AS
SELECT
        Num,
	Country,
        Description,
        Designation,
        Points,
        Price,
        Province,
        Region_1,
        Region_2,
        Variety,
        Winery 
FROM SBX.DDS_HUB_VINES hub
INNER JOIN SBX.DDS_ST_VINES_METRICS st
ON hub.HK_Num = st.HK_Num
LIMIT 1 OVER (PARTITION BY Num ORDER BY st.LOAD_TS DESC);


SELECT * FROM SBX.V_DDS_OPER






