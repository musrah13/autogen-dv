/****** Object:  StoredProcedure [dbo].[PCDR_SAT_HUB_CREATION]    Script Date: 14/04/2023 11:14:17 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[PCDR_SAT_HUB_CREATION] @muted BIT = 0
AS
DECLARE @tbl_name VARCHAR(128)
DECLARE @query_create_hub NVARCHAR(MAX)
DECLARE @query_create_sat NVARCHAR(MAX)

DECLARE tbl_cursor CURSOR LOCAL FOR
SELECT DISTINCT upper(tbl) as tbl_name
FROM DV_ENTITY
WHERE processed = 0 and upper(tbl) NOT IN (
	SELECT upper(name)
	FROM SYSOBJECTS
	WHERE xtype = 'U'
) AND upper(tbl) like 'SAT_%' and upper(tbl) not like 'SAT_LNK_%'

OPEN tbl_cursor
FETCH NEXT FROM tbl_cursor INTO @tbl_name

WHILE @@FETCH_STATUS = 0
BEGIN
	-- hub table creation query
	SET @query_create_hub = '
	CREATE TABLE ' + QUOTENAME('HUB_' + RIGHT(@tbl_name,LEN(@tbl_name)-4)) + '(
		HUB_ID VARCHAR(4000),
		SRC VARCHAR(4000) NOT NULL,
		SRC_KEY VARCHAR(4000) NOT NULL,
		IS_DELETED TINYINT NULL,
		LOAD_TS DATETIME NOT NULL
		PRIMARY KEY (HUB_ID)
	)'

	-- sat table creation query
	SET @query_create_sat = '
	CREATE TABLE ' + QUOTENAME(@tbl_name) + '(
		HUB_ID VARCHAR(4000),
		SRC VARCHAR(4000) NOT NULL,
		SRC_KEY VARCHAR(4000) NOT NULL,
		IS_DELETED TINYINT NULL,
		LOAD_TS DATETIME NOT NULL,
		PRIMARY KEY (HUB_ID)
	)'

	EXEC SP_EXECUTESQL @query_create_hub
	EXEC SP_EXECUTESQL @query_create_sat

	FETCH NEXT FROM tbl_cursor INTO @tbl_name 
END

CLOSE tbl_cursor  

DEALLOCATE tbl_cursor
