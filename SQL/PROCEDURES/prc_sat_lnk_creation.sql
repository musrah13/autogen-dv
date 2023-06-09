/****** Object:  StoredProcedure [dbo].[PCDR_SAT_LNK_CREATION]    Script Date: 14/04/2023 11:13:48 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[PCDR_SAT_LNK_CREATION] @muted BIT = 0
AS
DECLARE @tbl_name VARCHAR(128)
DECLARE @query_create NVARCHAR(MAX)

DECLARE tbl_cursor CURSOR LOCAL FOR
SELECT DISTINCT upper(tbl) as tbl_name
FROM DV_ENTITY
WHERE processed = 0 and upper(tbl) NOT IN (
	SELECT upper(name)
	FROM SYSOBJECTS
	WHERE xtype = 'U'
) AND upper(tbl) like 'SAT_LNK_%'

OPEN tbl_cursor
FETCH NEXT FROM tbl_cursor INTO @tbl_name

WHILE @@FETCH_STATUS = 0
BEGIN
	-- SAT_LNK table creation query
	SET @query_create = '
	CREATE TABLE ' + QUOTENAME(@tbl_name) + '(
		LNK_ID VARCHAR(4000),
		LOAD_TS DATETIME
		PRIMARY KEY (LNK_ID)
	)'

	BEGIN TRANSACTION
		EXEC SP_EXECUTESQL @query_create
	COMMIT TRANSACTION

	FETCH NEXT FROM tbl_cursor INTO @tbl_name 
END

CLOSE tbl_cursor  

DEALLOCATE tbl_cursor
