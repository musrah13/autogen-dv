/****** Object:  StoredProcedure [dbo].[PCDR_SAT_COL_ADD]    Script Date: 14/04/2023 11:14:27 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[PCDR_SAT_COL_ADD] @muted BIT = 0
AS
DECLARE @tbl_name VARCHAR(128)
DECLARE @col_name VARCHAR(128)
DECLARE @d_type VARCHAR(128)
DECLARE @d_fault VARCHAR(128)
DECLARE @query_alter NVARCHAR(MAX)
DECLARE @query_alter_pre NVARCHAR(MAX)
DECLARE @query_update NVARCHAR(MAX)

DECLARE col_cursor CURSOR LOCAL FOR
SELECT upper(tbl), upper(col), upper(dtype), upper(dfault) 
FROM DV_ENTITY 
WHERE processed = 0 and upper(tbl) like 'SAT_%'
AND upper(tbl) IN (
	SELECT upper(name)
	FROM SYSOBJECTS
	WHERE xtype = 'U'
);

OPEN col_cursor
FETCH NEXT FROM col_cursor INTO @tbl_name, @col_name, @d_type, @d_fault 

WHILE @@FETCH_STATUS = 0
BEGIN
	SET @query_alter_pre = 'ALTER TABLE ' + @tbl_name + ' ADD ' + @col_name + ' ' + @d_type
	IF @d_fault <> 'NULL' AND @d_fault <> 'NOT NULL' AND @d_fault IS NOT NULL AND ISNUMERIC(@d_fault) <> 1
	BEGIN
		SET @query_alter = @query_alter_pre + ' DEFAULT ''' + @d_fault + ''''
		IF @muted <> 0
		BEGIN
			print @query_alter
		END
	END
	ELSE
	BEGIN
		IF ISNUMERIC(@d_fault) = 1
		BEGIN
			SET @query_alter = @query_alter_pre + ' DEFAULT ' + @d_fault
			IF @muted <> 0
			BEGIN
				print @query_alter
			END
		END
		ELSE
		BEGIN
			SET @query_alter = @query_alter_pre + ' ' + ISNULL(@d_fault, 'NULL')
			IF @muted <> 0
			BEGIN
				print @query_alter
			END
		END
	END

	SET @query_update = 'UPDATE DV_ENTITY SET processed = 1 WHERE tbl = ''' + @tbl_name + ''' AND col = ''' + @col_name + ''''

	BEGIN TRANSACTION
		EXEC SP_EXECUTESQL @query_alter
		EXEC SP_EXECUTESQL @query_update
	COMMIT TRANSACTION

	FETCH NEXT FROM col_cursor INTO @tbl_name, @col_name, @d_type, @d_fault
END

CLOSE col_cursor  

DEALLOCATE col_cursor 