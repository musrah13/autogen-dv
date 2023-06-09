/****** Object:  StoredProcedure [dbo].[PCDR_LNK_CREATION]    Script Date: 14/04/2023 11:14:36 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
---- UNDER DEVELOPMENT
ALTER PROCEDURE [dbo].[PCDR_LNK_CREATION] @muted BIT = 0
AS
DECLARE @id INT
DECLARE @tbl_name VARCHAR(128)
DECLARE @col_name VARCHAR(128)
DECLARE @existing_links INT
DECLARE @lnk_id INT
DECLARE @existing_name VARCHAR(128)
DECLARE @new_name VARCHAR(128)
DECLARE @query_create NVARCHAR(MAX)
DECLARE @query_alter NVARCHAR(MAX)
DECLARE @query_update NVARCHAR(MAX)
DECLARE @lnk_prefix VARCHAR(20) = 'LNK_'
DECLARE @hubid_postfix VARCHAR(20) = '_HUB_ID'
DECLARE @aggswitch_postfix VARCHAR(20) = '_AGG_SWITCH'

SELECT @existing_links = COUNT(*) FROM DV_ENTITYLINKS WHERE ID IN (SELECT DISTINCT ID FROM DV_ENTITYLINKS WHERE PROCESSED = 0) AND PROCESSED = 1

IF @existing_links > 0
BEGIN
	DECLARE tbl_cursor CURSOR LOCAL FOR
	SELECT E.ID, E.EXISTING_NAME, N.NEW_NAME FROM
	(
		SELECT ID, CONCAT(@lnk_prefix,STRING_AGG(RIGHT(TBL, LEN(TBL)-4),'_') WITHIN GROUP (order by TBL)) AS EXISTING_NAME 
		FROM DV_ENTITYLINKS 
		WHERE ID IN (SELECT DISTINCT ID FROM DV_ENTITYLINKS WHERE PROCESSED = 0) AND PROCESSED = 1
		group by ID
	) e
	INNER JOIN 
	(
		SELECT ID, CONCAT(@lnk_prefix,STRING_AGG(RIGHT(TBL, LEN(TBL)-4),'_') WITHIN GROUP (order by TBL)) AS NEW_NAME
		FROM DV_ENTITYLINKS 
		WHERE ID IN (SELECT DISTINCT ID FROM DV_ENTITYLINKS WHERE PROCESSED = 0)
		group by ID
	) n ON e.ID = n.ID

	OPEN tbl_cursor
	FETCH NEXT FROM tbl_cursor INTO @id, @existing_name, @new_name

	WHILE @@FETCH_STATUS = 0
	BEGIN
		BEGIN TRANSACTION
			EXEC sp_rename @existing_name, @new_name
			IF @muted <> 0
			BEGIN
				PRINT 'EXEC sp_rename ' + @existing_name + ',' + @new_name
				PRINT 'Table ' + @existing_name + ' was renamed to ' + @new_name + CHAR(13)
			END
		COMMIT TRANSACTION
		
		DECLARE tbl_sub_cursor CURSOR LOCAL FOR
		SELECT ID, TBL FROM DV_ENTITYLINKS WHERE ID = @id AND PROCESSED = 0

		OPEN tbl_sub_cursor
		FETCH NEXT FROM tbl_sub_cursor INTO @lnk_id, @tbl_name

		WHILE  @@FETCH_STATUS = 0
		BEGIN
			BEGIN TRANSACTION
				SET @query_alter = 'ALTER TABLE ' + @new_name + ' ADD ' + CONCAT(RIGHT(@tbl_name, LEN(@tbl_name)-4),@hubid_postfix) + ' VARCHAR(4000) NOT NULL'
				EXEC SP_EXECUTESQL @query_alter
				IF @muted <> 0
				BEGIN
					PRINT @query_alter
					PRINT 'New column ' + CONCAT(RIGHT(@tbl_name, LEN(@tbl_name)-4),@hubid_postfix) + ' was added to the table ' + @new_name + CHAR(13)
				END

				SET @query_alter = 'ALTER TABLE ' + @new_name + ' ADD ' + CONCAT(RIGHT(@tbl_name, LEN(@tbl_name)-4),@aggswitch_postfix) + ' INT'
				EXEC SP_EXECUTESQL @query_alter
				IF @muted <> 0
				BEGIN
					PRINT @query_alter
					PRINT 'New column ' + CONCAT(RIGHT(@tbl_name, LEN(@tbl_name)-4),@aggswitch_postfix) + ' was added to the table ' + @new_name + CHAR(13)
				END

				SET @query_update = 'UPDATE ' + @new_name + ' SET ' + CONCAT(RIGHT(@tbl_name, LEN(@tbl_name)-4),@hubid_postfix) + '=-9999 WHERE ' + CONCAT(RIGHT(@tbl_name, LEN(@tbl_name)-4),@hubid_postfix) + ' = NULL'
				EXEC SP_EXECUTESQL @query_update
				IF @muted <> 0
				BEGIN
					PRINT @query_update
				END

				SET @query_update = 'UPDATE ' + @new_name + ' SET ' + CONCAT(RIGHT(@tbl_name, LEN(@tbl_name)-4),@aggswitch_postfix) + '=-9999 WHERE ' + CONCAT(RIGHT(@tbl_name, LEN(@tbl_name)-4),@aggswitch_postfix) + ' = NULL'
				EXEC SP_EXECUTESQL @query_update
				IF @muted <> 0
				BEGIN
					PRINT @query_update
				END

				UPDATE DV_ENTITYLINKS SET PROCESSED = 1 WHERE ID = @lnk_id AND TBL = @tbl_name
				IF @muted <> 0
				BEGIN
					PRINT 'UPDATE DV_ENTITYLINKS SET PROCESSED = 1 WHERE ID = ' + CAST(@lnk_id AS VARCHAR(128)) + ' AND TBL = ' + @tbl_name
				END
			COMMIT TRANSACTION

			FETCH NEXT FROM tbl_sub_cursor INTO @lnk_id, @tbl_name
		END
		CLOSE tbl_sub_cursor  
		DEALLOCATE tbl_sub_cursor 

		FETCH NEXT FROM tbl_cursor INTO @id, @existing_name, @new_name
	END
	CLOSE tbl_cursor  
	DEALLOCATE tbl_cursor
END

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

DECLARE tbl_cursor CURSOR LOCAL FOR
SELECT ID, CONCAT(@lnk_prefix,STRING_AGG(RIGHT(TBL, LEN(TBL)-4),'_') WITHIN GROUP (order by TBL)) AS LNK_TBL_NAME
FROM DV_ENTITYLINKS
WHERE PROCESSED = 0
GROUP BY ID

OPEN tbl_cursor
FETCH NEXT FROM tbl_cursor INTO @id, @tbl_name

WHILE @@FETCH_STATUS = 0
BEGIN
	SET @query_create = 'CREATE TABLE ' + QUOTENAME(@tbl_name) + '(
							LNK_ID VARCHAR(4000),
							LOAD_TS DATETIME

							PRIMARY KEY (LNK_ID)
						)'
		EXEC SP_EXECUTESQL @query_create
	IF @muted <> 0
	BEGIN
		PRINT @query_create
		PRINT 'Created table ' + @tbl_name + CHAR(13)
	END

	FETCH NEXT FROM tbl_cursor INTO @id, @tbl_name
END
CLOSE tbl_cursor  
DEALLOCATE tbl_cursor

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

DECLARE tbl_cursor CURSOR LOCAL FOR
SELECT tbl.ID, tbl.LNK_TBL_NAME, col.TBL as colname FROM
(
	SELECT ID, CONCAT(@lnk_prefix,STRING_AGG(RIGHT(TBL, LEN(TBL)-4),'_') WITHIN GROUP (order by TBL)) AS LNK_TBL_NAME
	FROM DV_ENTITYLINKS
	WHERE PROCESSED = 0
	GROUP BY ID
) tbl
INNER JOIN DV_ENTITYLINKS col ON tbl.ID = col.ID AND col.PROCESSED = 0
ORDER BY 1,3

OPEN tbl_cursor
FETCH NEXT FROM tbl_cursor INTO @id, @tbl_name, @col_name

WHILE @@FETCH_STATUS = 0
BEGIN
	BEGIN TRANSACTION
		SET @query_alter = 'ALTER TABLE ' + @tbl_name + ' ADD ' + CONCAT(RIGHT(@col_name, LEN(@col_name)-4),@hubid_postfix) + ' VARCHAR(4000) NOT NULL'
		EXEC SP_EXECUTESQL @query_alter
		IF @muted <> 0
		BEGIN
			PRINT @query_alter
			PRINT 'New column ' + CONCAT(RIGHT(@col_name, LEN(@col_name)-4),@hubid_postfix) + ' was added to the table ' + @tbl_name + CHAR(13)
		END

		SET @query_alter = 'ALTER TABLE ' + @tbl_name + ' ADD ' + CONCAT(RIGHT(@col_name, LEN(@col_name)-4),@aggswitch_postfix) + ' INT'
		EXEC SP_EXECUTESQL @query_alter
		IF @muted <> 0
		BEGIN
			PRINT @query_alter
			PRINT 'New column ' + CONCAT(RIGHT(@col_name, LEN(@col_name)-4),@aggswitch_postfix) + ' was added to the table ' + @tbl_name + CHAR(13)
		END

		SET @query_update = 'UPDATE ' + @tbl_name + ' SET ' + CONCAT(RIGHT(@col_name, LEN(@col_name)-4),@hubid_postfix) + '=-9999 WHERE ' + CONCAT(RIGHT(@col_name, LEN(@col_name)-4),@hubid_postfix) + ' = NULL'
		EXEC SP_EXECUTESQL @query_update
		IF @muted <> 0
		BEGIN
			PRINT @query_update
		END

		SET @query_update = 'UPDATE ' + @tbl_name + ' SET ' + CONCAT(RIGHT(@col_name, LEN(@col_name)-4),@aggswitch_postfix) + '=-9999 WHERE ' + CONCAT(RIGHT(@col_name, LEN(@col_name)-4),@aggswitch_postfix) + ' = NULL'
		EXEC SP_EXECUTESQL @query_update
		IF @muted <> 0
		BEGIN
			PRINT @query_update
		END

		UPDATE DV_ENTITYLINKS SET PROCESSED = 1 WHERE ID = @id AND TBL = @col_name
		IF @muted <> 0
		BEGIN
			PRINT 'UPDATE DV_ENTITYLINKS SET PROCESSED = 1 WHERE ID = ' + CAST(@id AS VARCHAR(MAX)) + ' AND TBL = ' + @col_name
		END
	COMMIT TRANSACTION

	FETCH NEXT FROM tbl_cursor INTO @id, @tbl_name, @col_name
END
CLOSE tbl_cursor
DEALLOCATE tbl_cursor