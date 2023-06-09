/****** Object:  StoredProcedure [dbo].[PCDR_ENTITYLINKS_INSERT]    Script Date: 14/04/2023 11:15:05 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[PCDR_ENTITYLINKS_INSERT] @lnk_tbl_id int = null, @tbl_names varchar(max), @muted BIT = 0
AS
DECLARE @tbl_name VARCHAR(128)
DECLARE @id INT
DECLARE @tbl_para_count INT
DECLARE @tbl_lnk_count INT
DECLARE @no_of_records INT
DECLARE @max_id INT
DECLARE @tbl_err INT = 0
DECLARE @potential_lnk_count INT

SELECT @tbl_para_count = COUNT(*) FROM STRING_SPLIT(REPLACE(UPPER(@tbl_names), ' ', ''), ',')

IF @tbl_para_count = 1
BEGIN
	IF @lnk_tbl_id IS NULL
	BEGIN
		IF @muted <> 0
		BEGIN
			PRINT 'Please provide an id to add the table to an existing lnk. Record not inserted.'
		END
	END
	ELSE
	BEGIN
		IF EXISTS (SELECT 1 FROM DV_ENTITYLINKS WHERE ID = @lnk_tbl_id)
		BEGIN
			SELECT @potential_lnk_count = COUNT(*) + 1 FROM DV_ENTITYLINKS WHERE ID = @lnk_tbl_id
			IF NOT EXISTS (SELECT 1 FROM DV_ENTITYLINKS WHERE ID IN
							(SELECT ID FROM 
								(SELECT ID, COUNT(*) AS existing_lnk_count FROM DV_ENTITYLINKS WHERE ID IN 
									(SELECT DISTINCT ID FROM DV_ENTITYLINKS WHERE TBL IN 
										(SELECT TBL FROM DV_ENTITYLINKS WHERE ID = @lnk_tbl_id) AND ID <> @lnk_tbl_id) GROUP BY ID) sub WHERE sub.existing_lnk_count = 3) AND TBL = (SELECT TOP 1 * FROM STRING_SPLIT(REPLACE(UPPER(@tbl_names), ' ', ''), ',')))
			BEGIN
				BEGIN TRANSACTION
					INSERT INTO DV_ENTITYLINKS (ID, TBL) VALUES (@lnk_tbl_id,(SELECT TOP 1 * FROM STRING_SPLIT(REPLACE(UPPER(@tbl_names), ' ', ''), ',')))
				COMMIT TRANSACTION
			END
			ELSE
			BEGIN
				IF @muted <> 0
				BEGIN
					PRINT 'In the table DV_ENTITYLINKS, the TBL values associated with the ID: ' + CAST(@lnk_tbl_id AS VARCHAR(128)) + ' already has a lnk combination with the value you are trying to insert. Therefore, the record is not inserted.'
				END
			END
		END
		ELSE
		BEGIN
			IF @muted <> 0
			BEGIN
				PRINT 'No entry found with the ID: ' + CAST(@lnk_tbl_id AS VARCHAR(128)) + '. Record not inserted.'
			END
		END
	END
END
ELSE
BEGIN
	-- If no id is given in procedure parameter

	DECLARE tbl_cursor CURSOR LOCAL FOR
	SELECT * FROM STRING_SPLIT(REPLACE(UPPER(@tbl_names), ' ', ''), ',')

	OPEN tbl_cursor
	FETCH NEXT FROM tbl_cursor INTO @tbl_name

	WHILE @@FETCH_STATUS = 0
	BEGIN
		IF NOT EXISTS(SELECT 1 FROM SYSOBJECTS WHERE xtype = 'U' AND upper(name) = @tbl_name) OR [dbo].[IS_VALID_SAT](@tbl_name) <> 1
		BEGIN
			SET @tbl_err = 1
			IF @muted <> 0
			BEGIN
				PRINT 'The table ''' + @tbl_name + ''' either does not exist in the database or named incorrectly. Name should start with ''SAT_''. No record inserted.'
			END
		END
		FETCH NEXT FROM tbl_cursor INTO @tbl_name
	END
	CLOSE tbl_cursor  
	DEALLOCATE tbl_cursor

	IF @lnk_tbl_id IS NULL
	BEGIN
		-- if same combination of tables do not already exists
		IF NOT EXISTS (SELECT COUNT(*) FROM DV_ENTITYLINKS WHERE ID IN (
							SELECT DISTINCT ID FROM DV_ENTITYLINKS WHERE TBL IN (
								SELECT * FROM STRING_SPLIT(REPLACE(UPPER(@tbl_names), ' ', ''), ',')
							)
						) GROUP BY ID HAVING COUNT(*) = @tbl_para_count)
		BEGIN
			SELECT @no_of_records = COUNT(*) FROM DV_ENTITYLINKS

			IF @no_of_records > 0
			BEGIN
				SELECT @max_id = MAX(ID) FROM DV_ENTITYLINKS
			END

			ELSE
			BEGIN
				SET @max_id = 0
			END

			IF @tbl_err = 0
			BEGIN
				DECLARE tbl_cursor CURSOR LOCAL FOR
				SELECT * FROM STRING_SPLIT(REPLACE(UPPER(@tbl_names), ' ', ''), ',')

				OPEN tbl_cursor
				FETCH NEXT FROM tbl_cursor INTO @tbl_name

				WHILE @@FETCH_STATUS = 0
				BEGIN
					BEGIN TRANSACTION
						INSERT INTO DV_ENTITYLINKS (ID, TBL) VALUES (@max_id + 1, @tbl_name)
					COMMIT TRANSACTION

					FETCH NEXT FROM tbl_cursor INTO @tbl_name
				END
				CLOSE tbl_cursor  
				DEALLOCATE tbl_cursor
			END
		END
		ELSE
		BEGIN
			IF @muted <> 0
			BEGIN
				PRINT 'The lnk table combination already exists in the table dv_entitylinks. Records not inserted.'
			END
		END
	END
	ELSE
	BEGIN
		-- if same combination of tables do not already exists
		IF NOT EXISTS (SELECT COUNT(*) FROM DV_ENTITYLINKS WHERE ID IN (
							SELECT DISTINCT ID FROM DV_ENTITYLINKS WHERE TBL IN (
								SELECT * FROM STRING_SPLIT(REPLACE(UPPER(@tbl_names), ' ', ''), ',')
							)
						) GROUP BY ID HAVING COUNT(*) = @tbl_para_count)
		BEGIN
			IF @tbl_err = 0
			BEGIN
				DECLARE tbl_cursor CURSOR LOCAL FOR
				SELECT * FROM STRING_SPLIT(REPLACE(UPPER(@tbl_names), ' ', ''), ',')

				OPEN tbl_cursor
				FETCH NEXT FROM tbl_cursor INTO @tbl_name

				WHILE @@FETCH_STATUS = 0
				BEGIN
					IF NOT EXISTS (SELECT 1 FROM DV_ENTITYLINKS WHERE ID = @lnk_tbl_id and TBL = @tbl_name)
					BEGIN
						BEGIN TRANSACTION
							INSERT INTO DV_ENTITYLINKS (ID, TBL) VALUES (@lnk_tbl_id, @tbl_name)
						COMMIT TRANSACTION
					END
					ELSE
					BEGIN
						IF @muted <> 0
						BEGIN
							PRINT '('+CAST(@lnk_tbl_id AS VARCHAR(128))+','+@tbl_name+') Record already exists in DV_ENTITYLINKS. Not inserted.'
						END
					END

					FETCH NEXT FROM tbl_cursor INTO @tbl_name 
				END

				CLOSE tbl_cursor  
				DEALLOCATE tbl_cursor
			END
		END
		ELSE
		BEGIN
			IF @muted <> 0
			BEGIN
				PRINT 'The lnk table combination already exists in the table dv_entitylinks. Records not inserted.'
			END
		END
	END
END