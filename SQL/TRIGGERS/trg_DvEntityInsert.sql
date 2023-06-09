/****** Object:  Trigger [dbo].[Trig_DvEntityInsert]    Script Date: 14/04/2023 11:12:42 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER TRIGGER [dbo].[Trig_DvEntityInsert] ON [dbo].[DV_ENTITY]
	AFTER INSERT
AS
DECLARE @tbl_name VARCHAR(128)
DECLARE @sat_prefix VARCHAR(128)
DECLARE @lnk_prefix VARCHAR(128)
DECLARE @separator_count INT
DECLARE @err_invalid VARCHAR(MAX)
DECLARE @err_lnk VARCHAR(MAX)
BEGIN
	SET @err_invalid = 'ERROR: INVALID DATAVAULT TABLE NAME, SHOULD START WITH ''SAT_'' OR ''SAT_LNK_''. DELETING INSERTED RECORD. TRY AGAIN...'
	--SET NOCOUNT ON;
	SELECT @tbl_name = upper(tbl) FROM INSERTED

	SET @separator_count = len(@tbl_name) - len(replace(@tbl_name, '_', ''))

	IF @separator_count = 0
	BEGIN
		BEGIN TRANSACTION
			DELETE FROM [dbo].DV_ENTITY WHERE tbl = @tbl_name
		COMMIT TRANSACTION
		raiserror(@err_invalid, 11, 0)
	END
	ELSE IF @separator_count = 1
	BEGIN
		SET @sat_prefix = LEFT(@tbl_name, 4)

		IF @sat_prefix <> 'SAT_'
		BEGIN
			BEGIN TRANSACTION
				DELETE FROM [dbo].DV_ENTITY WHERE tbl = @tbl_name
			COMMIT TRANSACTION
			raiserror(@err_invalid, 11, 0)
		END
		ELSE
		BEGIN
			EXEC [dbo].[PCDR_SAT_HUB_CREATION]
			EXEC [dbo].[PCDR_SAT_COL_ADD]
		END
	END
	ELSE IF @separator_count > 1
	BEGIN
		SET @sat_prefix = LEFT(@tbl_name, 4)
		SET @lnk_prefix = SUBSTRING(@tbl_name, 5, 4)

		IF @sat_prefix <> 'SAT_' OR @lnk_prefix <> 'LNK_'
		BEGIN
			BEGIN TRANSACTION
				DELETE FROM [dbo].DV_ENTITY WHERE tbl = @tbl_name
			COMMIT TRANSACTION
			raiserror(@err_invalid, 11, 0)
		END
		ELSE
		BEGIN
			IF EXISTS (SELECT 1 FROM SYSOBJECTS WHERE xtype = 'U' AND upper(name) = RIGHT(@tbl_name, LEN(@tbl_name)-4))
			BEGIN
				EXEC [dbo].[PCDR_SAT_LNK_CREATION]
				EXEC [dbo].[PCDR_SAT_COL_ADD]
			END
			ELSE
			BEGIN
				SET @err_lnk = 'ERROR: Unable to create ' + @tbl_name + ' because the table ' + RIGHT(@tbl_name, LEN(@tbl_name)-4) + ' was not found. Please create the LNK table first. DELETING INSERTED RECORD. TRY AGAIN...'
				BEGIN TRANSACTION
					DELETE FROM [dbo].DV_ENTITY WHERE tbl = @tbl_name
				COMMIT TRANSACTION
				raiserror(@err_lnk, 11, 0)
			END
		END
	END
END