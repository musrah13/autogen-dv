/****** Object:  Trigger [dbo].[Trig_DvEntityLinksInsert]    Script Date: 14/04/2023 11:13:27 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER TRIGGER [dbo].[Trig_DvEntityLinksInsert] ON [dbo].[DV_ENTITYLINKS]
	AFTER INSERT
AS
DECLARE @tbl_name VARCHAR(128)
DECLARE @id INT
DECLARE @separator_count INT
DECLARE @sat_prefix VARCHAR(128)
DECLARE @err_invalid VARCHAR(MAX)
DECLARE @err_not_tbl VARCHAR(MAX)
BEGIN
	SELECT @id = ID, @tbl_name = upper(tbl) FROM INSERTED

	SET @err_not_tbl = 'The table '''+@tbl_name+''' is not valid either because it doesn''t exist in the Data Vault or the naming convention is incorrect (name should start with ''SAT_'').  Deleting inserted record. Try again...'

	SET @sat_prefix = LEFT(@tbl_name, 4)

	IF @sat_prefix <> 'SAT_' OR NOT EXISTS (SELECT 1 FROM SYSOBJECTS WHERE xtype = 'U' AND upper(name) = @tbl_name)
	BEGIN
		DELETE FROM [dbo].DV_ENTITYLINKS WHERE tbl = @tbl_name and id = @id
		raiserror(@err_not_tbl, 11, 0)	
	END
END