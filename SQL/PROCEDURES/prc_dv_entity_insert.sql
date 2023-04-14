/****** Object:  StoredProcedure [dbo].[PCDR_ENTITY_INSERT]    Script Date: 14/04/2023 11:15:16 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[PCDR_ENTITY_INSERT] @tbl VARCHAR(128), @col VARCHAR(128), @dtype VARCHAR(128), @dfault VARCHAR(128) = NULL
AS
INSERT INTO DV_ENTITY (TBL, COL, DTYPE, DFAULT) VALUES (@tbl, @col, @dtype, @dfault)