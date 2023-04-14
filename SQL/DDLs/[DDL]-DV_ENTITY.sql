/****** Object:  Table [dbo].[DV_ENTITY]    Script Date: 09/02/2023 11:26:26 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[DV_ENTITY](
	[tbl] [varchar](4000) NOT NULL,
	[col] [varchar](4000) NOT NULL,
	[dtype] [varchar](4000) NOT NULL,
	[dfault] [varchar](4000) NULL,
	[processed] [bit] NULL,
PRIMARY KEY CLUSTERED 
(
	[tbl] ASC,
	[col] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[DV_ENTITY] ADD  DEFAULT ((0)) FOR [processed]
GO

