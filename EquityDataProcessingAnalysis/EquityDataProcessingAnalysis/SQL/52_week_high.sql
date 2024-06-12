SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		John Snow
-- Create date: 6/11/2024
-- Description:	Get the highest price in the last 52 weeks, by ticker ID
-- =============================================
CREATE PROCEDURE Get_52_Week_High_Price
	@TickerID INT
AS
BEGIN
	DECLARE @EndDate DATE = GETDATE()
	DECLARE @StartDate DATE = DATEADD(WEEk, -52, @EndDate);

	SELECT MAX(price_high) FROM Prices WHERE ticker_id = @TickerID AND price_date BETWEEN @StartDate AND @EndDate;
END
GO