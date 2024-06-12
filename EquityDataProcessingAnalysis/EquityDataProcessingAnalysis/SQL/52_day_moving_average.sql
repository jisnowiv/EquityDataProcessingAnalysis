SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		John Snow
-- Create date: 6/11/2024
-- Description:	Find the moving average price by ticker id for the last 52 days.
-- =============================================
CREATE PROCEDURE Get_52_day_moving_average 
	@TickerID INT
AS
BEGIN
	SELECT p.price_date AVG(p.price_close) OVER (
		PARTITION BY Prices.ticker_id
		ORDER BY p.price_date
		ROWS BETWEEN 51 PRECEDING AND CURRENT ROW
	) AS '52_day_moving_average'
	FROM
		Prices p WHERE p.ticker_id = TickerID
	ORDER BY p.price_date
END
GO
