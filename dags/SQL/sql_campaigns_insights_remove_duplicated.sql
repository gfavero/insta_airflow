SELECT * FROM `instagram-project-337102.insta_database.CampaignsInsights_Temp`
order by date_start

CREATE OR REPLACE TABLE `instagram-project-337102.insta_database.CampaignsInsights_Temp`
AS
SELECT *
FROM (
  SELECT
      *,
      ROW_NUMBER()
          OVER (PARTITION BY campaign_id,date_start)
          row_number
  FROM `instagram-project-337102.insta_database.CampaignsInsights`
)
WHERE row_number = 1
order by date_start