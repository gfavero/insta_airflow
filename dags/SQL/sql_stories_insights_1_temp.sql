CREATE OR REPLACE TABLE `instagram-project-337102.insta_database.Stories_Insights_Temp`
AS
SELECT *
FROM (
  SELECT
        *,
        ROW_NUMBER()
          OVER (PARTITION BY story_id,dt,hour order by story_id )
          row_number,
          CONCAT(extracted_date,",",story_id) as key_1
  FROM 
        ( SELECT
          *,
          EXTRACT(DATE FROM extracted_date) dt,
          FORMAT_TIMESTAMP("%H", TIMESTAMP_TRUNC(extracted_date, HOUR)) hour
          FROM `instagram-project-337102.insta_database.Stories_Insights`
          order by extracted_date)
)
WHERE row_number = 1 and impressions IS NOT NULL
order by story_id, extracted_date 
