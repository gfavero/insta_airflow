CREATE OR REPLACE TABLE `instagram-project-337102.insta_database.Stories_Temp`
AS
SELECT *
FROM (
  SELECT
        *,
        ROW_NUMBER()
          OVER (PARTITION BY story_id order by extracted_date)
          row_number
  FROM `instagram-project-337102.insta_database.Stories`
)
WHERE row_number = 1
order by created_date
