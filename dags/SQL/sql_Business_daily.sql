CREATE OR REPLACE TABLE `instagram-project-337102.insta_database.Business_Discovery_daily`
AS
SELECT *
  FROM (
        SELECT
              *,
              username  AS Especialista,
              ROW_NUMBER()OVER (PARTITION BY username order by extracted_date Desc) as row_number,
              FROM ( SELECT
                      *,
                      EXTRACT(DATE FROM extracted_date) dt
                      FROM `instagram-project-337102.insta_database.Business_Discovery`)
        )
   WHERE row_number = 1