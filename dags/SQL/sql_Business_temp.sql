CREATE OR REPLACE TABLE `instagram-project-337102.insta_database.Business_Discovery_Temp`
AS
 SELECT *,
        username  AS Especialista
   FROM ( select *,
        followers_count - IFNULL(LAG(followers_count) OVER(PARTITION BY username ORDER BY extracted_date),0) as net_followers_count,
        FROM (SELECT *,
            ROW_NUMBER()OVER (PARTITION BY username order by extracted_date) as row_number
            FROM `instagram-project-337102.insta_database.Business_Discovery`)
      )
    WHERE row_number > 1
    ORDER BY 	extracted_date DESC