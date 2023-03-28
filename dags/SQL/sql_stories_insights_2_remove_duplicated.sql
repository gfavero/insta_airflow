CREATE OR REPLACE TABLE `instagram-project-337102.insta_database.Stories_Insights`
AS
SELECT
        extracted_date,
        story_id,
        impressions,
        reach,
        exits,
        taps_forward,
        taps_back,
        replies, 
FROM `instagram-project-337102.insta_database.Stories_Insights_Temp`
order by extracted_date