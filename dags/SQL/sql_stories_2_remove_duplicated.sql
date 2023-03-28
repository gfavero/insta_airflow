CREATE OR REPLACE TABLE `instagram-project-337102.insta_database.Stories`
AS
SELECT
        extracted_date,
        story_id,
        caption,
        comments_count,
        like_count,
        media_type,
        media_product_type,
        media_url,
        permalink,
        thumbnail_url,
        created_date,
        name
 
FROM `instagram-project-337102.insta_database.Stories_Temp`
order by created_date