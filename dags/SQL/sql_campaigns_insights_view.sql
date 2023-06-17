WITH CampaignsInsights as 
          (SELECT *
          FROM (
            SELECT
                *,
                ROW_NUMBER()
                    OVER (PARTITION BY campaign_id,date_start)
                    row_number
            FROM `instagram-project-337102.insta_database.CampaignsInsights_Temp`
          )
          WHERE row_number = 1
order by date_start)

SELECT
        CampaignsInsights.campaign_id,
        Campaigns.campaign_name,
        CampaignsInsights.impressions,
        CAST(CampaignsInsights.spend AS FLOAT64) as spend,
        CAST(CampaignsInsights.cost_per_unique_click AS FLOAT64) as cost_per_unique_click,
        CAST(CampaignsInsights.purchases_convertion_value AS FLOAT64) as purchases_convertion_value,
        CAST(CampaignsInsights.clicks AS INT64) as clicks,
        CAST(CampaignsInsights.cpc AS FLOAT64) as cpc,
        CAST(CampaignsInsights.date_start AS DATE) as date_start,        
        CONCAT(CAST(EXTRACT(YEAR from CAST(CampaignsInsights.date_start AS DATE)) as string),'-', LPAD(CAST(EXTRACT(MONTH from CAST(CampaignsInsights.date_start AS DATE)) as string),2,'0') )  as year_month,
        CampaignsInsights.view_content,
        CAST(CampaignsInsights.purchase_qty  AS INT64) as purchase_qty,
        CampaignsInsights.initiate_checkout,
        CampaignsInsights.post_engagement,
        CampaignsInsights.landing_page_view,
        CampaignsInsights.post_reaction,
        CampaignsInsights.link_click,
        CampaignsInsights.comment,
        Campaigns.status,
        Campaigns.account_id,
        Campaigns.account_name
FROM CampaignsInsights
inner join `instagram-project-337102.insta_database.Campaigns_Temp` as Campaigns
on CampaignsInsights.campaign_id = Campaigns.campaign_id



CREATE OR REPLACE TABLE `instagram-project-337102.insta_database.CampaignsInsights_View`
AS
SELECT
        CampaignsInsights.campaign_id,
        Campaigns.campaign_name,
        CampaignsInsights.impressions,
        CAST(CampaignsInsights.spend AS FLOAT64) as spend,
        CAST(CampaignsInsights.cost_per_unique_click AS FLOAT64) as cost_per_unique_click,
        CAST(CampaignsInsights.purchases_convertion_value AS FLOAT64) as purchases_convertion_value,
        CAST(CampaignsInsights.clicks AS INT64) as clicks,
        CAST(CampaignsInsights.cpc AS FLOAT64) as cpc,
        CAST(CampaignsInsights.date_start AS DATE) as date_start,        
        CONCAT(CAST(EXTRACT(YEAR from CAST(CampaignsInsights.date_start AS DATE)) as string),'-', LPAD(CAST(EXTRACT(MONTH from CAST(CampaignsInsights.date_start AS DATE)) as string),2,'0') )  as year_month,
        CampaignsInsights.view_content,
        CAST(CampaignsInsights.purchase_qty  AS INT64) as purchase_qty,
        CampaignsInsights.initiate_checkout,
        CampaignsInsights.post_engagement,
        CampaignsInsights.landing_page_view,
        CampaignsInsights.post_reaction,
        CampaignsInsights.link_click,
        CampaignsInsights.comment,
        Campaigns.status,
        Campaigns.account_id,
        Campaigns.account_name
FROM (SELECT *
          FROM (
            SELECT
                *,
                ROW_NUMBER()
                    OVER (PARTITION BY campaign_id,date_start)
                    row_number_2
            FROM `instagram-project-337102.insta_database.CampaignsInsights_Temp`
          )
          WHERE row_number_2 = 1
        order by date_start) as CampaignsInsights
inner join `instagram-project-337102.insta_database.Campaigns` as Campaigns
on CampaignsInsights.campaign_id = Campaigns.campaign_id