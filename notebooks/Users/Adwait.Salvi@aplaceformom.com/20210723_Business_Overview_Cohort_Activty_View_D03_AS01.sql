-- Databricks notebook source
set workspace = workspace_as;

-- COMMAND ----------

set db_env = prod;

-- COMMAND ----------

DROP TABLE IF EXISTS ${hiveconf:workspace}.BOCR_date_input;
CREATE TABLE ${hiveconf:workspace}.BOCR_date_input

SELECT DISTINCT date(created_at) as activity_date
FROM ${hiveconf:db_env}.inquiries
WHERE year(created_at)>=2017
ORDER BY activity_date;

-- COMMAND ----------


DROP TABLE IF EXISTS ${hiveconf:workspace}.BOCR_set_RL;
CREATE TABLE ${hiveconf:workspace}.BOCR_set_RL
AS
(
    SELECT DISTINCT lead_id as lead_id FROM ${hiveconf:db_env}.referred_leads
); 

DROP TABLE IF EXISTS ${hiveconf:workspace}.BOCR_set_LSTF;
CREATE TABLE ${hiveconf:workspace}.BOCR_set_LSTF
AS
(
    SELECT DISTINCT lead_id as lead_id FROM ${hiveconf:db_env}.leads_sent_to_field
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Lead Input

-- COMMAND ----------

-- REFRESH TABLE ${hiveconf:workspace}.filtered_leads;

DROP TABLE IF EXISTS ${hiveconf:workspace}.filtered_leads_CV;
CREATE TABLE ${hiveconf:workspace}.filtered_leads_CV
AS
(
SELECT i.inquiry_id+1000000000 as lead_id
      ,i.created_at
      ,i.primary_desired_zip
      ,i.src_employeeid as employee_id
      ,i.inquiry_method as lead_created_type
      ,itl2.inquiries_duplicate_status as lead_duplicate_status
--       ,null as lead_duplicate_status
--       ,i.inquiry_method
      ,i.lead_standing_group
      ,i.lead_standing
      ,i.requested_care_type as resident_care_type 
      ,i.status 
      ,i.sub_status 
      ,i.domain 
      ,i.page_type_group
      ,i.page_type
      ,i.channel as initial_channel
      ,i.initial_sub_channel
      ,i.created_interaction
      ,i.initial_channel_brand 
      ,i.initial_channel_source 
      ,i.initial_referral_source
      ,i.initial_sub_referral_source
      ,i.channel_reattributed
      , "No Value" as page_care_type
      , 0 as hand_delivered_lead
      ,i.domain_group
      ,i.relation_to_resident as contact_relation_to_resident -- need to make lower case except for the first one
      
FROM ${hiveconf:db_env}.inquiries i
-- LEFT JOIN (SELECT DISTINCT inquiry_id FROM ${hiveconf:db_env}.inquiries_to_leads 
-- WHERE lead_created_by_oppgen is True
-- ) itl

-- temp change
LEFT JOIN (
  SELECT inquiry_id, inquiries_duplicate_status --, created_at
FROM
  (SELECT 
   *,
   ROW_NUMBER() OVER (
      PARTITION BY inquiry_id
      ORDER BY created_at
   ) AS row_num
FROM 
   ${hiveconf:db_env}.inquiries_to_leads
WHERE lead_created_by_oppgen is True 
-- AND inquiries_duplicate_status is not null
-- AND lead_id IS NULL
) copy_itl
WHERE copy_itl.row_num = 1
  ) itl
   
ON i.inquiry_id=itl.inquiry_id

LEFT JOIN (
    SELECT inquiry_id, inquiries_duplicate_status
	FROM
	  (SELECT 
	   *,
	   ROW_NUMBER() OVER (
		  PARTITION BY inquiry_id
		  ORDER BY created_at DESC
	   ) AS row_num
	FROM 
	   ${hiveconf:db_env}.inquiries_to_leads
	   ) copy_itl
WHERE copy_itl.row_num = 1
) itl2

ON i.inquiry_id=itl2.inquiry_id

  -- temp change
  
WHERE 1=1
AND itl.inquiry_id is null
AND trim(LOWER(i.merge_status)) = 'current'

UNION

SELECT   l.lead_id
       , l.created_at
       , l.primary_desired_zip
       , l.employee_id
      ,l.inquiry_method as lead_created_type
      ,l.lead_duplicate_status as lead_duplicate_status
--       ,l.inquiry_method
      ,l.lead_standing_group
      ,l.lead_standing
      ,l.resident_care_type as requested_care_type
      ,l.status
      ,l.sub_status
      ,l.domain
      ,l.page_type_group
      ,l.page_type
      ,l.initial_channel
      ,l.initial_sub_channel
      , 0 as created_interaction
      ,l.initial_channel_brand as initial_marketing_brand
      ,l.initial_channel_source as initial_marketing_source
      ,l.initial_referral_source
      ,l.initial_sub_referral_source
      ,l.channel_reattributed
      , "No Value" as page_care_type
      ,l.hand_delivered_lead
      ,l.domain_group
      ,l.contact_relation_to_resident
FROM ${hiveconf:db_env}.leads l
);

DROP TABLE IF EXISTS ${hiveconf:workspace}.BOCR_inq_to_lead;
CREATE TABLE ${hiveconf:workspace}.BOCR_inq_to_lead AS(
SELECT inq_lead.lead_id as lead_id, inq.* FROM ${hiveconf:db_env}.inquiries_to_leads AS inq_lead INNER JOIN ${hiveconf:db_env}.inquiries AS inq ON inq_lead.inquiry_id = inq.inquiry_id
WHERE lead_created_by_oppgen is True
-- WHERE trim(LOWER(inq.merge_status)) = 'current'
);

DROP TABLE IF EXISTS ${hiveconf:workspace}.BOCR_filtered_inquiries;
CREATE TABLE ${hiveconf:workspace}.BOCR_filtered_inquiries AS
--     (SELECT DISTINCT ON (lead_id) * FROM ${hiveconf:workspace}.BOAR_inq_to_lead ORDER BY lead_id);
  (
  SELECT *
FROM
  (SELECT 
   *,
   ROW_NUMBER() OVER (
      PARTITION BY lead_id
      ORDER BY created_at
   ) AS row_num
FROM 
   ${hiveconf:workspace}.BOCR_inq_to_lead
WHERE merge_status = 'Current') bitl
WHERE bitl.row_num = 1
  );
  
DROP TABLE IF EXISTS ${hiveconf:workspace}.BOCR_lead_input;
CREATE TABLE ${hiveconf:workspace}.BOCR_lead_input
AS
SELECT 
  year(l.created_at) as year
  ,month(l.created_at) as month
  ,floor(dayofyear(l.created_at)/ 7) +1 as week
  ,dayofweek(l.created_at) as day_of_week
  ,l.created_at
  ,l.lead_id
  ,l.employee_id
  ,l.lead_created_type
  ,l.lead_duplicate_status
  ,l.resident_care_type as requested_care_type
  ,l.status
  ,l.sub_status
  ,l.domain
  ,l.page_type_group
  ,l.page_type
  ,l.initial_channel
  ,l.initial_sub_channel
  ,l.initial_channel_brand as initial_marketing_brand
  ,l.initial_channel_source as initial_marketing_source
  ,l.initial_referral_source
  ,l.initial_sub_referral_source
  ,l.channel_reattributed
  ,l.lead_standing_group
  ,l.lead_standing
  ,l.created_interaction
  ,l.domain_group
  ,l.page_care_type
  ,l.hand_delivered_lead
  ,l.contact_relation_to_resident
  ,g.country as desired_country
  ,UPPER(g.state) as desired_state
  ,g.city as desired_city
  ,g.zip as desired_zip
  ,g.local_pool_region as desired_local_pool_region
  ,g.local_pool_area as desired_local_pool_area
  ,g.local_pool as desired_local_pool
  ,'' as clean_landing_url -- ,i.clean_landing_url MUST CHANGE BACK
  ,'' as clean_source_url -- ,i.clean_source_url MUST CHANGE BACK
  ,i.inquiry_id
--   ,i.page_type
--   ,i.page_type_group
  ,i.vendor_id
  ,i.sem_account
  ,i.campaign
  ,i.contact_relation_category
  ,i.creative_id
  ,LOWER(i.keyword) as keyword 
  ,CASE	WHEN i.device = 'c' THEN 'c-Desktop'
    WHEN i.device = 'm' THEN 'm-Smartphone'
	WHEN i.device = 't' THEN 't-Tablet'					
	ELSE 'No Value'	END	AS device
  ,CASE WHEN i.campaign LIKE '%:D:%' THEN 1 ELSE 0 END AS display_campaign
  ,de.type as SLA_Type
  ,concat(de.first_name, ' ', de.last_name) as SLA_ASLA_Name
  ,concat(de_man.first_name, ' ', de_man.last_name) as regional_manager
  ,concat(de_rvp.first_name, ' ', de_rvp.last_name) as RVP_Region
  ,concat(de_redir.first_name, ' ', de_redir.last_name) as Regional_Director
  ,concat(de_cra.first_name, ' ', de_cra.last_name) as HCAE_Name
  ,CASE WHEN rl_set.lead_id is not null then true else false end as is_RL
  ,CASE WHEN lstf_set.lead_id is not null then true else false end as is_LSTF
  
FROM ${hiveconf:workspace}.filtered_leads_CV l
left join 
${hiveconf:workspace}.BOCR_date_input d
ON date(l.created_at) = d.activity_date

-- INNER JOIN 
-- prod_edw_dbo.dim_initial_channel i_c
-- ON l.lead_id=i_c.lead_id
-- AND i_c._fivetran_deleted IS false

LEFT JOIN prod_edw_dbo.dim_geography g
ON l.primary_desired_zip=g.zip
AND year(g.date_end)=2099

LEFT JOIN ${hiveconf:workspace}.BOCR_filtered_inquiries i
ON l.lead_id = i.lead_id

LEFT JOIN (SELECT *
FROM
  (SELECT 
   *,
   ROW_NUMBER() OVER (
      PARTITION BY EMPLOYEE_ID
      ORDER BY DATE_END DESC
   ) AS row_num
FROM 
   ${hiveconf:db_env}.employees) bitl
WHERE bitl.row_num = 1) de
ON l.employee_id = de.employee_id

LEFT JOIN (SELECT employee_id, manager_id, first_name, last_name
FROM
  (SELECT 
   *,
   ROW_NUMBER() OVER (
      PARTITION BY EMPLOYEE_ID
      ORDER BY DATE_END DESC
   ) AS row_num
FROM 
   ${hiveconf:db_env}.employees) bitl
WHERE bitl.row_num = 1) de_man
ON de.manager_id = de_man.employee_id

LEFT JOIN (SELECT  employee_id, first_name, last_name
FROM
  (SELECT 
   *,
   ROW_NUMBER() OVER (
      PARTITION BY EMPLOYEE_ID
      ORDER BY DATE_END DESC
   ) AS row_num
FROM 
   ${hiveconf:db_env}.employees) bitl
WHERE bitl.row_num = 1) de_rvp
ON de_man.manager_id = de_rvp.employee_id

LEFT JOIN (SELECT  employee_id, manager_id, username, first_name, last_name
FROM
  (SELECT 
   *,
   ROW_NUMBER() OVER (
      PARTITION BY EMPLOYEE_ID
      ORDER BY DATE_END DESC
   ) AS row_num
FROM 
   ${hiveconf:db_env}.employees) bitl
WHERE bitl.row_num = 1) de_cra
ON i.cra_username = de_cra.username

LEFT JOIN (SELECT  employee_id, first_name, last_name
FROM
  (SELECT 
   *,
   ROW_NUMBER() OVER (
      PARTITION BY EMPLOYEE_ID
      ORDER BY DATE_END DESC
   ) AS row_num
FROM 
   ${hiveconf:db_env}.employees) bitl
WHERE bitl.row_num = 1) de_redir
ON de_cra.manager_id = de_redir.employee_id

LEFT JOIN ${hiveconf:workspace}.BOCR_set_RL rl_set
ON l.lead_id = rl_set.lead_id

LEFT JOIN ${hiveconf:workspace}.BOCR_set_LSTF lstf_set
ON l.lead_id = lstf_set.lead_id


-- LEFT JOIN ${hiveconf:db_env}.inquiries_to_leads itl
-- ON l.lead_id=itl.lead_id
-- AND itl.lead_created_by_oppgen=true

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Leads calc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Leads Original Deagg Table

-- COMMAND ----------

-- DROP TABLE IF EXISTS ${hiveconf:workspace}.BOAR_filtered_inquiries;
-- CREATE TABLE ${hiveconf:workspace}.BOAR_filtered_inquiries
-- AS
-- SELECT *
-- FROM ${hiveconf:db_env}.inquiries
-- WHERE merge_status='Current'
-- AND spam_inquiry = False
-- -- AND lead_created_by_oppgen is True
-- ;

DROP TABLE IF EXISTS ${hiveconf:workspace}.BOCR_dim_leads;
CREATE TABLE ${hiveconf:workspace}.BOCR_dim_leads
AS
(
    select
      inq.lead_id
      ,inq.created_at as activity_date
      ,inq.primary_desired_zip
      ,inq.employee_id
-- --       ,li.inquiry_id
      ,li.initial_channel
      ,li.initial_sub_channel
      ,li.initial_marketing_brand
      ,li.initial_marketing_source
      ,li.initial_referral_source
      ,li.initial_sub_referral_source
      ,li.channel_reattributed
      ,li.desired_country
      ,li.desired_state
      ,li.desired_city
      ,li.desired_zip
      ,li.desired_local_pool_region
      ,li.desired_local_pool_area
      ,li.desired_local_pool
      ,"No Value" as page_care_type
      ,li.page_type
      ,li.page_type_group
      ,li.vendor_id
      ,li.sem_account
      ,li.campaign
      ,li.keyword
      ,li.device
      ,li.display_campaign
--       ,li.lead_created_type
      ,li.lead_duplicate_status
      ,li.requested_care_type
      ,li.status
      ,li.sub_status
      ,li.domain
      ,li.SLA_ASLA_Name
      ,li.lead_created_type 
      ,"No Value" as referral_care_type 
      ,"No Value" as placement_care_type 
      ,li.lead_standing_group
      ,li.created_interaction
      ,li.domain_group
      ,li.clean_landing_url
      ,li.clean_source_url
      ,li.hand_delivered_lead
      ,li.RVP_Region
      ,li.Regional_Manager
      ,li.Regional_Director
      ,li.SLA_Type
      ,li.HCAE_Name
      ,li.contact_relation_category 
      ,li.contact_relation_to_resident 
      ,li.creative_id 
      ,li.lead_standing
      ,li.is_RL
      ,li.is_LSTF
      ,comm.organization_name as org_name
      ,comm.country as country_property
      ,comm.state as state_property
      ,comm.city as city_property
      ,comm.postal_code as zip_property
      ,comm.contract_status_type as contract_type
    
    from ${hiveconf:workspace}.filtered_leads_CV as inq
    left join ${hiveconf:workspace}.BOCR_lead_input li
    on li.lead_id = inq.lead_id

    left join 
      (SELECT *
        FROM
          (SELECT 
           *,
           ROW_NUMBER() OVER (
              PARTITION BY lead_id
              ORDER BY referral_accept_date
           ) AS row_num
        FROM 
           ${hiveconf:db_env}.referrals
        WHERE  referral_round = 1) devref
        WHERE devref.row_num = 1) r
    on inq.lead_id=r.lead_id

    LEFT JOIN ${hiveconf:db_env}.communities comm 
    ON r.community_id = comm.community_id
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Leads Agg Table Cohort and Activity View

-- COMMAND ----------

DROP TABLE IF EXISTS ${hiveconf:workspace}.BOCR_Leads_Agg;
CREATE TABLE ${hiveconf:workspace}.BOCR_Leads_Agg AS 
SELECT 
  ---- Activity Row Dim
--   DATE_SUB(activity_date, day(activity_date)) AS Cohort_Month_Start_Date,
  year(activity_date) AS Year_Activity, 
  year(activity_date)*100 + month(activity_date) AS Month_Activity, 
  DATE_SUB(activity_date, dayofweek(activity_date) - 1) AS Week_Activity, 
  dayofweek(activity_date) AS Day_of_week_Activity, 
  activity_date AS Date_of_Activity, 
  
  ---- Channel Row Dim
  initial_channel, 
  initial_sub_channel, 
  initial_marketing_brand, 
  initial_marketing_source, 
  initial_referral_source, 
  initial_sub_referral_source, 
  channel_reattributed AS reattributed_channel, 
  
  ---- General Attributes Row Dim (In Progress)
  lead_created_type,
  lead_duplicate_status,
  requested_care_type,
  referral_care_type,
  placement_care_type,
  
  ---- Lead Standing Row Dim (In Progress)
  lead_standing_group as standing_group,
  lead_standing,
  status,
  sub_status,
  created_interaction,
  
  ---- Property Row Dim
  org_name,
  country_property,
  state_property,
  city_property,
  zip_property,
  contract_type,
  
  ---- Geography Row Dim
  desired_country AS Primary_desired_country, 
  desired_state AS Primary_desired_state, 
  desired_city AS Primary_desired_city, 
  desired_zip AS Primary_desired_zip, 
  desired_local_pool_region AS Primary_desired_region, 
  desired_local_pool_area AS Primary_desired_area, 
  desired_local_pool AS Primary_desired_pool, 

  ---- Landing/Source
  domain_group,
  domain,
  page_type_group,
  page_type,
  page_care_type,
  clean_landing_URL,
  clean_source_URL,

  -- SEM
  vendor_id,
  sem_account,
  campaign,
  keyword,
  device,
  display_campaign,
  
  ---- Sales
  hand_delivered_lead,
  RVP_Region,
  Regional_Manager,
  Regional_Director,
  SLA_ASLA_Name,
  SLA_Type,
  HCAE_Name,
  
  ---- Other
--   warm_transfer_type,
  contact_relation_category,
  contact_relation_to_resident,
  lead_id, -- check 
  creative_id,
  
  COUNT(DISTINCT (CASE WHEN lead_duplicate_status = "New" OR lead_duplicate_status = "Repeat Business" THEN lead_id END)) AS Unique_Leads,
  COUNT(distinct lead_id) AS Total_Leads, -- Measures (subject to change)
  0 AS Total_LSTF, -- Measures (subject to change)
  0 AS Unique_LSTF, -- Measures (subject to change)
  0 AS Total_RLs,
  0 AS Unique_RLs,
  0 AS Move_ins_Booked,
  0 AS Move_ins_Booked_month_end,
  0 AS move_ins_booked_after_month,
  0 AS Total_Referrals
  
--   COUNT(DISTINCT (CASE WHEN lead_standing_group = 'Open' AND is_RL is False AND is_LSTF is False THEN lead_id END)) AS Open_Call_Center_Leads,
--   COUNT(DISTINCT (CASE WHEN lead_standing_group = 'Closed' AND is_RL is False AND is_LSTF is False THEN lead_id END)) AS Closed_Call_Center_Leads,
--   0 AS Open_LSTF_Not_Referred,
--   0 AS Closed_LSTF_Not_Referred,
--   0 AS Open_RLs_Not_Moved,
--   0 AS Closed_RLs_Not_Moved
  
FROM 
  ${hiveconf:workspace}.BOCR_dim_Leads
-- WHERE 
--   year(activity_date) = 2019 and month(activity_date) = 7
-- GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 34, 35, 36, 37, 38
GROUP BY 
---- Activity Row Dim
--   DATE_SUB(activity_date, day(activity_date)),-- AS Cohort_Start_Date,
  year(activity_date),
  year(activity_date)*100 + month(activity_date),
  DATE_SUB(activity_date, dayofweek(activity_date) - 1),
  dayofweek(activity_date),
  activity_date,
  
  ---- Channel Row Dim
  initial_channel, 
  initial_sub_channel, 
  initial_marketing_brand, 
  initial_marketing_source, 
  initial_referral_source, 
  initial_sub_referral_source, 
  channel_reattributed, 
  
  ---- General Attributes Row Dim (In Progress)
  lead_created_type,
  lead_duplicate_status,
  requested_care_type,
  referral_care_type,
  placement_care_type,
  
  ---- Lead Standing Row Dim (In Progress)
  lead_standing_group,
  lead_standing,
  status,
  sub_status,
  created_interaction,
  
  ---- Property Row Dim
  org_name,
  country_property,
  state_property,
  city_property,
  zip_property,
  contract_type,
  
  ---- Geography Row Dim
  desired_country,
  desired_state,
  desired_city,
  desired_zip,
  desired_local_pool_region,
  desired_local_pool_area, 
  desired_local_pool,

  ---- Landing/Source
    domain_group,
  domain,
  page_type_group,
  page_type,
  page_care_type,
  clean_landing_URL,
  clean_source_URL,
  
  -- SEM
  vendor_id,
  sem_account,
  campaign,
  keyword,
  device,
  display_campaign,
  
  ---- Sales
  hand_delivered_lead,
  RVP_Region,
  Regional_Manager,
  Regional_Director,
  SLA_ASLA_Name,
  SLA_Type,
  HCAE_Name,
  
  ---- Other
--   warm_transfer_type,
  contact_relation_category,
  contact_relation_to_resident,
  lead_id, -- check 
  creative_id
;

SELECT * FROM ${hiveconf:workspace}.BOCR_Leads_Agg

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## LSTF calc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### LSTF Original Deagg Table 

-- COMMAND ----------

DROP TABLE IF EXISTS ${hiveconf:workspace}.filtered_lstf_CV;
CREATE TABLE ${hiveconf:workspace}.filtered_lstf_CV
AS
SELECT lstf.*
        FROM
          (SELECT 
           *,
           ROW_NUMBER() OVER (
              PARTITION BY lead_id
              ORDER BY activity_date
           ) AS row_num
FROM 
   ${hiveconf:db_env}.leads_sent_to_field) lstf
-- LEFT JOIN prod_edw_dbo.dim_initial_channel ic
-- ON lstf.lead_id=ic.lead_id
-- and ic._fivetran_deleted=False

WHERE lstf.title_employee NOT IN ('AC')
-- AND ic.initial_sub_referral_source NOT IN ('Secret Shop')
and lstf.row_num = 1;


DROP TABLE IF EXISTS ${hiveconf:workspace}.BOCR_dim_lstf;
CREATE TABLE ${hiveconf:workspace}.BOCR_dim_lstf
AS


select
  lstf.*
  ,li.created_at
  ,li.inquiry_id
  ,li.initial_channel
  ,li.initial_sub_channel
  ,li.initial_marketing_brand
  ,li.initial_marketing_source
  ,li.initial_referral_source
  ,li.initial_sub_referral_source
  ,li.channel_reattributed
  ,li.desired_country
  ,li.desired_state
  ,li.desired_city
  ,li.desired_zip
  ,li.desired_local_pool_region
  ,li.desired_local_pool_area
  ,li.desired_local_pool
  ,"No Value" as page_care_type
  ,li.page_type
  ,li.page_type_group
  ,li.vendor_id
  ,li.sem_account
  ,li.campaign
  ,li.keyword
  ,li.device
  ,li.display_campaign
--   ,li.lead_created_type
  ,li.lead_duplicate_status
  ,li.requested_care_type
  ,li.status
  ,li.sub_status
  ,li.domain
  ,li.SLA_ASLA_Name
  ,li.lead_created_type 
  ,"No Value" as referral_care_type 
  ,"No Value" as placement_care_type 
  ,li.lead_standing_group
  ,li.created_interaction
  ,li.domain_group
  ,li.clean_landing_url
  ,li.clean_source_url
  ,li.hand_delivered_lead
  ,li.RVP_Region
  ,li.Regional_Manager
  ,li.Regional_Director
  ,li.SLA_Type
  ,li.HCAE_Name
  ,li.contact_relation_category 
  ,li.contact_relation_to_resident 
  ,li.creative_id 
  ,li.lead_standing
--   ,li.is_RL
--   ,li.is_LSTF
  ,comm.organization_name as org_name
  ,comm.country as country_property
  ,comm.state as state_property
  ,comm.city as city_property
  ,comm.postal_code as zip_property
  ,comm.contract_status_type as contract_type
  
from ${hiveconf:workspace}.filtered_lstf_CV lstf
left join ${hiveconf:workspace}.BOCR_lead_input li
on lstf.lead_id=li.lead_id

left join 
  (SELECT *
        FROM
          (SELECT 
           *,
           ROW_NUMBER() OVER (
              PARTITION BY lead_id
              ORDER BY referral_accept_date
           ) AS row_num
        FROM 
           ${hiveconf:db_env}.referrals
        WHERE  referral_round = 1) devref
        WHERE devref.row_num = 1) r
on lstf.lead_id=r.lead_id

LEFT JOIN ${hiveconf:db_env}.communities comm 
ON r.community_id = comm.community_id
;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### LSTF Agg Table Cohort View

-- COMMAND ----------

DROP TABLE IF EXISTS ${hiveconf:workspace}.BOCR_LSTF_Agg;
CREATE TABLE ${hiveconf:workspace}.BOCR_LSTF_Agg AS 
SELECT 
  ---- Activity Row Dim
--   DATE_SUB(activity_date, day(activity_date)) AS Cohort_Month_Start_Date,
  year(created_at) AS Year_Activity, 
  year(created_at)*100 + month(created_at) AS Month_Activity, 
  DATE_SUB(created_at, dayofweek(created_at) - 1) AS Week_Activity, 
  dayofweek(created_at) AS Day_of_week_Activity, 
  created_at AS Date_of_Activity, 
  
  ---- Channel Row Dim
  initial_channel, 
  initial_sub_channel, 
  initial_marketing_brand, 
  initial_marketing_source, 
  initial_referral_source, 
  initial_sub_referral_source, 
  channel_reattributed AS reattributed_channel, 
  
  ---- General Attributes Row Dim (In Progress)
  lead_created_type,
  lead_duplicate_status,
  requested_care_type,
  referral_care_type,
  placement_care_type,
  
  ---- Lead Standing Row Dim (In Progress)
  lead_standing_group as standing_group,
  lead_standing,
  status,
  sub_status,
  created_interaction,
  
  ---- Property Row Dim
  org_name,
  country_property,
  state_property,
  city_property,
  zip_property,
  contract_type,
  
  ---- Geography Row Dim
  desired_country AS Primary_desired_country, 
  desired_state AS Primary_desired_state, 
  desired_city AS Primary_desired_city, 
  desired_zip AS Primary_desired_zip, 
  desired_local_pool_region AS Primary_desired_region, 
  desired_local_pool_area AS Primary_desired_area, 
  desired_local_pool AS Primary_desired_pool, 

  ---- Landing/Source
  domain_group,
  domain,
  page_type_group,
  page_type,
  page_care_type,
  clean_landing_URL,
  clean_source_URL,

  -- SEM
  vendor_id,
  sem_account,
  campaign,
  keyword,
  device,
  display_campaign,
  
  ---- Sales
  hand_delivered_lead,
  RVP_Region,
  Regional_Manager,
  Regional_Director,
  SLA_ASLA_Name,
  SLA_Type,
  HCAE_Name,
  
  ---- Other
--   warm_transfer_type,
  contact_relation_category,
  contact_relation_to_resident,
  lead_id, -- check 
  creative_id,
  
  0 AS Unique_Leads,
  0 AS Total_Leads, -- Measures (subject to change)
  COUNT(DISTINCT lead_id) AS Total_LSTF, -- Measures (subject to change)
  COUNT(DISTINCT (CASE WHEN lead_duplicate_status = "New" OR lead_duplicate_status = "Repeat Business" THEN lead_id END)) AS Unique_LSTF, -- Measures (subject to change)
  0 AS Total_RLs,
  0 AS Unique_RLs,
  0 AS Move_ins_Booked,
  0 AS Move_ins_Booked_month_end,
  0 AS move_ins_booked_after_month,
  0 AS Total_Referrals
  
--   0 AS Open_Call_Center_Leads,
--   0 AS Closed_Call_Center_Leads,
--   COUNT(DISTINCT (CASE WHEN lead_standing_group = "Open" AND is_RL is False THEN lead_id END)) Open_LSTF_Not_Referred,
--   COUNT(DISTINCT (CASE WHEN lead_standing_group = "Closed" AND is_RL is False THEN lead_id END)) Closed_LSTF_Not_Referred,
--   0 AS Open_RLs_Not_Moved,
--   0 AS Closed_RLs_Not_Moved
FROM 
  ${hiveconf:workspace}.BOCR_dim_lstf
-- WHERE 
--   year(activity_date) = 2019 and month(activity_date) = 7
-- GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 34, 35, 36, 37, 38
GROUP BY 
---- Activity Row Dim
--   DATE_SUB(activity_date, day(activity_date)),-- AS Cohort_Start_Date,
  year(created_at),
  year(created_at)*100 + month(created_at),
  DATE_SUB(created_at, dayofweek(created_at) - 1),
  dayofweek(created_at),
  created_at,
  
  ---- Channel Row Dim
  initial_channel, 
  initial_sub_channel, 
  initial_marketing_brand, 
  initial_marketing_source, 
  initial_referral_source, 
  initial_sub_referral_source, 
  channel_reattributed, 
  
  ---- General Attributes Row Dim (In Progress)
  lead_created_type,
  lead_duplicate_status,
  requested_care_type,
  referral_care_type,
  placement_care_type,
  
  ---- Lead Standing Row Dim (In Progress)
  lead_standing_group,
  lead_standing,
  status,
  sub_status,
  created_interaction,
  
  ---- Property Row Dim
  org_name,
  country_property,
  state_property,
  city_property,
  zip_property,
  contract_type,
  
  ---- Geography Row Dim
  desired_country,
  desired_state,
  desired_city,
  desired_zip,
  desired_local_pool_region,
  desired_local_pool_area, 
  desired_local_pool,

  ---- Landing/Source
    domain_group,
  domain,
  page_type_group,
  page_type,
  page_care_type,
  clean_landing_URL,
  clean_source_URL,
  
  -- SEM
  vendor_id,
  sem_account,
  campaign,
  keyword,
  device,
  display_campaign,
  
  ---- Sales
  hand_delivered_lead,
  RVP_Region,
  Regional_Manager,
  Regional_Director,
  SLA_ASLA_Name,
  SLA_Type,
  HCAE_Name,
  
  ---- Other
--   warm_transfer_type,
  contact_relation_category,
  contact_relation_to_resident,
  lead_id, -- check 
  creative_id
;

SELECT * FROM ${hiveconf:workspace}.BOCR_LSTF_Agg

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### LSTF Agg Table Activity View

-- COMMAND ----------

DROP TABLE IF EXISTS ${hiveconf:workspace}.BOAR_LSTF_Agg_1;
CREATE TABLE ${hiveconf:workspace}.BOAR_LSTF_Agg_1 AS 
SELECT 
  ---- Activity Row Dim
--   DATE_SUB(activity_date, day(activity_date)) AS Cohort_Month_Start_Date,
  year(activity_date) AS Year_Activity, 
  year(activity_date)*100 + month(activity_date) AS Month_Activity, 
  DATE_SUB(activity_date, dayofweek(activity_date) - 1) AS Week_Activity, 
  dayofweek(activity_date) AS Day_of_week_Activity, 
  activity_date AS Date_of_Activity, 
  
  ---- Channel Row Dim
  initial_channel, 
  initial_sub_channel, 
  initial_marketing_brand, 
  initial_marketing_source, 
  initial_referral_source, 
  initial_sub_referral_source, 
  channel_reattributed AS reattributed_channel, 
  
  ---- General Attributes Row Dim (In Progress)
  lead_created_type,
  lead_duplicate_status,
  requested_care_type,
  referral_care_type,
  placement_care_type,
  
  ---- Lead Standing Row Dim (In Progress)
  lead_standing_group as standing_group,
  lead_standing,
  status,
  sub_status,
  created_interaction,
  
  ---- Property Row Dim
  org_name,
  country_property,
  state_property,
  city_property,
  zip_property,
  contract_type,
  
  ---- Geography Row Dim
  desired_country AS Primary_desired_country, 
  desired_state AS Primary_desired_state, 
  desired_city AS Primary_desired_city, 
  desired_zip AS Primary_desired_zip, 
  desired_local_pool_region AS Primary_desired_region, 
  desired_local_pool_area AS Primary_desired_area, 
  desired_local_pool AS Primary_desired_pool, 

  ---- Landing/Source
  domain_group,
  domain,
  page_type_group,
  page_type,
  page_care_type,
  clean_landing_URL,
  clean_source_URL,

  -- SEM
  vendor_id,
  sem_account,
  campaign,
  keyword,
  device,
  display_campaign,
  
  ---- Sales
  hand_delivered_lead,
  RVP_Region,
  Regional_Manager,
  Regional_Director,
  SLA_ASLA_Name,
  SLA_Type,
  HCAE_Name,
  
  ---- Other
--   warm_transfer_type,
  contact_relation_category,
  contact_relation_to_resident,
  lead_id, -- check 
  creative_id,
  
  0 AS Unique_Leads,
  0 AS Total_Leads, -- Measures (subject to change)
  COUNT(DISTINCT lead_id) AS Total_LSTF, -- Measures (subject to change)
  COUNT(DISTINCT (CASE WHEN lead_duplicate_status = "New" OR lead_duplicate_status = "Repeat Business" THEN lead_id END)) AS Unique_LSTF, -- Measures (subject to change)
  0 AS Total_RLs,
  0 AS Unique_RLs,
  0 AS Move_ins_Booked,
  0 AS Move_ins_Booked_month_end,
  0 AS move_ins_booked_after_month,
  0 AS Total_Referrals
  
--   0 AS Open_Call_Center_Leads,
--   0 AS Closed_Call_Center_Leads,
--   COUNT(DISTINCT (CASE WHEN lead_standing_group = "Open" AND is_RL is False THEN lead_id END)) Open_LSTF_Not_Referred,
--   COUNT(DISTINCT (CASE WHEN lead_standing_group = "Closed" AND is_RL is False THEN lead_id END)) Closed_LSTF_Not_Referred,
--   0 AS Open_RLs_Not_Moved,
--   0 AS Closed_RLs_Not_Moved
FROM 
  ${hiveconf:workspace}.BOCR_dim_lstf
-- WHERE 
--   year(activity_date) = 2019 and month(activity_date) = 7
-- GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 34, 35, 36, 37, 38
GROUP BY 
---- Activity Row Dim
--   DATE_SUB(activity_date, day(activity_date)),-- AS Cohort_Start_Date,
  year(activity_date),
  year(activity_date)*100 + month(activity_date),
  DATE_SUB(activity_date, dayofweek(activity_date) - 1),
  dayofweek(activity_date),
  activity_date,
  
  ---- Channel Row Dim
  initial_channel, 
  initial_sub_channel, 
  initial_marketing_brand, 
  initial_marketing_source, 
  initial_referral_source, 
  initial_sub_referral_source, 
  channel_reattributed, 
  
  ---- General Attributes Row Dim (In Progress)
  lead_created_type,
  lead_duplicate_status,
  requested_care_type,
  referral_care_type,
  placement_care_type,
  
  ---- Lead Standing Row Dim (In Progress)
  lead_standing_group,
  lead_standing,
  status,
  sub_status,
  created_interaction,
  
  ---- Property Row Dim
  org_name,
  country_property,
  state_property,
  city_property,
  zip_property,
  contract_type,
  
  ---- Geography Row Dim
  desired_country,
  desired_state,
  desired_city,
  desired_zip,
  desired_local_pool_region,
  desired_local_pool_area, 
  desired_local_pool,

  ---- Landing/Source
    domain_group,
  domain,
  page_type_group,
  page_type,
  page_care_type,
  clean_landing_URL,
  clean_source_URL,
  
  -- SEM
  vendor_id,
  sem_account,
  campaign,
  keyword,
  device,
  display_campaign,
  
  ---- Sales
  hand_delivered_lead,
  RVP_Region,
  Regional_Manager,
  Regional_Director,
  SLA_ASLA_Name,
  SLA_Type,
  HCAE_Name,
  
  ---- Other
--   warm_transfer_type,
  contact_relation_category,
  contact_relation_to_resident,
  lead_id, -- check 
  creative_id
;

SELECT * FROM ${hiveconf:workspace}.BOAR_LSTF_Agg_1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## RLs calc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### RLs Original Deagg Table

-- COMMAND ----------

DROP TABLE IF EXISTS ${hiveconf:workspace}.filtered_referred_leads_CV;
CREATE TABLE ${hiveconf:workspace}.filtered_referred_leads_CV
AS
SELECT rl.*
FROM ${hiveconf:db_env}.referred_leads rl



-- LEFT JOIN prod_edw_dbo.dim_initial_channel ic
-- ON rl.lead_id=ic.lead_id
-- and ic._fivetran_deleted=False

WHERE rl.employee_title  NOT IN ('AC')
-- AND ic.initial_sub_referral_source NOT IN ('Secret Shop')
;


-- COMMAND ----------


DROP TABLE IF EXISTS ${hiveconf:workspace}.BOCR_dim_referred_leads;
CREATE TABLE ${hiveconf:workspace}.BOCR_dim_referred_leads
AS


select
  rl.*
  ,li.created_at
  ,li.initial_sub_channel
  ,li.initial_marketing_brand
  ,li.initial_marketing_source
  ,li.initial_referral_source
  ,li.initial_sub_referral_source
  ,li.channel_reattributed
  ,li.desired_country
  ,li.desired_state
  ,li.desired_city
  ,li.desired_zip
  ,li.desired_local_pool_region
  ,li.desired_local_pool_area
  ,li.desired_local_pool
  ,"No Value" as page_care_type
  ,li.page_type
  ,li.page_type_group
  ,li.vendor_id
  ,li.sem_account
  ,li.campaign
  ,li.keyword
  ,li.device
  ,li.display_campaign
--   ,li.lead_created_type
  ,li.lead_duplicate_status
  ,li.requested_care_type
  ,li.status
  ,li.sub_status
  ,li.domain
  ,li.SLA_ASLA_Name
  ,li.lead_created_type 
  ,"No Value" as referral_care_type 
  ,"No Value" as placement_care_type 
  ,li.lead_standing_group
  ,li.created_interaction
  ,li.domain_group
  ,li.clean_landing_url
  ,li.clean_source_url
  ,li.hand_delivered_lead
  ,li.RVP_Region
  ,li.Regional_Manager
  ,li.Regional_Director
  ,li.SLA_Type
  ,li.HCAE_Name
  ,li.contact_relation_category 
  ,li.contact_relation_to_resident 
  ,li.creative_id 
  ,li.lead_standing
--   ,c.organization_name as community_organization
--   ,c.state as community_state
--   ,c.city as community_city
--   ,c.postal_code as community_zip
  ,comm.organization_name as org_name
  ,comm.country as country_property
  ,comm.state as state_property
  ,comm.city as city_property
  ,comm.postal_code as zip_property
  ,comm.contract_status_type as contract_type
  
from ${hiveconf:workspace}.filtered_referred_leads_CV rl
left join ${hiveconf:workspace}.BOCR_lead_input li
on rl.lead_id=li.lead_id

left join 
  (SELECT *
        FROM
          (SELECT 
           *,
           ROW_NUMBER() OVER (
              PARTITION BY lead_id
              ORDER BY referral_accept_date
           ) AS row_num
        FROM 
           ${hiveconf:db_env}.referrals
        WHERE  referral_round = 1) devref
        WHERE devref.row_num = 1) r
on rl.lead_id=r.lead_id
and r.referral_round=1

LEFT JOIN ${hiveconf:db_env}.communities comm 
ON r.community_id = comm.community_id
-- inner join ${hiveconf:db_env}.communities c
-- on r.community_id= c.community_id
;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### RLs Agg Table Cohort View

-- COMMAND ----------

DROP TABLE IF EXISTS ${hiveconf:workspace}.BOCR_RLs_Agg;
CREATE TABLE ${hiveconf:workspace}.BOCR_RLs_Agg AS
SELECT 
  ---- Activity Row Dim
--   DATE_SUB(activity_date, day(activity_date)) AS Cohort_Month_Start_Date,
  year(created_at) AS Year_Activity, 
  year(created_at)*100 + month(created_at) AS Month_Activity, 
  DATE_SUB(created_at, dayofweek(created_at) - 1) AS Week_Activity, 
  dayofweek(created_at) AS Day_of_week_Activity, 
  created_at AS Date_of_Activity, 
  
  ---- Channel Row Dim
  initial_channel, 
  initial_sub_channel, 
  initial_marketing_brand, 
  initial_marketing_source, 
  initial_referral_source, 
  initial_sub_referral_source, 
  channel_reattributed AS reattributed_channel, 
  
  ---- General Attributes Row Dim (In Progress)
  lead_created_type,
  lead_duplicate_status,
  requested_care_type,
  referral_care_type,
  placement_care_type,
  
  ---- Lead Standing Row Dim (In Progress)
  lead_standing_group as standing_group,
  lead_standing,
  status,
  sub_status,
  created_interaction,
  
  ---- Property Row Dim
  org_name,
  country_property,
  state_property,
  city_property,
  zip_property,
  contract_type,
  
  ---- Geography Row Dim
  desired_country AS Primary_desired_country, 
  desired_state AS Primary_desired_state, 
  desired_city AS Primary_desired_city, 
  desired_zip AS Primary_desired_zip, 
  desired_local_pool_region AS Primary_desired_region, 
  desired_local_pool_area AS Primary_desired_area, 
  desired_local_pool AS Primary_desired_pool, 

  ---- Landing/Source
  domain_group,
  domain,
  page_type_group,
  page_type,
  page_care_type,
  clean_landing_URL,
  clean_source_URL,

  -- SEM
  vendor_id,
  sem_account,
  campaign,
  keyword,
  device,
  display_campaign,
  
  ---- Sales
  hand_delivered_lead,
  RVP_Region,
  Regional_Manager,
  Regional_Director,
  SLA_ASLA_Name,
  SLA_Type,
  HCAE_Name,
  
  ---- Other
--   warm_transfer_type,
  contact_relation_category,
  contact_relation_to_resident,
  lead_id, -- check 
  creative_id,
  
  0 AS Unique_Leads,
  0 AS Total_Leads,
  0 AS Total_LSTF, -- Measures (subject to change)
  0 AS Unique_LSTF, -- Measures (subject to change)
  COUNT(DISTINCT lead_id) AS Total_RLs,
  COUNT(DISTINCT (CASE WHEN lead_duplicate_status = "New" OR lead_duplicate_status = "Repeat Business" OR lead_duplicate_status = "Re-Engagement" THEN lead_id END)) AS Unique_RLs,
  0 AS Move_ins_Booked,
  0 AS Move_ins_Booked_month_end,
  0 AS move_ins_booked_after_month,
  0 AS Total_Referrals
FROM 
  ${hiveconf:workspace}.BOCR_dim_referred_leads
-- WHERE 
--   year(activity_date) = 2019 and month(activity_date) = 7
-- GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 34, 35, 36, 37, 38
GROUP BY 
---- Activity Row Dim
--   DATE_SUB(activity_date, day(activity_date)),-- AS Cohort_Start_Date,
  year(created_at),
  year(created_at)*100 + month(created_at),
  DATE_SUB(created_at, dayofweek(created_at) - 1),
  dayofweek(created_at),
  created_at,
  
  ---- Channel Row Dim
  initial_channel, 
  initial_sub_channel, 
  initial_marketing_brand, 
  initial_marketing_source, 
  initial_referral_source, 
  initial_sub_referral_source, 
  channel_reattributed, 
  
  ---- General Attributes Row Dim (In Progress)
  lead_created_type,
  lead_duplicate_status,
  requested_care_type,
  referral_care_type,
  placement_care_type,
  
  ---- Lead Standing Row Dim (In Progress)
  lead_standing_group,
  lead_standing,
  status,
  sub_status,
  created_interaction,
  
  ---- Property Row Dim
  org_name,
  country_property,
  state_property,
  city_property,
  zip_property,
  contract_type,
  
  ---- Geography Row Dim
  desired_country,
  desired_state,
  desired_city,
  desired_zip,
  desired_local_pool_region,
  desired_local_pool_area, 
  desired_local_pool,

  ---- Landing/Source
    domain_group,
  domain,
  page_type_group,
  page_type,
  page_care_type,
  clean_landing_URL,
  clean_source_URL,
  
  -- SEM
  vendor_id,
  sem_account,
  campaign,
  keyword,
  device,
  display_campaign,
  
  ---- Sales
  hand_delivered_lead,
  RVP_Region,
  Regional_Manager,
  Regional_Director,
  SLA_ASLA_Name,
  SLA_Type,
  HCAE_Name,
  
  ---- Other
--   warm_transfer_type,
  contact_relation_category,
  contact_relation_to_resident,
  lead_id, -- check 
  creative_id
;

SELECT * FROM ${hiveconf:workspace}.BOCR_RLs_Agg

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### RLs Agg Table Activity View

-- COMMAND ----------

DROP TABLE IF EXISTS ${hiveconf:workspace}.BOAR_RLs_Agg_1;
CREATE TABLE ${hiveconf:workspace}.BOAR_RLs_Agg_1 AS
SELECT 
  ---- Activity Row Dim
--   DATE_SUB(activity_date, day(activity_date)) AS Cohort_Month_Start_Date,
  year(initial_referral_sent_date) AS Year_Activity, 
  year(initial_referral_sent_date)*100 + month(initial_referral_sent_date) AS Month_Activity, 
  DATE_SUB(initial_referral_sent_date, dayofweek(initial_referral_sent_date) - 1) AS Week_Activity, 
  dayofweek(initial_referral_sent_date) AS Day_of_week_Activity, 
  initial_referral_sent_date AS Date_of_Activity, 
  
  ---- Channel Row Dim
  initial_channel, 
  initial_sub_channel, 
  initial_marketing_brand, 
  initial_marketing_source, 
  initial_referral_source, 
  initial_sub_referral_source, 
  channel_reattributed AS reattributed_channel, 
  
  ---- General Attributes Row Dim (In Progress)
  lead_created_type,
  lead_duplicate_status,
  requested_care_type,
  referral_care_type,
  placement_care_type,
  
  ---- Lead Standing Row Dim (In Progress)
  lead_standing_group as standing_group,
  lead_standing,
  status,
  sub_status,
  created_interaction,
  
  ---- Property Row Dim
  org_name,
  country_property,
  state_property,
  city_property,
  zip_property,
  contract_type,
  
  ---- Geography Row Dim
  desired_country AS Primary_desired_country, 
  desired_state AS Primary_desired_state, 
  desired_city AS Primary_desired_city, 
  desired_zip AS Primary_desired_zip, 
  desired_local_pool_region AS Primary_desired_region, 
  desired_local_pool_area AS Primary_desired_area, 
  desired_local_pool AS Primary_desired_pool, 

  ---- Landing/Source
  domain_group,
  domain,
  page_type_group,
  page_type,
  page_care_type,
  clean_landing_URL,
  clean_source_URL,

  -- SEM
  vendor_id,
  sem_account,
  campaign,
  keyword,
  device,
  display_campaign,
  
  ---- Sales
  hand_delivered_lead,
  RVP_Region,
  Regional_Manager,
  Regional_Director,
  SLA_ASLA_Name,
  SLA_Type,
  HCAE_Name,
  
  ---- Other
--   warm_transfer_type,
  contact_relation_category,
  contact_relation_to_resident,
  lead_id, -- check 
  creative_id,
  
  0 AS Unique_Leads,
  0 AS Total_Leads,
  0 AS Total_LSTF, -- Measures (subject to change)
  0 AS Unique_LSTF, -- Measures (subject to change)
  COUNT(DISTINCT lead_id) AS Total_RLs,
  COUNT(DISTINCT (CASE WHEN lead_duplicate_status = "New" OR lead_duplicate_status = "Repeat Business" OR lead_duplicate_status = "Re-Engagement" THEN lead_id END)) AS Unique_RLs,
  0 AS Move_ins_Booked,
  0 AS Move_ins_Booked_month_end,
  0 AS move_ins_booked_after_month,
  0 AS Total_Referrals
FROM 
  ${hiveconf:workspace}.BOCR_dim_referred_leads
-- WHERE 
--   year(activity_date) = 2019 and month(activity_date) = 7
-- GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 34, 35, 36, 37, 38
GROUP BY 
---- Activity Row Dim
--   DATE_SUB(activity_date, day(activity_date)),-- AS Cohort_Start_Date,
  year(initial_referral_sent_date),
  year(initial_referral_sent_date)*100 + month(initial_referral_sent_date),
  DATE_SUB(initial_referral_sent_date, dayofweek(initial_referral_sent_date) - 1),
  dayofweek(initial_referral_sent_date),
  initial_referral_sent_date,
  
  ---- Channel Row Dim
  initial_channel, 
  initial_sub_channel, 
  initial_marketing_brand, 
  initial_marketing_source, 
  initial_referral_source, 
  initial_sub_referral_source, 
  channel_reattributed, 
  
  ---- General Attributes Row Dim (In Progress)
  lead_created_type,
  lead_duplicate_status,
  requested_care_type,
  referral_care_type,
  placement_care_type,
  
  ---- Lead Standing Row Dim (In Progress)
  lead_standing_group,
  lead_standing,
  status,
  sub_status,
  created_interaction,
  
  ---- Property Row Dim
  org_name,
  country_property,
  state_property,
  city_property,
  zip_property,
  contract_type,
  
  ---- Geography Row Dim
  desired_country,
  desired_state,
  desired_city,
  desired_zip,
  desired_local_pool_region,
  desired_local_pool_area, 
  desired_local_pool,

  ---- Landing/Source
    domain_group,
  domain,
  page_type_group,
  page_type,
  page_care_type,
  clean_landing_URL,
  clean_source_URL,
  
  -- SEM
  vendor_id,
  sem_account,
  campaign,
  keyword,
  device,
  display_campaign,
  
  ---- Sales
  hand_delivered_lead,
  RVP_Region,
  Regional_Manager,
  Regional_Director,
  SLA_ASLA_Name,
  SLA_Type,
  HCAE_Name,
  
  ---- Other
--   warm_transfer_type,
  contact_relation_category,
  contact_relation_to_resident,
  lead_id, -- check 
  creative_id
;

SELECT * FROM ${hiveconf:workspace}.BOAR_RLs_Agg_1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Move ins calc

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Move ins Original Deagg Table

-- COMMAND ----------

DROP TABLE IF EXISTS ${hiveconf:workspace}.filtered_move_ins_CV;
CREATE TABLE ${hiveconf:workspace}.filtered_move_ins_CV
AS

SELECT m.*
FROM ${hiveconf:db_env}.move_ins m

-- LEFT JOIN prod_edw_dbo.dim_initial_channel ic
-- ON m.lead_id=ic.lead_id
-- and ic._fivetran_deleted=False

WHERE m.employee_title  NOT IN ('AC')
;


DROP TABLE IF EXISTS ${hiveconf:workspace}.BOCR_dim_move_ins;
CREATE TABLE ${hiveconf:workspace}.BOCR_dim_move_ins
AS


select
  mv.*
  ,li.created_at
  ,li.initial_sub_channel
  ,li.initial_marketing_brand
  ,li.initial_marketing_source
  ,li.initial_referral_source
  ,li.initial_sub_referral_source
  ,li.channel_reattributed
  ,li.desired_country
  ,li.desired_state
  ,li.desired_city
  ,li.desired_zip
  ,li.desired_local_pool_region
  ,li.desired_local_pool_area
  ,li.desired_local_pool
  ,"No Value" as page_care_type
  ,li.page_type
  ,li.page_type_group
  ,li.vendor_id
  ,li.sem_account
  ,li.campaign
  ,li.keyword
  ,li.device
  ,li.display_campaign
--   ,li.lead_created_type
  ,li.lead_duplicate_status
  ,li.requested_care_type
  ,li.status
  ,li.sub_status
  ,li.domain
  ,li.SLA_ASLA_Name
  ,li.lead_created_type 
  ,"No Value" as referral_care_type 
--   ,"No Value" as placement_care_type 
  ,li.lead_standing_group
  ,li.created_interaction
  ,li.domain_group
  ,li.clean_landing_url
  ,li.clean_source_url
  ,li.hand_delivered_lead
  ,li.RVP_Region
  ,li.Regional_Manager
  ,li.Regional_Director
  ,li.SLA_Type
  ,li.HCAE_Name
  ,li.contact_relation_category 
  ,li.contact_relation_to_resident 
  ,li.creative_id 
  ,li.lead_standing
--   ,c.organization_name as community_organization
--   ,c.state as community_state
--   ,c.city as community_city
--   ,c.postal_code as community_zip
  ,comm.organization_name as org_name
  ,comm.country as country_property
  ,comm.state as state_property
  ,comm.city as city_property
  ,comm.postal_code as zip_property
  ,comm.contract_status_type as contract_type
  
from ${hiveconf:workspace}.filtered_move_ins_CV mv
left join ${hiveconf:workspace}.BOCR_lead_input li
on mv.lead_id=li.lead_id

left join 
  (SELECT *
        FROM
          (SELECT 
           *,
           ROW_NUMBER() OVER (
              PARTITION BY lead_id
              ORDER BY referral_accept_date
           ) AS row_num
        FROM 
           ${hiveconf:db_env}.referrals
        WHERE  referral_round = 1) devref
        WHERE devref.row_num = 1) r
on mv.lead_id=r.lead_id
and r.referral_round=1

LEFT JOIN ${hiveconf:db_env}.communities comm 
ON r.community_id = comm.community_id
-- inner join ${hiveconf:db_env}.communities c
-- on r.community_id= c.community_id
;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Move ins Agg Table Cohort View

-- COMMAND ----------

DROP TABLE IF EXISTS ${hiveconf:workspace}.BOCR_Move_Ins_Agg;
CREATE TABLE ${hiveconf:workspace}.BOCR_Move_Ins_Agg AS
SELECT 
  ---- Activity Row Dim
--   DATE_SUB(activity_date, day(activity_date)) AS Cohort_Month_Start_Date,
  year(created_at) AS Year_Activity, 
  year(created_at)*100 + month(created_at) AS Month_Activity, 
  DATE_SUB(created_at, dayofweek(created_at) - 1) AS Week_Activity, 
  dayofweek(created_at) AS Day_of_week_Activity, 
  created_at AS Date_of_Activity, 
  
  ---- Channel Row Dim
  initial_channel, 
  initial_sub_channel, 
  initial_marketing_brand, 
  initial_marketing_source, 
  initial_referral_source, 
  initial_sub_referral_source, 
  channel_reattributed AS reattributed_channel, 
  
  ---- General Attributes Row Dim (In Progress)
  lead_created_type,
  lead_duplicate_status,
  requested_care_type,
  referral_care_type,
  care_type_code AS placement_care_type,
  
  ---- Lead Standing Row Dim (In Progress)
  lead_standing_group as standing_group,
  lead_standing,
  status,
  sub_status,
  created_interaction,
  
  ---- Property Row Dim
  org_name,
  country_property,
  state_property,
  city_property,
  zip_property,
  contract_type,
  
  ---- Geography Row Dim
  desired_country AS Primary_desired_country, 
  desired_state AS Primary_desired_state, 
  desired_city AS Primary_desired_city, 
  desired_zip AS Primary_desired_zip, 
  desired_local_pool_region AS Primary_desired_region, 
  desired_local_pool_area AS Primary_desired_area, 
  desired_local_pool AS Primary_desired_pool, 

  ---- Landing/Source
  domain_group,
  domain,
  page_type_group,
  page_type,
  page_care_type,
  clean_landing_URL,
  clean_source_URL,

  -- SEM
  vendor_id,
  sem_account,
  campaign,
  keyword,
  device,
  display_campaign,
  
  ---- Sales
  hand_delivered_lead,
  RVP_Region,
  Regional_Manager,
  Regional_Director,
  SLA_ASLA_Name,
  SLA_Type,
  HCAE_Name,
  
  ---- Other
--   warm_transfer_type,
  contact_relation_category,
  contact_relation_to_resident,
  lead_id, -- check 
  creative_id,
  
  0 AS Unique_Leads,
  0 AS Total_Leads,
  0 AS Total_LSTF, -- Measures (subject to change)
  0 AS Unique_LSTF, -- Measures (subject to change)
  0 AS Total_RLs,
  0 AS Unique_RLs,
  SUM(CASE WHEN move_in_backout_date is NULL then 1 else 0 end) AS Move_ins_Booked,
  SUM (CASE WHEN move_in_backout_date is NULL then 1 else (case when left(move_in_backout_date,7)=left(move_in_booked_date,7) then 0 else 1 end) end) AS Move_ins_Booked_month_end,
  SUM (case when move_in_backout_date is null then 0 else case when left(move_in_backout_date,7)=left(move_in_booked_date,7) then 0 else -1 end end) AS move_ins_booked_after_month,
  0 AS Total_Referrals
FROM 
  ${hiveconf:workspace}.BOCR_dim_move_ins
-- WHERE 
--   year(activity_date) = 2019 and month(activity_date) = 7
-- GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 34, 35, 36, 37, 38
GROUP BY 
---- Activity Row Dim
--   DATE_SUB(activity_date, day(activity_date)),-- AS Cohort_Start_Date,
  year(created_at),
  year(created_at)*100 + month(created_at),
  DATE_SUB(created_at, dayofweek(created_at) - 1),
  dayofweek(created_at),
  created_at,
  
  ---- Channel Row Dim
  initial_channel, 
  initial_sub_channel, 
  initial_marketing_brand, 
  initial_marketing_source, 
  initial_referral_source, 
  initial_sub_referral_source, 
  channel_reattributed, 
  
  ---- General Attributes Row Dim (In Progress)
  lead_created_type,
  lead_duplicate_status,
  requested_care_type,
  referral_care_type,
  care_type_code,
  
  ---- Lead Standing Row Dim (In Progress)
  lead_standing_group,
  lead_standing,
  status,
  sub_status,
  created_interaction,
  
  ---- Property Row Dim
  org_name,
  country_property,
  state_property,
  city_property,
  zip_property,
  contract_type,
  
  ---- Geography Row Dim
  desired_country,
  desired_state,
  desired_city,
  desired_zip,
  desired_local_pool_region,
  desired_local_pool_area, 
  desired_local_pool,

  ---- Landing/Source
    domain_group,
  domain,
  page_type_group,
  page_type,
  page_care_type,
  clean_landing_URL,
  clean_source_URL,
  
  -- SEM
  vendor_id,
  sem_account,
  campaign,
  keyword,
  device,
  display_campaign,
  
  ---- Sales
  hand_delivered_lead,
  RVP_Region,
  Regional_Manager,
  Regional_Director,
  SLA_ASLA_Name,
  SLA_Type,
  HCAE_Name,
  
  ---- Other
--   warm_transfer_type,
  contact_relation_category,
  contact_relation_to_resident,
  lead_id, -- check 
  creative_id
;

SELECT * FROM ${hiveconf:workspace}.BOCR_Move_Ins_Agg

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Move ins Agg Table Activity View

-- COMMAND ----------

DROP TABLE IF EXISTS ${hiveconf:workspace}.BOAR_Move_Ins_Agg_1;
CREATE TABLE ${hiveconf:workspace}.BOAR_Move_Ins_Agg_1 AS
SELECT 
  ---- Activity Row Dim
--   DATE_SUB(activity_date, day(activity_date)) AS Cohort_Month_Start_Date,
  year(move_in_booked_date) AS Year_Activity, 
  year(move_in_booked_date)*100 + month(move_in_booked_date) AS Month_Activity, 
  DATE_SUB(move_in_booked_date, dayofweek(move_in_booked_date) - 1) AS Week_Activity, 
  dayofweek(move_in_booked_date) AS Day_of_week_Activity, 
  move_in_booked_date AS Date_of_Activity, 
  
  ---- Channel Row Dim
  initial_channel, 
  initial_sub_channel, 
  initial_marketing_brand, 
  initial_marketing_source, 
  initial_referral_source, 
  initial_sub_referral_source, 
  channel_reattributed AS reattributed_channel, 
  
  ---- General Attributes Row Dim (In Progress)
  lead_created_type,
  lead_duplicate_status,
  requested_care_type,
  referral_care_type,
  care_type_code AS placement_care_type,
  
  ---- Lead Standing Row Dim (In Progress)
  lead_standing_group as standing_group,
  lead_standing,
  status,
  sub_status,
  created_interaction,
  
  ---- Property Row Dim
  org_name,
  country_property,
  state_property,
  city_property,
  zip_property,
  contract_type,
  
  ---- Geography Row Dim
  desired_country AS Primary_desired_country, 
  desired_state AS Primary_desired_state, 
  desired_city AS Primary_desired_city, 
  desired_zip AS Primary_desired_zip, 
  desired_local_pool_region AS Primary_desired_region, 
  desired_local_pool_area AS Primary_desired_area, 
  desired_local_pool AS Primary_desired_pool, 

  ---- Landing/Source
  domain_group,
  domain,
  page_type_group,
  page_type,
  page_care_type,
  clean_landing_URL,
  clean_source_URL,

  -- SEM
  vendor_id,
  sem_account,
  campaign,
  keyword,
  device,
  display_campaign,
  
  ---- Sales
  hand_delivered_lead,
  RVP_Region,
  Regional_Manager,
  Regional_Director,
  SLA_ASLA_Name,
  SLA_Type,
  HCAE_Name,
  
  ---- Other
--   warm_transfer_type,
  contact_relation_category,
  contact_relation_to_resident,
  lead_id, -- check 
  creative_id,
  
  0 AS Unique_Leads,
  0 AS Total_Leads,
  0 AS Total_LSTF, -- Measures (subject to change)
  0 AS Unique_LSTF, -- Measures (subject to change)
  0 AS Total_RLs,
  0 AS Unique_RLs,
  SUM(CASE WHEN move_in_backout_date is NULL then 1 else 0 end) AS Move_ins_Booked,
  SUM (CASE WHEN move_in_backout_date is NULL then 1 else (case when left(move_in_backout_date,7)=left(move_in_booked_date,7) then 0 else 1 end) end) AS Move_ins_Booked_month_end,
  SUM (case when move_in_backout_date is null then 0 else case when left(move_in_backout_date,7)=left(move_in_booked_date,7) then 0 else -1 end end) AS move_ins_booked_after_month,
  0 AS Total_Referrals
FROM 
  ${hiveconf:workspace}.BOCR_dim_move_ins
-- WHERE 
--   year(activity_date) = 2019 and month(activity_date) = 7
-- GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 34, 35, 36, 37, 38
GROUP BY 
---- Activity Row Dim
--   DATE_SUB(activity_date, day(activity_date)),-- AS Cohort_Start_Date,
  year(move_in_booked_date),
  year(move_in_booked_date)*100 + month(move_in_booked_date),
  DATE_SUB(move_in_booked_date, dayofweek(move_in_booked_date) - 1),
  dayofweek(move_in_booked_date),
  move_in_booked_date,
  
  ---- Channel Row Dim
  initial_channel, 
  initial_sub_channel, 
  initial_marketing_brand, 
  initial_marketing_source, 
  initial_referral_source, 
  initial_sub_referral_source, 
  channel_reattributed, 
  
  ---- General Attributes Row Dim (In Progress)
  lead_created_type,
  lead_duplicate_status,
  requested_care_type,
  referral_care_type,
  care_type_code,
  
  ---- Lead Standing Row Dim (In Progress)
  lead_standing_group,
  lead_standing,
  status,
  sub_status,
  created_interaction,
  
  ---- Property Row Dim
  org_name,
  country_property,
  state_property,
  city_property,
  zip_property,
  contract_type,
  
  ---- Geography Row Dim
  desired_country,
  desired_state,
  desired_city,
  desired_zip,
  desired_local_pool_region,
  desired_local_pool_area, 
  desired_local_pool,

  ---- Landing/Source
    domain_group,
  domain,
  page_type_group,
  page_type,
  page_care_type,
  clean_landing_URL,
  clean_source_URL,
  
  -- SEM
  vendor_id,
  sem_account,
  campaign,
  keyword,
  device,
  display_campaign,
  
  ---- Sales
  hand_delivered_lead,
  RVP_Region,
  Regional_Manager,
  Regional_Director,
  SLA_ASLA_Name,
  SLA_Type,
  HCAE_Name,
  
  ---- Other
--   warm_transfer_type,
  contact_relation_category,
  contact_relation_to_resident,
  lead_id, -- check 
  creative_id
;

SELECT * FROM ${hiveconf:workspace}.BOAR_Move_Ins_Agg_1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Referrals calc

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Referrals Original Deagg Table

-- COMMAND ----------

DROP TABLE IF EXISTS ${hiveconf:workspace}.filtered_referrals_CV;
CREATE TABLE ${hiveconf:workspace}.filtered_referrals_CV
AS
SELECT r.*
FROM ${hiveconf:db_env}.referrals r

-- LEFT JOIN prod_edw_dbo.dim_initial_channel ic
-- ON r.lead_id=ic.lead_id
-- and ic._fivetran_deleted=False

WHERE r.employee_title  NOT IN ('AC')
-- AND ic.initial_sub_referral_source NOT IN ('Secret Shop')
;


DROP TABLE IF EXISTS ${hiveconf:workspace}.BOCR_dim_referrals;
CREATE TABLE ${hiveconf:workspace}.BOCR_dim_referrals
AS


select
  rf.*
  ,li.created_at
  ,li.initial_sub_channel
  ,li.initial_marketing_brand
  ,li.initial_marketing_source
  ,li.initial_referral_source
  ,li.initial_sub_referral_source
  ,li.channel_reattributed
  ,li.desired_country
  ,li.desired_state
  ,li.desired_city
  ,li.desired_zip
  ,li.desired_local_pool_region
  ,li.desired_local_pool_area
  ,li.desired_local_pool
  ,"No Value" as page_care_type
  ,li.page_type
  ,li.page_type_group
  ,li.vendor_id
  ,li.sem_account
  ,li.campaign
  ,li.keyword
  ,li.device
  ,li.display_campaign
--   ,li.lead_created_type
  ,li.lead_duplicate_status
  ,li.requested_care_type
  ,li.status
  ,li.sub_status
  ,li.domain
  ,li.SLA_ASLA_Name
  ,li.lead_created_type 
  ,"No Value" as referral_care_type 
  ,"No Value" as placement_care_type 
  ,li.lead_standing_group
  ,li.created_interaction
  ,li.domain_group
  ,li.clean_landing_url
  ,li.clean_source_url
  ,li.hand_delivered_lead
  ,li.RVP_Region
  ,li.Regional_Manager
  ,li.Regional_Director
  ,li.SLA_Type
  ,li.HCAE_Name
  ,li.contact_relation_category 
  ,li.contact_relation_to_resident 
  ,li.creative_id 
  ,li.lead_standing
--   ,c.organization_name as community_organization
--   ,c.state as community_state
--   ,c.city as community_city
--   ,c.postal_code as community_zip
  ,comm.organization_name as org_name
  ,comm.country as country_property
  ,comm.state as state_property
  ,comm.city as city_property
  ,comm.postal_code as zip_property
  ,comm.contract_status_type as contract_type
  
from ${hiveconf:workspace}.filtered_referrals_CV rf
left join ${hiveconf:workspace}.BOCR_lead_input li
on rf.lead_id=li.lead_id

left join 
  (SELECT *
        FROM
          (SELECT 
           *,
           ROW_NUMBER() OVER (
              PARTITION BY lead_id
              ORDER BY referral_accept_date
           ) AS row_num
        FROM 
           ${hiveconf:db_env}.referrals
        WHERE  referral_round = 1) devref
        WHERE devref.row_num = 1) r
on rf.lead_id=r.lead_id
and r.referral_round=1

LEFT JOIN ${hiveconf:db_env}.communities comm 
ON r.community_id = comm.community_id
-- inner join ${hiveconf:db_env}.communities c
-- on r.community_id= c.community_id
;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Referrals Agg Table Cohort View

-- COMMAND ----------

DROP TABLE IF EXISTS ${hiveconf:workspace}.BOCR_Referrals_Agg;
CREATE TABLE ${hiveconf:workspace}.BOCR_Referrals_Agg AS
SELECT 
  ---- Activity Row Dim
--   DATE_SUB(activity_date, day(activity_date)) AS Cohort_Month_Start_Date,
  year(created_at) AS Year_Activity, 
  year(created_at)*100 + month(created_at) AS Month_Activity, 
  DATE_SUB(created_at, dayofweek(created_at) - 1) AS Week_Activity, 
  dayofweek(created_at) AS Day_of_week_Activity, 
  created_at AS Date_of_Activity, 
  
  ---- Channel Row Dim
  initial_channel, 
  initial_sub_channel, 
  initial_marketing_brand, 
  initial_marketing_source, 
  initial_referral_source, 
  initial_sub_referral_source, 
  channel_reattributed AS reattributed_channel, 
  
  ---- General Attributes Row Dim (In Progress)
  lead_created_type,
  lead_duplicate_status,
  requested_care_type,
  referral_care_type,
  placement_care_type,
  
  ---- Lead Standing Row Dim (In Progress)
  lead_standing_group as standing_group,
  lead_standing,
  status,
  sub_status,
  created_interaction,
  
  ---- Property Row Dim
  org_name,
  country_property,
  state_property,
  city_property,
  zip_property,
  contract_type,
  
  ---- Geography Row Dim
  desired_country AS Primary_desired_country, 
  desired_state AS Primary_desired_state, 
  desired_city AS Primary_desired_city, 
  desired_zip AS Primary_desired_zip, 
  desired_local_pool_region AS Primary_desired_region, 
  desired_local_pool_area AS Primary_desired_area, 
  desired_local_pool AS Primary_desired_pool, 

  ---- Landing/Source
  domain_group,
  domain,
  page_type_group,
  page_type,
  page_care_type,
  clean_landing_URL,
  clean_source_URL,

  -- SEM
  vendor_id,
  sem_account,
  campaign,
  keyword,
  device,
  display_campaign,
  
  ---- Sales
  hand_delivered_lead,
  RVP_Region,
  Regional_Manager,
  Regional_Director,
  SLA_ASLA_Name,
  SLA_Type,
  HCAE_Name,
  
  ---- Other
--   warm_transfer_type,
  contact_relation_category,
  contact_relation_to_resident,
  lead_id, -- check 
  creative_id,
  
  0 AS Unique_Leads,
  0 AS Total_Leads,
  0 AS Total_LSTF, -- Measures (subject to change)
  0 AS Unique_LSTF, -- Measures (subject to change)
  0 AS Total_RLs,
  0 AS Unique_RLs,
  0 AS Move_ins_Booked,
  0 AS Move_ins_Booked_month_end,
  0 AS move_ins_booked_after_month,
  COUNT(DISTINCT referral_id) AS Total_Referrals
FROM 
  ${hiveconf:workspace}.BOCR_dim_referrals
-- WHERE 
--   year(activity_date) = 2019 and month(activity_date) = 7
-- GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 34, 35, 36, 37, 38
GROUP BY 
---- Activity Row Dim
--   DATE_SUB(activity_date, day(activity_date)),-- AS Cohort_Start_Date,
  year(created_at),
  year(created_at)*100 + month(created_at),
  DATE_SUB(created_at, dayofweek(created_at) - 1),
  dayofweek(created_at),
  created_at,
  
  ---- Channel Row Dim
  initial_channel, 
  initial_sub_channel, 
  initial_marketing_brand, 
  initial_marketing_source, 
  initial_referral_source, 
  initial_sub_referral_source, 
  channel_reattributed, 
  
  ---- General Attributes Row Dim (In Progress)
  lead_created_type,
  lead_duplicate_status,
  requested_care_type,
  referral_care_type,
  placement_care_type,
  
  ---- Lead Standing Row Dim (In Progress)
  lead_standing_group,
  lead_standing,
  status,
  sub_status,
  created_interaction,
  
  ---- Property Row Dim
  org_name,
  country_property,
  state_property,
  city_property,
  zip_property,
  contract_type,
  
  ---- Geography Row Dim
  desired_country,
  desired_state,
  desired_city,
  desired_zip,
  desired_local_pool_region,
  desired_local_pool_area, 
  desired_local_pool,

  ---- Landing/Source
    domain_group,
  domain,
  page_type_group,
  page_type,
  page_care_type,
  clean_landing_URL,
  clean_source_URL,
  
  -- SEM
  vendor_id,
  sem_account,
  campaign,
  keyword,
  device,
  display_campaign,
  
  ---- Sales
  hand_delivered_lead,
  RVP_Region,
  Regional_Manager,
  Regional_Director,
  SLA_ASLA_Name,
  SLA_Type,
  HCAE_Name,
  
  ---- Other
--   warm_transfer_type,
  contact_relation_category,
  contact_relation_to_resident,
  lead_id, -- check 
  creative_id
;

SELECT * FROM ${hiveconf:workspace}.BOCR_Referrals_Agg

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Referrals Agg Table Activity View

-- COMMAND ----------

DROP TABLE IF EXISTS ${hiveconf:workspace}.BOAR_Referrals_Agg_1;
CREATE TABLE ${hiveconf:workspace}.BOAR_Referrals_Agg_1 AS
SELECT 
  ---- Activity Row Dim
--   DATE_SUB(activity_date, day(activity_date)) AS Cohort_Month_Start_Date,
  year(referral_date) AS Year_Activity, 
  year(referral_date)*100 + month(referral_date) AS Month_Activity, 
  DATE_SUB(referral_date, dayofweek(referral_date) - 1) AS Week_Activity, 
  dayofweek(referral_date) AS Day_of_week_Activity, 
  referral_date AS Date_of_Activity, 
  
  ---- Channel Row Dim
  initial_channel, 
  initial_sub_channel, 
  initial_marketing_brand, 
  initial_marketing_source, 
  initial_referral_source, 
  initial_sub_referral_source, 
  channel_reattributed AS reattributed_channel, 
  
  ---- General Attributes Row Dim (In Progress)
  lead_created_type,
  lead_duplicate_status,
  requested_care_type,
  referral_care_type,
  placement_care_type,
  
  ---- Lead Standing Row Dim (In Progress)
  lead_standing_group as standing_group,
  lead_standing,
  status,
  sub_status,
  created_interaction,
  
  ---- Property Row Dim
  org_name,
  country_property,
  state_property,
  city_property,
  zip_property,
  contract_type,
  
  ---- Geography Row Dim
  desired_country AS Primary_desired_country, 
  desired_state AS Primary_desired_state, 
  desired_city AS Primary_desired_city, 
  desired_zip AS Primary_desired_zip, 
  desired_local_pool_region AS Primary_desired_region, 
  desired_local_pool_area AS Primary_desired_area, 
  desired_local_pool AS Primary_desired_pool, 

  ---- Landing/Source
  domain_group,
  domain,
  page_type_group,
  page_type,
  page_care_type,
  clean_landing_URL,
  clean_source_URL,

  -- SEM
  vendor_id,
  sem_account,
  campaign,
  keyword,
  device,
  display_campaign,
  
  ---- Sales
  hand_delivered_lead,
  RVP_Region,
  Regional_Manager,
  Regional_Director,
  SLA_ASLA_Name,
  SLA_Type,
  HCAE_Name,
  
  ---- Other
--   warm_transfer_type,
  contact_relation_category,
  contact_relation_to_resident,
  lead_id, -- check 
  creative_id,
  
  0 AS Unique_Leads,
  0 AS Total_Leads,
  0 AS Total_LSTF, -- Measures (subject to change)
  0 AS Unique_LSTF, -- Measures (subject to change)
  0 AS Total_RLs,
  0 AS Unique_RLs,
  0 AS Move_ins_Booked,
  0 AS Move_ins_Booked_month_end,
  0 AS move_ins_booked_after_month,
  COUNT(DISTINCT referral_id) AS Total_Referrals
FROM 
  ${hiveconf:workspace}.BOCR_dim_referrals
-- WHERE 
--   year(activity_date) = 2019 and month(activity_date) = 7
-- GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 34, 35, 36, 37, 38
GROUP BY 
---- Activity Row Dim
--   DATE_SUB(activity_date, day(activity_date)),-- AS Cohort_Start_Date,
  year(referral_date),
  year(referral_date)*100 + month(referral_date),
  DATE_SUB(referral_date, dayofweek(referral_date) - 1),
  dayofweek(referral_date),
  referral_date,
  
  ---- Channel Row Dim
  initial_channel, 
  initial_sub_channel, 
  initial_marketing_brand, 
  initial_marketing_source, 
  initial_referral_source, 
  initial_sub_referral_source, 
  channel_reattributed, 
  
  ---- General Attributes Row Dim (In Progress)
  lead_created_type,
  lead_duplicate_status,
  requested_care_type,
  referral_care_type,
  placement_care_type,
  
  ---- Lead Standing Row Dim (In Progress)
  lead_standing_group,
  lead_standing,
  status,
  sub_status,
  created_interaction,
  
  ---- Property Row Dim
  org_name,
  country_property,
  state_property,
  city_property,
  zip_property,
  contract_type,
  
  ---- Geography Row Dim
  desired_country,
  desired_state,
  desired_city,
  desired_zip,
  desired_local_pool_region,
  desired_local_pool_area, 
  desired_local_pool,

  ---- Landing/Source
    domain_group,
  domain,
  page_type_group,
  page_type,
  page_care_type,
  clean_landing_URL,
  clean_source_URL,
  
  -- SEM
  vendor_id,
  sem_account,
  campaign,
  keyword,
  device,
  display_campaign,
  
  ---- Sales
  hand_delivered_lead,
  RVP_Region,
  Regional_Manager,
  Regional_Director,
  SLA_ASLA_Name,
  SLA_Type,
  HCAE_Name,
  
  ---- Other
--   warm_transfer_type,
  contact_relation_category,
  contact_relation_to_resident,
  lead_id, -- check 
  creative_id
;

SELECT * FROM ${hiveconf:workspace}.BOAR_Referrals_Agg_1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Final Aggregation

-- COMMAND ----------

DROP TABLE IF EXISTS ${hiveconf:workspace}.BOCR_Final_Agg;
CREATE TABLE ${hiveconf:workspace}.BOCR_Final_Agg AS
SELECT * FROM ${hiveconf:workspace}.BOCR_RLs_Agg
UNION 
SELECT * FROM ${hiveconf:workspace}.BOCR_LSTF_Agg
UNION 
SELECT * FROM ${hiveconf:workspace}.BOCR_Leads_Agg
UNION 
SELECT * FROM ${hiveconf:workspace}.BOCR_Move_Ins_Agg
UNION 
SELECT * FROM ${hiveconf:workspace}.BOCR_Referrals_Agg
WHERE Year_Activity >= 2018;

SELECT * FROM ${hiveconf:workspace}.BOCR_Final_Agg

-- COMMAND ----------

DROP TABLE IF EXISTS ${hiveconf:workspace}.BOAR_Final_Agg_1;
CREATE TABLE ${hiveconf:workspace}.BOAR_Final_Agg_1 AS
SELECT * FROM ${hiveconf:workspace}.BOAR_RLs_Agg_1
UNION 
SELECT * FROM ${hiveconf:workspace}.BOAR_LSTF_Agg_1
UNION 
SELECT * FROM ${hiveconf:workspace}.BOCR_Leads_Agg
UNION
SELECT * FROM ${hiveconf:workspace}.BOAR_Move_Ins_Agg_1
UNION 
SELECT * FROM ${hiveconf:workspace}.BOAR_Referrals_Agg_1
WHERE Year_Activity >= 2018;

SELECT * FROM ${hiveconf:workspace}.BOAR_Final_Agg_1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Example

-- COMMAND ----------

drop table if exists ${hiveconf:workspace}.temp_empl_BOCR;
create table ${hiveconf:workspace}.temp_empl_BOCR as
(SELECT 
--   SLA_ASLA_Name -- Row Dim 
  Month_Activity -- Row Dim
  ,sum(Total_Leads) AS Total_Leads
  ,sum(Total_LSTF) AS Total_LSTF 
  ,sum(Total_RLs) AS Total_RLs
  ,sum(Unique_Leads) AS Unique_Leads
  ,sum(Unique_LSTF) AS Unique_LSTF 
  ,sum(Unique_RLs) AS Unique_RLs 
--   ,sum(Move_ins_Booked) AS Move_ins_Booked 
--   ,sum(Move_ins_Booked_month_end) AS Move_ins_Booked_month_end
--   ,sum(move_ins_booked_after_month) AS move_ins_booked_after_month
--   ,sum(Total_Referrals) AS Total_Referrals
FROM
  ${hiveconf:workspace}.BOCR_Final_Agg 
WHERE 1=1 -- Add Filters here 
-- AND Year_Activity IN (2021)
AND Month_Activity IN (202101, 202102, 202103, 202104, 202105)
GROUP BY 1 -- Add Row Dims
  );

select * from ${hiveconf:workspace}.temp_empl_BOCR

-- COMMAND ----------

select distinct contract_type from ${hiveconf:workspace}.BOAR_Final_Agg_1 

-- COMMAND ----------

