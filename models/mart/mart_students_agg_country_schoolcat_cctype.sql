WITH aggregation AS 
(SELECT
  school_id,
  jt_country,
  jt_school_category,
  career_center_type,
  COUNT(DISTINCT(school_id)) OVER (PARTITION BY jt_country, career_center_type, jt_school_category) AS nbschool,
  ROUND(COUNT(DISTINCT(user_id)) OVER (PARTITION BY jt_country, career_center_type, jt_school_category),2) AS agg_user_per_school,
  ROUND(SUM(active) OVER (PARTITION BY jt_country, career_center_type, jt_school_category),2) AS agg_active,
  ROUND(SUM(resume_uploaded) OVER (PARTITION BY jt_country, career_center_type, jt_school_category),2) AS agg_resume,
  ROUND(SUM(nb_shortlists) OVER (PARTITION BY jt_country, career_center_type, jt_school_category), 2) AS agg_nb_shortlists,
  ROUND(SUM(awaiting) OVER (PARTITION BY jt_country, career_center_type, jt_school_category), 2) AS agg_nb_awaiting,
  ROUND(SUM(not_interested) OVER (PARTITION BY jt_country, career_center_type, jt_school_category), 2) AS agg_nb_not_interested,
  ROUND(SUM(interested) OVER (PARTITION BY jt_country, career_center_type, jt_school_category), 2) AS agg_nb_interested,
  ROUND(SUM(approved) OVER (PARTITION BY jt_country, career_center_type, jt_school_category), 2) AS agg_nb_approved,
  ROUND(SUM(declined) OVER (PARTITION BY jt_country, career_center_type, jt_school_category), 2) AS agg_nb_declined

FROM 
  {{ ref('mart_students') }}),

divisionnbschool AS
 (
    SELECT 
    aggregation.agg_active / aggregation.nbschool AS avg_active,
    aggregation.agg_resume / aggregation.nbschool AS avg_resume,
    aggregation.agg_nb_shortlists / aggregation.nbschool AS avg_nb_shortlists,
    aggregation.agg_nb_awaiting / aggregation.nbschool AS avg_nb_awaiting,
    aggregation.agg_nb_declined / aggregation.nbschool AS avg_nb_declined,
    aggregation.agg_nb_interested / aggregation.nbschool AS avg_nb_interested,
    aggregation.agg_nb_not_interested / aggregation.nbschool AS avg_nb_not_interested,
    aggregation.agg_nb_approved/ aggregation.nbschool AS agg_nb_approved,

    FROM aggregation
)

SELECT DISTINCT *

FROM divisionnbschool