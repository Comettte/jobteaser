WITH aggregation AS 
(SELECT
  school_id,
  jt_country,
  jt_school_category,
  career_center_type,
  COUNT(DISTINCT(school_id)) OVER (PARTITION BY jt_country, career_center_type, jt_school_category) AS nbschool,
  COUNT(DISTINCT(user_id)) OVER (PARTITION BY jt_country, career_center_type, jt_school_category) AS agg_user_per_school,
  SUM(active) OVER (PARTITION BY jt_country, career_center_type, jt_school_category) AS agg_active,
  SUM(resume_uploaded) OVER (PARTITION BY jt_country, career_center_type, jt_school_category) AS agg_resume,
  SUM(nb_shortlists) OVER (PARTITION BY jt_country, career_center_type, jt_school_category) AS agg_nb_shortlists,
  SUM(awaiting) OVER (PARTITION BY jt_country, career_center_type, jt_school_category) AS agg_nb_awaiting,
  SUM(not_interested) OVER (PARTITION BY jt_country, career_center_type, jt_school_category) AS agg_nb_not_interested,
  SUM(interested) OVER (PARTITION BY jt_country, career_center_type, jt_school_category) AS agg_nb_interested,
  SUM(approved) OVER (PARTITION BY jt_country, career_center_type, jt_school_category) AS agg_nb_approved,
  SUM(declined) OVER (PARTITION BY jt_country, career_center_type, jt_school_category) AS agg_nb_declined

FROM 
  {{ ref('mart_students') }}),

divisionnbschool AS
 (
    SELECT 
    school_id,
    jt_country,
    jt_school_category,
    career_center_type,
    ROUND(aggregation.agg_user_per_school / aggregation.nbschool, 2) AS avg_user_per_school,
    ROUND(aggregation.agg_active / aggregation.nbschool, 2) AS avg_active,
    ROUND(aggregation.agg_resume / aggregation.nbschool, 2) AS avg_resume,
    ROUND(aggregation.agg_nb_shortlists / aggregation.nbschool, 2) AS avg_nb_shortlists,
    ROUND(aggregation.agg_nb_awaiting / aggregation.nbschool, 2) AS avg_nb_awaiting,
    ROUND(aggregation.agg_nb_declined / aggregation.nbschool, 2) AS avg_nb_declined,
    ROUND(aggregation.agg_nb_interested / aggregation.nbschool, 2) AS avg_nb_interested,
    ROUND(aggregation.agg_nb_not_interested / aggregation.nbschool, 2) AS avg_nb_not_interested,
    ROUND(aggregation.agg_nb_approved/ aggregation.nbschool, 2) AS agg_nb_approved,

    FROM aggregation
)

SELECT DISTINCT *

FROM divisionnbschool