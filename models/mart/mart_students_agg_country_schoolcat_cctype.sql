WITH aggregation AS 
(SELECT
  school_id,
  jt_country,
  jt_school_category,
  career_center_type,
  ROUND(COUNT(DISTINCT(user_id)) OVER (PARTITION BY jt_country, career_center_type, jt_school_category),2) AS avg_user_per_school,
  ROUND(SUM(active) OVER (PARTITION BY jt_country, career_center_type, jt_school_category),2) AS avg_active,
  ROUND(SUM(resume_uploaded) OVER (PARTITION BY jt_country, career_center_type, jt_school_category),2) AS avg_resume,
  ROUND(SUM(nb_shortlists) OVER (PARTITION BY jt_country, career_center_type, jt_school_category), 2) AS avg_nb_shortlists,
  ROUND(SUM(awaiting) OVER (PARTITION BY jt_country, career_center_type, jt_school_category), 2) AS avg_nb_awaiting,
  ROUND(SUM(not_interested) OVER (PARTITION BY jt_country, career_center_type, jt_school_category), 2) AS avg_nb_not_interested,
  ROUND(SUM(interested) OVER (PARTITION BY jt_country, career_center_type, jt_school_category), 2) AS avg_nb_interested,
  ROUND(SUM(declined) OVER (PARTITION BY jt_country, career_center_type, jt_school_category), 2) AS avg_nb_declined

FROM 
  {{ ref('mart_students') }})

SELECT DISTINCT *

FROM aggregation