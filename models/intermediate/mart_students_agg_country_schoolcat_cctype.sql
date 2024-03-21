SELECT
  school_id,
  jt_country,
  jt_school_category,
  career_center_type,
  ROUND(COUNT(DISTINCT(user_id)) OVER (PARTITION BY jt_country, career_center_type, jt_school_category) / COUNT(DISTINCT(school_id)) OVER (PARTITION BY jt_country, career_center_type, jt_school_category), 0) AS avg_user_per_school,
    ROUND(AVG(CASE WHEN active  = true THEN 1 ELSE 0 END) OVER (PARTITION BY jt_country, career_center_type, jt_school_category),2) AS avg_active,
  ROUND(AVG(CASE WHEN resume_uploaded  = true THEN 1 ELSE 0 END) OVER (PARTITION BY jt_country, career_center_type, jt_school_category),2) AS avg_resume,

  ROUND(AVG(nb_shortlists) OVER (PARTITION BY jt_country, career_center_type, jt_school_category), 2) AS avg_nb_shortlists,
  ROUND(AVG(awaiting) OVER (PARTITION BY jt_country, career_center_type, jt_school_category), 2) AS avg_nb_awaiting,
  ROUND(AVG(not_interested) OVER (PARTITION BY jt_country, career_center_type, jt_school_category), 2) AS avg_nb_not_interested,
  ROUND(AVG(interested) OVER (PARTITION BY jt_country, career_center_type, jt_school_category), 2) AS avg_nb_interested,
  ROUND(AVG(declined) OVER (PARTITION BY jt_country, career_center_type, jt_school_category), 2) AS avg_nb_declined
FROM 
  {{ ref('mart_students') }}