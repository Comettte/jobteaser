SELECT
  school_id,
  jt_country,
  jt_school_category,
  career_center_type,
  ROUND(COUNT(DISTINCT(user_id)) OVER (PARTITION BY jt_country, career_center_type, jt_school_category) / COUNT(DISTINCT(school_id)) OVER (PARTITION BY jt_country, career_center_type, jt_school_category), 0) AS avg_user_per_school,
  ROUND(COUNTIF(active) OVER (PARTITION BY jt_country, career_center_type, jt_school_category) / COUNT(*) OVER (PARTITION BY jt_country, career_center_type, jt_school_category) * 100, 2) AS percentage_active_students,
  ROUND(COUNTIF(resume_uploaded)OVER (PARTITION BY jt_country, career_center_type, jt_school_category) / COUNT(*) OVER (PARTITION BY jt_country, career_center_type, jt_school_category) * 100, 2) AS percentage_resume_uploaded,
  ROUND(AVG(nb_shortlists) OVER (PARTITION BY jt_country, career_center_type, jt_school_category), 2) AS avg_nb_shortlists,
  ROUND(AVG(awaiting) OVER (PARTITION BY jt_country, career_center_type, jt_school_category), 2) AS avg_nb_awaiting,
  ROUND(AVG(not_interested) OVER (PARTITION BY jt_country, career_center_type, jt_school_category), 2) AS avg_nb_not_interested,
  ROUND(AVG(interested) OVER (PARTITION BY jt_country, career_center_type, jt_school_category), 2) AS avg_nb_interested,
  ROUND(AVG(declined) OVER (PARTITION BY jt_country, career_center_type, jt_school_category), 2) AS avg_nb_declined
FROM 
  {{ ref('mart_students') }}