SELECT 
  jt_country,
  jt_school_category,
  career_center_type,
  ROUND(COUNT(DISTINCT(user_id))/COUNT(DISTINCT(school_id)),2) AS avg_user_per_schools,
  round(COUNTIF(active) / COUNT(*) * 100,2) AS avg_active,
  round(COUNTIF(resume_uploaded) / COUNT(*) *100,2) AS avg_resume_uploaded,
  round(avg(nb_shortlists)*100,2) as avg_nb_shortlists,
  round(avg(awaiting)*100,2) as avg_nb_awaiting,
  round(avg(not_interested)*100,2) as avg_nb_not_interested,
  round(avg(interested)*100,2) as avg_nb_interested,
  round(avg(declined)*100,2) as avg_nb_declined,
  round(avg(approved)*100,2) as avg_nb_approved

FROM 
  {{ ref('mart_students') }}
GROUP BY 
  jt_country,
  jt_school_category,
  career_center_type