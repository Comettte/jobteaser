WITH
  
  agg_schools AS(
  SELECT
    school_id,
    COUNT(user_id) AS nb_users,
    SUM(active) AS active_users,
    SUM(resume_uploaded) AS resume_uploaded,
    SUM(nb_shortlists) AS nb_shortlists,
    SUM(not_interested) AS not_interested,
    SUM(interested) AS interested,
    SUM(approved) AS approved,
    SUM(declined) AS declined
  FROM {{ ref('mart_students') }} 
    GROUP BY school_id)
  
  SELECT 
  schools.*,
  IFNULL(nb_users,0) as nb_users,
  IFNULL(active_users,0) as active_users,
  IFNULL(agg_schools.resume_uploaded,0) resume_uploaded,
  IFNULL(nb_shortlists,0) as nb_shortlists,
  IFNULL(not_interested,0) as not_interested,
  IFNULL(interested,0) as interested,
  IFNULL(approved,0) as approved,
  IFNULL(declined,0) as declined
  FROM {{ ref('int_school') }} as schools
  LEFT Join agg_schools on 
  agg_schools.school_id = schools.school_id