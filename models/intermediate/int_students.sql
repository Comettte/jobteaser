SELECT optin.*,jt_country,jt_school_category, career_center_type
from {{ ref("int_optin_ranked") }} AS optin
LEFT JOIN {{ ref("int_school") }} AS school
ON optin.school_id = school.school_id