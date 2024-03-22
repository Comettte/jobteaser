SELECT
  GEO_DESC AS country,
  TIME_PERIOD AS year,
  ROUND(SUM(OBS_VALUE)) AS nb_students, 
  FROM {{ ref('stg_raw__europe_data') }}
   WHERE
    GEO_DESC <> 'European Union' AND SEX = 'T' AND TIME_PERIOD =2018
  GROUP BY GEO_DESC, TIME_PERIOD, SEX