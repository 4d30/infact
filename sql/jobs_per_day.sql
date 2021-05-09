SELECT
  pub_date AS "time",
  count(jobmap.jk) AS "Listing Count"
FROM jobmap
INNER JOIN pub_dates ON 
pub_dates.jk = jobmap.jk
GROUP BY pub_dates.pub_date
ORDER BY 1
