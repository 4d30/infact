SELECT
    jobmap.cmp,
    count(jobmap.jk)
FROM jobmap
INNER JOIN pub_dates ON
    jobmap.jk = pub_dates.jk
WHERE pub_dates.pub_date >= CURRENT_DATE - 0
GROUP BY jobmap.cmp
ORDER BY 2 desc
LIMIT 10

