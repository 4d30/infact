SELECT
    jobmap.jk,
    cosine.cosine,
    pub_dates.pub_date,
    jobmap.title,
    jobmap.cmp,
    jobmap.loc,
    status.status
FROM jobmap
INNER JOIN pub_dates ON
    jobmap.jk = pub_dates.jk
INNER JOIN cosine ON
    jobmap.jk = cosine.jk
LEFT JOIN status ON
    jobmap.jk = status.jk
WHERE pub_dates.pub_date > CURRENT_DATE - 7
--    AND jobmap.title NOT LIKE '%Senior%'
--    AND jobmap.title NOT LIKE '%Sr%'
--    AND jobmap.title LIKE '%Data Scientist%'
    AND jobmap.cmp = 'PAREXEL'
    AND status.status is NULL
ORDER BY 2 desc;
