SELECT
  date_ AS "time",
  tfidf AS "aws",
  term
FROM leaderboard
WHERE
  date_ BETWEEN '2021-04-09T08:08:54.755Z' AND '2021-05-09T08:08:54.756Z' AND
  term = 'aws'
ORDER BY 1,2
