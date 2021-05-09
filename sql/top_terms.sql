SELECT  term,
		tfidf,
        time as time 
FROM( 
  SELECT	term,
            date_ as time,
         	tfidf as "tfidf",
            rn
  FROM
      ( SELECT term, date_, tfidf,
               ROW_NUMBER() OVER (PARTITION BY date_
                                   ORDER BY tfidf DESC
                                  )
       AS rn
       FROM leaderboard
       ) tmp
WHERE rn <= 1 /*and rn >=5*/
ORDER BY date_ desc, rn) tmp2 ;
