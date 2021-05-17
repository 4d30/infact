/* DROP TABLE leaderboard;*/ 
CREATE TABLE leaderboard (
	term 	text,
	date_	date,
	ndocs	integer,
	tfidf	decimal,
	PRIMARY KEY (term, date_));
