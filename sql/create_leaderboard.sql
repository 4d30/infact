DROP TABLE leaderboard;
CREATE TABLE leaderboard (
	term 	varchar(100),
	date_	date,
	tfidf	decimal,
	PRIMARY KEY (term, date_));
