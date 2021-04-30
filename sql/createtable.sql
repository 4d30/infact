DROP TABLE jobmap;
CREATE TABLE jobmap(
	col varchar(50),
	jk char(16) PRIMARY KEY,
	efccid char(18),
	srcid char(16),
	cmpid char(16),
	num int,
	srcname varchar(50),
	cmp varchar(75),
	cmpesc varchar(75),
	cmplnk varchar(100),
	loc varchar(50),
	country varchar(20),
	zip int,
	city char(16),
	title varchar(100),
	locid char(16),
	rd varchar(64),
	mtime decimal);

