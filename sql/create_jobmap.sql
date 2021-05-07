/*DROP TABLE jobmap;*/
CREATE TABLE jobmap(
	jk char(16) PRIMARY KEY,
	efccid char(16),
	srcid char(16),
	cmpid char(16),
	srcname varchar(100),
	cmp varchar(75),
	cmpesc varchar(75),
	cmplnk varchar(100),
	loc varchar(50),
	country varchar(20),
	zip char(5),
	city char(16),
	title varchar(150),
	locid char(16),
	rd varchar(64));
