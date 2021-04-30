COPY jobmap(
	ind,
	jk,
	efccid,
	srcid,
	cmpid,
	num,
	srcname,
	cmp,
	cmpesc,
	cmplnk,
	loc,
	country,
	zip,
	title,
	locid,
	rd,
	mtime)
FROM '/home/joey/github/infact/jobmap.csv'
DELIMITER ',' CSV;
