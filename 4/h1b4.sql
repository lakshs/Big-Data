use h1b;

SELECT employer_name,COUNT(case_status) AS total_petition from h1b_final where year='2011' GROUP BY employer_name ORDER BY total_petition desc limit 5;

SELECT employer_name,COUNT(case_status) AS total_petition from h1b_final where year='2012' GROUP BY employer_name ORDER BY total_petition desc limit 5;

SELECT employer_name,COUNT(case_status) AS total_petition from h1b_final where year='2013' GROUP BY employer_name ORDER BY total_petition desc limit 5;

SELECT employer_name,COUNT(case_status) AS total_petition from h1b_final where year='2014' GROUP BY employer_name ORDER BY total_petition desc limit 5;

SELECT employer_name,COUNT(case_status) AS total_petition from h1b_final where year='2015' GROUP BY employer_name ORDER BY total_petition desc limit 5;

SELECT employer_name,COUNT(case_status) AS total_petition from h1b_final where year='2016' GROUP BY employer_name ORDER BY total_petition desc limit 5;



