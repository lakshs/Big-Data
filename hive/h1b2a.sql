
--2 a) Which part of the US has the most Data Engineer jobs for each year?
use h1b;

insert overwrite local directory '/home/hduser/h1bproject/projectout/2a2' row format delimited 
FIELDS TERMINATED BY '\t' SELECT worksite,COUNT(case_status) AS number_of_petition,year from h1b_final WHERE job_title LIKE '%DATA ENGINEER%' and case_status = 'CERTIFIED' and year= '${hiveconf:year}' GROUP BY worksite,year ORDER BY number_of_petition desc limit 1;

--SELECT worksite,COUNT(case_status) AS number_of_petition,year from h1b_final WHERE job_title LIKE '%DATA ENGINEER%' and case_status = 'CERTIFIED' and year='2012' GROUP BY worksite,year ORDER BY number_of_petition desc limit 1;

--SELECT worksite,COUNT(case_status) AS number_of_petition,year from h1b_final WHERE job_title LIKE '%DATA ENGINEER%' and case_status = 'CERTIFIED' and  year='2013' GROUP BY worksite,year ORDER BY number_of_petition desc limit 1;

--SELECT worksite,COUNT(case_status) AS number_of_petition,year from h1b_final WHERE job_title LIKE '%DATA ENGINEER%' and case_status = 'CERTIFIED' and year='2014' GROUP BY worksite,year ORDER BY number_of_petition desc limit 1;

--SELECT worksite,COUNT(case_status) AS number_of_petition,year from h1b_final WHERE job_title LIKE '%DATA ENGINEER%' and case_status = 'CERTIFIED' and year='2015' GROUP BY worksite,year ORDER BY number_of_petition desc limit 1;

--SELECT worksite,COUNT(case_status) AS number_of_petition,year from h1b_final WHERE job_title LIKE '%DATA ENGINEER%' and case_status = 'CERTIFIED' and year='2016' GROUP BY worksite,year ORDER BY number_of_petition desc limit 1;
