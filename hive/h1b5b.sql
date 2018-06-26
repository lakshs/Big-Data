--5b) Find the most popular top 10 certified job positions for H1B visa applications for each year?

use h1b;

insert overwrite local directory '/home/hduser/h1bproject/projectout/5b1' row format delimited 
FIELDS TERMINATED BY '\t' select job_title,case_status,year,count(case_status ) as temp from h1b_final where year= '${hiveconf:year}' and case_status like 'CERTIFIED' group by job_title,case_status,year order by temp desc limit 10;
