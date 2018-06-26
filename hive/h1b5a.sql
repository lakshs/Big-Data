--5a) Find the most popular top 10 job positions for H1B visa applications for each year?

use h1b;

insert overwrite local directory '/home/hduser/h1bproject/projectout/5a1' row format delimited FIELDS TERMINATED BY '\t' select job_title,year,count(case_status) as temp from h1b_final where year= '{hiveconf:year}' group by job_title,year order by temp desc limit 10;
