--3) Which industry has the most number of Data Scientist positions?\n"
use h1b;

insert overwrite local directory '/home/hduser/h1bproject/projectout/3' row format delimited 
FIELDS TERMINATED BY '\t' select soc_name,case_status ,count(soc_name) as cnt from h1b_final where job_title like '%DATA SCIENTIST%' and case_status = 'CERTIFIED' group by soc_name,case_status order by cnt desc limit 1;
