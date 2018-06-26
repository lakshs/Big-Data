use h1b;

--select case_status,job_title,year from(select rank() over(partition by year order by TJob desc)as rank_1,case_status,job_title,year from(select count(job_title)as TJob,case_status,job_title,year from h1b_final group by case_status,job_title,year)a)b where rank_1<=10 and year!= 'YEAR' and year is not null and case_status== 'CERTIFIED';

--For All Applications
select job_title,year,count(case_status ) as temp from h1b_final where year= '2011' group by job_title,year order by temp desc limit 10;

--For Certified Applications
select job_title,case_status,year,count(case_status ) as temp from h1b_final where year= '2011' and case_status like 'CERTIFIED' group by job_title,case_status,year order by temp desc limit 10;
