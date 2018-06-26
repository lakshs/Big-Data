use h1b;

select soc_name,case_status ,count(soc_name) as cnt from h1b_final where job_title like '%DATA SCIENTIST%' and case_status = 'CERTIFIED' group by soc_name,case_status order by cnt desc;
