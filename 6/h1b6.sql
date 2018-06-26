--Find the percentage and the count of each case status on total applications for each year. Create a graph depicting the pattern of All the cases over the period of time.

use h1b;

--create table total
--create table if not exists total(total int,year string)
--row format delimited 
--fields terminated by ',';

--insert overwrite table total select count(*),year from h1b_final where h1b_final.case_status is not NULL group by year;



select a.case_status,count(*) as case_total,a.year,ROUND((count(*)/b.total)*100,2) as perOfCase_status from h1b_final a left outer join total b on (a.year=b.year) where a.year is not NULL group by a.case_status,b.total,a.year order by a.year;
