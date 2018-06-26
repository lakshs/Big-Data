
--8) Find the average Prevailing Wage for each Job for each Year (take part time and full time separate). Arrange the output in descending order.
use h1b;
select job_title,case_status,full_time_position,year,avg(prevailing_wage) as average from h1b_final where full_time_position = 'Y' and year = '2011' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,case_status,full_time_position,year order by average desc;

--select job_title,case_status,full_time_position,year,avg(prevailing_wage) as average from h1b_final where full_time_position = 'N' and year = '2011' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,case_status,full_time_position,year order by average desc;


select job_title,case_status,full_time_position,year,avg(prevailing_wage) as average from h1b_final where full_time_position = 'Y' and year = '2012' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,case_status,full_time_position,year order by average desc;

--select job_title,case_status,full_time_position,year,avg(prevailing_wage) as average from h1b_final where full_time_position = 'N' and year = '2012' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,case_status,full_time_position,year order by average desc;


select job_title,case_status,full_time_position,year,avg(prevailing_wage) as average from h1b_final where full_time_position = 'Y' and year = '2013' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,case_status,full_time_position,year order by average desc;

--select job_title,case_status,full_time_position,year,avg(prevailing_wage) as average from h1b_final where full_time_position = 'N' and year = '2013' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,case_status,full_time_position,year order by average desc;

select job_title,case_status,full_time_position,year,avg(prevailing_wage) as average from h1b_final where full_time_position = 'Y' and year = '2014' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,case_status,full_time_position,year order by average desc;

--select job_title,case_status,full_time_position,year,avg(prevailing_wage) as average from h1b_final where full_time_position = 'N' and year = '2014' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,case_status,full_time_position,year order by average desc;


select job_title,case_status,full_time_position,year,avg(prevailing_wage) as average from h1b_final where full_time_position = 'Y' and year = '2015' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,case_status,full_time_position,year order by average desc;

--select job_title,case_status,full_time_position,year,avg(prevailing_wage) as average from h1b_final where full_time_position = 'N' and year = '2015' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,case_status,full_time_position,year order by average desc;


select job_title,case_status,full_time_position,year,avg(prevailing_wage) as average from h1b_final where full_time_position = 'Y' and year = '2016' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,case_status,full_time_position,year order by average desc;

--select job_title,case_status,full_time_position,year,avg(prevailing_wage) as average from h1b_final where full_time_position = 'N' and year = '2016' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,case_status,full_time_position,year order by average desc;


