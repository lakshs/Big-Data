--8) Find the average Prevailing Wage for each Job for each Year (take part time and full time separate). Arrange the output in descending order - [Certified and Certified Withdrawn.]
h1b = load '/home/hduser/Desktop/h1bproject/h1bdata' using PigStorage('\t') as (s_no:int,case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray, full_time_position:chararray,prevailing_wage:int,year:chararray, worksite:chararray, longitute:double, latitute:double); 

h1b1 = filter h1b by $1 != 'CASE_STATUS';
h1b2 = filter h1b1 by ($1 == 'CERTIFIED'); -- AND ($1 == 'CERTIFIED-WITHDRAWN') ;
h1b2a = filter h1b1 by ($1 == 'CERTIFIED-WITHDRAWN');
h1b2a1 = union h1b2,h1b2a;


h1b3 = filter h1b2a1 by $5 == '$time';


h1b_required = foreach h1b3 generate $4,$7,$6,$5;

h1b_2011 = filter h1b_required by $1=='$year';

h1b_group1 = group h1b_2011 by ($0,$1,$3);

h1b_count1 =  foreach h1b_group1 generate ROUND_TO(AVG(h1b_2011.$2),2),group;

h1b_order_y = limit (order h1b_count1 by $0 desc) 5;

--dump h1b_order_y;

store h1b_order_y into '/home/hduser/Desktop/h1bproject/projectout/8a';

