--8) Find the average Prevailing Wage for each Job for each Year (take part time and full time separate). Arrange the output in descending order - [Certified and Certified Withdrawn.]
h1b = load '/home/hduser/Desktop/h1bproject/h1bdata' using PigStorage('\t') as (s_no:int,case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray, full_time_position:chararray,prevailing_wage:int,year:chararray, worksite:chararray, longitute:double, latitute:double); 

h1b1 = filter h1b by $1 != 'CASE_STATUS';
h1b2 = filter h1b1 by ($1 == 'CERTIFIED'); -- AND ($1 == 'CERTIFIED-WITHDRAWN') ;
h1b2a = filter h1b1 by ($1 == 'CERTIFIED-WITHDRAWN');
h1b2a1 = union h1b2,h1b2a;
--dump h1b2a1;
-- for fulltime
h1b3 = filter h1b2a1 by $5 == '$time';
--dump h1b3;

h1b_required = foreach h1b3 generate $4,$7,$6,$5;
h1b_2011 = filter h1b_required by $1=='2011';
h1b_2012 = filter h1b_required by $1=='2012';
h1b_2013 = filter h1b_required by $1=='2013';
h1b_2014 = filter h1b_required by $1=='2014';
h1b_2015 = filter h1b_required by $1=='2015';
h1b_2016 = filter h1b_required by $1=='2016';

--dump h1b_2011;

h1b_group1 = group h1b_2011 by ($0,$1,$3);
h1b_group2 = group h1b_2012 by ($0,$1,$3);
h1b_group3 = group h1b_2013 by ($0,$1,$3);
h1b_group4 = group h1b_2014 by ($0,$1,$3);
h1b_group5 = group h1b_2015 by ($0,$1,$3);
h1b_group6 = group h1b_2016 by ($0,$1,$3);

h1b_count1 =  limit (order(foreach h1b_group1 generate ROUND_TO(AVG(h1b_2011.$2),2),group) by $0 desc) 5;
h1b_count2 =  limit (order(foreach h1b_group2 generate ROUND_TO(AVG(h1b_2012.$2),2),group) by $0 desc) 5;
h1b_count3 =  limit (order(foreach h1b_group3 generate ROUND_TO(AVG(h1b_2013.$2),2),group) by $0 desc) 5;
h1b_count4 =  limit (order(foreach h1b_group4 generate ROUND_TO(AVG(h1b_2014.$2),2),group) by $0 desc) 5;
h1b_count5 =  limit (order(foreach h1b_group5 generate ROUND_TO(AVG(h1b_2015.$2),2),group) by $0 desc) 5;
h1b_count6 =  limit (order(foreach h1b_group6 generate ROUND_TO(AVG(h1b_2016.$2),2),group) by $0 desc) 5;

--h1b_check = order h1b_count6 by $0 asc;
--dump h1b_count6;
--store h1b_count6 into '/home/hduser/8out' using PigStorage(','); 
--dump h1b_count1;
--h1b_order_y = order h1b_count1 by $0 desc;

h1b_union_y = UNION h1b_count1,h1b_count2,h1b_count3,h1b_count4,h1b_count5,h1b_count6;
--h1b_order_y = order h1b_union_y by $0 desc;
--describe h1b_union_y;
--dump h1b_union_y;
store h1b_union_y into '/home/hduser/Desktop/h1bproject/projectout/8';
