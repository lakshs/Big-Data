--find top 5 locations in the US who have got certified visa for each year

L1 = load '/home/hduser/h1b' using PigStorage('\t') as (s_no: int,case_status: chararray, employer_name: chararray, soc_name: chararray, job_title: chararray, full_time_position: chararray,prevailing_wage: int,year: chararray, worksite: chararray, longitude: double, latitute: double);

group1 = foreach L1 generate $8, $1, $7;
group2 = filter group1 by $1 == 'CERTIFIED';
--dump group2;


group3_2011 = filter group2 by $2 =='2011';
group3_2012 = filter group2 by $2 =='2012';
group3_2013 = filter group2 by $2 =='2013';
group3_2014 = filter group2 by $2 =='2014';
group3_2015 = filter group2 by $2 =='2015';
group3_2016 = filter group2 by $2 =='2016';


group4_2011 = group group3_2011 by ($0,$1,$2);
--dump group4_2011;
group4_2012 = group group3_2012 by ($0,$1,$2);
group4_2013 = group group3_2013 by ($0,$1,$2);
group4_2014 = group group3_2014 by ($0,$1,$2);
group4_2015 = group group3_2015 by ($0,$1,$2);
group4_2016 = group group3_2016 by ($0,$1,$2);

group5_2011 = foreach group4_2011 generate group, COUNT(group3_2011.$1);
--dump group5_2011;
group5_2012 = foreach group4_2012 generate group, COUNT(group3_2012.$1);
group5_2013 = foreach group4_2013 generate group, COUNT(group3_2013.$1);
group5_2014 = foreach group4_2014 generate group, COUNT(group3_2014.$1);
group5_2015 = foreach group4_2015 generate group, COUNT(group3_2015.$1);
group5_2016 = foreach group4_2016 generate group, COUNT(group3_2016.$1);


group_desc2011 = order group5_2011 by $1 desc;
--dump group_desc2011;
group_desc2012 = order group5_2012 by $1 desc;
group_desc2013 = order group5_2013 by $1 desc;
group_desc2014 = order group5_2014 by $1 desc;
group_desc2015 = order group5_2015 by $1 desc;
group_desc2016 = order group5_2016 by $1 desc;

group_limit1 = limit group_desc2011 5;
group_limit2 = limit group_desc2012 5;
group_limit3 = limit group_desc2013 5;
group_limit4 = limit group_desc2014 5;
group_limit5 = limit group_desc2015 5;
group_limit6 = limit group_desc2016 5;

group_ans = UNION group_limit1, group_limit2, group_limit3, group_limit4, group_limit5, group_limit6;



store group_ans into '/home/hduser/projout/2b';

dump group_ans;
