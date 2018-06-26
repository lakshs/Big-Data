--Which part of the US has the most Data Engineer jobs for each year?


--REGISTER /home/hduser/Downloads/piggybank.jar;
--DEFINE CSV_Storage org.apache.pig.piggybank.storage.CSVExcelStorage(); 
L1 = load '/home/hduser/h1b' using PigStorage('\t') as (s_no: int,case_status: chararray, employer_name: chararray, soc_name: chararray, job_title: chararray, full_time_position: chararray,prevailing_wage: int,year: chararray, worksite: chararray, longitude: double, latitute: double);



group1 = foreach L1 generate $4,$8,$7;
group2 = FILTER group1 BY $0 == 'DATA ENGINEER';


group2011 = FILTER group2 BY $2 == '2011';
group2012 = FILTER group2 BY $2 == '2012';
group2013 = FILTER group2 BY $2 == '2013';
group2014 = FILTER group2 BY $2 == '2014';
group2015 = FILTER group2 BY $2 == '2015';
group2016 = FILTER group2 BY $2 == '2016';



group3_2011 = group group2011 by ($0,$1,$2);
group3_2012 = group group2012 by ($0,$1,$2);
group3_2013 = group group2013 by ($0,$1,$2);
group3_2014 = group group2014 by ($0,$1,$2);
group3_2015 = group group2015 by ($0,$1,$2);
group3_2016 = group group2016 by ($0,$1,$2);
--dump abc3;

group4_2011 = foreach group3_2011 generate group,COUNT(group2011.$0);
group4_2012 = foreach group3_2012 generate group,COUNT(group2012.$0);
group4_2013 = foreach group3_2013 generate group,COUNT(group2013.$0);
group4_2014 = foreach group3_2014 generate group,COUNT(group2014.$0);
group4_2015 = foreach group3_2015 generate group,COUNT(group2015.$0);
group4_2016 = foreach group3_2016 generate group,COUNT(group2016.$0);



g_2011 = order group4_2011 by $1 DESC;
g_2012 = order group4_2012 by $1 DESC;
g_2013 = order group4_2013 by $1 DESC;
g_2014 = order group4_2014 by $1 DESC;
g_2015 = order group4_2015 by $1 DESC;
g_2016 = order group4_2016 by $1 DESC;


ans_2011 = LIMIT g_2011 1;
ans_2012 = LIMIT g_2012 1;
ans_2013 = LIMIT g_2013 1;
ans_2014 = LIMIT g_2014 1;
ans_2015 = LIMIT g_2015 1;
ans_2016 = LIMIT g_2016 1;
--dump ans1;

final_ans = UNION ans_2011,ans_2012,ans_2013,ans_2014,ans_2015,ans_2016;
dump final_ans;

--store final_ans into '/home/hduser/projout2a';

