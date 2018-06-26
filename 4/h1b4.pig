--Which top 5 employers file the most petitions each year? - Case Status - ALL

data = load '/home/hduser/h1b/' using PigStorage('\t') as (s_no: int,case_status: chararray, employer_name: chararray, soc_name: chararray, job_title: chararray, full_time_position: chararray,prevailing_wage: int,year: chararray, worksite: chararray, longitude: double, latitute: double);
noheader = filter data by $0 > 1;
data = order noheader by $0;
data = foreach data generate $1,$2,$7;
data2011 = filter data by ($2 matches '2011');
data2012 = filter data by ($2 matches '2012');
data2013 = filter data by ($2 matches '2013');
data2014 = filter data by ($2 matches '2014');
data2015 = filter data by ($2 matches '2015');
data2016 = filter data by ($2 matches '2016');

groupdata2011 = group data2011 by ($1,$2);
groupdata2012 = group data2012 by ($1,$2);
groupdata2013 = group data2013 by ($1,$2);
groupdata2014 = group data2014 by ($1,$2);
groupdata2015 = group data2015 by ($1,$2);
groupdata2016 = group data2016 by ($1,$2);


data2011 = foreach groupdata2011 generate Flatten(group),COUNT(data2011.$0);
data2012 = foreach groupdata2012 generate FLATTEN(group),COUNT(data2012.$0);
data2013 = foreach groupdata2013 generate FLATTEN(group),COUNT(data2013.$0);
data2014 = foreach groupdata2014 generate FLATTEN(group),COUNT(data2014.$0);
data2015 = foreach groupdata2015 generate FLATTEN(group),COUNT(data2015.$0);
data2016 = foreach groupdata2016 generate FLATTEN(group),COUNT(data2016.$0);

dataorderd2011 = order data2011 by $2 desc;
dataorderd2012 = order data2012 by $2 desc;
dataorderd2013 = order data2013 by $2 desc;
dataorderd2014 = order data2014 by $2 desc;
dataorderd2015 = order data2015 by $2 desc;
dataorderd2016 = order data2016 by $2 desc;

top5_2011 = limit dataorderd2011 5;
top5_2012 = limit dataorderd2012 5;
top5_2013 = limit dataorderd2013 5;
top5_2014 = limit dataorderd2014 5;
top5_2015 = limit dataorderd2015 5;
top5_2016 = limit dataorderd2016 5;

uniondata = union top5_2011,top5_2012,top5_2013,top5_2014,top5_2015,top5_2016;
uniondata = order uniondata by $1;

store uniondata into '/home/hduser/projout/4';
dump uniondata;







