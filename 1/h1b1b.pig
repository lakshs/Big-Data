data = LOAD '/home/hduser/h1b/' USING PigStorage('\t') as 
(s_no:int,
case_status:chararray,
employer_name:chararray,
soc_name:chararray,
job_title:chararray,
full_time_position:chararray,
prevailing_wage:int,
year:chararray,
worksite:chararray,
longitute:double,
latitute:double);			
noheader= filter data by $0>=1;		 

table1= filter noheader  by $7 matches '2011'; 
--dump table1;
a= group table1 by $4;								
count1= foreach a generate group,COUNT($1);				


table1= filter noheader  by $7 matches '2012'; 
a= group table1 by $4;								
count2= foreach a generate group,COUNT($1);				


table1= filter noheader  by $7 matches '2013';
a= group table1 by $4;								
count3= foreach a generate group,COUNT($1);				


table1= filter noheader  by $7 matches '2014'; 
a= group table1 by $4;								
count4= foreach a generate group,COUNT($1);				

table1= filter noheader  by $7 matches '2015';
a= group table1 by $4;								
count5= foreach a generate group,COUNT($1);				


table1= filter noheader  by $7 matches '2016'; 
a= group table1 by $4;								
count6= foreach a generate group,COUNT($1);				


joined= join count1 by $0,count2 by $0,count3 by $0,count4 by $0,count5 by $0,count6 by $0;
yearwiseapplications= foreach joined generate $0,$1,$3,$5,$7,$9,$11;

--describe yearwiseapplications;
--dump yearwiseapplications;
--avg growth formula -> 

growth= foreach yearwiseapplications  generate $0,
(float)($6-$5)*100/$5,(float)($5-$4)*100/$4,
(float)($4-$3)*100/$3,(float)($3-$2)*100/$2,
(float)($2-$1)*100/$1;


avggrowth= foreach growth generate $0,ROUND_TO(($1+$2+$3+$4+$5)/5,2);

orderedavggrowth= order avggrowth by $1 desc;

answer = limit orderedavggrowth 5;
--dump answer;

store answer into '/home/hduser/projectout/1b';

dump answer;
