data = LOAD '/home/hduser/h1b' USING PigStorage('\t') as 
(s_no:int,case_status,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:int,year:chararray,worksite:chararray,longitude:double,latitude:double);



allstatus= filter data by ($1 != 'CERTIFIED' OR $1 != 'CERTIFIED-WITHDRAWN') ;

temp= group allstatus by $2;
total= foreach temp generate group,COUNT(allstatus.$1);

--dump total;

certified= filter data by $1 == 'CERTIFIED';
temp1= group certified by $2;
totalcertified= foreach temp1 generate group,COUNT(certified.$1);

--dump totalcertified;
certifiedwithdrawn= filter data by $1 == 'CERTIFIED-WITHDRAWN';
temp2= group certifiedwithdrawn by $2;
totalcertifiedwithdrawn= foreach temp2 generate group,COUNT(certifiedwithdrawn.$1);



joined= join totalcertified by $0,totalcertifiedwithdrawn by $0,total by $0;

joined2= foreach joined generate $0,$1,$3,$5;

--describe joined2;

intermediateoutput= foreach joined2 generate $0,((float)($1+$2)/($3))*100,$3;

intermediateoutput2= filter intermediateoutput by $1>70 and $2>1000;
	
finaloutput= order intermediateoutput2 by $1 DESC;

dump finaloutput;

