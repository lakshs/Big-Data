-- Create a bar graph to depict the number of applications for each year

use h1b;

--INSERT OVERWRITE LOCAL DIRECTORY '/home/hduser/h1bproject/graph/h1b7.dat' 
select a.year,count(a.year) as no_of_applications from h1b_final a where a.year is not NULL group by a.year order by a.year;
