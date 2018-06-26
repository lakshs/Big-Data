#!/bin/gnuplot
plot "part-r-00000.dat" using 1:5 title "Total" lt rgb "#406090",\
     "" using 3 title "From web" lt rgb "#40FF00",\
     "" using 4 title "denied" lt rgb "#40EE00"
