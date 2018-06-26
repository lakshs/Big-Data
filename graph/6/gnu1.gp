#!/bin/gnuplot
set grid
set title 'Case Status on total applications for each year '
set yrange [0:100]
set xrange [2011:2017]
set xlabel 'Year'
set ylabel 'Percentage'
#set label 'H1b-Applications' at 15, 140
#unset label
#set label 'finished walk' at 15, 105
plot "/home/hduser/Desktop/h1bproject/graph/6/data/filtcer/part-r-00000" u 1:5 w lp t 'CERTIFIED' lt rgb "#8B0000" lw 3 pt 6,\
     "/home/hduser/Desktop/h1bproject/graph/6/data/filtcerwith/part-r-00000" u 1:5 w lp t 'CERTIFIED-WITHDRAWN' lt rgb "#00008B" lw 3 pt 6,\
     "/home/hduser/Desktop/h1bproject/graph/6/data/filtden/part-r-00000" u 1:5 w lp t 'DENIED' lt rgb "#808000" lw 3 pt 6,\
     "/home/hduser/Desktop/h1bproject/graph/6/data/filtwith/part-r-00000" u 1:5 w lp t 'WITHDRAWN' lt rgb "#00FF00" lw 3 pt 6
pause 5
set terminal jpeg
set output '/home/hduser/Desktop/h1bproject/graph/6/h1b6graph.jpeg'
replot
exit gnuplot
