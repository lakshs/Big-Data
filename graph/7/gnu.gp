#!/bin/gnuplot
set style line 1 lc rgb 'grey30' ps 0 lt 1 lw 2
set style line 2 lc rgb 'grey70' lt 1 lw 2
set style fill solid 1.0 border rgb 'grey30'
plot '/home/hduser/Desktop/h1bproject/graph/7/h1b7.dat' every ::1 u 0:2:(0.7):xtic(1) w boxes
pause 5
#set terminal png size 400,300
#set output '/home/hduser/Desktop/h1bproject/graph/h1b7graph.png'
set terminal jpeg
set output '/home/hduser/Desktop/h1bproject/graph/7/h1b7graph.jpeg'
replot
exit gnuplot
