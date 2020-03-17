set ylabel "CCDF" 
set xlabel "Average Neighbor degree" 
set terminal postscript eps color "Helvetica" 22 
set output "../../data/caida/caida_metrics/20140101/ccdf_avg_nbr.ps" 
set key bottom right
set logscale
plot '../../data/caida/caida_metrics/20140101/nbr.ccdf' using 1:2 with linespoints