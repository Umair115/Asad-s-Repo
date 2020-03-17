set ylabel "CCDF" 
set xlabel "Average Neighbor degree" 
set terminal postscript eps color "Helvetica" 22 
set output "../../data/caida/caida_metrics/20150101-wixp/ccdf_avg_nbr.ps" 
set key bottom right
set logscale
plot '../../data/caida/caida_metrics/20150101-wixp/nbr.ccdf' using 1:2 with linespoints
