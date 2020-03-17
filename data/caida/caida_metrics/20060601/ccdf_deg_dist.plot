set ylabel "CCDF" 
set xlabel "Node degree" 
set terminal postscript eps color "Helvetica" 22 
set output "../../data/caida/caida_metrics/20060601/ccdf_deg.ps" 
set key bottom right
set logscale
plot '../../data/caida/caida_metrics/20060601/deg.ccdf' using 2:1 with linespoints
