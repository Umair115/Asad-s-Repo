set ylabel "Node Degree Distribution" 
set xlabel "Node degree" 
set terminal postscript eps color "Helvetica" 22 
set output "../../data/caida/caida_metrics/20050101/degree_distr.ps" 
set key bottom right
set logscale
plot '../../data/caida/caida_metrics/20050101/out.deg' using 1:2 title 'Data' with linespoints
