DMD=~/dmd2-053/linux/bin32/dmd
rm *.io
rm *.log
rm pbus
$DMD -O @dfiles-linux
rm *.o
