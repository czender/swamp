# $Id$
# 
# Purpose: Provide an example of script syntax which either 
# "should work" or is "targeted to work"
# 
# Copyright 2007 Daniel Wang, released under the GNU General Public License v3 (GPLv3)

export caseid='cssnc2050_02b'

tst='1' # [flg] Test mode
prd='0' # [flg] Production mode

typ=$prd

sequence=`seq 10 15`

ncwa $sequence
if [ "${typ}" = "${tst}" ] ; then
    ncra -O ~/nco/data/in.nc ~/foo.nc
elif [ "${typ}" = "${prd}" ] ; then 
    for yra in `seq 1 12` ; do
        yr=`printf "%02d" ${yra}`

        ncra -O ${yr}.nc oo${yr}.nc
    done
fi # !prd


