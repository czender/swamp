# $Header: /cvsroot/nco/nco/data/swamp.sh,v 1.4 2007/07/03 02:51:35 zender Exp $

# Purpose: Demonstrate SWAMP usage

# Usage:
# Using SWAMP requires two steps
# First, identify the SWAMP server with access to your data, e.g.:
# export SWAMPURL='http://sand.ess.uci.edu:8081/SOAP'
# export SWAMPURL='http://pbs.ess.uci.edu:8081/SOAP'
# Second, call your script as an argument to SWAMP's invocation:
# python ~/nco/src/ssdap/swamp_client.py ~/nco/data/swamp.sh
# Whitespace-separated list of directories in sand:${DATA}/swamp_include is exported
# to PBS (pbs.ess.uci.edu) for processing by SWAMP server there.


flg_tst='1' # [flg] Test mode
flg_prd='0' # [flg] Production mode

flg_typ=${flg_prd} # [enm] Run type

if [ "${flg_typ}" = "${flg_tst}" ] ; then
    ncra -O ~/nco/data/in.nc ~/foo.nc
elif [ "${flg_typ}" = "${flg_prd}" ] ; then 
    if [ "$SWAMPHOSTNAME" = "pbs.calit2.uci.edu" ] ; then
        caseid=cam_eul_dom_T85_4pdf_non_phys
        prefix="/esmf/scapps/${caseid}/${caseid}.cam2.h0"
    else
        export caseid='cssnc2050_02b'
        prefix="/data/zender/${caseid}/${caseid}.cam2.h0"
    fi

    # You can mix literal values with backtick expressions.
    for yr in 1 2 3 4 `seq 5 12` ; do
        yy=`printf "%02d" $yr`
        ncra -O ${prefix}.????-${yy}.nc ~/${caseid}_clm${yy}.nc
    done
    ncra -O ~/${caseid}_clm??.nc ~/${caseid}_clm.nc
fi # !prd


