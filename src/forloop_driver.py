import swamp_common
#from pyparsing import *
class FakeFactory:
    def newCommand(self, cmd, argtriple, inouts, referenceLineNum):
        print "fake newcmd:", cmd, argtriple, "inouts",inouts,"line:", referenceLineNum
a = swamp_common.Parser()
bc = swamp_common.Parser.BaseContext(a)
#a.parseScript(open("fortest.sh").read(), FakeFactory())


iftest = """
caseid="mycase"
flg_tst='1' # [flg] Test mode
flg_prd='0' # [flg] Production mode

flg_typ=${flg_prd} # [enm] Run type

if [ "${flg_typ}" = "${flg_tst}" ] ; then
    ncra -O ~/nco/data/in.nc ~/foo.nc
elif [ "${flg_typ}" = "${flg_prd}" ] ; then
    ncra -O /data/zender/${caseid}/${caseid}.cam2.h0.????-01.nc ~/${caseid}_clm01.nc
    ncra -O ~/${caseid}_clm??.nc ~/${caseid}_clm.nc
fi # !prd
ncra dummy.nc foo.nc
"""

fortest = """export caseid='cssnc2050_02b'
for i in 01 02 03 04 05 06 07 08 09 10 11 12 ; do
ncra -O /data/zender/${caseid}/${caseid}.cam2.h0.????-$i.nc ~/${caseid}_clm$i.nc
done
ncra -O ~/${caseid}_clm??.nc ~/${caseid}_clm.nc
"""

def test1():
    eee = swamp_common.Parser.BranchContext.Expr
    print eee.Expression.parseString('${flgtype}')
    #print eee.Dquoted.parseString('"hello"')

    print (Literal('"') + eee.Reference + Literal('"')).parseString('"$hello"')
    print (Literal('"') + OneOrMore(eee.Reference | eee.PlainString) + Literal('"')).parseString('"$hello"')

#swamp_common.Parser.BranchContext.Expr.IfHeading.parseString("""if [ "${flg_typ}" = "${flg_tst}" ] ; then""")

substituted = 'if [ "1" = "1" ] ; then'

#print swamp_common.Parser.BranchContext.Expr.IfHeading.parseString(substituted)
#a.parseScript(iftest, FakeFactory())
#a.parseScript(fortest, FakeFactory())
#a.parseScript(open("charlie_prod.sh").read(), FakeFactory())
a.parseScript(open("illustr.sh").read(), FakeFactory())
print a._variables
