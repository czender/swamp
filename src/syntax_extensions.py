#!/usr/bin/python
from pyparsing import *
import shlex
import subprocess
import os

class BacktickParser:

    # disallow pipe and other shell operators if using implicit shell exec
    #backtickContents = CharsNotIn("`;><&|").setResultsName("extExpr")

    # while not using shell, okay to pass metachars directly to process
    backtickContents = CharsNotIn("`").setResultsName("extExpr")

    backtickString = Literal("`") + backtickContents + Literal("`")
#    backtickString.setParseAction(lambda s,loc,toks:"harhar %s"%s)

    def __init__(self):
        self.safeBinaries = {
            "seq" : "/usr/bin/seq",
            "printf" : "/usr/bin/printf"
            }
        pass

    def safeToExecute(self, expr):
        lexed = shlex.split(expr)
        print expr,"-->",lexed
        #print "lexed",lexed
        if expr and  lexed[0] in self.safeBinaries:
            return lexed
        # if not in, returns None--> false

    def execute(self, lexed):
        print "hey, executing: ",lexed
        fixed = [self.safeBinaries[lexed[0]]] + lexed[1:]
        output = subprocess.Popen(fixed, stdout=subprocess.PIPE).communicate()[0]
        #print lexed,"returned",output,"and",output.split()
        return filter(lambda t: t, output.split(os.linesep))


    def evaluate(self, aString):
        try:
            results = self.backtickString.parseString(aString)
        except ParseException,e:
            print "parse error"
            return None
        ok =  self.safeToExecute(results.extExpr)
        if not ok:
            return None
        else:
            return self.execute(ok)

    def transform(self, s,loc,toks):
        print "transforming ",s,loc,toks
        
        lex = self.safeToExecute(toks[1])
        if lex:
            return self.execute(lex)
        return None
        #        self.backtickString.transformString(s)
        
    def evaluateTest(self, aString):
        out = self.evaluate(aString)
        if not out:
            print "parse or safety error in", aString
        else:
            print "ok:", aString, "gave", out
        return out
        

problems = [
    "`seq 1 5`",
    "`rm 1 5`",
    "`printf \"%02d\" ${nbr}`",
    "if [ -z \"$1\" ]; then",
    "mkdir -p ${drc_out}",
    "for ssn in DJF MAM JJA SON ; do",
    "  for yy in `seq ${START_YR} ${END_YR}`; do",
    "    YYYY=`printf \"%04d\" ${yy}`",
    "mv ${drc_in}${CASEID}_${YYYY}0101_${YYYY}_1800.nc ${YYYY}_yr_ts.nc",
    "echo \"Creating Duration Time Series.............\"",
    "    nbr=`printf \"%02d\" ${nbr}`",
    "if [ $? -ne 0 ]; then",
    "if [ $MM -le 11 ]; then",
    "for yr in `seq ${START_YR} ${END_YR}`; do",
    "if [ $? -eq 0 ]; then",
    "for nbr in {01..04}; do",
    "exit 0"
    ]

def testBacktickParser():
    bp = BacktickParser()
    #map(lambda n: bp.evaluate(problems[n]), [0,1,2,3])
    map(bp.evaluateTest, problems)

testBacktickParser()
