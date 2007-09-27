# $Id: server.py 36 2007-09-26 01:18:06Z daniel2196 $

"""
syntax - contains syntax extensions to the parser,
         notably subprocess evaluation of backtick expressions.
"""
# Copyright (c) 2007 Daniel Wang
# This file is part of SWAMP.
# SWAMP is released under the GNU General Public License version 3 (GPLv3)

# Std. Python imports

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
        if expr and  lexed[0] in self.safeBinaries:
            return lexed
        # if not in, returns None--> false

    def execute(self, lexed):
        fixed = [self.safeBinaries[lexed[0]]] + lexed[1:]
        output = subprocess.Popen(fixed, stdout=subprocess.PIPE).communicate()[0]
        f = filter(lambda t: t, output.split(os.linesep))
        return f


    def evaluate(self, aString):
        try:
            results = self.backtickString.parseString(aString)
        except ParseException,e:
            print "parse error" ## FIXME: convert to log debug
            return None
        ok =  self.safeToExecute(results.extExpr)
        if not ok:
            return None
        else:
            return self.execute(ok)

    def transform(self, s,loc,toks):
        lex = self.safeToExecute(toks[1])
        if lex:
            return self.execute(lex)
        return None
        #        self.backtickString.transformString(s)

    def quoteIfNeeded(self, aStr):
        # for now, only look for whitespace to decide on quoting
        # if there is whitespace in aStr...
        if reduce(lambda x,y: x or y.isspace(), aStr, False):
            # for now, just add single quotes.
            if "'" in aStr:
                raise StandardError("Quoting strings with quotes not implemented")
            return "'" + aStr + "'"
        return aStr
        
    def transformQuoted(self, s,loc,toks):
        transformed = self.transform(s, loc, toks)
        return " ".join(map(self.quoteIfNeeded, transformed))

        
    def evaluateTest(self, aString):
        out = self.evaluate(aString)
        if not out:
            print "parse or safety error in", aString
        else:
            print "ok:", aString, "gave", out
        return out
        

class SyntaxSelfTest:
    def __init__(self):
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

    def testBacktickParser(self):
        bp = BacktickParser()
        #map(lambda n: bp.evaluate(problems[n]), [0,1,2,3])
        map(bp.evaluateTest, problems)

if __name__ == '__main__':
    t = SyntaxSelfTest()
    t.testBacktickParser()
