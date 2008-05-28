# $Id: $
"""
tester of my code.

"""

# 
# Copyright (c) 2007 Daniel Wang, Charles S. Zender
# This sample file is part of SWAMP.
# SWAMP is released under the GNU General Public License version 3 (GPLv3)
#
import swamp.parser

class Tester:
    def __init__(self):
        self.parser = swamp.parser.Parser()
        self.sample = """
X=x
NUM=10
let TWENTY=NUM+10
let TWO=$TWENTY/10
let THIRTY=TWENTY+10

ncwa $X.$TWENTY.$TWO shouldbe.x.20.2
ncwa $THIRTY.x shouldbe.30.x

"""
        pass

    def run(self):
        self.varParseTest()
        self.testExpansion()
        self.testGrammar()
        pass

    def varParseTest(self):

        vp = swamp.parser.VariableParser({})
        vp.varMap = {"sub" : "<SUB>", "sub3" : "-SUB3-", "sub2" : "Two"}
        test1 = "This is a simple $sub test with Joe$sub3 and ${sub2}blah"
        test2 = "export x=adf"
        test3 = "Now x should be substituted, so x = $x or ${x}"
        test4 = "CASEID=$1"
        test5 = 'John="$CASEID1 double quote"'
        test6 = "Josiah='$CASEID1 single quote'"
        test7 = 'Jerry=asdf"blahblah"'
        test8 = "Junior=asdf'blahblah'"
        test9 = "let Y1=0000+1"
        
        print vp.varSub(test1)
        print vp.varMap    
        print vp.varDef(test2)
        print vp.varMap
        print vp.varSub(test3)
        print vp.varDef(test4)
        print vp.varLet(test9)
        print vp.varMap

    def testGrammar(self):
        ranges = ["1 2 3 4 5", "{1..5}", "a b c", "{a..c}", "c b a", "{c..a}"]
        for s in ranges:
            result = swamp.parser.Common.Range.parseString(s)
            print s, "---", result
    def testExpansion(self):
        class DummyFactory:
            def newCommand(self, cmd, argtriple, inouts, refnum):
                print "new command",cmd
                print "adict=",argtriple[0],
                print "alist=",argtriple[1],
                print "leftover=",argtriple[2],
                print "inouts", inouts,
                print "refnum=", refnum
                pass

            pass
        factory = DummyFactory()
        self.parser.parseScript(self.sample, factory)

if __name__ == '__main__':
    t = Tester()
    t.run()


