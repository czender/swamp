# $Id: server.py 36 2007-09-26 01:18:06Z daniel2196 $

"""
parser - contains general parsing code.


"""
# Copyright (c) 2007 Daniel L. Wang, Charles S. Zender
# This file is part of SWAMP.
# SWAMP is released under the GNU General Public License version 3 (GPLv3)

# Std. Python imports
import getopt
import os
import shlex
import types

# (semi-)third-party imports
from pyparsing import *


# SWAMP imports 
from swamp import log
from swamp.syntax import BacktickParser

class Common:
    Quoteclass = "[\"\']"
    Identifier = Word(alphas, alphanums+"_")
    UnpReference = Literal("$").suppress() + Identifier
    ProtReference = Literal("${").suppress() + Identifier + Literal("}").suppress()
    Sreference = ProtReference | UnpReference
    # should support double-quoted references
    Reference = Sreference

    Value = Word(alphanums+"_").setResultsName("realValue")
    CommandLine = OneOrMore(Word(alphanums + '_-'))
    
    BacktickExpr = BacktickParser.backtickString
    SubValue = BacktickExpr.copy().setResultsName("indValue")


    Range = OneOrMore(Value ^ SubValue ^ Reference).setResultsName("range")
    @staticmethod
    def p(t):
        print "rangetoks",t
        return t
    #Range.setParseAction(lambda s,l,t: [Common.p(t)])
    RangeAssignment = Literal('(').suppress() + Range + Literal(')').suppress()
    #ident = "[A-Za-z]\w*"
    #backtickString = Literal("`") + CharsNotIn("`") + Literal("`")

    pass


class VariableParser:
    """treats shell vars "a=b" and env vars "export a=b" as the same for now.

    Bourne shell grammar at:
    The Open Group Base Specifications Issue 6
    IEEE Std 1003.1, 2004 Edition
    http://www.opengroup.org/onlinepubs/009695399/toc.htm
    http://www.opengroup.org/onlinepubs/007904975/toc.htm
    """
    
    def __init__(self, varmap):
        self.varMap = varmap ## varMap is exposed/public.
        self.backtickp = BacktickParser()
        self.backtickString = self.backtickp.backtickString.copy()
        self.backtickString.setParseAction(self.backtickp.transform)

        def quoteIfNeeded(aStr):
            pass
        def flattenReference(tok):
            if isinstance(tok,str):
                return tok
            else:
                return " ".join(tok)
        def refParseAction(s, loc, toks):
            #print "s:",s,"loc",loc,"toks",toks
            return [flattenReference(self.varMap[t]) for t in toks]

        reference = Common.Reference.copy()
        # lookup each substitution reference in the dictionary
        reference.setParseAction(refParseAction)
        dblQuoted = dblQuotedString.copy()
        dblQuoted.setParseAction(lambda s,loc,toks: [self.varSub(t) for t in toks])
        
        sglQuoted = sglQuotedString.copy()
        sglQuoted.setParseAction(lambda s,loc,toks: [t.strip("'") for t in toks])
        self.reference = reference
        self.dblQuoted = dblQuoted
        self.sglQuoted = sglQuoted
        varValue = (self.dblQuoted
                    | self.sglQuoted
                    | self.backtickString
                    | Word(alphanums+"-_/.,")
                    | Common.RangeAssignment
                    | CharsNotIn(" `")                    
                    )
    
        
        varDefinition = Optional(Literal("export ")).suppress() \
                        + Common.Identifier \
                        + Literal("=").suppress() + varValue

        def assign(lhs, rhs):
            if len(rhs) == 1:
                rhs = rhs[0]
            self.varMap[lhs] = rhs
            #print "assigning %s = %s" % (lhs,rhs)

            log.debug("assigning %s = %s" % (lhs,rhs))
            return

        varDefinition.setParseAction(lambda s,loc,toks:
                                     assign(toks[0], toks[1:]))
        self.arithExpr = self.makeCalcGrammar()
        varArithmetic = Literal("let ").suppress() \
                        + Common.Identifier \
                        + Literal("=").suppress() + self.arithExpr
        def arithAssign(lhs,rhs):
            assign(lhs, str(eval("".join(rhs))))
        varArithmetic.setParseAction(lambda s,loc,toks:
                                     arithAssign(toks[0], toks[1:]))
                   

        self.varArithmetic = varArithmetic
        self.varDefinition = varDefinition

    @staticmethod
    def parse(original, argv):
        pass

    def accepts(argv):
        if argv[0] == "export":
            return isAssign(argv[1:])
        return isAssign(argv)

    def isAssign(argv):
        pass

    def makeCalcGrammar(self):
        # from:
        # http://pyparsing.wikispaces.com/space/showimage/simpleArith.py
        # and 
        # http://pyparsing.wikispaces.com/space/showimage/fourFn.py
        # Copyright 2006, by Paul McGuire
        integer = Word(nums).setParseAction(lambda t:int(t[0]))

        snum = Word( "+-"+nums, nums )
        lpar  = Literal( "(" )
        rpar  = Literal( ")" )
        addop  = Literal( "+" ) | Literal( "-" )
        multop = Literal("*") | Literal("/")
        identifier = Common.Identifier.copy()
        identifier.setParseAction(lambda s,loc,toks:
                                  [self.varMap[t] for t in toks])
        
        
        expr = Forward()
        atom = (Optional("-") + (snum | identifier|(lpar+expr+rpar)))

        # by defining exponentiation as "atom [ ^ factor ]..." instead of "atom [ ^ atom ]...", we get right-to-left exponents, instead of left-to-righ
        # that is, 2^3^2 = 2^(3^2), not (2^3)^2.
        term = atom #+ ZeroOrMore(multop + atom)
        expr << term + ZeroOrMore(addop + term)

        return expr


    def varSub(self, fragment):
        
        return self.reference.transformString(fragment)
    def varDef(self, fragment):        
        try:
            return [x for x in self.varDefinition.parseString(fragment)]
        except ParseException, e:
            return 
        pass
    def varLet(self, fragment):        
        try:
            return [x for x in self.varArithmetic.parseString(fragment)]
        except ParseException, e:
            return 
        pass
    def expand2(self, fragment):
        return self.varSub(fragment)

    def quoteSplit(self, fragment):
        quoted = sglQuoted | doubleQuoted
        scanString

    def apply(self, fragment):
        fragment = self.expand2(fragment)
        #print "try ", fragment
        identifier = Common.Identifier.copy()
        def tryconvert(v):
            if v in self.varMap:
                return self.varMap[v]
            return v
        identifier.setParseAction(lambda s,loc,toks:
                                  [tryconvert(t) for t in toks])

        #print "".join([str(x) for x in identifier.scanString(fragment)])
        #print "".join([str(x) for x in self.varArithmetic.scanString(fragment)])

        result = self.varLet(fragment)
        if result:
            return # we consumed, so nothing left after applying
        result = self.varDef(fragment)
        if result:
            return # we consumed, so nothing left after applying
        return fragment
    pass #End of class VariableParser

def testExpand():
    """testExpand() is a convenience tester for the VariableParser class"""
    vp = VariableParser({})
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

    return
    print VariableParser.expand("blahblah")
    print VariableParser.expand("'blahblah'")
    print VariableParser.expand('"blahblah"')
    print VariableParser.expand('asdf"blahblah"asdf')

class NcoParser:
    """stuff that should be identical between client and server code"""
    commands = ["ncap", "ncatted", "ncbo", "ncdiff",
                "ncea", "ncecat", "ncflint", "ncks",
                "ncpack", "ncpdq", "ncra", "ncrcat",
                "ncrename", "ncunpack", "ncwa"]
    parserShortOpt = "4Aa:Bb:CcD:d:FfHhl:Mmn:Oo:Pp:QqRrs:S:s:t:uv:w:xY:y:"
    parserLongOpt = ["4", "netcdf4", "apn", "append",
                     "attribute=", #ncatted, ncrename
                     "avg=", "average=" #ncwa
                     "bnr", "binary",
                     "fl_bnr=", "binary-file=",
                     "crd", "coords",
                     "nocoords", "dbg_lvl=", "debug-level=",
                     "dmn=", "dimension=", "ftn", "fortran",
                     "huh", "hmm",
                     "fnc_tbl", "prn_fnc_tbl", "hst", "history",
                     "Mtd", "Metadata", "mtd", "metadata",
                     "mmr_cln", # only on recent NCO release
                     "lcl=", "local=",
                     "nintap",
                     "output=", "fl_out=",
                     "ovr", "overwrite", "prn", "print", "quiet",
                     "pth=", "path=",
                     "rtn", "retain", "revision", "vrs", "version",
                     "spt=", "script=", "fl_spt=", "script-file=",
                     "sng_fmt=", "string=",
                     "thr_nbr=", "threads=", "omp_num_threads=",
                     "xcl", "exclude",
                     "variable=", "wgt_var=", "weight=", 
                     "op_typ=", "operation=" ]

    # special handling for ncap's parameters
    ncapShortOpt = parserShortOpt.replace("v:","v")
    ncapLongOpt = parserLongOpt[:]
    ncapLongOpt.remove('variable=')
    ncapLongOpt.append('variable')

    ncpackShortOpt = parserShortOpt.replace("M","M:")
    ncpackShortOpt = ncpackShortOpt.replace("P","P:")
    ncpackShortOpt = ncpackShortOpt.replace("u","Uu")
    
    ncpackLongOpt = parserLongOpt[:]
    ncpackLongOpt.extend(['arrange','permute','reorder', 'rdr',
                         'pck_map', 'map', 'pck_plc','pack_policy',
                         'upk', 'unpack'])
    ncksShortOpt = parserShortOpt.replace("a:","a")
    ncksLongOpt = parserLongOpt[:]
    ncksLongOpt.extend(['abc', 'alphabetize'])

    ncflintShortOpt = parserShortOpt.replace("hl","hi:l")
    ncflintLongOpt = parserLongOpt[:]
    ncflintLongOpt.extend(['ntp', 'interpolate'])

    # special handling for ncwa's params (mask-related)
    # B: msk_cnd= mask_condition= (rplc)
    # b rdd degenerate-dimensions (rplc)
    # I: wgt_msk_crd_var= 
    # M: msk_val= mask_value=  mask-value= (rplc)
    # m: msk_nm= msk_var= mask_variable= mask-variable= (rplc)
    # N nmr numerator
    # T: mask_comparitor= msk_cmp_typ= op_rlt=
    ncwaShortOpt = parserShortOpt.replace("Bb:","B:b")
    ncwaShortOpt = ncwaShortOpt.replace("h","hI:")
    ncwaShortOpt = ncwaShortOpt.replace("Mmn:","M:m:Nn:")
    ncwaShortOpt = ncwaShortOpt.replace("t:","T:t:")
    ncwaLongOpt = parserLongOpt[:]
    ncwaLongOpt.extend(["msk_cnd=", "mask_condition=",
                        "rdd", "degenerate-dimensions",
                        "wgt_msk_crd_var=", 
                        "msk_val=", "mask_value=",  "mask-value=",
                        "msk_nm=", "msk_var=", "mask_variable=",
                        "mask-variable=",
                        "nmr", "numerator",
                        "mask_comparitor=", "msk_cmp_typ=", "op_rlt="])
    
    @staticmethod
    def specialGetOpt(argv):
        """argv is the argvlist formed like you would get from sys.argv .
        It should include the nco command (e.g. ncwa) in the 0th index."""
        #consider special-case for ncwa ncflint -w: option
        # wgt_var, weight also for ncflint/ncwa
        cmd = argv[0]
        argvlist = argv[1:]
        if cmd == "ncap": # ncap has a different format
            return getopt.getopt(argvlist,
                                 NcoParser.ncapShortOpt,
                                 NcoParser.ncapLongOpt)
        elif cmd in ["ncpdq", "ncpack", "ncunpack"]:
            # ncpdq/ncpack/ncunpack have a different format too
            return getopt.getopt(argvlist,
                                 NcoParser.ncpackShortOpt,
                                 NcoParser.ncpackLongOpt)
        elif cmd == "ncks":
            return getopt.getopt(argvlist,
                                 NcoParser.ncksShortOpt,
                                 NcoParser.ncksLongOpt)
        elif cmd == "ncflint":
            return getopt.getopt(argvlist,
                                 NcoParser.ncflintShortOpt,
                                 NcoParser.ncflintLongOpt)
        elif cmd == "ncwa":
            return getopt.getopt(argvlist,
                                 NcoParser.ncwaShortOpt,
                                 NcoParser.ncwaLongOpt)
        else:
            return getopt.getopt(argvlist,
                                 NcoParser.parserShortOpt,
                                 NcoParser.parserLongOpt)
        pass

    @staticmethod
    def parse(original, argv, lineNumber=0, factory=None):
        log.debug("building cmd from: %s" %original)
        cmd = argv[0]
        (argList, leftover) = NcoParser.specialGetOpt(argv)
        argDict = dict(argList)
        (ins, outs) = NcoParser.findInOuts(cmd, argDict, argList, leftover)
        if factory is not None:
            # assert for now to catch stupid mistakes.
            #assert isinstance(factory, CommandFactory)
            return factory.newCommand(cmd,
                                      (argDict, argList, leftover),
                                      (ins,outs), lineNumber)
        else:
            print ":( no factory."
            return cmd

    @staticmethod
    def findInOuts(cmd, adict, alist, leftover):
        # look for output file first
        ofname = ""
        inPrefix = None
        for x in  ["-o", "--fl_out", "--output"]:
            if x in adict:
                assert ofname == ""
                keys = [y[0] for y in alist]
                o = alist.pop(keys.index(x)) # o is a tuple.
                ofname = adict.pop(x)
                
                assert o[1] == ofname
                
        if ofname == "":
            # don't steal output if it's actually the input.
            if len(leftover) > 1:
                ofname = leftover[-1] # take last arg
                leftover = leftover[:-1] # and drop it off

        inlist = leftover # Set input list.

        # Detect input prefix.
        if "-p" in adict: 
            inPrefix = adict["-p"] 
            keys = [x[0] for x in alist]
            p = alist.pop(keys.index("-p"))
            adict.pop("-p")
        # handle ncap -S script input
        if (cmd == "ncap") and adict.has_key("-S"):
            inlist.append(adict["-S"])
        # now patch all the inputs
        if inPrefix is not None:
            if not inPrefix.endswith(os.sep):
                inPrefix = inPrefix + os.sep
            inlist = [(inPrefix + x) for x in inlist]
        return (inlist,[ofname])
        pass

            
    
    @staticmethod
    def accepts(argv):
        if len(argv) < 1:
            return False
        else:
            return (argv[0] in NcoParser.commands)    


class Parser:
    """Parse a SWAMP script for NCO(for now) commands"""

    # Try to keep parse state SMALL!  Don't track too much for parsing.
    # Only a few things will cause us to keep state across multiple lines:
    # - \ continuation
    # - for-loop constructs
    # - defined variables
    # - if-then-else constructs not an impl priority.

    # Summary of support:
    # backticks.  be very careful about this.
    # nco commands.  allow quotes.
    # comments. must start the line or be >=1 space separated
    # matching quotes.  this might be annoying
    # output capture: we can actually do this:
    #           [> out.txt [2>&1]]
    #           We don't need to be fancy like bash and support alternate
    #              orderings and syntaxes.

    # Anti-support:
    # meta-commands.  Don't support constructs that will break
    #                 on a plain-old shell script interpreter

    # build with default handlers: print statements

    # Primary function:
    # From a well-formed script,
    # * extract an ordered list of command tuples
    # * command tuple = (original, command, inputlist, outputlist,
    #                    arglist, leftover)
    #
    # Modules are used to isolate logic pertaining to specific binaries.
    # A module *accepts* a command line and *parses* it.    
    
    # put in a sanity check
    #     if len(leftover) <= 1:
    #         # only one leftover...leave it to be captured by the inputter
    #         logger.warning("warning, unbound output for "
    #                        + self.original)
    # handle dep checking in scheduler, not parser.
    #        self.env.tinlist.update(map(lambda x:(x,1),leftover))

    @staticmethod
    def staticInit():
        loopStart = Parser.LoopContext.Expr.ForHeading
        ifStart = Parser.BranchContext.Expr.IfHeading
        Parser.ourFlowParsers = [(loopStart.parseString, Parser.LoopContext),
                                 (ifStart.parseString, Parser.BranchContext)]

    def __init__(self):
        Parser.staticInit()
        self.handlers = {} # a lookup table for parse accept handlers
        self._variables = {} # a lookup table for variables
        self.lineNum=0
        self.modules = []
        self.handlerDefaults()
        self.handleFunc = None
        self.commands = []
        self.variableParser = VariableParser(self._variables)
        self._context = Parser.BaseContext(self)
        self._rootContext = self._context
        self.continuation = None
        pass

    def updateVariables(self, defs):
        self._variables.update(defs)
        
    def handlers(self, newHandlers):
        def newNcoCommand(argv):
            print "--".join(argv)
            return True
        pass

    def handlerDefaults(self):
        self.modules = [(NcoParser, None)]

        pass
    
    def commandHandler(self, handle):
        """handler is a function reference.  the function is a unary
        function: handle(ParserCommand)"""
        self.handleFunc = handle
        pass

    def stripComments(self, line):
        original = line.strip()
        comment = None
        # for now, do not detect sh dialect 
        linesp = original.split("#", 1) # chop up comments
        
        if len(linesp) > 1:
            line, comment = linesp
        else:
            line = original
            
        if (len(line) < 1): # skip comment-only line
            return ""
        return line
                
    def extParse(self, line):
        for (test, newContext) in Parser.ourFlowParsers:
            try:
                parseResult = test(line)
            except ParseException, e:
                continue # didn't find a looping expression.
            nextContext = newContext(self._context, parseResult)
            return nextContext
        return None
        
    @staticmethod
    def matchNewContext(line, lineNumber, currentContext):
        for (test, newContext) in Parser.ourFlowParsers:
            try:
                parseResult = test(line)
            except ParseException, e:
                continue # didn't find a looping expression.
            lineTuple = (line, lineNumber, parseResult)
            nextContext = newContext(currentContext, lineTuple)
            return nextContext
        return None

    def stdAccept(self, lineTup):
        argv = shlex.split(lineTup[0])
        for mod in self.modules:
            if mod[0].accepts(argv):
                cmd = mod[0].parse(lineTup[0], argv,
                                   lineTup[1], self._factory)
                return cmd
        print "stdAccept reject", argv
        return False

    def _parseScriptLine4(self, line):
        """Parse a single script line.
        We will build a parse tree, deferring all evaluation until later.
        """

        self.lineNum += 1
        cline = self.stripComments(line)
        if self.continuation:
            cline = self.continuation + cline
            self.continuation = None
        if cline.endswith("\\"): # handle line continuation
            self.continuation = cline[:-1] # excise backslash
            return self._context
        if not cline:
            return self._context
        
        (r, nextContext) = self._context.addLine((cline, self.lineNum, line))
        self._context = nextContext
        if not r:
            log.debug("parse error: " + line) 
        return self._context
        
    
    def _parseScriptLine(self, factory, line):
        """parse a single script line"""
        # for now, force to test alternate parsing
        return self._parseScriptLine4(line)
    
    def parseScript(self, script, factory):
        """Parse and accept/reject commands in a script, where the script
        is a single string containing script lines"""
        self._factory = factory
        lineCount = 0
        for line in script.splitlines():
            self._parseScriptLine(factory, line)
            lineCount += 1
            if (lineCount % 500) == 0:
                log.debug("%d lines parsed" % (lineCount))
   
        #log.debug("factory cmd_By_log_in: " + str(factory.commandByLogicalIn))
        #log.debug("factory uselist " + str(factory.scrFileUseCount))
        print "root context has,", self._context
        assert self._context == self._rootContext #should have popped back to top.
        self._context.evaluateAll(self)
        pass

    class LeafContext:
        def __init__(self, lineTup):
            self._line = lineTup[0]
            self._lineNum = lineTup[1]
            self._original = lineTup[2]

        def __str__(self):
            return "%d : %s" %(self._lineNum, self._line)
        
        def evaluate(self, evalContext):
            """ Do "real" statement evaluation (generate command)
            return: a list of command objects?
            evalContext: object with variableParser, stdAccept, and handleFunc
            """

            # apply variable handling
            #print "evaluating:", self._lineNum, self._line
            line = evalContext.variableParser.apply(self._line)
            if not isinstance(line, str):
                return []
            command = evalContext.stdAccept((line, self._lineNum))
            if isinstance(command, types.InstanceType):
                command.referenceLineNum = self._lineNum
                command.original = self._original
        
            if not command:
                log.debug("reject:"+ self._line)
            elif evalContext.handleFunc is not None:
                evalContext.handleFunc(command)
            return [command]

        
    ## a few helper classes for Parser.
    class BaseContext:
        
        def __init__(self, parser):
            """handler is a function that accepts a Command instance
            as input (it's probably Scheduler.schedule)."""
            self.canPop = False
            # _parser gets exposed to subclasses, because they need
            # to invoke the the variable parser.  *sigh*
            self._parser = parser 
            self._factory = None
            self.isChildParseOnly = False
            self._members = []
            pass

        def __str__(self):
            return "Base context with members: \n%s" % "\n".join(map(str,self._members))
        
        def addLine(self, lineTup):
            # see if a child context is created:
            line = lineTup[0]
            num = lineTup[1]
            orig = lineTup[2]
            next = Parser.matchNewContext(line, num, self)
            if next:
                self._members.append(next)
                return (True, next)
            else:
                if len(line.strip()) > 0:
                    self._members.append(Parser.LeafContext(lineTup))
                return (True, self)
            
        def evaluateAll(self, evalContext):
            print "begin evaluation"
            for c in self._members:
                c.evaluate(evalContext)
            pass

        pass



    class BranchContext:
        # basically, the branching context is aware of a branch,
        # and is able to evaluate and take one branch versus another.
        class Expr:
            If = Literal("if")
            Elif = Literal("elif")

            Reference = Common.Reference

            #FIXME: can we use a looser definition?
            PlainString = Word(alphanums + "_,.")  
            Squoted = Literal("'") + OneOrMore(PlainString) + Literal("'")
            Dquoted = Literal('"').suppress() + OneOrMore(PlainString | Reference) + Literal('"').suppress()
            BinaryOp = MatchFirst([Literal("="), Literal("=="), Literal("!="),
                                  Literal("<"), Literal(">")])
            String = MatchFirst([Dquoted, Squoted, PlainString, Reference])
            Expression = String # for now, just strings
            Boolean = Expression.setResultsName("lhs") + BinaryOp.setResultsName("op") + Expression.setResultsName("rhs")
            Term = Boolean.setResultsName("boolean")
            Condition = Literal("[") + Term + Literal("]")
            
            IfHeading = If + Condition.setResultsName("condition") + Literal(";") + Literal("then")
            ElifHeading = Elif + Condition + Literal(";") + Literal("then")
            ElseHeading = Literal("else")
            IfClosing = Literal("fi")
            pass
        class Branch:
            def __init__(self, lineTuple, parseExpr):
                self._line = lineTuple[0]
                self._lineNum = lineTuple[1]
                self._parseExpr = parseExpr
                self._members = []
                pass
            
            def __str__(self):
                members = "\nBB".join(map(str, self._members))
                return "branch started with %s at %d has members %s" %(
                    self._line, self._lineNum, members)
            
            def add(self, node):
                self._members.append(node)
                pass

            def check(self, evalContext):
                # apply variables, and then re-parse
                if not self._parseExpr:
                    return True

                line = evalContext.variableParser.apply(self._line)
                print "checking if:",line
                parsed = self._parseExpr.parseString(line)
                boolean = parsed["boolean"]
                rhs = boolean["rhs"]
                op = parsed["op"]
                lhs = parsed["lhs"]
                rhs = parsed["rhs"]
                resolver = {"=" :  lambda l,r: l == r,
                            "==" : lambda l,r: l == r,
                            "!=" : lambda l,r: l != r,
                            "<" :  lambda l,r: l < r,
                            ">" :  lambda l,r: l > r,
                            }
                print "resolved", lhs, op, rhs, resolver[op](lhs,rhs)
                self._ifResult = resolver[op](lhs,rhs)
                return self._ifResult
                # FIXME: Figure out what sort of errors are possible here.

            def evaluate(self, evalContext):
                l = []
                for m in self._members:
                    l.extend(m.evaluate(evalContext))
                return l

                
        def __init__(self, parentContext, parseResult):
            if isinstance(parseResult, tuple):
                self.openLine = (parseResult[0], parseResult[1])
                parseResult = parseResult[2]
            self._parentContext = parentContext

            self._savedLines = []
            self._branchHeading = parseResult

            self._state = 1 # startup in state 1 (seen an if statement)
            self._parseFunc = { 1: self._state1Parse,
                                2: self._state2Parse }

            self._currentBranch = self.Branch(self.openLine,
                                              self.Expr.IfHeading)
            self._members = [self._currentBranch]
            

            print "new branch"
            pass

        def __str__(self):
            return "branch block: %s BEND" %("\n".join(map(str, self._members)))

        def _state1Parse(self, lineTup):
            # In state 1, the last statement we saw was an if or elif,
            # so the only valid keywords are 'else' and 'elif' or 'fi'
            print "state 1",lineTup
            try:
                return self._tryElif(lineTup) 
            except ParseException, e:
                pass
            except KeyError, e:
                pass # passthrough variable lookup errors.
            try:
                return self._tryElse(lineTup)
            except ParseException, e:
                pass
            try:
                return self._tryFi(lineTup)
            except ParseException, e:
                pass
            return # okay to stay in the same context.
        
        def _state2Parse(self, lineTup):
            # In state 2, the last statement seen is 'else' so the only
            # valid keyword is 'fi'
            print "state 2", lineTup
            try:
                return self._tryFi(lineTup)
            except ParseException, e:
                pass
            return

        def _tryElif(self, lineTup):
            check = self.Expr.ElifHeading.parseString(lineTup[0])
            print "elif parsing", check
            # need to create new branch.
            self._currentBranch = self.Branch(lineTup, self.Expr.ElifHeading)
            self._members.append(self._currentBranch)
            return self

        def _tryElse(self, lineTup):
            check = self.Expr.ElseHeading.parseString(lineTup[0])
            self._state = 2
            print "else parsing", check
            # need to create new branch.
            self._currentBranch = self.Branch(lineTup, None) # no condition check needed.
            self._members.append(self._currentBranch)
            return self


        def _tryFi(self, lineTup):
            check = self.Expr.IfClosing.parseString(lineTup[0])
            parent = self._parentContext #save my parent.
            print "match fi"
            return parent
            if matchOnly:
                return parent
            print "supposed to dump to", parent, "lines now", self._savedLines
            for l in self._savedLines:
                n = BaseContext.extParse(self, l)
                if n:
                    context = n
                    parent = parent.acceptSubParse(l)
                print parent
            assert parent is self._parentContext # should return from any nesting.

            #print "subdump results", rlist
            return self._parentContext


        def _branchParse(self, lineTup):
            # FSM:
            # 0: Nothing (not in this context)
            #  -> 1: receive if[] ; then
            #  -> 0: anything else
            # 1: In "taken" body
            #  -> 1: receive elif [] ; then
            #  -> 3: receive fi
            #  -> 2: receive else 
            # 2: In "not taken" body
            #  -> 3: receive fi
            #  -> 2: anything else
            # 3: (pseudostate)Resolution: evalutate branch condition
            #  -> 0: return to parent context.
            return self._parseFunc[self._state](lineTup)

            
        def _evaluateCondition(self, condition):
            parsed = condition
            boolean = parsed["boolean"]
            rhs = boolean["rhs"]
            op = parsed["op"]
            lhs = parsed["lhs"]
            rhs = parsed["rhs"]
            resolver = {"=" :  lambda l,r: l == r,
                        "==" : lambda l,r: l == r,
                        "!=" : lambda l,r: l != r,
                        "<" :  lambda l,r: l < r,
                        ">" :  lambda l,r: l > r,
                        }
            print "resolved", lhs, op, rhs, resolver[op](lhs,rhs)
            self._ifResult = resolver[op](lhs,rhs)
            return self._ifResult
            # FIXME: Figure out what sort of errors are possible here.
            pass
            

        def addLine(self, lineTup):
            # see if a child context is created:
            (line, num, orig) = lineTup
            next = Parser.matchNewContext(line, num, self)
            if next:
                self._currentBranch.add(next)
                return (True, next)
            else:
                r = self._branchParse((line,num))
                if r:
                    if  r != self: #finish?
                        return (True, r)
                else: # if no parse results, then just add to current
                    if len(line.strip()) > 0:
                        self._currentBranch.add(Parser.LeafContext(lineTup))
                return (True, self)
            pass
            
        def evaluate(self, evalContext):
            """Perform an 'evaluation' as much as appropriate by the
            context, given current state.
            return True if the line was accepted by the backend.
                   False otherwise."""
            # match branches until we get to the end.
            for b in self._members:
                if b.check(evalContext):
                    return b.evaluate(evalContext)
            return [] # if with no matches and no else.
        pass
        
    
    class LoopContext:
        # basically, a loopcontext is aware of loops, so that when the 'end loop' token is parsed, the loop body is replicated for each value of the looping variable.  Is this (having a class) the right way to do it?  Are we just a glorified stack?  I think more correctly, loopcontext functions as the parse node for a loop.  
        class Expr: # move this to the context
            For = Literal('for')
            In = Literal('in')
            Do = Literal('do')
            Done = Literal('done')
            Semicolon = Literal(';')
            Identifier = Common.Identifier.copy()
            Identifier = Identifier.setResultsName("identifier")
            Range = Common.Range
            ForHeading = For + Identifier + In + Range + Semicolon + Do
            ForHeading2_1 = For + Identifier + In + Range
            ForHeading2_2 = Do
            ForClosing = Done
            pass
            
        def __init__(self, parentContext, parseResult):
            if isinstance(parseResult, tuple):
                self._openLine = (parseResult[0], parseResult[1])
                parseResult = parseResult[2]

            self._parentContext = parentContext
            self._loopHeading = parseResult
            self._members = []
            print "new loop"
            
            pass

        def _loopParse(self, line):
            try:
                check = self.Expr.ForClosing.parseString(line)
                return self._parentContext
            except ParseException, e:
                return None
            pass
            
        def _unroll(self, evalContext):
            varname = self._loopHeading["identifier"]
            iterations = []
            #apply variable substitution
            ##################################################
            line = evalContext.variableParser.apply(self._openLine[0])
            # detect additional expansion.
            bb = BacktickParser()
            expr = BacktickParser.backtickString.copy()
            expr.setParseAction(bb.transformQuoted)
            # transform potential backticks
            #print "line",line
            xf = expr.transformString(line)
            #print "xf",xf
            newone = self.Expr.ForHeading.parseString(xf)
            rr = newone["range"]
            print "loop range is ",rr, type(rr)
            for val in rr:
                evalContext.variableParser.varMap[varname] = val
                iterations.extend(self._iterateLoop(evalContext))
            return iterations

        def _iterateLoop(self, evalContext):
            iterations = []
            for m in self._members:
                iterations.extend(m.evaluate(evalContext))
            return iterations        

        def _reparseHeading(self):
            self._loopHeading = self.Expr.LoopHeading.parseString(self._openLine)
            pass

        def addLine(self, lineTup):
            # see if a child context is created:
            (line, num, orig) = lineTup
            next = Parser.matchNewContext(line, num, self)
            if next:
                self._members.append(next)
                return (True, next)
            else:
                r = self._loopParse(line)
                if r:
                    return (True, r)
                else: # if no parse results, then just add to current
                    if len(line.strip()) > 0:
                        self._members.append(Parser.LeafContext(lineTup))
                return (True, self)
            pass

            
        def evaluate(self, evalContext):
            """Perform an 'evaluation' as much as appropriate by the
            context, given current state.
            return True if the line was accepted by the backend.
                   False otherwise."""
            # re-evaluate the loop heading.
            
            return self._unroll(evalContext)

        pass
    
    pass
