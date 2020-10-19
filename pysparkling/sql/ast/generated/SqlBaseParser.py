# Generated from ../grammar/SqlBase.g4 by ANTLR 4.7.1
# encoding: utf-8
from antlr4 import *
from io import StringIO
from typing.io import TextIO
import sys

def serializedATN():
    with StringIO() as buf:
        buf.write("\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\u012b")
        buf.write("\u0bcc\4\2\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7")
        buf.write("\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t\13\4\f\t\f\4\r\t\r\4\16")
        buf.write("\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22\4\23\t\23")
        buf.write("\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31")
        buf.write("\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36")
        buf.write("\4\37\t\37\4 \t \4!\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t")
        buf.write("&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4,\t,\4-\t-\4.\t.\4")
        buf.write("/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t\64")
        buf.write("\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t")
        buf.write(";\4<\t<\4=\t=\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\t")
        buf.write("D\4E\tE\4F\tF\4G\tG\4H\tH\4I\tI\4J\tJ\4K\tK\4L\tL\4M\t")
        buf.write("M\4N\tN\4O\tO\4P\tP\4Q\tQ\4R\tR\4S\tS\4T\tT\4U\tU\4V\t")
        buf.write("V\4W\tW\4X\tX\4Y\tY\4Z\tZ\4[\t[\4\\\t\\\4]\t]\4^\t^\4")
        buf.write("_\t_\4`\t`\4a\ta\4b\tb\4c\tc\4d\td\4e\te\4f\tf\4g\tg\4")
        buf.write("h\th\4i\ti\4j\tj\4k\tk\4l\tl\4m\tm\4n\tn\4o\to\4p\tp\4")
        buf.write("q\tq\4r\tr\4s\ts\4t\tt\4u\tu\4v\tv\4w\tw\4x\tx\4y\ty\4")
        buf.write("z\tz\4{\t{\4|\t|\4}\t}\4~\t~\4\177\t\177\4\u0080\t\u0080")
        buf.write("\4\u0081\t\u0081\4\u0082\t\u0082\4\u0083\t\u0083\4\u0084")
        buf.write("\t\u0084\4\u0085\t\u0085\4\u0086\t\u0086\4\u0087\t\u0087")
        buf.write("\4\u0088\t\u0088\4\u0089\t\u0089\4\u008a\t\u008a\3\2\3")
        buf.write("\2\7\2\u0117\n\2\f\2\16\2\u011a\13\2\3\2\3\2\3\3\3\3\3")
        buf.write("\3\3\4\3\4\3\4\3\5\3\5\3\5\3\6\3\6\3\6\3\7\3\7\3\7\3\b")
        buf.write("\3\b\3\b\3\t\3\t\5\t\u0132\n\t\3\t\3\t\3\t\5\t\u0137\n")
        buf.write("\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u013f\n\t\3\t\3\t\3\t\3")
        buf.write("\t\3\t\3\t\7\t\u0147\n\t\f\t\16\t\u014a\13\t\3\t\3\t\3")
        buf.write("\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t")
        buf.write("\3\t\5\t\u015d\n\t\3\t\3\t\5\t\u0161\n\t\3\t\3\t\3\t\3")
        buf.write("\t\5\t\u0167\n\t\3\t\5\t\u016a\n\t\3\t\5\t\u016d\n\t\3")
        buf.write("\t\3\t\3\t\3\t\3\t\3\t\5\t\u0175\n\t\3\t\5\t\u0178\n\t")
        buf.write("\3\t\3\t\5\t\u017c\n\t\3\t\5\t\u017f\n\t\3\t\3\t\3\t\3")
        buf.write("\t\3\t\3\t\5\t\u0187\n\t\3\t\3\t\3\t\5\t\u018c\n\t\3\t")
        buf.write("\5\t\u018f\n\t\3\t\3\t\3\t\3\t\3\t\5\t\u0196\n\t\3\t\3")
        buf.write("\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u01a2\n\t\3\t\3")
        buf.write("\t\3\t\3\t\3\t\3\t\3\t\7\t\u01ab\n\t\f\t\16\t\u01ae\13")
        buf.write("\t\3\t\5\t\u01b1\n\t\3\t\5\t\u01b4\n\t\3\t\3\t\3\t\3\t")
        buf.write("\3\t\5\t\u01bb\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t")
        buf.write("\7\t\u01c6\n\t\f\t\16\t\u01c9\13\t\3\t\3\t\3\t\3\t\3\t")
        buf.write("\5\t\u01d0\n\t\3\t\3\t\3\t\5\t\u01d5\n\t\3\t\5\t\u01d8")
        buf.write("\n\t\3\t\3\t\3\t\3\t\5\t\u01de\n\t\3\t\3\t\3\t\3\t\3\t")
        buf.write("\3\t\3\t\3\t\3\t\5\t\u01e9\n\t\3\t\3\t\3\t\3\t\3\t\3\t")
        buf.write("\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3")
        buf.write("\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t")
        buf.write("\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3")
        buf.write("\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t")
        buf.write("\3\t\3\t\5\t\u0229\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t")
        buf.write("\u0232\n\t\3\t\3\t\5\t\u0236\n\t\3\t\3\t\3\t\3\t\5\t\u023c")
        buf.write("\n\t\3\t\3\t\5\t\u0240\n\t\3\t\3\t\3\t\5\t\u0245\n\t\3")
        buf.write("\t\3\t\3\t\3\t\5\t\u024b\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3")
        buf.write("\t\3\t\3\t\3\t\5\t\u0257\n\t\3\t\3\t\3\t\3\t\3\t\3\t\5")
        buf.write("\t\u025f\n\t\3\t\3\t\3\t\3\t\5\t\u0265\n\t\3\t\3\t\3\t")
        buf.write("\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u0272\n\t\3\t\6\t")
        buf.write("\u0275\n\t\r\t\16\t\u0276\3\t\3\t\3\t\3\t\3\t\3\t\3\t")
        buf.write("\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u0287\n\t\3\t\3\t\3\t")
        buf.write("\7\t\u028c\n\t\f\t\16\t\u028f\13\t\3\t\5\t\u0292\n\t\3")
        buf.write("\t\3\t\3\t\3\t\5\t\u0298\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3")
        buf.write("\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u02a7\n\t\3\t\3\t\5\t\u02ab")
        buf.write("\n\t\3\t\3\t\3\t\3\t\5\t\u02b1\n\t\3\t\3\t\3\t\3\t\5\t")
        buf.write("\u02b7\n\t\3\t\5\t\u02ba\n\t\3\t\5\t\u02bd\n\t\3\t\3\t")
        buf.write("\3\t\3\t\5\t\u02c3\n\t\3\t\3\t\5\t\u02c7\n\t\3\t\3\t\3")
        buf.write("\t\3\t\3\t\3\t\7\t\u02cf\n\t\f\t\16\t\u02d2\13\t\3\t\3")
        buf.write("\t\3\t\3\t\3\t\3\t\5\t\u02da\n\t\3\t\5\t\u02dd\n\t\3\t")
        buf.write("\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u02e6\n\t\3\t\3\t\3\t\5\t")
        buf.write("\u02eb\n\t\3\t\3\t\3\t\3\t\5\t\u02f1\n\t\3\t\3\t\3\t\3")
        buf.write("\t\3\t\5\t\u02f8\n\t\3\t\5\t\u02fb\n\t\3\t\3\t\3\t\3\t")
        buf.write("\5\t\u0301\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\7\t\u030a\n")
        buf.write("\t\f\t\16\t\u030d\13\t\5\t\u030f\n\t\3\t\3\t\5\t\u0313")
        buf.write("\n\t\3\t\3\t\3\t\5\t\u0318\n\t\3\t\3\t\3\t\5\t\u031d\n")
        buf.write("\t\3\t\3\t\3\t\3\t\3\t\5\t\u0324\n\t\3\t\5\t\u0327\n\t")
        buf.write("\3\t\5\t\u032a\n\t\3\t\3\t\3\t\3\t\3\t\5\t\u0331\n\t\3")
        buf.write("\t\3\t\3\t\5\t\u0336\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5")
        buf.write("\t\u033f\n\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u0347\n\t\3\t")
        buf.write("\3\t\3\t\3\t\5\t\u034d\n\t\3\t\5\t\u0350\n\t\3\t\5\t\u0353")
        buf.write("\n\t\3\t\3\t\3\t\3\t\5\t\u0359\n\t\3\t\3\t\5\t\u035d\n")
        buf.write("\t\3\t\3\t\5\t\u0361\n\t\3\t\3\t\5\t\u0365\n\t\5\t\u0367")
        buf.write("\n\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u036f\n\t\3\t\3\t\3\t")
        buf.write("\3\t\3\t\3\t\5\t\u0377\n\t\3\t\3\t\3\t\3\t\5\t\u037d\n")
        buf.write("\t\3\t\3\t\3\t\3\t\5\t\u0383\n\t\3\t\5\t\u0386\n\t\3\t")
        buf.write("\3\t\5\t\u038a\n\t\3\t\5\t\u038d\n\t\3\t\3\t\5\t\u0391")
        buf.write("\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3")
        buf.write("\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\7\t\u03a8\n\t\f\t\16")
        buf.write("\t\u03ab\13\t\5\t\u03ad\n\t\3\t\3\t\5\t\u03b1\n\t\3\t")
        buf.write("\3\t\3\t\3\t\5\t\u03b7\n\t\3\t\5\t\u03ba\n\t\3\t\5\t\u03bd")
        buf.write("\n\t\3\t\3\t\3\t\3\t\5\t\u03c3\n\t\3\t\3\t\3\t\3\t\3\t")
        buf.write("\3\t\5\t\u03cb\n\t\3\t\3\t\3\t\5\t\u03d0\n\t\3\t\3\t\3")
        buf.write("\t\3\t\5\t\u03d6\n\t\3\t\3\t\3\t\3\t\5\t\u03dc\n\t\3\t")
        buf.write("\3\t\3\t\3\t\3\t\3\t\3\t\3\t\7\t\u03e6\n\t\f\t\16\t\u03e9")
        buf.write("\13\t\5\t\u03eb\n\t\3\t\3\t\3\t\7\t\u03f0\n\t\f\t\16\t")
        buf.write("\u03f3\13\t\3\t\3\t\7\t\u03f7\n\t\f\t\16\t\u03fa\13\t")
        buf.write("\3\t\3\t\3\t\7\t\u03ff\n\t\f\t\16\t\u0402\13\t\5\t\u0404")
        buf.write("\n\t\3\n\3\n\3\n\3\n\3\n\3\n\5\n\u040c\n\n\3\n\3\n\5\n")
        buf.write("\u0410\n\n\3\n\3\n\3\n\3\n\3\n\5\n\u0417\n\n\3\n\3\n\3")
        buf.write("\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n")
        buf.write("\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3")
        buf.write("\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n")
        buf.write("\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3")
        buf.write("\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n")
        buf.write("\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3")
        buf.write("\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n")
        buf.write("\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3")
        buf.write("\n\3\n\3\n\3\n\5\n\u048b\n\n\3\n\3\n\3\n\3\n\3\n\3\n\5")
        buf.write("\n\u0493\n\n\3\n\3\n\3\n\3\n\3\n\3\n\5\n\u049b\n\n\3\n")
        buf.write("\3\n\3\n\3\n\3\n\3\n\3\n\5\n\u04a4\n\n\3\n\3\n\3\n\3\n")
        buf.write("\3\n\3\n\3\n\3\n\5\n\u04ae\n\n\3\13\3\13\5\13\u04b2\n")
        buf.write("\13\3\13\5\13\u04b5\n\13\3\13\3\13\3\13\3\13\5\13\u04bb")
        buf.write("\n\13\3\13\3\13\3\f\3\f\5\f\u04c1\n\f\3\f\3\f\3\f\3\f")
        buf.write("\3\r\3\r\3\r\3\r\3\r\3\r\5\r\u04cd\n\r\3\r\3\r\3\r\3\r")
        buf.write("\3\16\3\16\3\16\3\16\3\16\3\16\5\16\u04d9\n\16\3\16\3")
        buf.write("\16\3\16\5\16\u04de\n\16\3\17\3\17\3\17\3\20\3\20\3\20")
        buf.write("\3\21\5\21\u04e7\n\21\3\21\3\21\3\21\3\22\3\22\3\22\5")
        buf.write("\22\u04ef\n\22\3\22\3\22\3\22\3\22\3\22\5\22\u04f6\n\22")
        buf.write("\5\22\u04f8\n\22\3\22\3\22\3\22\5\22\u04fd\n\22\3\22\3")
        buf.write("\22\5\22\u0501\n\22\3\22\3\22\3\22\5\22\u0506\n\22\3\22")
        buf.write("\3\22\3\22\5\22\u050b\n\22\3\22\3\22\3\22\5\22\u0510\n")
        buf.write("\22\3\22\5\22\u0513\n\22\3\22\3\22\3\22\5\22\u0518\n\22")
        buf.write("\3\22\3\22\5\22\u051c\n\22\3\22\3\22\3\22\5\22\u0521\n")
        buf.write("\22\5\22\u0523\n\22\3\23\3\23\5\23\u0527\n\23\3\24\3\24")
        buf.write("\3\24\3\24\3\24\7\24\u052e\n\24\f\24\16\24\u0531\13\24")
        buf.write("\3\24\3\24\3\25\3\25\3\25\5\25\u0538\n\25\3\26\3\26\3")
        buf.write("\27\3\27\3\27\3\27\3\27\5\27\u0541\n\27\3\30\3\30\3\30")
        buf.write("\7\30\u0546\n\30\f\30\16\30\u0549\13\30\3\31\3\31\3\31")
        buf.write("\3\31\7\31\u054f\n\31\f\31\16\31\u0552\13\31\3\32\3\32")
        buf.write("\5\32\u0556\n\32\3\32\5\32\u0559\n\32\3\32\3\32\3\32\3")
        buf.write("\32\3\33\3\33\3\33\3\34\3\34\3\34\3\34\3\34\3\34\3\34")
        buf.write("\3\34\3\34\3\34\7\34\u056c\n\34\f\34\16\34\u056f\13\34")
        buf.write("\3\35\3\35\3\35\3\35\7\35\u0575\n\35\f\35\16\35\u0578")
        buf.write("\13\35\3\35\3\35\3\36\3\36\5\36\u057e\n\36\3\36\5\36\u0581")
        buf.write("\n\36\3\37\3\37\3\37\7\37\u0586\n\37\f\37\16\37\u0589")
        buf.write("\13\37\3\37\5\37\u058c\n\37\3 \3 \3 \3 \5 \u0592\n \3")
        buf.write("!\3!\3!\3!\7!\u0598\n!\f!\16!\u059b\13!\3!\3!\3\"\3\"")
        buf.write("\3\"\3\"\7\"\u05a3\n\"\f\"\16\"\u05a6\13\"\3\"\3\"\3#")
        buf.write("\3#\3#\3#\3#\3#\5#\u05b0\n#\3$\3$\3$\3$\3$\5$\u05b7\n")
        buf.write("$\3%\3%\3%\3%\5%\u05bd\n%\3&\3&\3&\3\'\3\'\3\'\3\'\3\'")
        buf.write("\3\'\6\'\u05c8\n\'\r\'\16\'\u05c9\3\'\3\'\3\'\3\'\3\'")
        buf.write("\5\'\u05d1\n\'\3\'\3\'\3\'\3\'\3\'\5\'\u05d8\n\'\3\'\3")
        buf.write("\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\5\'\u05e4\n\'\3\'\3")
        buf.write("\'\3\'\3\'\7\'\u05ea\n\'\f\'\16\'\u05ed\13\'\3\'\7\'\u05f0")
        buf.write("\n\'\f\'\16\'\u05f3\13\'\5\'\u05f5\n\'\3(\3(\3(\3(\3(")
        buf.write("\7(\u05fc\n(\f(\16(\u05ff\13(\5(\u0601\n(\3(\3(\3(\3(")
        buf.write("\3(\7(\u0608\n(\f(\16(\u060b\13(\5(\u060d\n(\3(\3(\3(")
        buf.write("\3(\3(\7(\u0614\n(\f(\16(\u0617\13(\5(\u0619\n(\3(\3(")
        buf.write("\3(\3(\3(\7(\u0620\n(\f(\16(\u0623\13(\5(\u0625\n(\3(")
        buf.write("\5(\u0628\n(\3(\3(\3(\5(\u062d\n(\5(\u062f\n(\3)\3)\3")
        buf.write(")\3*\3*\3*\3*\3*\3*\3*\5*\u063b\n*\3*\3*\3*\3*\3*\5*\u0642")
        buf.write("\n*\3*\3*\3*\3*\3*\5*\u0649\n*\3*\7*\u064c\n*\f*\16*\u064f")
        buf.write("\13*\3+\3+\3+\3+\3+\3+\3+\3+\3+\5+\u065a\n+\3,\3,\5,\u065e")
        buf.write("\n,\3,\3,\5,\u0662\n,\3-\3-\6-\u0666\n-\r-\16-\u0667\3")
        buf.write(".\3.\5.\u066c\n.\3.\3.\3.\3.\7.\u0672\n.\f.\16.\u0675")
        buf.write("\13.\3.\5.\u0678\n.\3.\5.\u067b\n.\3.\5.\u067e\n.\3.\5")
        buf.write(".\u0681\n.\3.\3.\5.\u0685\n.\3/\3/\5/\u0689\n/\3/\5/\u068c")
        buf.write("\n/\3/\3/\5/\u0690\n/\3/\7/\u0693\n/\f/\16/\u0696\13/")
        buf.write("\3/\5/\u0699\n/\3/\5/\u069c\n/\3/\5/\u069f\n/\3/\5/\u06a2")
        buf.write("\n/\5/\u06a4\n/\3\60\3\60\3\60\3\60\3\60\3\60\3\60\3\60")
        buf.write("\3\60\3\60\5\60\u06b0\n\60\3\60\5\60\u06b3\n\60\3\60\3")
        buf.write("\60\5\60\u06b7\n\60\3\60\3\60\3\60\3\60\3\60\3\60\3\60")
        buf.write("\3\60\5\60\u06c1\n\60\3\60\3\60\5\60\u06c5\n\60\5\60\u06c7")
        buf.write("\n\60\3\60\5\60\u06ca\n\60\3\60\3\60\5\60\u06ce\n\60\3")
        buf.write("\61\3\61\7\61\u06d2\n\61\f\61\16\61\u06d5\13\61\3\61\5")
        buf.write("\61\u06d8\n\61\3\61\3\61\3\62\3\62\3\62\3\63\3\63\3\63")
        buf.write("\3\63\5\63\u06e3\n\63\3\63\3\63\3\63\3\64\3\64\3\64\3")
        buf.write("\64\3\64\5\64\u06ed\n\64\3\64\3\64\3\64\3\65\3\65\3\65")
        buf.write("\3\65\3\65\3\65\3\65\5\65\u06f9\n\65\3\66\3\66\3\66\3")
        buf.write("\66\3\66\3\66\3\66\3\66\3\66\3\66\3\66\7\66\u0706\n\66")
        buf.write("\f\66\16\66\u0709\13\66\3\66\3\66\5\66\u070d\n\66\3\67")
        buf.write("\3\67\3\67\7\67\u0712\n\67\f\67\16\67\u0715\13\67\38\3")
        buf.write("8\38\38\39\39\39\3:\3:\3:\3;\3;\3;\5;\u0724\n;\3;\7;\u0727")
        buf.write("\n;\f;\16;\u072a\13;\3;\3;\3<\3<\3<\3<\3<\3<\7<\u0734")
        buf.write("\n<\f<\16<\u0737\13<\3<\3<\5<\u073b\n<\3=\3=\3=\3=\7=")
        buf.write("\u0741\n=\f=\16=\u0744\13=\3=\7=\u0747\n=\f=\16=\u074a")
        buf.write("\13=\3=\5=\u074d\n=\3>\3>\3>\3>\3>\7>\u0754\n>\f>\16>")
        buf.write("\u0757\13>\3>\3>\3>\3>\3>\3>\3>\3>\3>\3>\7>\u0763\n>\f")
        buf.write(">\16>\u0766\13>\3>\3>\5>\u076a\n>\3>\3>\3>\3>\3>\3>\3")
        buf.write(">\3>\7>\u0774\n>\f>\16>\u0777\13>\3>\3>\5>\u077b\n>\3")
        buf.write("?\3?\3?\3?\7?\u0781\n?\f?\16?\u0784\13?\5?\u0786\n?\3")
        buf.write("?\3?\5?\u078a\n?\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\7@\u0796")
        buf.write("\n@\f@\16@\u0799\13@\3@\3@\3@\3A\3A\3A\3A\3A\7A\u07a3")
        buf.write("\nA\fA\16A\u07a6\13A\3A\3A\5A\u07aa\nA\3B\3B\5B\u07ae")
        buf.write("\nB\3B\5B\u07b1\nB\3C\3C\3C\5C\u07b6\nC\3C\3C\3C\3C\3")
        buf.write("C\7C\u07bd\nC\fC\16C\u07c0\13C\5C\u07c2\nC\3C\3C\3C\5")
        buf.write("C\u07c7\nC\3C\3C\3C\7C\u07cc\nC\fC\16C\u07cf\13C\5C\u07d1")
        buf.write("\nC\3D\3D\3E\3E\7E\u07d7\nE\fE\16E\u07da\13E\3F\3F\3F")
        buf.write("\3F\5F\u07e0\nF\3F\3F\3F\3F\3F\5F\u07e7\nF\3G\5G\u07ea")
        buf.write("\nG\3G\3G\3G\5G\u07ef\nG\3G\5G\u07f2\nG\3G\3G\3G\5G\u07f7")
        buf.write("\nG\3G\3G\5G\u07fb\nG\3G\5G\u07fe\nG\3G\5G\u0801\nG\3")
        buf.write("H\3H\3H\3H\5H\u0807\nH\3I\3I\3I\5I\u080c\nI\3I\3I\3J\5")
        buf.write("J\u0811\nJ\3J\3J\3J\3J\3J\3J\3J\3J\3J\3J\3J\3J\3J\3J\3")
        buf.write("J\3J\5J\u0823\nJ\5J\u0825\nJ\3J\5J\u0828\nJ\3K\3K\3K\3")
        buf.write("K\3L\3L\3L\7L\u0831\nL\fL\16L\u0834\13L\3M\3M\3M\3M\7")
        buf.write("M\u083a\nM\fM\16M\u083d\13M\3M\3M\3N\3N\5N\u0843\nN\3")
        buf.write("O\3O\3O\3O\7O\u0849\nO\fO\16O\u084c\13O\3O\3O\3P\3P\5")
        buf.write("P\u0852\nP\3Q\3Q\5Q\u0856\nQ\3Q\3Q\3Q\3Q\3Q\3Q\5Q\u085e")
        buf.write("\nQ\3Q\3Q\3Q\3Q\3Q\3Q\5Q\u0866\nQ\3Q\3Q\3Q\3Q\5Q\u086c")
        buf.write("\nQ\3R\3R\3R\3R\7R\u0872\nR\fR\16R\u0875\13R\3R\3R\3S")
        buf.write("\3S\3S\3S\3S\7S\u087e\nS\fS\16S\u0881\13S\5S\u0883\nS")
        buf.write("\3S\3S\3S\3T\5T\u0889\nT\3T\3T\5T\u088d\nT\5T\u088f\n")
        buf.write("T\3U\3U\3U\3U\3U\3U\3U\5U\u0898\nU\3U\3U\3U\3U\3U\3U\3")
        buf.write("U\3U\3U\3U\5U\u08a4\nU\5U\u08a6\nU\3U\3U\3U\3U\3U\5U\u08ad")
        buf.write("\nU\3U\3U\3U\3U\3U\5U\u08b4\nU\3U\3U\3U\3U\5U\u08ba\n")
        buf.write("U\3U\3U\3U\3U\5U\u08c0\nU\5U\u08c2\nU\3V\3V\3V\7V\u08c7")
        buf.write("\nV\fV\16V\u08ca\13V\3W\3W\3W\7W\u08cf\nW\fW\16W\u08d2")
        buf.write("\13W\3X\3X\3X\5X\u08d7\nX\3X\3X\3Y\3Y\3Y\5Y\u08de\nY\3")
        buf.write("Y\3Y\3Z\3Z\5Z\u08e4\nZ\3Z\3Z\5Z\u08e8\nZ\5Z\u08ea\nZ\3")
        buf.write("[\3[\3[\7[\u08ef\n[\f[\16[\u08f2\13[\3\\\3\\\3\\\3\\\7")
        buf.write("\\\u08f8\n\\\f\\\16\\\u08fb\13\\\3\\\3\\\3]\3]\3]\3]\3")
        buf.write("]\3]\7]\u0905\n]\f]\16]\u0908\13]\3]\3]\5]\u090c\n]\3")
        buf.write("^\3^\5^\u0910\n^\3_\3_\3`\3`\3`\3`\3`\3`\3`\3`\3`\3`\5")
        buf.write("`\u091e\n`\5`\u0920\n`\3`\3`\3`\3`\3`\3`\7`\u0928\n`\f")
        buf.write("`\16`\u092b\13`\3a\5a\u092e\na\3a\3a\3a\3a\3a\3a\5a\u0936")
        buf.write("\na\3a\3a\3a\3a\3a\7a\u093d\na\fa\16a\u0940\13a\3a\3a")
        buf.write("\3a\5a\u0945\na\3a\3a\3a\3a\3a\3a\5a\u094d\na\3a\3a\3")
        buf.write("a\5a\u0952\na\3a\3a\3a\3a\3a\3a\3a\3a\7a\u095c\na\fa\16")
        buf.write("a\u095f\13a\3a\3a\5a\u0963\na\3a\5a\u0966\na\3a\3a\3a")
        buf.write("\3a\5a\u096c\na\3a\3a\5a\u0970\na\3a\3a\3a\5a\u0975\n")
        buf.write("a\3a\3a\3a\5a\u097a\na\3a\3a\3a\5a\u097f\na\3b\3b\3b\3")
        buf.write("b\5b\u0985\nb\3b\3b\3b\3b\3b\3b\3b\3b\3b\3b\3b\3b\3b\3")
        buf.write("b\3b\3b\3b\3b\3b\7b\u099a\nb\fb\16b\u099d\13b\3c\3c\3")
        buf.write("c\3c\6c\u09a3\nc\rc\16c\u09a4\3c\3c\5c\u09a9\nc\3c\3c")
        buf.write("\3c\3c\3c\6c\u09b0\nc\rc\16c\u09b1\3c\3c\5c\u09b6\nc\3")
        buf.write("c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\7c\u09c6\nc\f")
        buf.write("c\16c\u09c9\13c\5c\u09cb\nc\3c\3c\3c\3c\3c\3c\5c\u09d3")
        buf.write("\nc\3c\3c\3c\3c\3c\3c\3c\5c\u09dc\nc\3c\3c\3c\3c\3c\3")
        buf.write("c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\6c\u09f1\nc\r")
        buf.write("c\16c\u09f2\3c\3c\3c\3c\3c\3c\3c\3c\3c\5c\u09fe\nc\3c")
        buf.write("\3c\3c\7c\u0a03\nc\fc\16c\u0a06\13c\5c\u0a08\nc\3c\3c")
        buf.write("\3c\3c\3c\3c\3c\5c\u0a11\nc\3c\3c\5c\u0a15\nc\3c\3c\3")
        buf.write("c\3c\3c\3c\3c\3c\6c\u0a1f\nc\rc\16c\u0a20\3c\3c\3c\3c")
        buf.write("\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3")
        buf.write("c\3c\5c\u0a3a\nc\3c\3c\3c\3c\3c\5c\u0a41\nc\3c\5c\u0a44")
        buf.write("\nc\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\5c\u0a53\n")
        buf.write("c\3c\3c\5c\u0a57\nc\3c\3c\3c\3c\3c\3c\3c\3c\7c\u0a61\n")
        buf.write("c\fc\16c\u0a64\13c\3d\3d\3d\3d\3d\3d\3d\3d\6d\u0a6e\n")
        buf.write("d\rd\16d\u0a6f\5d\u0a72\nd\3e\3e\3f\3f\3g\3g\3h\3h\3i")
        buf.write("\3i\3i\5i\u0a7f\ni\3j\3j\5j\u0a83\nj\3k\3k\3k\6k\u0a88")
        buf.write("\nk\rk\16k\u0a89\3l\3l\3l\5l\u0a8f\nl\3m\3m\3m\3m\3m\3")
        buf.write("n\5n\u0a97\nn\3n\3n\5n\u0a9b\nn\3o\3o\3o\3o\3o\3o\3o\5")
        buf.write("o\u0aa4\no\3p\3p\3p\5p\u0aa9\np\3q\3q\3q\3q\3q\3q\3q\3")
        buf.write("q\3q\3q\3q\3q\3q\3q\3q\5q\u0aba\nq\3q\3q\5q\u0abe\nq\3")
        buf.write("q\3q\3q\3q\3q\7q\u0ac5\nq\fq\16q\u0ac8\13q\3q\5q\u0acb")
        buf.write("\nq\5q\u0acd\nq\3r\3r\3r\7r\u0ad2\nr\fr\16r\u0ad5\13r")
        buf.write("\3s\3s\3s\3s\5s\u0adb\ns\3s\5s\u0ade\ns\3s\5s\u0ae1\n")
        buf.write("s\3t\3t\3t\7t\u0ae6\nt\ft\16t\u0ae9\13t\3u\3u\3u\3u\5")
        buf.write("u\u0aef\nu\3u\5u\u0af2\nu\3v\3v\3v\7v\u0af7\nv\fv\16v")
        buf.write("\u0afa\13v\3w\3w\3w\3w\3w\5w\u0b01\nw\3w\5w\u0b04\nw\3")
        buf.write("x\3x\3x\3x\3x\3y\3y\3y\3y\7y\u0b0f\ny\fy\16y\u0b12\13")
        buf.write("y\3z\3z\3z\3z\3{\3{\3{\3{\3{\3{\3{\3{\3{\3{\3{\7{\u0b23")
        buf.write("\n{\f{\16{\u0b26\13{\3{\3{\3{\3{\3{\7{\u0b2d\n{\f{\16")
        buf.write("{\u0b30\13{\5{\u0b32\n{\3{\3{\3{\3{\3{\7{\u0b39\n{\f{")
        buf.write("\16{\u0b3c\13{\5{\u0b3e\n{\5{\u0b40\n{\3{\5{\u0b43\n{")
        buf.write("\3{\5{\u0b46\n{\3|\3|\3|\3|\3|\3|\3|\3|\3|\3|\3|\3|\3")
        buf.write("|\3|\3|\3|\5|\u0b58\n|\3}\3}\3}\3}\3}\3}\3}\5}\u0b61\n")
        buf.write("}\3~\3~\3~\7~\u0b66\n~\f~\16~\u0b69\13~\3\177\3\177\3")
        buf.write("\177\3\177\5\177\u0b6f\n\177\3\u0080\3\u0080\3\u0080\7")
        buf.write("\u0080\u0b74\n\u0080\f\u0080\16\u0080\u0b77\13\u0080\3")
        buf.write("\u0081\3\u0081\3\u0081\3\u0082\3\u0082\6\u0082\u0b7e\n")
        buf.write("\u0082\r\u0082\16\u0082\u0b7f\3\u0082\5\u0082\u0b83\n")
        buf.write("\u0082\3\u0083\3\u0083\3\u0083\5\u0083\u0b88\n\u0083\3")
        buf.write("\u0084\3\u0084\3\u0084\3\u0084\3\u0084\3\u0084\5\u0084")
        buf.write("\u0b90\n\u0084\3\u0085\3\u0085\3\u0086\3\u0086\5\u0086")
        buf.write("\u0b96\n\u0086\3\u0086\3\u0086\3\u0086\5\u0086\u0b9b\n")
        buf.write("\u0086\3\u0086\3\u0086\3\u0086\5\u0086\u0ba0\n\u0086\3")
        buf.write("\u0086\3\u0086\5\u0086\u0ba4\n\u0086\3\u0086\3\u0086\5")
        buf.write("\u0086\u0ba8\n\u0086\3\u0086\3\u0086\5\u0086\u0bac\n\u0086")
        buf.write("\3\u0086\3\u0086\5\u0086\u0bb0\n\u0086\3\u0086\3\u0086")
        buf.write("\5\u0086\u0bb4\n\u0086\3\u0086\3\u0086\5\u0086\u0bb8\n")
        buf.write("\u0086\3\u0086\5\u0086\u0bbb\n\u0086\3\u0087\3\u0087\3")
        buf.write("\u0087\3\u0087\3\u0087\3\u0087\3\u0087\5\u0087\u0bc4\n")
        buf.write("\u0087\3\u0088\3\u0088\3\u0089\3\u0089\3\u008a\3\u008a")
        buf.write("\3\u008a\7\u03a9\u03e7\u03f1\u03f8\u0400\6R\u00be\u00c2")
        buf.write("\u00c4\u008b\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"")
        buf.write("$&(*,.\60\62\64\668:<>@BDFHJLNPRTVXZ\\^`bdfhjlnprtvxz")
        buf.write("|~\u0080\u0082\u0084\u0086\u0088\u008a\u008c\u008e\u0090")
        buf.write("\u0092\u0094\u0096\u0098\u009a\u009c\u009e\u00a0\u00a2")
        buf.write("\u00a4\u00a6\u00a8\u00aa\u00ac\u00ae\u00b0\u00b2\u00b4")
        buf.write("\u00b6\u00b8\u00ba\u00bc\u00be\u00c0\u00c2\u00c4\u00c6")
        buf.write("\u00c8\u00ca\u00cc\u00ce\u00d0\u00d2\u00d4\u00d6\u00d8")
        buf.write("\u00da\u00dc\u00de\u00e0\u00e2\u00e4\u00e6\u00e8\u00ea")
        buf.write("\u00ec\u00ee\u00f0\u00f2\u00f4\u00f6\u00f8\u00fa\u00fc")
        buf.write("\u00fe\u0100\u0102\u0104\u0106\u0108\u010a\u010c\u010e")
        buf.write("\u0110\u0112\2,\4\2CC\u00b6\u00b6\4\2\"\"\u00c4\u00c4")
        buf.write("\4\2AA\u0098\u0098\4\2ffss\3\2-.\4\2\u00e5\u00e5\u0103")
        buf.write("\u0103\4\2\21\21%%\7\2**\66\66XXee\u008f\u008f\3\2GH\4")
        buf.write("\2XXee\4\2\u009c\u009c\u011d\u011d\4\2\16\16\u0089\u0089")
        buf.write("\5\2@@\u0097\u0097\u00ce\u00ce\6\2SSzz\u00d7\u00d7\u00f9")
        buf.write("\u00f9\5\2SS\u00d7\u00d7\u00f9\u00f9\4\2\31\31GG\4\2`")
        buf.write("`\u0081\u0081\4\2\20\20LL\4\2\u0121\u0121\u0123\u0123")
        buf.write("\5\2\20\20\25\25\u00db\u00db\5\2[[\u00f3\u00f3\u00fb\u00fb")
        buf.write("\4\2\u0112\u0113\u0118\u0118\3\2\u0114\u0117\4\2\u0112")
        buf.write("\u0113\u011b\u011b\4\2;;==\3\2\u00e3\u00e4\4\2\6\6ff\4")
        buf.write("\2\6\6bb\5\2\35\35\u0084\u0084\u00ee\u00ee\3\2\u010a\u0111")
        buf.write("\3\2\u0112\u011c\6\2\23\23ss\u009b\u009b\u00a3\u00a3\4")
        buf.write("\2[[\u00f3\u00f3\3\2\u0112\u0113\4\2MM\u00ac\u00ac\4\2")
        buf.write("\u00a4\u00a4\u00dc\u00dc\4\2aa\u00b3\u00b3\3\2\u0122\u0123")
        buf.write("\4\2NN\u00d6\u00d6\65\2\16\17\21\22\26\27\31\32\34\34")
        buf.write("\36\"%%\'*,,.\64\66\669:?ACKMNRRTZ]]_adehjmmprtuwy{{~")
        buf.write("~\u0080\u0083\u0086\u0093\u0096\u0098\u009a\u009a\u009d")
        buf.write("\u009e\u00a1\u00a2\u00a5\u00a5\u00a7\u00a8\u00aa\u00b3")
        buf.write("\u00b5\u00bd\u00bf\u00c5\u00c7\u00ce\u00d2\u00d4\u00d6")
        buf.write("\u00d6\u00d8\u00da\u00dc\u00e4\u00e6\u00ea\u00ed\u00ed")
        buf.write("\u00ef\u00f4\u00f6\u00f8\u00fc\u00ff\u0102\u0104\u0107")
        buf.write("\u0107\u0117\u0117\21\2\24\2488SSggvvzz\177\177\u0085")
        buf.write("\u0085\u0099\u0099\u009f\u009f\u00c6\u00c6\u00d1\u00d1")
        buf.write("\u00d7\u00d7\u00f9\u00f9\u0101\u0101\23\2\16\23\25\67")
        buf.write("9RTfhuwy{~\u0080\u0084\u0086\u0098\u009a\u009e\u00a0\u00c5")
        buf.write("\u00c7\u00d0\u00d2\u00d6\u00d8\u00f8\u00fa\u0100\u0102")
        buf.write("\u0109\u0117\u0117\2\u0da4\2\u0114\3\2\2\2\4\u011d\3\2")
        buf.write("\2\2\6\u0120\3\2\2\2\b\u0123\3\2\2\2\n\u0126\3\2\2\2\f")
        buf.write("\u0129\3\2\2\2\16\u012c\3\2\2\2\20\u0403\3\2\2\2\22\u04ad")
        buf.write("\3\2\2\2\24\u04af\3\2\2\2\26\u04c0\3\2\2\2\30\u04c6\3")
        buf.write("\2\2\2\32\u04d2\3\2\2\2\34\u04df\3\2\2\2\36\u04e2\3\2")
        buf.write("\2\2 \u04e6\3\2\2\2\"\u0522\3\2\2\2$\u0524\3\2\2\2&\u0528")
        buf.write("\3\2\2\2(\u0534\3\2\2\2*\u0539\3\2\2\2,\u0540\3\2\2\2")
        buf.write(".\u0542\3\2\2\2\60\u054a\3\2\2\2\62\u0553\3\2\2\2\64\u055e")
        buf.write("\3\2\2\2\66\u056d\3\2\2\28\u0570\3\2\2\2:\u057b\3\2\2")
        buf.write("\2<\u058b\3\2\2\2>\u0591\3\2\2\2@\u0593\3\2\2\2B\u059e")
        buf.write("\3\2\2\2D\u05af\3\2\2\2F\u05b6\3\2\2\2H\u05b8\3\2\2\2")
        buf.write("J\u05be\3\2\2\2L\u05f4\3\2\2\2N\u0600\3\2\2\2P\u0630\3")
        buf.write("\2\2\2R\u0633\3\2\2\2T\u0659\3\2\2\2V\u065b\3\2\2\2X\u0663")
        buf.write("\3\2\2\2Z\u0684\3\2\2\2\\\u06a3\3\2\2\2^\u06af\3\2\2\2")
        buf.write("`\u06cf\3\2\2\2b\u06db\3\2\2\2d\u06de\3\2\2\2f\u06e7\3")
        buf.write("\2\2\2h\u06f8\3\2\2\2j\u070c\3\2\2\2l\u070e\3\2\2\2n\u0716")
        buf.write("\3\2\2\2p\u071a\3\2\2\2r\u071d\3\2\2\2t\u0720\3\2\2\2")
        buf.write("v\u073a\3\2\2\2x\u073c\3\2\2\2z\u077a\3\2\2\2|\u0789\3")
        buf.write("\2\2\2~\u078b\3\2\2\2\u0080\u07a9\3\2\2\2\u0082\u07ab")
        buf.write("\3\2\2\2\u0084\u07b2\3\2\2\2\u0086\u07d2\3\2\2\2\u0088")
        buf.write("\u07d4\3\2\2\2\u008a\u07e6\3\2\2\2\u008c\u0800\3\2\2\2")
        buf.write("\u008e\u0806\3\2\2\2\u0090\u0808\3\2\2\2\u0092\u0827\3")
        buf.write("\2\2\2\u0094\u0829\3\2\2\2\u0096\u082d\3\2\2\2\u0098\u0835")
        buf.write("\3\2\2\2\u009a\u0840\3\2\2\2\u009c\u0844\3\2\2\2\u009e")
        buf.write("\u084f\3\2\2\2\u00a0\u086b\3\2\2\2\u00a2\u086d\3\2\2\2")
        buf.write("\u00a4\u0878\3\2\2\2\u00a6\u088e\3\2\2\2\u00a8\u08c1\3")
        buf.write("\2\2\2\u00aa\u08c3\3\2\2\2\u00ac\u08cb\3\2\2\2\u00ae\u08d6")
        buf.write("\3\2\2\2\u00b0\u08dd\3\2\2\2\u00b2\u08e1\3\2\2\2\u00b4")
        buf.write("\u08eb\3\2\2\2\u00b6\u08f3\3\2\2\2\u00b8\u090b\3\2\2\2")
        buf.write("\u00ba\u090f\3\2\2\2\u00bc\u0911\3\2\2\2\u00be\u091f\3")
        buf.write("\2\2\2\u00c0\u097e\3\2\2\2\u00c2\u0984\3\2\2\2\u00c4\u0a56")
        buf.write("\3\2\2\2\u00c6\u0a71\3\2\2\2\u00c8\u0a73\3\2\2\2\u00ca")
        buf.write("\u0a75\3\2\2\2\u00cc\u0a77\3\2\2\2\u00ce\u0a79\3\2\2\2")
        buf.write("\u00d0\u0a7b\3\2\2\2\u00d2\u0a80\3\2\2\2\u00d4\u0a87\3")
        buf.write("\2\2\2\u00d6\u0a8b\3\2\2\2\u00d8\u0a90\3\2\2\2\u00da\u0a9a")
        buf.write("\3\2\2\2\u00dc\u0aa3\3\2\2\2\u00de\u0aa8\3\2\2\2\u00e0")
        buf.write("\u0acc\3\2\2\2\u00e2\u0ace\3\2\2\2\u00e4\u0ad6\3\2\2\2")
        buf.write("\u00e6\u0ae2\3\2\2\2\u00e8\u0aea\3\2\2\2\u00ea\u0af3\3")
        buf.write("\2\2\2\u00ec\u0afb\3\2\2\2\u00ee\u0b05\3\2\2\2\u00f0\u0b0a")
        buf.write("\3\2\2\2\u00f2\u0b13\3\2\2\2\u00f4\u0b45\3\2\2\2\u00f6")
        buf.write("\u0b57\3\2\2\2\u00f8\u0b60\3\2\2\2\u00fa\u0b62\3\2\2\2")
        buf.write("\u00fc\u0b6e\3\2\2\2\u00fe\u0b70\3\2\2\2\u0100\u0b78\3")
        buf.write("\2\2\2\u0102\u0b82\3\2\2\2\u0104\u0b87\3\2\2\2\u0106\u0b8f")
        buf.write("\3\2\2\2\u0108\u0b91\3\2\2\2\u010a\u0bba\3\2\2\2\u010c")
        buf.write("\u0bc3\3\2\2\2\u010e\u0bc5\3\2\2\2\u0110\u0bc7\3\2\2\2")
        buf.write("\u0112\u0bc9\3\2\2\2\u0114\u0118\5\20\t\2\u0115\u0117")
        buf.write("\7\3\2\2\u0116\u0115\3\2\2\2\u0117\u011a\3\2\2\2\u0118")
        buf.write("\u0116\3\2\2\2\u0118\u0119\3\2\2\2\u0119\u011b\3\2\2\2")
        buf.write("\u011a\u0118\3\2\2\2\u011b\u011c\7\2\2\3\u011c\3\3\2\2")
        buf.write("\2\u011d\u011e\5\u00b2Z\2\u011e\u011f\7\2\2\3\u011f\5")
        buf.write("\3\2\2\2\u0120\u0121\5\u00aeX\2\u0121\u0122\7\2\2\3\u0122")
        buf.write("\7\3\2\2\2\u0123\u0124\5\u00acW\2\u0124\u0125\7\2\2\3")
        buf.write("\u0125\t\3\2\2\2\u0126\u0127\5\u00b0Y\2\u0127\u0128\7")
        buf.write("\2\2\3\u0128\13\3\2\2\2\u0129\u012a\5\u00e0q\2\u012a\u012b")
        buf.write("\7\2\2\3\u012b\r\3\2\2\2\u012c\u012d\5\u00e6t\2\u012d")
        buf.write("\u012e\7\2\2\3\u012e\17\3\2\2\2\u012f\u0404\5 \21\2\u0130")
        buf.write("\u0132\5\60\31\2\u0131\u0130\3\2\2\2\u0131\u0132\3\2\2")
        buf.write("\2\u0132\u0133\3\2\2\2\u0133\u0404\5L\'\2\u0134\u0136")
        buf.write("\7\u00ff\2\2\u0135\u0137\7\u0097\2\2\u0136\u0135\3\2\2")
        buf.write("\2\u0136\u0137\3\2\2\2\u0137\u0138\3\2\2\2\u0138\u0404")
        buf.write("\5\u00acW\2\u0139\u013a\7\67\2\2\u013a\u013e\5*\26\2\u013b")
        buf.write("\u013c\7p\2\2\u013c\u013d\7\u009b\2\2\u013d\u013f\7U\2")
        buf.write("\2\u013e\u013b\3\2\2\2\u013e\u013f\3\2\2\2\u013f\u0140")
        buf.write("\3\2\2\2\u0140\u0148\5\u00acW\2\u0141\u0147\5\36\20\2")
        buf.write("\u0142\u0147\5\34\17\2\u0143\u0144\7\u0108\2\2\u0144\u0145")
        buf.write("\t\2\2\2\u0145\u0147\58\35\2\u0146\u0141\3\2\2\2\u0146")
        buf.write("\u0142\3\2\2\2\u0146\u0143\3\2\2\2\u0147\u014a\3\2\2\2")
        buf.write("\u0148\u0146\3\2\2\2\u0148\u0149\3\2\2\2\u0149\u0404\3")
        buf.write("\2\2\2\u014a\u0148\3\2\2\2\u014b\u014c\7\21\2\2\u014c")
        buf.write("\u014d\5*\26\2\u014d\u014e\5\u00acW\2\u014e\u014f\7\u00d6")
        buf.write("\2\2\u014f\u0150\t\2\2\2\u0150\u0151\58\35\2\u0151\u0404")
        buf.write("\3\2\2\2\u0152\u0153\7\21\2\2\u0153\u0154\5*\26\2\u0154")
        buf.write("\u0155\5\u00acW\2\u0155\u0156\7\u00d6\2\2\u0156\u0157")
        buf.write("\5\34\17\2\u0157\u0404\3\2\2\2\u0158\u0159\7N\2\2\u0159")
        buf.write("\u015c\5*\26\2\u015a\u015b\7p\2\2\u015b\u015d\7U\2\2\u015c")
        buf.write("\u015a\3\2\2\2\u015c\u015d\3\2\2\2\u015d\u015e\3\2\2\2")
        buf.write("\u015e\u0160\5\u00acW\2\u015f\u0161\t\3\2\2\u0160\u015f")
        buf.write("\3\2\2\2\u0160\u0161\3\2\2\2\u0161\u0404\3\2\2\2\u0162")
        buf.write("\u0163\7\u00d9\2\2\u0163\u0166\t\4\2\2\u0164\u0165\t\5")
        buf.write("\2\2\u0165\u0167\5\u00acW\2\u0166\u0164\3\2\2\2\u0166")
        buf.write("\u0167\3\2\2\2\u0167\u016c\3\2\2\2\u0168\u016a\7\u0086")
        buf.write("\2\2\u0169\u0168\3\2\2\2\u0169\u016a\3\2\2\2\u016a\u016b")
        buf.write("\3\2\2\2\u016b\u016d\7\u011d\2\2\u016c\u0169\3\2\2\2\u016c")
        buf.write("\u016d\3\2\2\2\u016d\u0404\3\2\2\2\u016e\u016f\6\t\2\2")
        buf.write("\u016f\u0174\5\24\13\2\u0170\u0171\7\4\2\2\u0171\u0172")
        buf.write("\5\u00e6t\2\u0172\u0173\7\5\2\2\u0173\u0175\3\2\2\2\u0174")
        buf.write("\u0170\3\2\2\2\u0174\u0175\3\2\2\2\u0175\u0177\3\2\2\2")
        buf.write("\u0176\u0178\5\64\33\2\u0177\u0176\3\2\2\2\u0177\u0178")
        buf.write("\3\2\2\2\u0178\u0179\3\2\2\2\u0179\u017e\5\66\34\2\u017a")
        buf.write("\u017c\7\30\2\2\u017b\u017a\3\2\2\2\u017b\u017c\3\2\2")
        buf.write("\2\u017c\u017d\3\2\2\2\u017d\u017f\5 \21\2\u017e\u017b")
        buf.write("\3\2\2\2\u017e\u017f\3\2\2\2\u017f\u0404\3\2\2\2\u0180")
        buf.write("\u0181\6\t\3\2\u0181\u0186\5\24\13\2\u0182\u0183\7\4\2")
        buf.write("\2\u0183\u0184\5\u00e6t\2\u0184\u0185\7\5\2\2\u0185\u0187")
        buf.write("\3\2\2\2\u0186\u0182\3\2\2\2\u0186\u0187\3\2\2\2\u0187")
        buf.write("\u0188\3\2\2\2\u0188\u0189\5\64\33\2\u0189\u018e\5\66")
        buf.write("\34\2\u018a\u018c\7\30\2\2\u018b\u018a\3\2\2\2\u018b\u018c")
        buf.write("\3\2\2\2\u018c\u018d\3\2\2\2\u018d\u018f\5 \21\2\u018e")
        buf.write("\u018b\3\2\2\2\u018e\u018f\3\2\2\2\u018f\u0404\3\2\2\2")
        buf.write("\u0190\u0195\5\24\13\2\u0191\u0192\7\4\2\2\u0192\u0193")
        buf.write("\5\u00e6t\2\u0193\u0194\7\5\2\2\u0194\u0196\3\2\2\2\u0195")
        buf.write("\u0191\3\2\2\2\u0195\u0196\3\2\2\2\u0196\u01ac\3\2\2\2")
        buf.write("\u0197\u01ab\5\36\20\2\u0198\u0199\7\u00ad\2\2\u0199\u019a")
        buf.write("\7 \2\2\u019a\u019b\7\4\2\2\u019b\u019c\5\u00e6t\2\u019c")
        buf.write("\u019d\7\5\2\2\u019d\u01a2\3\2\2\2\u019e\u019f\7\u00ad")
        buf.write("\2\2\u019f\u01a0\7 \2\2\u01a0\u01a2\5\u0094K\2\u01a1\u0198")
        buf.write("\3\2\2\2\u01a1\u019e\3\2\2\2\u01a2\u01ab\3\2\2\2\u01a3")
        buf.write("\u01ab\5\30\r\2\u01a4\u01ab\5\32\16\2\u01a5\u01ab\5\u00a8")
        buf.write("U\2\u01a6\u01ab\5D#\2\u01a7\u01ab\5\34\17\2\u01a8\u01a9")
        buf.write("\7\u00e8\2\2\u01a9\u01ab\58\35\2\u01aa\u0197\3\2\2\2\u01aa")
        buf.write("\u01a1\3\2\2\2\u01aa\u01a3\3\2\2\2\u01aa\u01a4\3\2\2\2")
        buf.write("\u01aa\u01a5\3\2\2\2\u01aa\u01a6\3\2\2\2\u01aa\u01a7\3")
        buf.write("\2\2\2\u01aa\u01a8\3\2\2\2\u01ab\u01ae\3\2\2\2\u01ac\u01aa")
        buf.write("\3\2\2\2\u01ac\u01ad\3\2\2\2\u01ad\u01b3\3\2\2\2\u01ae")
        buf.write("\u01ac\3\2\2\2\u01af\u01b1\7\30\2\2\u01b0\u01af\3\2\2")
        buf.write("\2\u01b0\u01b1\3\2\2\2\u01b1\u01b2\3\2\2\2\u01b2\u01b4")
        buf.write("\5 \21\2\u01b3\u01b0\3\2\2\2\u01b3\u01b4\3\2\2\2\u01b4")
        buf.write("\u0404\3\2\2\2\u01b5\u01b6\7\67\2\2\u01b6\u01ba\7\u00e5")
        buf.write("\2\2\u01b7\u01b8\7p\2\2\u01b8\u01b9\7\u009b\2\2\u01b9")
        buf.write("\u01bb\7U\2\2\u01ba\u01b7\3\2\2\2\u01ba\u01bb\3\2\2\2")
        buf.write("\u01bb\u01bc\3\2\2\2\u01bc\u01bd\5\u00aeX\2\u01bd\u01be")
        buf.write("\7\u0086\2\2\u01be\u01c7\5\u00aeX\2\u01bf\u01c6\5\64\33")
        buf.write("\2\u01c0\u01c6\5\u00a8U\2\u01c1\u01c6\5D#\2\u01c2\u01c6")
        buf.write("\5\34\17\2\u01c3\u01c4\7\u00e8\2\2\u01c4\u01c6\58\35\2")
        buf.write("\u01c5\u01bf\3\2\2\2\u01c5\u01c0\3\2\2\2\u01c5\u01c1\3")
        buf.write("\2\2\2\u01c5\u01c2\3\2\2\2\u01c5\u01c3\3\2\2\2\u01c6\u01c9")
        buf.write("\3\2\2\2\u01c7\u01c5\3\2\2\2\u01c7\u01c8\3\2\2\2\u01c8")
        buf.write("\u0404\3\2\2\2\u01c9\u01c7\3\2\2\2\u01ca\u01cf\5\26\f")
        buf.write("\2\u01cb\u01cc\7\4\2\2\u01cc\u01cd\5\u00e6t\2\u01cd\u01ce")
        buf.write("\7\5\2\2\u01ce\u01d0\3\2\2\2\u01cf\u01cb\3\2\2\2\u01cf")
        buf.write("\u01d0\3\2\2\2\u01d0\u01d1\3\2\2\2\u01d1\u01d2\5\64\33")
        buf.write("\2\u01d2\u01d7\5\66\34\2\u01d3\u01d5\7\30\2\2\u01d4\u01d3")
        buf.write("\3\2\2\2\u01d4\u01d5\3\2\2\2\u01d5\u01d6\3\2\2\2\u01d6")
        buf.write("\u01d8\5 \21\2\u01d7\u01d4\3\2\2\2\u01d7\u01d8\3\2\2\2")
        buf.write("\u01d8\u0404\3\2\2\2\u01d9\u01da\7\22\2\2\u01da\u01db")
        buf.write("\7\u00e5\2\2\u01db\u01dd\5\u00acW\2\u01dc\u01de\5&\24")
        buf.write("\2\u01dd\u01dc\3\2\2\2\u01dd\u01de\3\2\2\2\u01de\u01df")
        buf.write("\3\2\2\2\u01df\u01e0\7\63\2\2\u01e0\u01e8\7\u00df\2\2")
        buf.write("\u01e1\u01e9\5\u0104\u0083\2\u01e2\u01e3\7b\2\2\u01e3")
        buf.write("\u01e4\7.\2\2\u01e4\u01e9\5\u0096L\2\u01e5\u01e6\7b\2")
        buf.write("\2\u01e6\u01e7\7\20\2\2\u01e7\u01e9\7.\2\2\u01e8\u01e1")
        buf.write("\3\2\2\2\u01e8\u01e2\3\2\2\2\u01e8\u01e5\3\2\2\2\u01e8")
        buf.write("\u01e9\3\2\2\2\u01e9\u0404\3\2\2\2\u01ea\u01eb\7\21\2")
        buf.write("\2\u01eb\u01ec\7\u00e5\2\2\u01ec\u01ed\5\u00acW\2\u01ed")
        buf.write("\u01ee\7\16\2\2\u01ee\u01ef\t\6\2\2\u01ef\u01f0\5\u00e2")
        buf.write("r\2\u01f0\u0404\3\2\2\2\u01f1\u01f2\7\21\2\2\u01f2\u01f3")
        buf.write("\7\u00e5\2\2\u01f3\u01f4\5\u00acW\2\u01f4\u01f5\7\16\2")
        buf.write("\2\u01f5\u01f6\t\6\2\2\u01f6\u01f7\7\4\2\2\u01f7\u01f8")
        buf.write("\5\u00e2r\2\u01f8\u01f9\7\5\2\2\u01f9\u0404\3\2\2\2\u01fa")
        buf.write("\u01fb\7\21\2\2\u01fb\u01fc\7\u00e5\2\2\u01fc\u01fd\5")
        buf.write("\u00acW\2\u01fd\u01fe\7\u00c0\2\2\u01fe\u01ff\7-\2\2\u01ff")
        buf.write("\u0200\5\u00acW\2\u0200\u0201\7\u00ec\2\2\u0201\u0202")
        buf.write("\5\u0100\u0081\2\u0202\u0404\3\2\2\2\u0203\u0204\7\21")
        buf.write("\2\2\u0204\u0205\7\u00e5\2\2\u0205\u0206\5\u00acW\2\u0206")
        buf.write("\u0207\7N\2\2\u0207\u0208\t\6\2\2\u0208\u0209\7\4\2\2")
        buf.write("\u0209\u020a\5\u00aaV\2\u020a\u020b\7\5\2\2\u020b\u0404")
        buf.write("\3\2\2\2\u020c\u020d\7\21\2\2\u020d\u020e\7\u00e5\2\2")
        buf.write("\u020e\u020f\5\u00acW\2\u020f\u0210\7N\2\2\u0210\u0211")
        buf.write("\t\6\2\2\u0211\u0212\5\u00aaV\2\u0212\u0404\3\2\2\2\u0213")
        buf.write("\u0214\7\21\2\2\u0214\u0215\t\7\2\2\u0215\u0216\5\u00ac")
        buf.write("W\2\u0216\u0217\7\u00c0\2\2\u0217\u0218\7\u00ec\2\2\u0218")
        buf.write("\u0219\5\u00acW\2\u0219\u0404\3\2\2\2\u021a\u021b\7\21")
        buf.write("\2\2\u021b\u021c\t\7\2\2\u021c\u021d\5\u00acW\2\u021d")
        buf.write("\u021e\7\u00d6\2\2\u021e\u021f\7\u00e8\2\2\u021f\u0220")
        buf.write("\58\35\2\u0220\u0404\3\2\2\2\u0221\u0222\7\21\2\2\u0222")
        buf.write("\u0223\t\7\2\2\u0223\u0224\5\u00acW\2\u0224\u0225\7\u00fd")
        buf.write("\2\2\u0225\u0228\7\u00e8\2\2\u0226\u0227\7p\2\2\u0227")
        buf.write("\u0229\7U\2\2\u0228\u0226\3\2\2\2\u0228\u0229\3\2\2\2")
        buf.write("\u0229\u022a\3\2\2\2\u022a\u022b\58\35\2\u022b\u0404\3")
        buf.write("\2\2\2\u022c\u022d\7\21\2\2\u022d\u022e\7\u00e5\2\2\u022e")
        buf.write("\u022f\5\u00acW\2\u022f\u0231\t\b\2\2\u0230\u0232\7-\2")
        buf.write("\2\u0231\u0230\3\2\2\2\u0231\u0232\3\2\2\2\u0232\u0233")
        buf.write("\3\2\2\2\u0233\u0235\5\u00acW\2\u0234\u0236\5\u010c\u0087")
        buf.write("\2\u0235\u0234\3\2\2\2\u0235\u0236\3\2\2\2\u0236\u0404")
        buf.write("\3\2\2\2\u0237\u0238\7\21\2\2\u0238\u0239\7\u00e5\2\2")
        buf.write("\u0239\u023b\5\u00acW\2\u023a\u023c\5&\24\2\u023b\u023a")
        buf.write("\3\2\2\2\u023b\u023c\3\2\2\2\u023c\u023d\3\2\2\2\u023d")
        buf.write("\u023f\7%\2\2\u023e\u0240\7-\2\2\u023f\u023e\3\2\2\2\u023f")
        buf.write("\u0240\3\2\2\2\u0240\u0241\3\2\2\2\u0241\u0242\5\u00ac")
        buf.write("W\2\u0242\u0244\5\u00e8u\2\u0243\u0245\5\u00dep\2\u0244")
        buf.write("\u0243\3\2\2\2\u0244\u0245\3\2\2\2\u0245\u0404\3\2\2\2")
        buf.write("\u0246\u0247\7\21\2\2\u0247\u0248\7\u00e5\2\2\u0248\u024a")
        buf.write("\5\u00acW\2\u0249\u024b\5&\24\2\u024a\u0249\3\2\2\2\u024a")
        buf.write("\u024b\3\2\2\2\u024b\u024c\3\2\2\2\u024c\u024d\7\u00c2")
        buf.write("\2\2\u024d\u024e\7.\2\2\u024e\u024f\7\4\2\2\u024f\u0250")
        buf.write("\5\u00e2r\2\u0250\u0251\7\5\2\2\u0251\u0404\3\2\2\2\u0252")
        buf.write("\u0253\7\21\2\2\u0253\u0254\7\u00e5\2\2\u0254\u0256\5")
        buf.write("\u00acW\2\u0255\u0257\5&\24\2\u0256\u0255\3\2\2\2\u0256")
        buf.write("\u0257\3\2\2\2\u0257\u0258\3\2\2\2\u0258\u0259\7\u00d6")
        buf.write("\2\2\u0259\u025a\7\u00d3\2\2\u025a\u025e\7\u011d\2\2\u025b")
        buf.write("\u025c\7\u0108\2\2\u025c\u025d\7\u00d4\2\2\u025d\u025f")
        buf.write("\58\35\2\u025e\u025b\3\2\2\2\u025e\u025f\3\2\2\2\u025f")
        buf.write("\u0404\3\2\2\2\u0260\u0261\7\21\2\2\u0261\u0262\7\u00e5")
        buf.write("\2\2\u0262\u0264\5\u00acW\2\u0263\u0265\5&\24\2\u0264")
        buf.write("\u0263\3\2\2\2\u0264\u0265\3\2\2\2\u0265\u0266\3\2\2\2")
        buf.write("\u0266\u0267\7\u00d6\2\2\u0267\u0268\7\u00d4\2\2\u0268")
        buf.write("\u0269\58\35\2\u0269\u0404\3\2\2\2\u026a\u026b\7\21\2")
        buf.write("\2\u026b\u026c\t\7\2\2\u026c\u026d\5\u00acW\2\u026d\u0271")
        buf.write("\7\16\2\2\u026e\u026f\7p\2\2\u026f\u0270\7\u009b\2\2\u0270")
        buf.write("\u0272\7U\2\2\u0271\u026e\3\2\2\2\u0271\u0272\3\2\2\2")
        buf.write("\u0272\u0274\3\2\2\2\u0273\u0275\5$\23\2\u0274\u0273\3")
        buf.write("\2\2\2\u0275\u0276\3\2\2\2\u0276\u0274\3\2\2\2\u0276\u0277")
        buf.write("\3\2\2\2\u0277\u0404\3\2\2\2\u0278\u0279\7\21\2\2\u0279")
        buf.write("\u027a\7\u00e5\2\2\u027a\u027b\5\u00acW\2\u027b\u027c")
        buf.write("\5&\24\2\u027c\u027d\7\u00c0\2\2\u027d\u027e\7\u00ec\2")
        buf.write("\2\u027e\u027f\5&\24\2\u027f\u0404\3\2\2\2\u0280\u0281")
        buf.write("\7\21\2\2\u0281\u0282\t\7\2\2\u0282\u0283\5\u00acW\2\u0283")
        buf.write("\u0286\7N\2\2\u0284\u0285\7p\2\2\u0285\u0287\7U\2\2\u0286")
        buf.write("\u0284\3\2\2\2\u0286\u0287\3\2\2\2\u0287\u0288\3\2\2\2")
        buf.write("\u0288\u028d\5&\24\2\u0289\u028a\7\6\2\2\u028a\u028c\5")
        buf.write("&\24\2\u028b\u0289\3\2\2\2\u028c\u028f\3\2\2\2\u028d\u028b")
        buf.write("\3\2\2\2\u028d\u028e\3\2\2\2\u028e\u0291\3\2\2\2\u028f")
        buf.write("\u028d\3\2\2\2\u0290\u0292\7\u00b7\2\2\u0291\u0290\3\2")
        buf.write("\2\2\u0291\u0292\3\2\2\2\u0292\u0404\3\2\2\2\u0293\u0294")
        buf.write("\7\21\2\2\u0294\u0295\7\u00e5\2\2\u0295\u0297\5\u00ac")
        buf.write("W\2\u0296\u0298\5&\24\2\u0297\u0296\3\2\2\2\u0297\u0298")
        buf.write("\3\2\2\2\u0298\u0299\3\2\2\2\u0299\u029a\7\u00d6\2\2\u029a")
        buf.write("\u029b\5\34\17\2\u029b\u0404\3\2\2\2\u029c\u029d\7\21")
        buf.write("\2\2\u029d\u029e\7\u00e5\2\2\u029e\u029f\5\u00acW\2\u029f")
        buf.write("\u02a0\7\u00bc\2\2\u02a0\u02a1\7\u00ae\2\2\u02a1\u0404")
        buf.write("\3\2\2\2\u02a2\u02a3\7N\2\2\u02a3\u02a6\7\u00e5\2\2\u02a4")
        buf.write("\u02a5\7p\2\2\u02a5\u02a7\7U\2\2\u02a6\u02a4\3\2\2\2\u02a6")
        buf.write("\u02a7\3\2\2\2\u02a7\u02a8\3\2\2\2\u02a8\u02aa\5\u00ac")
        buf.write("W\2\u02a9\u02ab\7\u00b7\2\2\u02aa\u02a9\3\2\2\2\u02aa")
        buf.write("\u02ab\3\2\2\2\u02ab\u0404\3\2\2\2\u02ac\u02ad\7N\2\2")
        buf.write("\u02ad\u02b0\7\u0103\2\2\u02ae\u02af\7p\2\2\u02af\u02b1")
        buf.write("\7U\2\2\u02b0\u02ae\3\2\2\2\u02b0\u02b1\3\2\2\2\u02b1")
        buf.write("\u02b2\3\2\2\2\u02b2\u0404\5\u00acW\2\u02b3\u02b6\7\67")
        buf.write("\2\2\u02b4\u02b5\7\u00a3\2\2\u02b5\u02b7\7\u00c2\2\2\u02b6")
        buf.write("\u02b4\3\2\2\2\u02b6\u02b7\3\2\2\2\u02b7\u02bc\3\2\2\2")
        buf.write("\u02b8\u02ba\7j\2\2\u02b9\u02b8\3\2\2\2\u02b9\u02ba\3")
        buf.write("\2\2\2\u02ba\u02bb\3\2\2\2\u02bb\u02bd\7\u00e9\2\2\u02bc")
        buf.write("\u02b9\3\2\2\2\u02bc\u02bd\3\2\2\2\u02bd\u02be\3\2\2\2")
        buf.write("\u02be\u02c2\7\u0103\2\2\u02bf\u02c0\7p\2\2\u02c0\u02c1")
        buf.write("\7\u009b\2\2\u02c1\u02c3\7U\2\2\u02c2\u02bf\3\2\2\2\u02c2")
        buf.write("\u02c3\3\2\2\2\u02c3\u02c4\3\2\2\2\u02c4\u02c6\5\u00ac")
        buf.write("W\2\u02c5\u02c7\5\u009cO\2\u02c6\u02c5\3\2\2\2\u02c6\u02c7")
        buf.write("\3\2\2\2\u02c7\u02d0\3\2\2\2\u02c8\u02cf\5\36\20\2\u02c9")
        buf.write("\u02ca\7\u00ad\2\2\u02ca\u02cb\7\u009f\2\2\u02cb\u02cf")
        buf.write("\5\u0094K\2\u02cc\u02cd\7\u00e8\2\2\u02cd\u02cf\58\35")
        buf.write("\2\u02ce\u02c8\3\2\2\2\u02ce\u02c9\3\2\2\2\u02ce\u02cc")
        buf.write("\3\2\2\2\u02cf\u02d2\3\2\2\2\u02d0\u02ce\3\2\2\2\u02d0")
        buf.write("\u02d1\3\2\2\2\u02d1\u02d3\3\2\2\2\u02d2\u02d0\3\2\2\2")
        buf.write("\u02d3\u02d4\7\30\2\2\u02d4\u02d5\5 \21\2\u02d5\u0404")
        buf.write("\3\2\2\2\u02d6\u02d9\7\67\2\2\u02d7\u02d8\7\u00a3\2\2")
        buf.write("\u02d8\u02da\7\u00c2\2\2\u02d9\u02d7\3\2\2\2\u02d9\u02da")
        buf.write("\3\2\2\2\u02da\u02dc\3\2\2\2\u02db\u02dd\7j\2\2\u02dc")
        buf.write("\u02db\3\2\2\2\u02dc\u02dd\3\2\2\2\u02dd\u02de\3\2\2\2")
        buf.write("\u02de\u02df\7\u00e9\2\2\u02df\u02e0\7\u0103\2\2\u02e0")
        buf.write("\u02e5\5\u00aeX\2\u02e1\u02e2\7\4\2\2\u02e2\u02e3\5\u00e6")
        buf.write("t\2\u02e3\u02e4\7\5\2\2\u02e4\u02e6\3\2\2\2\u02e5\u02e1")
        buf.write("\3\2\2\2\u02e5\u02e6\3\2\2\2\u02e6\u02e7\3\2\2\2\u02e7")
        buf.write("\u02ea\5\64\33\2\u02e8\u02e9\7\u00a2\2\2\u02e9\u02eb\5")
        buf.write("8\35\2\u02ea\u02e8\3\2\2\2\u02ea\u02eb\3\2\2\2\u02eb\u0404")
        buf.write("\3\2\2\2\u02ec\u02ed\7\21\2\2\u02ed\u02ee\7\u0103\2\2")
        buf.write("\u02ee\u02f0\5\u00acW\2\u02ef\u02f1\7\30\2\2\u02f0\u02ef")
        buf.write("\3\2\2\2\u02f0\u02f1\3\2\2\2\u02f1\u02f2\3\2\2\2\u02f2")
        buf.write("\u02f3\5 \21\2\u02f3\u0404\3\2\2\2\u02f4\u02f7\7\67\2")
        buf.write("\2\u02f5\u02f6\7\u00a3\2\2\u02f6\u02f8\7\u00c2\2\2\u02f7")
        buf.write("\u02f5\3\2\2\2\u02f7\u02f8\3\2\2\2\u02f8\u02fa\3\2\2\2")
        buf.write("\u02f9\u02fb\7\u00e9\2\2\u02fa\u02f9\3\2\2\2\u02fa\u02fb")
        buf.write("\3\2\2\2\u02fb\u02fc\3\2\2\2\u02fc\u0300\7h\2\2\u02fd")
        buf.write("\u02fe\7p\2\2\u02fe\u02ff\7\u009b\2\2\u02ff\u0301\7U\2")
        buf.write("\2\u0300\u02fd\3\2\2\2\u0300\u0301\3\2\2\2\u0301\u0302")
        buf.write("\3\2\2\2\u0302\u0303\5\u00acW\2\u0303\u0304\7\30\2\2\u0304")
        buf.write("\u030e\7\u011d\2\2\u0305\u0306\7\u0101\2\2\u0306\u030b")
        buf.write("\5J&\2\u0307\u0308\7\6\2\2\u0308\u030a\5J&\2\u0309\u0307")
        buf.write("\3\2\2\2\u030a\u030d\3\2\2\2\u030b\u0309\3\2\2\2\u030b")
        buf.write("\u030c\3\2\2\2\u030c\u030f\3\2\2\2\u030d\u030b\3\2\2\2")
        buf.write("\u030e\u0305\3\2\2\2\u030e\u030f\3\2\2\2\u030f\u0404\3")
        buf.write("\2\2\2\u0310\u0312\7N\2\2\u0311\u0313\7\u00e9\2\2\u0312")
        buf.write("\u0311\3\2\2\2\u0312\u0313\3\2\2\2\u0313\u0314\3\2\2\2")
        buf.write("\u0314\u0317\7h\2\2\u0315\u0316\7p\2\2\u0316\u0318\7U")
        buf.write("\2\2\u0317\u0315\3\2\2\2\u0317\u0318\3\2\2\2\u0318\u0319")
        buf.write("\3\2\2\2\u0319\u0404\5\u00acW\2\u031a\u031c\7V\2\2\u031b")
        buf.write("\u031d\t\t\2\2\u031c\u031b\3\2\2\2\u031c\u031d\3\2\2\2")
        buf.write("\u031d\u031e\3\2\2\2\u031e\u0404\5\20\t\2\u031f\u0320")
        buf.write("\7\u00d9\2\2\u0320\u0323\7\u00e6\2\2\u0321\u0322\t\5\2")
        buf.write("\2\u0322\u0324\5\u00acW\2\u0323\u0321\3\2\2\2\u0323\u0324")
        buf.write("\3\2\2\2\u0324\u0329\3\2\2\2\u0325\u0327\7\u0086\2\2\u0326")
        buf.write("\u0325\3\2\2\2\u0326\u0327\3\2\2\2\u0327\u0328\3\2\2\2")
        buf.write("\u0328\u032a\7\u011d\2\2\u0329\u0326\3\2\2\2\u0329\u032a")
        buf.write("\3\2\2\2\u032a\u0404\3\2\2\2\u032b\u032c\7\u00d9\2\2\u032c")
        buf.write("\u032d\7\u00e5\2\2\u032d\u0330\7X\2\2\u032e\u032f\t\5")
        buf.write("\2\2\u032f\u0331\5\u00acW\2\u0330\u032e\3\2\2\2\u0330")
        buf.write("\u0331\3\2\2\2\u0331\u0332\3\2\2\2\u0332\u0333\7\u0086")
        buf.write("\2\2\u0333\u0335\7\u011d\2\2\u0334\u0336\5&\24\2\u0335")
        buf.write("\u0334\3\2\2\2\u0335\u0336\3\2\2\2\u0336\u0404\3\2\2\2")
        buf.write("\u0337\u0338\7\u00d9\2\2\u0338\u0339\7\u00e8\2\2\u0339")
        buf.write("\u033e\5\u00acW\2\u033a\u033b\7\4\2\2\u033b\u033c\5<\37")
        buf.write("\2\u033c\u033d\7\5\2\2\u033d\u033f\3\2\2\2\u033e\u033a")
        buf.write("\3\2\2\2\u033e\u033f\3\2\2\2\u033f\u0404\3\2\2\2\u0340")
        buf.write("\u0341\7\u00d9\2\2\u0341\u0342\7.\2\2\u0342\u0343\t\5")
        buf.write("\2\2\u0343\u0346\5\u00acW\2\u0344\u0345\t\5\2\2\u0345")
        buf.write("\u0347\5\u00acW\2\u0346\u0344\3\2\2\2\u0346\u0347\3\2")
        buf.write("\2\2\u0347\u0404\3\2\2\2\u0348\u0349\7\u00d9\2\2\u0349")
        buf.write("\u034c\7\u0104\2\2\u034a\u034b\t\5\2\2\u034b\u034d\5\u00ac")
        buf.write("W\2\u034c\u034a\3\2\2\2\u034c\u034d\3\2\2\2\u034d\u0352")
        buf.write("\3\2\2\2\u034e\u0350\7\u0086\2\2\u034f\u034e\3\2\2\2\u034f")
        buf.write("\u0350\3\2\2\2\u0350\u0351\3\2\2\2\u0351\u0353\7\u011d")
        buf.write("\2\2\u0352\u034f\3\2\2\2\u0352\u0353\3\2\2\2\u0353\u0404")
        buf.write("\3\2\2\2\u0354\u0355\7\u00d9\2\2\u0355\u0356\7\u00ae\2")
        buf.write("\2\u0356\u0358\5\u00acW\2\u0357\u0359\5&\24\2\u0358\u0357")
        buf.write("\3\2\2\2\u0358\u0359\3\2\2\2\u0359\u0404\3\2\2\2\u035a")
        buf.write("\u035c\7\u00d9\2\2\u035b\u035d\5\u0104\u0083\2\u035c\u035b")
        buf.write("\3\2\2\2\u035c\u035d\3\2\2\2\u035d\u035e\3\2\2\2\u035e")
        buf.write("\u0366\7i\2\2\u035f\u0361\7\u0086\2\2\u0360\u035f\3\2")
        buf.write("\2\2\u0360\u0361\3\2\2\2\u0361\u0364\3\2\2\2\u0362\u0365")
        buf.write("\5\u00acW\2\u0363\u0365\7\u011d\2\2\u0364\u0362\3\2\2")
        buf.write("\2\u0364\u0363\3\2\2\2\u0365\u0367\3\2\2\2\u0366\u0360")
        buf.write("\3\2\2\2\u0366\u0367\3\2\2\2\u0367\u0404\3\2\2\2\u0368")
        buf.write("\u0369\7\u00d9\2\2\u0369\u036a\7\67\2\2\u036a\u036b\7")
        buf.write("\u00e5\2\2\u036b\u036e\5\u00acW\2\u036c\u036d\7\30\2\2")
        buf.write("\u036d\u036f\7\u00d3\2\2\u036e\u036c\3\2\2\2\u036e\u036f")
        buf.write("\3\2\2\2\u036f\u0404\3\2\2\2\u0370\u0371\7\u00d9\2\2\u0371")
        buf.write("\u0372\7:\2\2\u0372\u0404\7\u0097\2\2\u0373\u0374\t\n")
        buf.write("\2\2\u0374\u0376\7h\2\2\u0375\u0377\7X\2\2\u0376\u0375")
        buf.write("\3\2\2\2\u0376\u0377\3\2\2\2\u0377\u0378\3\2\2\2\u0378")
        buf.write("\u0404\5,\27\2\u0379\u037a\t\n\2\2\u037a\u037c\5*\26\2")
        buf.write("\u037b\u037d\7X\2\2\u037c\u037b\3\2\2\2\u037c\u037d\3")
        buf.write("\2\2\2\u037d\u037e\3\2\2\2\u037e\u037f\5\u00acW\2\u037f")
        buf.write("\u0404\3\2\2\2\u0380\u0382\t\n\2\2\u0381\u0383\7\u00e5")
        buf.write("\2\2\u0382\u0381\3\2\2\2\u0382\u0383\3\2\2\2\u0383\u0385")
        buf.write("\3\2\2\2\u0384\u0386\t\13\2\2\u0385\u0384\3\2\2\2\u0385")
        buf.write("\u0386\3\2\2\2\u0386\u0387\3\2\2\2\u0387\u0389\5\u00ac")
        buf.write("W\2\u0388\u038a\5&\24\2\u0389\u0388\3\2\2\2\u0389\u038a")
        buf.write("\3\2\2\2\u038a\u038c\3\2\2\2\u038b\u038d\5.\30\2\u038c")
        buf.write("\u038b\3\2\2\2\u038c\u038d\3\2\2\2\u038d\u0404\3\2\2\2")
        buf.write("\u038e\u0390\t\n\2\2\u038f\u0391\7\u00b8\2\2\u0390\u038f")
        buf.write("\3\2\2\2\u0390\u0391\3\2\2\2\u0391\u0392\3\2\2\2\u0392")
        buf.write("\u0404\5 \21\2\u0393\u0394\7/\2\2\u0394\u0395\7\u009f")
        buf.write("\2\2\u0395\u0396\5*\26\2\u0396\u0397\5\u00acW\2\u0397")
        buf.write("\u0398\7}\2\2\u0398\u0399\t\f\2\2\u0399\u0404\3\2\2\2")
        buf.write("\u039a\u039b\7/\2\2\u039b\u039c\7\u009f\2\2\u039c\u039d")
        buf.write("\7\u00e5\2\2\u039d\u039e\5\u00acW\2\u039e\u039f\7}\2\2")
        buf.write("\u039f\u03a0\t\f\2\2\u03a0\u0404\3\2\2\2\u03a1\u03a2\7")
        buf.write("\u00bf\2\2\u03a2\u03a3\7\u00e5\2\2\u03a3\u0404\5\u00ac")
        buf.write("W\2\u03a4\u03ac\7\u00bf\2\2\u03a5\u03ad\7\u011d\2\2\u03a6")
        buf.write("\u03a8\13\2\2\2\u03a7\u03a6\3\2\2\2\u03a8\u03ab\3\2\2")
        buf.write("\2\u03a9\u03aa\3\2\2\2\u03a9\u03a7\3\2\2\2\u03aa\u03ad")
        buf.write("\3\2\2\2\u03ab\u03a9\3\2\2\2\u03ac\u03a5\3\2\2\2\u03ac")
        buf.write("\u03a9\3\2\2\2\u03ad\u0404\3\2\2\2\u03ae\u03b0\7!\2\2")
        buf.write("\u03af\u03b1\7\u0083\2\2\u03b0\u03af\3\2\2\2\u03b0\u03b1")
        buf.write("\3\2\2\2\u03b1\u03b2\3\2\2\2\u03b2\u03b3\7\u00e5\2\2\u03b3")
        buf.write("\u03b6\5\u00acW\2\u03b4\u03b5\7\u00a2\2\2\u03b5\u03b7")
        buf.write("\58\35\2\u03b6\u03b4\3\2\2\2\u03b6\u03b7\3\2\2\2\u03b7")
        buf.write("\u03bc\3\2\2\2\u03b8\u03ba\7\30\2\2\u03b9\u03b8\3\2\2")
        buf.write("\2\u03b9\u03ba\3\2\2\2\u03ba\u03bb\3\2\2\2\u03bb\u03bd")
        buf.write("\5 \21\2\u03bc\u03b9\3\2\2\2\u03bc\u03bd\3\2\2\2\u03bd")
        buf.write("\u0404\3\2\2\2\u03be\u03bf\7\u00f8\2\2\u03bf\u03c2\7\u00e5")
        buf.write("\2\2\u03c0\u03c1\7p\2\2\u03c1\u03c3\7U\2\2\u03c2\u03c0")
        buf.write("\3\2\2\2\u03c2\u03c3\3\2\2\2\u03c3\u03c4\3\2\2\2\u03c4")
        buf.write("\u0404\5\u00acW\2\u03c5\u03c6\7\'\2\2\u03c6\u0404\7!\2")
        buf.write("\2\u03c7\u03c8\7\u008a\2\2\u03c8\u03ca\7?\2\2\u03c9\u03cb")
        buf.write("\7\u008b\2\2\u03ca\u03c9\3\2\2\2\u03ca\u03cb\3\2\2\2\u03cb")
        buf.write("\u03cc\3\2\2\2\u03cc\u03cd\7w\2\2\u03cd\u03cf\7\u011d")
        buf.write("\2\2\u03ce\u03d0\7\u00ab\2\2\u03cf\u03ce\3\2\2\2\u03cf")
        buf.write("\u03d0\3\2\2\2\u03d0\u03d1\3\2\2\2\u03d1\u03d2\7|\2\2")
        buf.write("\u03d2\u03d3\7\u00e5\2\2\u03d3\u03d5\5\u00acW\2\u03d4")
        buf.write("\u03d6\5&\24\2\u03d5\u03d4\3\2\2\2\u03d5\u03d6\3\2\2\2")
        buf.write("\u03d6\u0404\3\2\2\2\u03d7\u03d8\7\u00f4\2\2\u03d8\u03d9")
        buf.write("\7\u00e5\2\2\u03d9\u03db\5\u00acW\2\u03da\u03dc\5&\24")
        buf.write("\2\u03db\u03da\3\2\2\2\u03db\u03dc\3\2\2\2\u03dc\u0404")
        buf.write("\3\2\2\2\u03dd\u03de\7\u0096\2\2\u03de\u03df\7\u00c1\2")
        buf.write("\2\u03df\u03e0\7\u00e5\2\2\u03e0\u0404\5\u00acW\2\u03e1")
        buf.write("\u03e2\t\r\2\2\u03e2\u03ea\5\u0104\u0083\2\u03e3\u03eb")
        buf.write("\7\u011d\2\2\u03e4\u03e6\13\2\2\2\u03e5\u03e4\3\2\2\2")
        buf.write("\u03e6\u03e9\3\2\2\2\u03e7\u03e8\3\2\2\2\u03e7\u03e5\3")
        buf.write("\2\2\2\u03e8\u03eb\3\2\2\2\u03e9\u03e7\3\2\2\2\u03ea\u03e3")
        buf.write("\3\2\2\2\u03ea\u03e7\3\2\2\2\u03eb\u0404\3\2\2\2\u03ec")
        buf.write("\u03ed\7\u00d6\2\2\u03ed\u03f1\7\u00c8\2\2\u03ee\u03f0")
        buf.write("\13\2\2\2\u03ef\u03ee\3\2\2\2\u03f0\u03f3\3\2\2\2\u03f1")
        buf.write("\u03f2\3\2\2\2\u03f1\u03ef\3\2\2\2\u03f2\u0404\3\2\2\2")
        buf.write("\u03f3\u03f1\3\2\2\2\u03f4\u03f8\7\u00d6\2\2\u03f5\u03f7")
        buf.write("\13\2\2\2\u03f6\u03f5\3\2\2\2\u03f7\u03fa\3\2\2\2\u03f8")
        buf.write("\u03f9\3\2\2\2\u03f8\u03f6\3\2\2\2\u03f9\u0404\3\2\2\2")
        buf.write("\u03fa\u03f8\3\2\2\2\u03fb\u0404\7\u00c3\2\2\u03fc\u0400")
        buf.write("\5\22\n\2\u03fd\u03ff\13\2\2\2\u03fe\u03fd\3\2\2\2\u03ff")
        buf.write("\u0402\3\2\2\2\u0400\u0401\3\2\2\2\u0400\u03fe\3\2\2\2")
        buf.write("\u0401\u0404\3\2\2\2\u0402\u0400\3\2\2\2\u0403\u012f\3")
        buf.write("\2\2\2\u0403\u0131\3\2\2\2\u0403\u0134\3\2\2\2\u0403\u0139")
        buf.write("\3\2\2\2\u0403\u014b\3\2\2\2\u0403\u0152\3\2\2\2\u0403")
        buf.write("\u0158\3\2\2\2\u0403\u0162\3\2\2\2\u0403\u016e\3\2\2\2")
        buf.write("\u0403\u0180\3\2\2\2\u0403\u0190\3\2\2\2\u0403\u01b5\3")
        buf.write("\2\2\2\u0403\u01ca\3\2\2\2\u0403\u01d9\3\2\2\2\u0403\u01ea")
        buf.write("\3\2\2\2\u0403\u01f1\3\2\2\2\u0403\u01fa\3\2\2\2\u0403")
        buf.write("\u0203\3\2\2\2\u0403\u020c\3\2\2\2\u0403\u0213\3\2\2\2")
        buf.write("\u0403\u021a\3\2\2\2\u0403\u0221\3\2\2\2\u0403\u022c\3")
        buf.write("\2\2\2\u0403\u0237\3\2\2\2\u0403\u0246\3\2\2\2\u0403\u0252")
        buf.write("\3\2\2\2\u0403\u0260\3\2\2\2\u0403\u026a\3\2\2\2\u0403")
        buf.write("\u0278\3\2\2\2\u0403\u0280\3\2\2\2\u0403\u0293\3\2\2\2")
        buf.write("\u0403\u029c\3\2\2\2\u0403\u02a2\3\2\2\2\u0403\u02ac\3")
        buf.write("\2\2\2\u0403\u02b3\3\2\2\2\u0403\u02d6\3\2\2\2\u0403\u02ec")
        buf.write("\3\2\2\2\u0403\u02f4\3\2\2\2\u0403\u0310\3\2\2\2\u0403")
        buf.write("\u031a\3\2\2\2\u0403\u031f\3\2\2\2\u0403\u032b\3\2\2\2")
        buf.write("\u0403\u0337\3\2\2\2\u0403\u0340\3\2\2\2\u0403\u0348\3")
        buf.write("\2\2\2\u0403\u0354\3\2\2\2\u0403\u035a\3\2\2\2\u0403\u0368")
        buf.write("\3\2\2\2\u0403\u0370\3\2\2\2\u0403\u0373\3\2\2\2\u0403")
        buf.write("\u0379\3\2\2\2\u0403\u0380\3\2\2\2\u0403\u038e\3\2\2\2")
        buf.write("\u0403\u0393\3\2\2\2\u0403\u039a\3\2\2\2\u0403\u03a1\3")
        buf.write("\2\2\2\u0403\u03a4\3\2\2\2\u0403\u03ae\3\2\2\2\u0403\u03be")
        buf.write("\3\2\2\2\u0403\u03c5\3\2\2\2\u0403\u03c7\3\2\2\2\u0403")
        buf.write("\u03d7\3\2\2\2\u0403\u03dd\3\2\2\2\u0403\u03e1\3\2\2\2")
        buf.write("\u0403\u03ec\3\2\2\2\u0403\u03f4\3\2\2\2\u0403\u03fb\3")
        buf.write("\2\2\2\u0403\u03fc\3\2\2\2\u0404\21\3\2\2\2\u0405\u0406")
        buf.write("\7\67\2\2\u0406\u04ae\7\u00c8\2\2\u0407\u0408\7N\2\2\u0408")
        buf.write("\u04ae\7\u00c8\2\2\u0409\u040b\7k\2\2\u040a\u040c\7\u00c8")
        buf.write("\2\2\u040b\u040a\3\2\2\2\u040b\u040c\3\2\2\2\u040c\u04ae")
        buf.write("\3\2\2\2\u040d\u040f\7\u00c5\2\2\u040e\u0410\7\u00c8\2")
        buf.write("\2\u040f\u040e\3\2\2\2\u040f\u0410\3\2\2\2\u0410\u04ae")
        buf.write("\3\2\2\2\u0411\u0412\7\u00d9\2\2\u0412\u04ae\7k\2\2\u0413")
        buf.write("\u0414\7\u00d9\2\2\u0414\u0416\7\u00c8\2\2\u0415\u0417")
        buf.write("\7k\2\2\u0416\u0415\3\2\2\2\u0416\u0417\3\2\2\2\u0417")
        buf.write("\u04ae\3\2\2\2\u0418\u0419\7\u00d9\2\2\u0419\u04ae\7\u00b5")
        buf.write("\2\2\u041a\u041b\7\u00d9\2\2\u041b\u04ae\7\u00c9\2\2\u041c")
        buf.write("\u041d\7\u00d9\2\2\u041d\u041e\7:\2\2\u041e\u04ae\7\u00c9")
        buf.write("\2\2\u041f\u0420\7W\2\2\u0420\u04ae\7\u00e5\2\2\u0421")
        buf.write("\u0422\7r\2\2\u0422\u04ae\7\u00e5\2\2\u0423\u0424\7\u00d9")
        buf.write("\2\2\u0424\u04ae\7\62\2\2\u0425\u0426\7\u00d9\2\2\u0426")
        buf.write("\u0427\7\67\2\2\u0427\u04ae\7\u00e5\2\2\u0428\u0429\7")
        buf.write("\u00d9\2\2\u0429\u04ae\7\u00f0\2\2\u042a\u042b\7\u00d9")
        buf.write("\2\2\u042b\u04ae\7u\2\2\u042c\u042d\7\u00d9\2\2\u042d")
        buf.write("\u04ae\7\u008e\2\2\u042e\u042f\7\67\2\2\u042f\u04ae\7")
        buf.write("t\2\2\u0430\u0431\7N\2\2\u0431\u04ae\7t\2\2\u0432\u0433")
        buf.write("\7\21\2\2\u0433\u04ae\7t\2\2\u0434\u0435\7\u008d\2\2\u0435")
        buf.write("\u04ae\7\u00e5\2\2\u0436\u0437\7\u008d\2\2\u0437\u04ae")
        buf.write("\7@\2\2\u0438\u0439\7\u00fc\2\2\u0439\u04ae\7\u00e5\2")
        buf.write("\2\u043a\u043b\7\u00fc\2\2\u043b\u04ae\7@\2\2\u043c\u043d")
        buf.write("\7\67\2\2\u043d\u043e\7\u00e9\2\2\u043e\u04ae\7\u0090")
        buf.write("\2\2\u043f\u0440\7N\2\2\u0440\u0441\7\u00e9\2\2\u0441")
        buf.write("\u04ae\7\u0090\2\2\u0442\u0443\7\21\2\2\u0443\u0444\7")
        buf.write("\u00e5\2\2\u0444\u0445\5\u00aeX\2\u0445\u0446\7\u009b")
        buf.write("\2\2\u0446\u0447\7)\2\2\u0447\u04ae\3\2\2\2\u0448\u0449")
        buf.write("\7\21\2\2\u0449\u044a\7\u00e5\2\2\u044a\u044b\5\u00ae")
        buf.write("X\2\u044b\u044c\7)\2\2\u044c\u044d\7 \2\2\u044d\u04ae")
        buf.write("\3\2\2\2\u044e\u044f\7\21\2\2\u044f\u0450\7\u00e5\2\2")
        buf.write("\u0450\u0451\5\u00aeX\2\u0451\u0452\7\u009b\2\2\u0452")
        buf.write("\u0453\7\u00dd\2\2\u0453\u04ae\3\2\2\2\u0454\u0455\7\21")
        buf.write("\2\2\u0455\u0456\7\u00e5\2\2\u0456\u0457\5\u00aeX\2\u0457")
        buf.write("\u0458\7\u00da\2\2\u0458\u0459\7 \2\2\u0459\u04ae\3\2")
        buf.write("\2\2\u045a\u045b\7\21\2\2\u045b\u045c\7\u00e5\2\2\u045c")
        buf.write("\u045d\5\u00aeX\2\u045d\u045e\7\u009b\2\2\u045e\u045f")
        buf.write("\7\u00da\2\2\u045f\u04ae\3\2\2\2\u0460\u0461\7\21\2\2")
        buf.write("\u0461\u0462\7\u00e5\2\2\u0462\u0463\5\u00aeX\2\u0463")
        buf.write("\u0464\7\u009b\2\2\u0464\u0465\7\u00e0\2\2\u0465\u0466")
        buf.write("\7\30\2\2\u0466\u0467\7J\2\2\u0467\u04ae\3\2\2\2\u0468")
        buf.write("\u0469\7\21\2\2\u0469\u046a\7\u00e5\2\2\u046a\u046b\5")
        buf.write("\u00aeX\2\u046b\u046c\7\u00d6\2\2\u046c\u046d\7\u00da")
        buf.write("\2\2\u046d\u046e\7\u008c\2\2\u046e\u04ae\3\2\2\2\u046f")
        buf.write("\u0470\7\21\2\2\u0470\u0471\7\u00e5\2\2\u0471\u0472\5")
        buf.write("\u00aeX\2\u0472\u0473\7T\2\2\u0473\u0474\7\u00ac\2\2\u0474")
        buf.write("\u04ae\3\2\2\2\u0475\u0476\7\21\2\2\u0476\u0477\7\u00e5")
        buf.write("\2\2\u0477\u0478\5\u00aeX\2\u0478\u0479\7\26\2\2\u0479")
        buf.write("\u047a\7\u00ac\2\2\u047a\u04ae\3\2\2\2\u047b\u047c\7\21")
        buf.write("\2\2\u047c\u047d\7\u00e5\2\2\u047d\u047e\5\u00aeX\2\u047e")
        buf.write("\u047f\7\u00f6\2\2\u047f\u0480\7\u00ac\2\2\u0480\u04ae")
        buf.write("\3\2\2\2\u0481\u0482\7\21\2\2\u0482\u0483\7\u00e5\2\2")
        buf.write("\u0483\u0484\5\u00aeX\2\u0484\u0485\7\u00ed\2\2\u0485")
        buf.write("\u04ae\3\2\2\2\u0486\u0487\7\21\2\2\u0487\u0488\7\u00e5")
        buf.write("\2\2\u0488\u048a\5\u00aeX\2\u0489\u048b\5&\24\2\u048a")
        buf.write("\u0489\3\2\2\2\u048a\u048b\3\2\2\2\u048b\u048c\3\2\2\2")
        buf.write("\u048c\u048d\7\61\2\2\u048d\u04ae\3\2\2\2\u048e\u048f")
        buf.write("\7\21\2\2\u048f\u0490\7\u00e5\2\2\u0490\u0492\5\u00ae")
        buf.write("X\2\u0491\u0493\5&\24\2\u0492\u0491\3\2\2\2\u0492\u0493")
        buf.write("\3\2\2\2\u0493\u0494\3\2\2\2\u0494\u0495\7\64\2\2\u0495")
        buf.write("\u04ae\3\2\2\2\u0496\u0497\7\21\2\2\u0497\u0498\7\u00e5")
        buf.write("\2\2\u0498\u049a\5\u00aeX\2\u0499\u049b\5&\24\2\u049a")
        buf.write("\u0499\3\2\2\2\u049a\u049b\3\2\2\2\u049b\u049c\3\2\2\2")
        buf.write("\u049c\u049d\7\u00d6\2\2\u049d\u049e\7_\2\2\u049e\u04ae")
        buf.write("\3\2\2\2\u049f\u04a0\7\21\2\2\u04a0\u04a1\7\u00e5\2\2")
        buf.write("\u04a1\u04a3\5\u00aeX\2\u04a2\u04a4\5&\24\2\u04a3\u04a2")
        buf.write("\3\2\2\2\u04a3\u04a4\3\2\2\2\u04a4\u04a5\3\2\2\2\u04a5")
        buf.write("\u04a6\7\u00c2\2\2\u04a6\u04a7\7.\2\2\u04a7\u04ae\3\2")
        buf.write("\2\2\u04a8\u04a9\7\u00de\2\2\u04a9\u04ae\7\u00ef\2\2\u04aa")
        buf.write("\u04ae\7\60\2\2\u04ab\u04ae\7\u00ca\2\2\u04ac\u04ae\7")
        buf.write("I\2\2\u04ad\u0405\3\2\2\2\u04ad\u0407\3\2\2\2\u04ad\u0409")
        buf.write("\3\2\2\2\u04ad\u040d\3\2\2\2\u04ad\u0411\3\2\2\2\u04ad")
        buf.write("\u0413\3\2\2\2\u04ad\u0418\3\2\2\2\u04ad\u041a\3\2\2\2")
        buf.write("\u04ad\u041c\3\2\2\2\u04ad\u041f\3\2\2\2\u04ad\u0421\3")
        buf.write("\2\2\2\u04ad\u0423\3\2\2\2\u04ad\u0425\3\2\2\2\u04ad\u0428")
        buf.write("\3\2\2\2\u04ad\u042a\3\2\2\2\u04ad\u042c\3\2\2\2\u04ad")
        buf.write("\u042e\3\2\2\2\u04ad\u0430\3\2\2\2\u04ad\u0432\3\2\2\2")
        buf.write("\u04ad\u0434\3\2\2\2\u04ad\u0436\3\2\2\2\u04ad\u0438\3")
        buf.write("\2\2\2\u04ad\u043a\3\2\2\2\u04ad\u043c\3\2\2\2\u04ad\u043f")
        buf.write("\3\2\2\2\u04ad\u0442\3\2\2\2\u04ad\u0448\3\2\2\2\u04ad")
        buf.write("\u044e\3\2\2\2\u04ad\u0454\3\2\2\2\u04ad\u045a\3\2\2\2")
        buf.write("\u04ad\u0460\3\2\2\2\u04ad\u0468\3\2\2\2\u04ad\u046f\3")
        buf.write("\2\2\2\u04ad\u0475\3\2\2\2\u04ad\u047b\3\2\2\2\u04ad\u0481")
        buf.write("\3\2\2\2\u04ad\u0486\3\2\2\2\u04ad\u048e\3\2\2\2\u04ad")
        buf.write("\u0496\3\2\2\2\u04ad\u049f\3\2\2\2\u04ad\u04a8\3\2\2\2")
        buf.write("\u04ad\u04aa\3\2\2\2\u04ad\u04ab\3\2\2\2\u04ad\u04ac\3")
        buf.write("\2\2\2\u04ae\23\3\2\2\2\u04af\u04b1\7\67\2\2\u04b0\u04b2")
        buf.write("\7\u00e9\2\2\u04b1\u04b0\3\2\2\2\u04b1\u04b2\3\2\2\2\u04b2")
        buf.write("\u04b4\3\2\2\2\u04b3\u04b5\7Y\2\2\u04b4\u04b3\3\2\2\2")
        buf.write("\u04b4\u04b5\3\2\2\2\u04b5\u04b6\3\2\2\2\u04b6\u04ba\7")
        buf.write("\u00e5\2\2\u04b7\u04b8\7p\2\2\u04b8\u04b9\7\u009b\2\2")
        buf.write("\u04b9\u04bb\7U\2\2\u04ba\u04b7\3\2\2\2\u04ba\u04bb\3")
        buf.write("\2\2\2\u04bb\u04bc\3\2\2\2\u04bc\u04bd\5\u00acW\2\u04bd")
        buf.write("\25\3\2\2\2\u04be\u04bf\7\67\2\2\u04bf\u04c1\7\u00a3\2")
        buf.write("\2\u04c0\u04be\3\2\2\2\u04c0\u04c1\3\2\2\2\u04c1\u04c2")
        buf.write("\3\2\2\2\u04c2\u04c3\7\u00c2\2\2\u04c3\u04c4\7\u00e5\2")
        buf.write("\2\u04c4\u04c5\5\u00acW\2\u04c5\27\3\2\2\2\u04c6\u04c7")
        buf.write("\7)\2\2\u04c7\u04c8\7 \2\2\u04c8\u04cc\5\u0094K\2\u04c9")
        buf.write("\u04ca\7\u00dd\2\2\u04ca\u04cb\7 \2\2\u04cb\u04cd\5\u0098")
        buf.write("M\2\u04cc\u04c9\3\2\2\2\u04cc\u04cd\3\2\2\2\u04cd\u04ce")
        buf.write("\3\2\2\2\u04ce\u04cf\7|\2\2\u04cf\u04d0\7\u0121\2\2\u04d0")
        buf.write("\u04d1\7\37\2\2\u04d1\31\3\2\2\2\u04d2\u04d3\7\u00da\2")
        buf.write("\2\u04d3\u04d4\7 \2\2\u04d4\u04d5\5\u0094K\2\u04d5\u04d8")
        buf.write("\7\u009f\2\2\u04d6\u04d9\5@!\2\u04d7\u04d9\5B\"\2\u04d8")
        buf.write("\u04d6\3\2\2\2\u04d8\u04d7\3\2\2\2\u04d9\u04dd\3\2\2\2")
        buf.write("\u04da\u04db\7\u00e0\2\2\u04db\u04dc\7\30\2\2\u04dc\u04de")
        buf.write("\7J\2\2\u04dd\u04da\3\2\2\2\u04dd\u04de\3\2\2\2\u04de")
        buf.write("\33\3\2\2\2\u04df\u04e0\7\u008c\2\2\u04e0\u04e1\7\u011d")
        buf.write("\2\2\u04e1\35\3\2\2\2\u04e2\u04e3\7/\2\2\u04e3\u04e4\7")
        buf.write("\u011d\2\2\u04e4\37\3\2\2\2\u04e5\u04e7\5\60\31\2\u04e6")
        buf.write("\u04e5\3\2\2\2\u04e6\u04e7\3\2\2\2\u04e7\u04e8\3\2\2\2")
        buf.write("\u04e8\u04e9\5R*\2\u04e9\u04ea\5N(\2\u04ea!\3\2\2\2\u04eb")
        buf.write("\u04ec\7y\2\2\u04ec\u04ee\7\u00ab\2\2\u04ed\u04ef\7\u00e5")
        buf.write("\2\2\u04ee\u04ed\3\2\2\2\u04ee\u04ef\3\2\2\2\u04ef\u04f0")
        buf.write("\3\2\2\2\u04f0\u04f7\5\u00acW\2\u04f1\u04f5\5&\24\2\u04f2")
        buf.write("\u04f3\7p\2\2\u04f3\u04f4\7\u009b\2\2\u04f4\u04f6\7U\2")
        buf.write("\2\u04f5\u04f2\3\2\2\2\u04f5\u04f6\3\2\2\2\u04f6\u04f8")
        buf.write("\3\2\2\2\u04f7\u04f1\3\2\2\2\u04f7\u04f8\3\2\2\2\u04f8")
        buf.write("\u0523\3\2\2\2\u04f9\u04fa\7y\2\2\u04fa\u04fc\7|\2\2\u04fb")
        buf.write("\u04fd\7\u00e5\2\2\u04fc\u04fb\3\2\2\2\u04fc\u04fd\3\2")
        buf.write("\2\2\u04fd\u04fe\3\2\2\2\u04fe\u0500\5\u00acW\2\u04ff")
        buf.write("\u0501\5&\24\2\u0500\u04ff\3\2\2\2\u0500\u0501\3\2\2\2")
        buf.write("\u0501\u0505\3\2\2\2\u0502\u0503\7p\2\2\u0503\u0504\7")
        buf.write("\u009b\2\2\u0504\u0506\7U\2\2\u0505\u0502\3\2\2\2\u0505")
        buf.write("\u0506\3\2\2\2\u0506\u0523\3\2\2\2\u0507\u0508\7y\2\2")
        buf.write("\u0508\u050a\7\u00ab\2\2\u0509\u050b\7\u008b\2\2\u050a")
        buf.write("\u0509\3\2\2\2\u050a\u050b\3\2\2\2\u050b\u050c\3\2\2\2")
        buf.write("\u050c\u050d\7K\2\2\u050d\u050f\7\u011d\2\2\u050e\u0510")
        buf.write("\5\u00a8U\2\u050f\u050e\3\2\2\2\u050f\u0510\3\2\2\2\u0510")
        buf.write("\u0512\3\2\2\2\u0511\u0513\5D#\2\u0512\u0511\3\2\2\2\u0512")
        buf.write("\u0513\3\2\2\2\u0513\u0523\3\2\2\2\u0514\u0515\7y\2\2")
        buf.write("\u0515\u0517\7\u00ab\2\2\u0516\u0518\7\u008b\2\2\u0517")
        buf.write("\u0516\3\2\2\2\u0517\u0518\3\2\2\2\u0518\u0519\3\2\2\2")
        buf.write("\u0519\u051b\7K\2\2\u051a\u051c\7\u011d\2\2\u051b\u051a")
        buf.write("\3\2\2\2\u051b\u051c\3\2\2\2\u051c\u051d\3\2\2\2\u051d")
        buf.write("\u0520\5\64\33\2\u051e\u051f\7\u00a2\2\2\u051f\u0521\5")
        buf.write("8\35\2\u0520\u051e\3\2\2\2\u0520\u0521\3\2\2\2\u0521\u0523")
        buf.write("\3\2\2\2\u0522\u04eb\3\2\2\2\u0522\u04f9\3\2\2\2\u0522")
        buf.write("\u0507\3\2\2\2\u0522\u0514\3\2\2\2\u0523#\3\2\2\2\u0524")
        buf.write("\u0526\5&\24\2\u0525\u0527\5\34\17\2\u0526\u0525\3\2\2")
        buf.write("\2\u0526\u0527\3\2\2\2\u0527%\3\2\2\2\u0528\u0529\7\u00ac")
        buf.write("\2\2\u0529\u052a\7\4\2\2\u052a\u052f\5(\25\2\u052b\u052c")
        buf.write("\7\6\2\2\u052c\u052e\5(\25\2\u052d\u052b\3\2\2\2\u052e")
        buf.write("\u0531\3\2\2\2\u052f\u052d\3\2\2\2\u052f\u0530\3\2\2\2")
        buf.write("\u0530\u0532\3\2\2\2\u0531\u052f\3\2\2\2\u0532\u0533\7")
        buf.write("\5\2\2\u0533\'\3\2\2\2\u0534\u0537\5\u0104\u0083\2\u0535")
        buf.write("\u0536\7\u010a\2\2\u0536\u0538\5\u00c6d\2\u0537\u0535")
        buf.write("\3\2\2\2\u0537\u0538\3\2\2\2\u0538)\3\2\2\2\u0539\u053a")
        buf.write("\t\16\2\2\u053a+\3\2\2\2\u053b\u0541\5\u00fe\u0080\2\u053c")
        buf.write("\u0541\7\u011d\2\2\u053d\u0541\5\u00c8e\2\u053e\u0541")
        buf.write("\5\u00caf\2\u053f\u0541\5\u00ccg\2\u0540\u053b\3\2\2\2")
        buf.write("\u0540\u053c\3\2\2\2\u0540\u053d\3\2\2\2\u0540\u053e\3")
        buf.write("\2\2\2\u0540\u053f\3\2\2\2\u0541-\3\2\2\2\u0542\u0547")
        buf.write("\5\u0104\u0083\2\u0543\u0544\7\7\2\2\u0544\u0546\5\u0104")
        buf.write("\u0083\2\u0545\u0543\3\2\2\2\u0546\u0549\3\2\2\2\u0547")
        buf.write("\u0545\3\2\2\2\u0547\u0548\3\2\2\2\u0548/\3\2\2\2\u0549")
        buf.write("\u0547\3\2\2\2\u054a\u054b\7\u0108\2\2\u054b\u0550\5\62")
        buf.write("\32\2\u054c\u054d\7\6\2\2\u054d\u054f\5\62\32\2\u054e")
        buf.write("\u054c\3\2\2\2\u054f\u0552\3\2\2\2\u0550\u054e\3\2\2\2")
        buf.write("\u0550\u0551\3\2\2\2\u0551\61\3\2\2\2\u0552\u0550\3\2")
        buf.write("\2\2\u0553\u0555\5\u0100\u0081\2\u0554\u0556\5\u0094K")
        buf.write("\2\u0555\u0554\3\2\2\2\u0555\u0556\3\2\2\2\u0556\u0558")
        buf.write("\3\2\2\2\u0557\u0559\7\30\2\2\u0558\u0557\3\2\2\2\u0558")
        buf.write("\u0559\3\2\2\2\u0559\u055a\3\2\2\2\u055a\u055b\7\4\2\2")
        buf.write("\u055b\u055c\5 \21\2\u055c\u055d\7\5\2\2\u055d\63\3\2")
        buf.write("\2\2\u055e\u055f\7\u0101\2\2\u055f\u0560\5\u00acW\2\u0560")
        buf.write("\65\3\2\2\2\u0561\u0562\7\u00a2\2\2\u0562\u056c\58\35")
        buf.write("\2\u0563\u0564\7\u00ad\2\2\u0564\u0565\7 \2\2\u0565\u056c")
        buf.write("\5\u00b6\\\2\u0566\u056c\5\30\r\2\u0567\u056c\5\34\17")
        buf.write("\2\u0568\u056c\5\36\20\2\u0569\u056a\7\u00e8\2\2\u056a")
        buf.write("\u056c\58\35\2\u056b\u0561\3\2\2\2\u056b\u0563\3\2\2\2")
        buf.write("\u056b\u0566\3\2\2\2\u056b\u0567\3\2\2\2\u056b\u0568\3")
        buf.write("\2\2\2\u056b\u0569\3\2\2\2\u056c\u056f\3\2\2\2\u056d\u056b")
        buf.write("\3\2\2\2\u056d\u056e\3\2\2\2\u056e\67\3\2\2\2\u056f\u056d")
        buf.write("\3\2\2\2\u0570\u0571\7\4\2\2\u0571\u0576\5:\36\2\u0572")
        buf.write("\u0573\7\6\2\2\u0573\u0575\5:\36\2\u0574\u0572\3\2\2\2")
        buf.write("\u0575\u0578\3\2\2\2\u0576\u0574\3\2\2\2\u0576\u0577\3")
        buf.write("\2\2\2\u0577\u0579\3\2\2\2\u0578\u0576\3\2\2\2\u0579\u057a")
        buf.write("\7\5\2\2\u057a9\3\2\2\2\u057b\u0580\5<\37\2\u057c\u057e")
        buf.write("\7\u010a\2\2\u057d\u057c\3\2\2\2\u057d\u057e\3\2\2\2\u057e")
        buf.write("\u057f\3\2\2\2\u057f\u0581\5> \2\u0580\u057d\3\2\2\2\u0580")
        buf.write("\u0581\3\2\2\2\u0581;\3\2\2\2\u0582\u0587\5\u0104\u0083")
        buf.write("\2\u0583\u0584\7\7\2\2\u0584\u0586\5\u0104\u0083\2\u0585")
        buf.write("\u0583\3\2\2\2\u0586\u0589\3\2\2\2\u0587\u0585\3\2\2\2")
        buf.write("\u0587\u0588\3\2\2\2\u0588\u058c\3\2\2\2\u0589\u0587\3")
        buf.write("\2\2\2\u058a\u058c\7\u011d\2\2\u058b\u0582\3\2\2\2\u058b")
        buf.write("\u058a\3\2\2\2\u058c=\3\2\2\2\u058d\u0592\7\u0121\2\2")
        buf.write("\u058e\u0592\7\u0123\2\2\u058f\u0592\5\u00ceh\2\u0590")
        buf.write("\u0592\7\u011d\2\2\u0591\u058d\3\2\2\2\u0591\u058e\3\2")
        buf.write("\2\2\u0591\u058f\3\2\2\2\u0591\u0590\3\2\2\2\u0592?\3")
        buf.write("\2\2\2\u0593\u0594\7\4\2\2\u0594\u0599\5\u00c6d\2\u0595")
        buf.write("\u0596\7\6\2\2\u0596\u0598\5\u00c6d\2\u0597\u0595\3\2")
        buf.write("\2\2\u0598\u059b\3\2\2\2\u0599\u0597\3\2\2\2\u0599\u059a")
        buf.write("\3\2\2\2\u059a\u059c\3\2\2\2\u059b\u0599\3\2\2\2\u059c")
        buf.write("\u059d\7\5\2\2\u059dA\3\2\2\2\u059e\u059f\7\4\2\2\u059f")
        buf.write("\u05a4\5@!\2\u05a0\u05a1\7\6\2\2\u05a1\u05a3\5@!\2\u05a2")
        buf.write("\u05a0\3\2\2\2\u05a3\u05a6\3\2\2\2\u05a4\u05a2\3\2\2\2")
        buf.write("\u05a4\u05a5\3\2\2\2\u05a5\u05a7\3\2\2\2\u05a6\u05a4\3")
        buf.write("\2\2\2\u05a7\u05a8\7\5\2\2\u05a8C\3\2\2\2\u05a9\u05aa")
        buf.write("\7\u00e0\2\2\u05aa\u05ab\7\30\2\2\u05ab\u05b0\5F$\2\u05ac")
        buf.write("\u05ad\7\u00e0\2\2\u05ad\u05ae\7 \2\2\u05ae\u05b0\5H%")
        buf.write("\2\u05af\u05a9\3\2\2\2\u05af\u05ac\3\2\2\2\u05b0E\3\2")
        buf.write("\2\2\u05b1\u05b2\7x\2\2\u05b2\u05b3\7\u011d\2\2\u05b3")
        buf.write("\u05b4\7\u00a7\2\2\u05b4\u05b7\7\u011d\2\2\u05b5\u05b7")
        buf.write("\5\u0104\u0083\2\u05b6\u05b1\3\2\2\2\u05b6\u05b5\3\2\2")
        buf.write("\2\u05b7G\3\2\2\2\u05b8\u05bc\7\u011d\2\2\u05b9\u05ba")
        buf.write("\7\u0108\2\2\u05ba\u05bb\7\u00d4\2\2\u05bb\u05bd\58\35")
        buf.write("\2\u05bc\u05b9\3\2\2\2\u05bc\u05bd\3\2\2\2\u05bdI\3\2")
        buf.write("\2\2\u05be\u05bf\5\u0104\u0083\2\u05bf\u05c0\7\u011d\2")
        buf.write("\2\u05c0K\3\2\2\2\u05c1\u05c2\5\"\22\2\u05c2\u05c3\5R")
        buf.write("*\2\u05c3\u05c4\5N(\2\u05c4\u05f5\3\2\2\2\u05c5\u05c7")
        buf.write("\5x=\2\u05c6\u05c8\5P)\2\u05c7\u05c6\3\2\2\2\u05c8\u05c9")
        buf.write("\3\2\2\2\u05c9\u05c7\3\2\2\2\u05c9\u05ca\3\2\2\2\u05ca")
        buf.write("\u05f5\3\2\2\2\u05cb\u05cc\7E\2\2\u05cc\u05cd\7f\2\2\u05cd")
        buf.write("\u05ce\5\u00acW\2\u05ce\u05d0\5\u00a6T\2\u05cf\u05d1\5")
        buf.write("p9\2\u05d0\u05cf\3\2\2\2\u05d0\u05d1\3\2\2\2\u05d1\u05f5")
        buf.write("\3\2\2\2\u05d2\u05d3\7\u00fe\2\2\u05d3\u05d4\5\u00acW")
        buf.write("\2\u05d4\u05d5\5\u00a6T\2\u05d5\u05d7\5b\62\2\u05d6\u05d8")
        buf.write("\5p9\2\u05d7\u05d6\3\2\2\2\u05d7\u05d8\3\2\2\2\u05d8\u05f5")
        buf.write("\3\2\2\2\u05d9\u05da\7\u0093\2\2\u05da\u05db\7|\2\2\u05db")
        buf.write("\u05dc\5\u00acW\2\u05dc\u05dd\5\u00a6T\2\u05dd\u05e3\7")
        buf.write("\u0101\2\2\u05de\u05e4\5\u00acW\2\u05df\u05e0\7\4\2\2")
        buf.write("\u05e0\u05e1\5 \21\2\u05e1\u05e2\7\5\2\2\u05e2\u05e4\3")
        buf.write("\2\2\2\u05e3\u05de\3\2\2\2\u05e3\u05df\3\2\2\2\u05e4\u05e5")
        buf.write("\3\2\2\2\u05e5\u05e6\5\u00a6T\2\u05e6\u05e7\7\u009f\2")
        buf.write("\2\u05e7\u05eb\5\u00be`\2\u05e8\u05ea\5d\63\2\u05e9\u05e8")
        buf.write("\3\2\2\2\u05ea\u05ed\3\2\2\2\u05eb\u05e9\3\2\2\2\u05eb")
        buf.write("\u05ec\3\2\2\2\u05ec\u05f1\3\2\2\2\u05ed\u05eb\3\2\2\2")
        buf.write("\u05ee\u05f0\5f\64\2\u05ef\u05ee\3\2\2\2\u05f0\u05f3\3")
        buf.write("\2\2\2\u05f1\u05ef\3\2\2\2\u05f1\u05f2\3\2\2\2\u05f2\u05f5")
        buf.write("\3\2\2\2\u05f3\u05f1\3\2\2\2\u05f4\u05c1\3\2\2\2\u05f4")
        buf.write("\u05c5\3\2\2\2\u05f4\u05cb\3\2\2\2\u05f4\u05d2\3\2\2\2")
        buf.write("\u05f4\u05d9\3\2\2\2\u05f5M\3\2\2\2\u05f6\u05f7\7\u00a4")
        buf.write("\2\2\u05f7\u05f8\7 \2\2\u05f8\u05fd\5V,\2\u05f9\u05fa")
        buf.write("\7\6\2\2\u05fa\u05fc\5V,\2\u05fb\u05f9\3\2\2\2\u05fc\u05ff")
        buf.write("\3\2\2\2\u05fd\u05fb\3\2\2\2\u05fd\u05fe\3\2\2\2\u05fe")
        buf.write("\u0601\3\2\2\2\u05ff\u05fd\3\2\2\2\u0600\u05f6\3\2\2\2")
        buf.write("\u0600\u0601\3\2\2\2\u0601\u060c\3\2\2\2\u0602\u0603\7")
        buf.write("(\2\2\u0603\u0604\7 \2\2\u0604\u0609\5\u00bc_\2\u0605")
        buf.write("\u0606\7\6\2\2\u0606\u0608\5\u00bc_\2\u0607\u0605\3\2")
        buf.write("\2\2\u0608\u060b\3\2\2\2\u0609\u0607\3\2\2\2\u0609\u060a")
        buf.write("\3\2\2\2\u060a\u060d\3\2\2\2\u060b\u0609\3\2\2\2\u060c")
        buf.write("\u0602\3\2\2\2\u060c\u060d\3\2\2\2\u060d\u0618\3\2\2\2")
        buf.write("\u060e\u060f\7M\2\2\u060f\u0610\7 \2\2\u0610\u0615\5\u00bc")
        buf.write("_\2\u0611\u0612\7\6\2\2\u0612\u0614\5\u00bc_\2\u0613\u0611")
        buf.write("\3\2\2\2\u0614\u0617\3\2\2\2\u0615\u0613\3\2\2\2\u0615")
        buf.write("\u0616\3\2\2\2\u0616\u0619\3\2\2\2\u0617\u0615\3\2\2\2")
        buf.write("\u0618\u060e\3\2\2\2\u0618\u0619\3\2\2\2\u0619\u0624\3")
        buf.write("\2\2\2\u061a\u061b\7\u00dc\2\2\u061b\u061c\7 \2\2\u061c")
        buf.write("\u0621\5V,\2\u061d\u061e\7\6\2\2\u061e\u0620\5V,\2\u061f")
        buf.write("\u061d\3\2\2\2\u0620\u0623\3\2\2\2\u0621\u061f\3\2\2\2")
        buf.write("\u0621\u0622\3\2\2\2\u0622\u0625\3\2\2\2\u0623\u0621\3")
        buf.write("\2\2\2\u0624\u061a\3\2\2\2\u0624\u0625\3\2\2\2\u0625\u0627")
        buf.write("\3\2\2\2\u0626\u0628\5\u00f0y\2\u0627\u0626\3\2\2\2\u0627")
        buf.write("\u0628\3\2\2\2\u0628\u062e\3\2\2\2\u0629\u062c\7\u0087")
        buf.write("\2\2\u062a\u062d\7\20\2\2\u062b\u062d\5\u00bc_\2\u062c")
        buf.write("\u062a\3\2\2\2\u062c\u062b\3\2\2\2\u062d\u062f\3\2\2\2")
        buf.write("\u062e\u0629\3\2\2\2\u062e\u062f\3\2\2\2\u062fO\3\2\2")
        buf.write("\2\u0630\u0631\5\"\22\2\u0631\u0632\5Z.\2\u0632Q\3\2\2")
        buf.write("\2\u0633\u0634\b*\1\2\u0634\u0635\5T+\2\u0635\u064d\3")
        buf.write("\2\2\2\u0636\u0637\f\5\2\2\u0637\u0638\6*\5\2\u0638\u063a")
        buf.write("\t\17\2\2\u0639\u063b\5\u0086D\2\u063a\u0639\3\2\2\2\u063a")
        buf.write("\u063b\3\2\2\2\u063b\u063c\3\2\2\2\u063c\u064c\5R*\6\u063d")
        buf.write("\u063e\f\4\2\2\u063e\u063f\6*\7\2\u063f\u0641\7z\2\2\u0640")
        buf.write("\u0642\5\u0086D\2\u0641\u0640\3\2\2\2\u0641\u0642\3\2")
        buf.write("\2\2\u0642\u0643\3\2\2\2\u0643\u064c\5R*\5\u0644\u0645")
        buf.write("\f\3\2\2\u0645\u0646\6*\t\2\u0646\u0648\t\20\2\2\u0647")
        buf.write("\u0649\5\u0086D\2\u0648\u0647\3\2\2\2\u0648\u0649\3\2")
        buf.write("\2\2\u0649\u064a\3\2\2\2\u064a\u064c\5R*\4\u064b\u0636")
        buf.write("\3\2\2\2\u064b\u063d\3\2\2\2\u064b\u0644\3\2\2\2\u064c")
        buf.write("\u064f\3\2\2\2\u064d\u064b\3\2\2\2\u064d\u064e\3\2\2\2")
        buf.write("\u064eS\3\2\2\2\u064f\u064d\3\2\2\2\u0650\u065a\5\\/\2")
        buf.write("\u0651\u065a\5X-\2\u0652\u0653\7\u00e5\2\2\u0653\u065a")
        buf.write("\5\u00acW\2\u0654\u065a\5\u00a2R\2\u0655\u0656\7\4\2\2")
        buf.write("\u0656\u0657\5 \21\2\u0657\u0658\7\5\2\2\u0658\u065a\3")
        buf.write("\2\2\2\u0659\u0650\3\2\2\2\u0659\u0651\3\2\2\2\u0659\u0652")
        buf.write("\3\2\2\2\u0659\u0654\3\2\2\2\u0659\u0655\3\2\2\2\u065a")
        buf.write("U\3\2\2\2\u065b\u065d\5\u00bc_\2\u065c\u065e\t\21\2\2")
        buf.write("\u065d\u065c\3\2\2\2\u065d\u065e\3\2\2\2\u065e\u0661\3")
        buf.write("\2\2\2\u065f\u0660\7\u009d\2\2\u0660\u0662\t\22\2\2\u0661")
        buf.write("\u065f\3\2\2\2\u0661\u0662\3\2\2\2\u0662W\3\2\2\2\u0663")
        buf.write("\u0665\5x=\2\u0664\u0666\5Z.\2\u0665\u0664\3\2\2\2\u0666")
        buf.write("\u0667\3\2\2\2\u0667\u0665\3\2\2\2\u0667\u0668\3\2\2\2")
        buf.write("\u0668Y\3\2\2\2\u0669\u066b\5^\60\2\u066a\u066c\5p9\2")
        buf.write("\u066b\u066a\3\2\2\2\u066b\u066c\3\2\2\2\u066c\u066d\3")
        buf.write("\2\2\2\u066d\u066e\5N(\2\u066e\u0685\3\2\2\2\u066f\u0673")
        buf.write("\5`\61\2\u0670\u0672\5\u0084C\2\u0671\u0670\3\2\2\2\u0672")
        buf.write("\u0675\3\2\2\2\u0673\u0671\3\2\2\2\u0673\u0674\3\2\2\2")
        buf.write("\u0674\u0677\3\2\2\2\u0675\u0673\3\2\2\2\u0676\u0678\5")
        buf.write("p9\2\u0677\u0676\3\2\2\2\u0677\u0678\3\2\2\2\u0678\u067a")
        buf.write("\3\2\2\2\u0679\u067b\5z>\2\u067a\u0679\3\2\2\2\u067a\u067b")
        buf.write("\3\2\2\2\u067b\u067d\3\2\2\2\u067c\u067e\5r:\2\u067d\u067c")
        buf.write("\3\2\2\2\u067d\u067e\3\2\2\2\u067e\u0680\3\2\2\2\u067f")
        buf.write("\u0681\5\u00f0y\2\u0680\u067f\3\2\2\2\u0680\u0681\3\2")
        buf.write("\2\2\u0681\u0682\3\2\2\2\u0682\u0683\5N(\2\u0683\u0685")
        buf.write("\3\2\2\2\u0684\u0669\3\2\2\2\u0684\u066f\3\2\2\2\u0685")
        buf.write("[\3\2\2\2\u0686\u0688\5^\60\2\u0687\u0689\5x=\2\u0688")
        buf.write("\u0687\3\2\2\2\u0688\u0689\3\2\2\2\u0689\u068b\3\2\2\2")
        buf.write("\u068a\u068c\5p9\2\u068b\u068a\3\2\2\2\u068b\u068c\3\2")
        buf.write("\2\2\u068c\u06a4\3\2\2\2\u068d\u068f\5`\61\2\u068e\u0690")
        buf.write("\5x=\2\u068f\u068e\3\2\2\2\u068f\u0690\3\2\2\2\u0690\u0694")
        buf.write("\3\2\2\2\u0691\u0693\5\u0084C\2\u0692\u0691\3\2\2\2\u0693")
        buf.write("\u0696\3\2\2\2\u0694\u0692\3\2\2\2\u0694\u0695\3\2\2\2")
        buf.write("\u0695\u0698\3\2\2\2\u0696\u0694\3\2\2\2\u0697\u0699\5")
        buf.write("p9\2\u0698\u0697\3\2\2\2\u0698\u0699\3\2\2\2\u0699\u069b")
        buf.write("\3\2\2\2\u069a\u069c\5z>\2\u069b\u069a\3\2\2\2\u069b\u069c")
        buf.write("\3\2\2\2\u069c\u069e\3\2\2\2\u069d\u069f\5r:\2\u069e\u069d")
        buf.write("\3\2\2\2\u069e\u069f\3\2\2\2\u069f\u06a1\3\2\2\2\u06a0")
        buf.write("\u06a2\5\u00f0y\2\u06a1\u06a0\3\2\2\2\u06a1\u06a2\3\2")
        buf.write("\2\2\u06a2\u06a4\3\2\2\2\u06a3\u0686\3\2\2\2\u06a3\u068d")
        buf.write("\3\2\2\2\u06a4]\3\2\2\2\u06a5\u06a6\7\u00d0\2\2\u06a6")
        buf.write("\u06a7\7\u00f1\2\2\u06a7\u06a8\7\4\2\2\u06a8\u06a9\5\u00b4")
        buf.write("[\2\u06a9\u06aa\7\5\2\2\u06aa\u06b0\3\2\2\2\u06ab\u06ac")
        buf.write("\7\u0091\2\2\u06ac\u06b0\5\u00b4[\2\u06ad\u06ae\7\u00bd")
        buf.write("\2\2\u06ae\u06b0\5\u00b4[\2\u06af\u06a5\3\2\2\2\u06af")
        buf.write("\u06ab\3\2\2\2\u06af\u06ad\3\2\2\2\u06b0\u06b2\3\2\2\2")
        buf.write("\u06b1\u06b3\5\u00a8U\2\u06b2\u06b1\3\2\2\2\u06b2\u06b3")
        buf.write("\3\2\2\2\u06b3\u06b6\3\2\2\2\u06b4\u06b5\7\u00bb\2\2\u06b5")
        buf.write("\u06b7\7\u011d\2\2\u06b6\u06b4\3\2\2\2\u06b6\u06b7\3\2")
        buf.write("\2\2\u06b7\u06b8\3\2\2\2\u06b8\u06b9\7\u0101\2\2\u06b9")
        buf.write("\u06c6\7\u011d\2\2\u06ba\u06c4\7\30\2\2\u06bb\u06c5\5")
        buf.write("\u0096L\2\u06bc\u06c5\5\u00e6t\2\u06bd\u06c0\7\4\2\2\u06be")
        buf.write("\u06c1\5\u0096L\2\u06bf\u06c1\5\u00e6t\2\u06c0\u06be\3")
        buf.write("\2\2\2\u06c0\u06bf\3\2\2\2\u06c1\u06c2\3\2\2\2\u06c2\u06c3")
        buf.write("\7\5\2\2\u06c3\u06c5\3\2\2\2\u06c4\u06bb\3\2\2\2\u06c4")
        buf.write("\u06bc\3\2\2\2\u06c4\u06bd\3\2\2\2\u06c5\u06c7\3\2\2\2")
        buf.write("\u06c6\u06ba\3\2\2\2\u06c6\u06c7\3\2\2\2\u06c7\u06c9\3")
        buf.write("\2\2\2\u06c8\u06ca\5\u00a8U\2\u06c9\u06c8\3\2\2\2\u06c9")
        buf.write("\u06ca\3\2\2\2\u06ca\u06cd\3\2\2\2\u06cb\u06cc\7\u00ba")
        buf.write("\2\2\u06cc\u06ce\7\u011d\2\2\u06cd\u06cb\3\2\2\2\u06cd")
        buf.write("\u06ce\3\2\2\2\u06ce_\3\2\2\2\u06cf\u06d3\7\u00d0\2\2")
        buf.write("\u06d0\u06d2\5t;\2\u06d1\u06d0\3\2\2\2\u06d2\u06d5\3\2")
        buf.write("\2\2\u06d3\u06d1\3\2\2\2\u06d3\u06d4\3\2\2\2\u06d4\u06d7")
        buf.write("\3\2\2\2\u06d5\u06d3\3\2\2\2\u06d6\u06d8\5\u0086D\2\u06d7")
        buf.write("\u06d6\3\2\2\2\u06d7\u06d8\3\2\2\2\u06d8\u06d9\3\2\2\2")
        buf.write("\u06d9\u06da\5\u00b4[\2\u06daa\3\2\2\2\u06db\u06dc\7\u00d6")
        buf.write("\2\2\u06dc\u06dd\5l\67\2\u06ddc\3\2\2\2\u06de\u06df\7")
        buf.write("\u0105\2\2\u06df\u06e2\7\u0092\2\2\u06e0\u06e1\7\23\2")
        buf.write("\2\u06e1\u06e3\5\u00be`\2\u06e2\u06e0\3\2\2\2\u06e2\u06e3")
        buf.write("\3\2\2\2\u06e3\u06e4\3\2\2\2\u06e4\u06e5\7\u00eb\2\2\u06e5")
        buf.write("\u06e6\5h\65\2\u06e6e\3\2\2\2\u06e7\u06e8\7\u0105\2\2")
        buf.write("\u06e8\u06e9\7\u009b\2\2\u06e9\u06ec\7\u0092\2\2\u06ea")
        buf.write("\u06eb\7\23\2\2\u06eb\u06ed\5\u00be`\2\u06ec\u06ea\3\2")
        buf.write("\2\2\u06ec\u06ed\3\2\2\2\u06ed\u06ee\3\2\2\2\u06ee\u06ef")
        buf.write("\7\u00eb\2\2\u06ef\u06f0\5j\66\2\u06f0g\3\2\2\2\u06f1")
        buf.write("\u06f9\7E\2\2\u06f2\u06f3\7\u00fe\2\2\u06f3\u06f4\7\u00d6")
        buf.write("\2\2\u06f4\u06f9\7\u0114\2\2\u06f5\u06f6\7\u00fe\2\2\u06f6")
        buf.write("\u06f7\7\u00d6\2\2\u06f7\u06f9\5l\67\2\u06f8\u06f1\3\2")
        buf.write("\2\2\u06f8\u06f2\3\2\2\2\u06f8\u06f5\3\2\2\2\u06f9i\3")
        buf.write("\2\2\2\u06fa\u06fb\7y\2\2\u06fb\u070d\7\u0114\2\2\u06fc")
        buf.write("\u06fd\7y\2\2\u06fd\u06fe\7\4\2\2\u06fe\u06ff\5\u00aa")
        buf.write("V\2\u06ff\u0700\7\5\2\2\u0700\u0701\7\u0102\2\2\u0701")
        buf.write("\u0702\7\4\2\2\u0702\u0707\5\u00bc_\2\u0703\u0704\7\6")
        buf.write("\2\2\u0704\u0706\5\u00bc_\2\u0705\u0703\3\2\2\2\u0706")
        buf.write("\u0709\3\2\2\2\u0707\u0705\3\2\2\2\u0707\u0708\3\2\2\2")
        buf.write("\u0708\u070a\3\2\2\2\u0709\u0707\3\2\2\2\u070a\u070b\7")
        buf.write("\5\2\2\u070b\u070d\3\2\2\2\u070c\u06fa\3\2\2\2\u070c\u06fc")
        buf.write("\3\2\2\2\u070dk\3\2\2\2\u070e\u0713\5n8\2\u070f\u0710")
        buf.write("\7\6\2\2\u0710\u0712\5n8\2\u0711\u070f\3\2\2\2\u0712\u0715")
        buf.write("\3\2\2\2\u0713\u0711\3\2\2\2\u0713\u0714\3\2\2\2\u0714")
        buf.write("m\3\2\2\2\u0715\u0713\3\2\2\2\u0716\u0717\5\u00acW\2\u0717")
        buf.write("\u0718\7\u010a\2\2\u0718\u0719\5\u00bc_\2\u0719o\3\2\2")
        buf.write("\2\u071a\u071b\7\u0106\2\2\u071b\u071c\5\u00be`\2\u071c")
        buf.write("q\3\2\2\2\u071d\u071e\7n\2\2\u071e\u071f\5\u00be`\2\u071f")
        buf.write("s\3\2\2\2\u0720\u0721\7\b\2\2\u0721\u0728\5v<\2\u0722")
        buf.write("\u0724\7\6\2\2\u0723\u0722\3\2\2\2\u0723\u0724\3\2\2\2")
        buf.write("\u0724\u0725\3\2\2\2\u0725\u0727\5v<\2\u0726\u0723\3\2")
        buf.write("\2\2\u0727\u072a\3\2\2\2\u0728\u0726\3\2\2\2\u0728\u0729")
        buf.write("\3\2\2\2\u0729\u072b\3\2\2\2\u072a\u0728\3\2\2\2\u072b")
        buf.write("\u072c\7\t\2\2\u072cu\3\2\2\2\u072d\u073b\5\u0104\u0083")
        buf.write("\2\u072e\u072f\5\u0104\u0083\2\u072f\u0730\7\4\2\2\u0730")
        buf.write("\u0735\5\u00c4c\2\u0731\u0732\7\6\2\2\u0732\u0734\5\u00c4")
        buf.write("c\2\u0733\u0731\3\2\2\2\u0734\u0737\3\2\2\2\u0735\u0733")
        buf.write("\3\2\2\2\u0735\u0736\3\2\2\2\u0736\u0738\3\2\2\2\u0737")
        buf.write("\u0735\3\2\2\2\u0738\u0739\7\5\2\2\u0739\u073b\3\2\2\2")
        buf.write("\u073a\u072d\3\2\2\2\u073a\u072e\3\2\2\2\u073bw\3\2\2")
        buf.write("\2\u073c\u073d\7f\2\2\u073d\u0742\5\u0088E\2\u073e\u073f")
        buf.write("\7\6\2\2\u073f\u0741\5\u0088E\2\u0740\u073e\3\2\2\2\u0741")
        buf.write("\u0744\3\2\2\2\u0742\u0740\3\2\2\2\u0742\u0743\3\2\2\2")
        buf.write("\u0743\u0748\3\2\2\2\u0744\u0742\3\2\2\2\u0745\u0747\5")
        buf.write("\u0084C\2\u0746\u0745\3\2\2\2\u0747\u074a\3\2\2\2\u0748")
        buf.write("\u0746\3\2\2\2\u0748\u0749\3\2\2\2\u0749\u074c\3\2\2\2")
        buf.write("\u074a\u0748\3\2\2\2\u074b\u074d\5~@\2\u074c\u074b\3\2")
        buf.write("\2\2\u074c\u074d\3\2\2\2\u074dy\3\2\2\2\u074e\u074f\7")
        buf.write("l\2\2\u074f\u0750\7 \2\2\u0750\u0755\5\u00bc_\2\u0751")
        buf.write("\u0752\7\6\2\2\u0752\u0754\5\u00bc_\2\u0753\u0751\3\2")
        buf.write("\2\2\u0754\u0757\3\2\2\2\u0755\u0753\3\2\2\2\u0755\u0756")
        buf.write("\3\2\2\2\u0756\u0769\3\2\2\2\u0757\u0755\3\2\2\2\u0758")
        buf.write("\u0759\7\u0108\2\2\u0759\u076a\7\u00cb\2\2\u075a\u075b")
        buf.write("\7\u0108\2\2\u075b\u076a\79\2\2\u075c\u075d\7m\2\2\u075d")
        buf.write("\u075e\7\u00d8\2\2\u075e\u075f\7\4\2\2\u075f\u0764\5|")
        buf.write("?\2\u0760\u0761\7\6\2\2\u0761\u0763\5|?\2\u0762\u0760")
        buf.write("\3\2\2\2\u0763\u0766\3\2\2\2\u0764\u0762\3\2\2\2\u0764")
        buf.write("\u0765\3\2\2\2\u0765\u0767\3\2\2\2\u0766\u0764\3\2\2\2")
        buf.write("\u0767\u0768\7\5\2\2\u0768\u076a\3\2\2\2\u0769\u0758\3")
        buf.write("\2\2\2\u0769\u075a\3\2\2\2\u0769\u075c\3\2\2\2\u0769\u076a")
        buf.write("\3\2\2\2\u076a\u077b\3\2\2\2\u076b\u076c\7l\2\2\u076c")
        buf.write("\u076d\7 \2\2\u076d\u076e\7m\2\2\u076e\u076f\7\u00d8\2")
        buf.write("\2\u076f\u0770\7\4\2\2\u0770\u0775\5|?\2\u0771\u0772\7")
        buf.write("\6\2\2\u0772\u0774\5|?\2\u0773\u0771\3\2\2\2\u0774\u0777")
        buf.write("\3\2\2\2\u0775\u0773\3\2\2\2\u0775\u0776\3\2\2\2\u0776")
        buf.write("\u0778\3\2\2\2\u0777\u0775\3\2\2\2\u0778\u0779\7\5\2\2")
        buf.write("\u0779\u077b\3\2\2\2\u077a\u074e\3\2\2\2\u077a\u076b\3")
        buf.write("\2\2\2\u077b{\3\2\2\2\u077c\u0785\7\4\2\2\u077d\u0782")
        buf.write("\5\u00bc_\2\u077e\u077f\7\6\2\2\u077f\u0781\5\u00bc_\2")
        buf.write("\u0780\u077e\3\2\2\2\u0781\u0784\3\2\2\2\u0782\u0780\3")
        buf.write("\2\2\2\u0782\u0783\3\2\2\2\u0783\u0786\3\2\2\2\u0784\u0782")
        buf.write("\3\2\2\2\u0785\u077d\3\2\2\2\u0785\u0786\3\2\2\2\u0786")
        buf.write("\u0787\3\2\2\2\u0787\u078a\7\5\2\2\u0788\u078a\5\u00bc")
        buf.write("_\2\u0789\u077c\3\2\2\2\u0789\u0788\3\2\2\2\u078a}\3\2")
        buf.write("\2\2\u078b\u078c\7\u00b0\2\2\u078c\u078d\7\4\2\2\u078d")
        buf.write("\u078e\5\u00b4[\2\u078e\u078f\7b\2\2\u078f\u0790\5\u0080")
        buf.write("A\2\u0790\u0791\7s\2\2\u0791\u0792\7\4\2\2\u0792\u0797")
        buf.write("\5\u0082B\2\u0793\u0794\7\6\2\2\u0794\u0796\5\u0082B\2")
        buf.write("\u0795\u0793\3\2\2\2\u0796\u0799\3\2\2\2\u0797\u0795\3")
        buf.write("\2\2\2\u0797\u0798\3\2\2\2\u0798\u079a\3\2\2\2\u0799\u0797")
        buf.write("\3\2\2\2\u079a\u079b\7\5\2\2\u079b\u079c\7\5\2\2\u079c")
        buf.write("\177\3\2\2\2\u079d\u07aa\5\u0104\u0083\2\u079e\u079f\7")
        buf.write("\4\2\2\u079f\u07a4\5\u0104\u0083\2\u07a0\u07a1\7\6\2\2")
        buf.write("\u07a1\u07a3\5\u0104\u0083\2\u07a2\u07a0\3\2\2\2\u07a3")
        buf.write("\u07a6\3\2\2\2\u07a4\u07a2\3\2\2\2\u07a4\u07a5\3\2\2\2")
        buf.write("\u07a5\u07a7\3\2\2\2\u07a6\u07a4\3\2\2\2\u07a7\u07a8\7")
        buf.write("\5\2\2\u07a8\u07aa\3\2\2\2\u07a9\u079d\3\2\2\2\u07a9\u079e")
        buf.write("\3\2\2\2\u07aa\u0081\3\2\2\2\u07ab\u07b0\5\u00bc_\2\u07ac")
        buf.write("\u07ae\7\30\2\2\u07ad\u07ac\3\2\2\2\u07ad\u07ae\3\2\2")
        buf.write("\2\u07ae\u07af\3\2\2\2\u07af\u07b1\5\u0104\u0083\2\u07b0")
        buf.write("\u07ad\3\2\2\2\u07b0\u07b1\3\2\2\2\u07b1\u0083\3\2\2\2")
        buf.write("\u07b2\u07b3\7\u0082\2\2\u07b3\u07b5\7\u0103\2\2\u07b4")
        buf.write("\u07b6\7\u00a6\2\2\u07b5\u07b4\3\2\2\2\u07b5\u07b6\3\2")
        buf.write("\2\2\u07b6\u07b7\3\2\2\2\u07b7\u07b8\5\u00fe\u0080\2\u07b8")
        buf.write("\u07c1\7\4\2\2\u07b9\u07be\5\u00bc_\2\u07ba\u07bb\7\6")
        buf.write("\2\2\u07bb\u07bd\5\u00bc_\2\u07bc\u07ba\3\2\2\2\u07bd")
        buf.write("\u07c0\3\2\2\2\u07be\u07bc\3\2\2\2\u07be\u07bf\3\2\2\2")
        buf.write("\u07bf\u07c2\3\2\2\2\u07c0\u07be\3\2\2\2\u07c1\u07b9\3")
        buf.write("\2\2\2\u07c1\u07c2\3\2\2\2\u07c2\u07c3\3\2\2\2\u07c3\u07c4")
        buf.write("\7\5\2\2\u07c4\u07d0\5\u0104\u0083\2\u07c5\u07c7\7\30")
        buf.write("\2\2\u07c6\u07c5\3\2\2\2\u07c6\u07c7\3\2\2\2\u07c7\u07c8")
        buf.write("\3\2\2\2\u07c8\u07cd\5\u0104\u0083\2\u07c9\u07ca\7\6\2")
        buf.write("\2\u07ca\u07cc\5\u0104\u0083\2\u07cb\u07c9\3\2\2\2\u07cc")
        buf.write("\u07cf\3\2\2\2\u07cd\u07cb\3\2\2\2\u07cd\u07ce\3\2\2\2")
        buf.write("\u07ce\u07d1\3\2\2\2\u07cf\u07cd\3\2\2\2\u07d0\u07c6\3")
        buf.write("\2\2\2\u07d0\u07d1\3\2\2\2\u07d1\u0085\3\2\2\2\u07d2\u07d3")
        buf.write("\t\23\2\2\u07d3\u0087\3\2\2\2\u07d4\u07d8\5\u00a0Q\2\u07d5")
        buf.write("\u07d7\5\u008aF\2\u07d6\u07d5\3\2\2\2\u07d7\u07da\3\2")
        buf.write("\2\2\u07d8\u07d6\3\2\2\2\u07d8\u07d9\3\2\2\2\u07d9\u0089")
        buf.write("\3\2\2\2\u07da\u07d8\3\2\2\2\u07db\u07dc\5\u008cG\2\u07dc")
        buf.write("\u07dd\7\177\2\2\u07dd\u07df\5\u00a0Q\2\u07de\u07e0\5")
        buf.write("\u008eH\2\u07df\u07de\3\2\2\2\u07df\u07e0\3\2\2\2\u07e0")
        buf.write("\u07e7\3\2\2\2\u07e1\u07e2\7\u0099\2\2\u07e2\u07e3\5\u008c")
        buf.write("G\2\u07e3\u07e4\7\177\2\2\u07e4\u07e5\5\u00a0Q\2\u07e5")
        buf.write("\u07e7\3\2\2\2\u07e6\u07db\3\2\2\2\u07e6\u07e1\3\2\2\2")
        buf.write("\u07e7\u008b\3\2\2\2\u07e8\u07ea\7v\2\2\u07e9\u07e8\3")
        buf.write("\2\2\2\u07e9\u07ea\3\2\2\2\u07ea\u0801\3\2\2\2\u07eb\u0801")
        buf.write("\78\2\2\u07ec\u07ee\7\u0085\2\2\u07ed\u07ef\7\u00a6\2")
        buf.write("\2\u07ee\u07ed\3\2\2\2\u07ee\u07ef\3\2\2\2\u07ef\u0801")
        buf.write("\3\2\2\2\u07f0\u07f2\7\u0085\2\2\u07f1\u07f0\3\2\2\2\u07f1")
        buf.write("\u07f2\3\2\2\2\u07f2\u07f3\3\2\2\2\u07f3\u0801\7\u00d1")
        buf.write("\2\2\u07f4\u07f6\7\u00c6\2\2\u07f5\u07f7\7\u00a6\2\2\u07f6")
        buf.write("\u07f5\3\2\2\2\u07f6\u07f7\3\2\2\2\u07f7\u0801\3\2\2\2")
        buf.write("\u07f8\u07fa\7g\2\2\u07f9\u07fb\7\u00a6\2\2\u07fa\u07f9")
        buf.write("\3\2\2\2\u07fa\u07fb\3\2\2\2\u07fb\u0801\3\2\2\2\u07fc")
        buf.write("\u07fe\7\u0085\2\2\u07fd\u07fc\3\2\2\2\u07fd\u07fe\3\2")
        buf.write("\2\2\u07fe\u07ff\3\2\2\2\u07ff\u0801\7\24\2\2\u0800\u07e9")
        buf.write("\3\2\2\2\u0800\u07eb\3\2\2\2\u0800\u07ec\3\2\2\2\u0800")
        buf.write("\u07f1\3\2\2\2\u0800\u07f4\3\2\2\2\u0800\u07f8\3\2\2\2")
        buf.write("\u0800\u07fd\3\2\2\2\u0801\u008d\3\2\2\2\u0802\u0803\7")
        buf.write("\u009f\2\2\u0803\u0807\5\u00be`\2\u0804\u0805\7\u0101")
        buf.write("\2\2\u0805\u0807\5\u0094K\2\u0806\u0802\3\2\2\2\u0806")
        buf.write("\u0804\3\2\2\2\u0807\u008f\3\2\2\2\u0808\u0809\7\u00e7")
        buf.write("\2\2\u0809\u080b\7\4\2\2\u080a\u080c\5\u0092J\2\u080b")
        buf.write("\u080a\3\2\2\2\u080b\u080c\3\2\2\2\u080c\u080d\3\2\2\2")
        buf.write("\u080d\u080e\7\5\2\2\u080e\u0091\3\2\2\2\u080f\u0811\7")
        buf.write("\u0113\2\2\u0810\u080f\3\2\2\2\u0810\u0811\3\2\2\2\u0811")
        buf.write("\u0812\3\2\2\2\u0812\u0813\t\24\2\2\u0813\u0828\7\u00af")
        buf.write("\2\2\u0814\u0815\5\u00bc_\2\u0815\u0816\7\u00cd\2\2\u0816")
        buf.write("\u0828\3\2\2\2\u0817\u0818\7\36\2\2\u0818\u0819\7\u0121")
        buf.write("\2\2\u0819\u081a\7\u00a5\2\2\u081a\u081b\7\u009e\2\2\u081b")
        buf.write("\u0824\7\u0121\2\2\u081c\u0822\7\u009f\2\2\u081d\u0823")
        buf.write("\5\u0104\u0083\2\u081e\u081f\5\u00fe\u0080\2\u081f\u0820")
        buf.write("\7\4\2\2\u0820\u0821\7\5\2\2\u0821\u0823\3\2\2\2\u0822")
        buf.write("\u081d\3\2\2\2\u0822\u081e\3\2\2\2\u0823\u0825\3\2\2\2")
        buf.write("\u0824\u081c\3\2\2\2\u0824\u0825\3\2\2\2\u0825\u0828\3")
        buf.write("\2\2\2\u0826\u0828\5\u00bc_\2\u0827\u0810\3\2\2\2\u0827")
        buf.write("\u0814\3\2\2\2\u0827\u0817\3\2\2\2\u0827\u0826\3\2\2\2")
        buf.write("\u0828\u0093\3\2\2\2\u0829\u082a\7\4\2\2\u082a\u082b\5")
        buf.write("\u0096L\2\u082b\u082c\7\5\2\2\u082c\u0095\3\2\2\2\u082d")
        buf.write("\u0832\5\u0100\u0081\2\u082e\u082f\7\6\2\2\u082f\u0831")
        buf.write("\5\u0100\u0081\2\u0830\u082e\3\2\2\2\u0831\u0834\3\2\2")
        buf.write("\2\u0832\u0830\3\2\2\2\u0832\u0833\3\2\2\2\u0833\u0097")
        buf.write("\3\2\2\2\u0834\u0832\3\2\2\2\u0835\u0836\7\4\2\2\u0836")
        buf.write("\u083b\5\u009aN\2\u0837\u0838\7\6\2\2\u0838\u083a\5\u009a")
        buf.write("N\2\u0839\u0837\3\2\2\2\u083a\u083d\3\2\2\2\u083b\u0839")
        buf.write("\3\2\2\2\u083b\u083c\3\2\2\2\u083c\u083e\3\2\2\2\u083d")
        buf.write("\u083b\3\2\2\2\u083e\u083f\7\5\2\2\u083f\u0099\3\2\2\2")
        buf.write("\u0840\u0842\5\u0100\u0081\2\u0841\u0843\t\21\2\2\u0842")
        buf.write("\u0841\3\2\2\2\u0842\u0843\3\2\2\2\u0843\u009b\3\2\2\2")
        buf.write("\u0844\u0845\7\4\2\2\u0845\u084a\5\u009eP\2\u0846\u0847")
        buf.write("\7\6\2\2\u0847\u0849\5\u009eP\2\u0848\u0846\3\2\2\2\u0849")
        buf.write("\u084c\3\2\2\2\u084a\u0848\3\2\2\2\u084a\u084b\3\2\2\2")
        buf.write("\u084b\u084d\3\2\2\2\u084c\u084a\3\2\2\2\u084d\u084e\7")
        buf.write("\5\2\2\u084e\u009d\3\2\2\2\u084f\u0851\5\u0104\u0083\2")
        buf.write("\u0850\u0852\5\36\20\2\u0851\u0850\3\2\2\2\u0851\u0852")
        buf.write("\3\2\2\2\u0852\u009f\3\2\2\2\u0853\u0855\5\u00acW\2\u0854")
        buf.write("\u0856\5\u0090I\2\u0855\u0854\3\2\2\2\u0855\u0856\3\2")
        buf.write("\2\2\u0856\u0857\3\2\2\2\u0857\u0858\5\u00a6T\2\u0858")
        buf.write("\u086c\3\2\2\2\u0859\u085a\7\4\2\2\u085a\u085b\5 \21\2")
        buf.write("\u085b\u085d\7\5\2\2\u085c\u085e\5\u0090I\2\u085d\u085c")
        buf.write("\3\2\2\2\u085d\u085e\3\2\2\2\u085e\u085f\3\2\2\2\u085f")
        buf.write("\u0860\5\u00a6T\2\u0860\u086c\3\2\2\2\u0861\u0862\7\4")
        buf.write("\2\2\u0862\u0863\5\u0088E\2\u0863\u0865\7\5\2\2\u0864")
        buf.write("\u0866\5\u0090I\2\u0865\u0864\3\2\2\2\u0865\u0866\3\2")
        buf.write("\2\2\u0866\u0867\3\2\2\2\u0867\u0868\5\u00a6T\2\u0868")
        buf.write("\u086c\3\2\2\2\u0869\u086c\5\u00a2R\2\u086a\u086c\5\u00a4")
        buf.write("S\2\u086b\u0853\3\2\2\2\u086b\u0859\3\2\2\2\u086b\u0861")
        buf.write("\3\2\2\2\u086b\u0869\3\2\2\2\u086b\u086a\3\2\2\2\u086c")
        buf.write("\u00a1\3\2\2\2\u086d\u086e\7\u0102\2\2\u086e\u0873\5\u00bc")
        buf.write("_\2\u086f\u0870\7\6\2\2\u0870\u0872\5\u00bc_\2\u0871\u086f")
        buf.write("\3\2\2\2\u0872\u0875\3\2\2\2\u0873\u0871\3\2\2\2\u0873")
        buf.write("\u0874\3\2\2\2\u0874\u0876\3\2\2\2\u0875\u0873\3\2\2\2")
        buf.write("\u0876\u0877\5\u00a6T\2\u0877\u00a3\3\2\2\2\u0878\u0879")
        buf.write("\5\u0100\u0081\2\u0879\u0882\7\4\2\2\u087a\u087f\5\u00bc")
        buf.write("_\2\u087b\u087c\7\6\2\2\u087c\u087e\5\u00bc_\2\u087d\u087b")
        buf.write("\3\2\2\2\u087e\u0881\3\2\2\2\u087f\u087d\3\2\2\2\u087f")
        buf.write("\u0880\3\2\2\2\u0880\u0883\3\2\2\2\u0881\u087f\3\2\2\2")
        buf.write("\u0882\u087a\3\2\2\2\u0882\u0883\3\2\2\2\u0883\u0884\3")
        buf.write("\2\2\2\u0884\u0885\7\5\2\2\u0885\u0886\5\u00a6T\2\u0886")
        buf.write("\u00a5\3\2\2\2\u0887\u0889\7\30\2\2\u0888\u0887\3\2\2")
        buf.write("\2\u0888\u0889\3\2\2\2\u0889\u088a\3\2\2\2\u088a\u088c")
        buf.write("\5\u0106\u0084\2\u088b\u088d\5\u0094K\2\u088c\u088b\3")
        buf.write("\2\2\2\u088c\u088d\3\2\2\2\u088d\u088f\3\2\2\2\u088e\u0888")
        buf.write("\3\2\2\2\u088e\u088f\3\2\2\2\u088f\u00a7\3\2\2\2\u0890")
        buf.write("\u0891\7\u00cc\2\2\u0891\u0892\7d\2\2\u0892\u0893\7\u00d3")
        buf.write("\2\2\u0893\u0897\7\u011d\2\2\u0894\u0895\7\u0108\2\2\u0895")
        buf.write("\u0896\7\u00d4\2\2\u0896\u0898\58\35\2\u0897\u0894\3\2")
        buf.write("\2\2\u0897\u0898\3\2\2\2\u0898\u08c2\3\2\2\2\u0899\u089a")
        buf.write("\7\u00cc\2\2\u089a\u089b\7d\2\2\u089b\u08a5\7F\2\2\u089c")
        buf.write("\u089d\7]\2\2\u089d\u089e\7\u00ea\2\2\u089e\u089f\7 \2")
        buf.write("\2\u089f\u08a3\7\u011d\2\2\u08a0\u08a1\7R\2\2\u08a1\u08a2")
        buf.write("\7 \2\2\u08a2\u08a4\7\u011d\2\2\u08a3\u08a0\3\2\2\2\u08a3")
        buf.write("\u08a4\3\2\2\2\u08a4\u08a6\3\2\2\2\u08a5\u089c\3\2\2\2")
        buf.write("\u08a5\u08a6\3\2\2\2\u08a6\u08ac\3\2\2\2\u08a7\u08a8\7")
        buf.write(",\2\2\u08a8\u08a9\7~\2\2\u08a9\u08aa\7\u00ea\2\2\u08aa")
        buf.write("\u08ab\7 \2\2\u08ab\u08ad\7\u011d\2\2\u08ac\u08a7\3\2")
        buf.write("\2\2\u08ac\u08ad\3\2\2\2\u08ad\u08b3\3\2\2\2\u08ae\u08af")
        buf.write("\7\u0091\2\2\u08af\u08b0\7\u0080\2\2\u08b0\u08b1\7\u00ea")
        buf.write("\2\2\u08b1\u08b2\7 \2\2\u08b2\u08b4\7\u011d\2\2\u08b3")
        buf.write("\u08ae\3\2\2\2\u08b3\u08b4\3\2\2\2\u08b4\u08b9\3\2\2\2")
        buf.write("\u08b5\u08b6\7\u0088\2\2\u08b6\u08b7\7\u00ea\2\2\u08b7")
        buf.write("\u08b8\7 \2\2\u08b8\u08ba\7\u011d\2\2\u08b9\u08b5\3\2")
        buf.write("\2\2\u08b9\u08ba\3\2\2\2\u08ba\u08bf\3\2\2\2\u08bb\u08bc")
        buf.write("\7\u009c\2\2\u08bc\u08bd\7D\2\2\u08bd\u08be\7\30\2\2\u08be")
        buf.write("\u08c0\7\u011d\2\2\u08bf\u08bb\3\2\2\2\u08bf\u08c0\3\2")
        buf.write("\2\2\u08c0\u08c2\3\2\2\2\u08c1\u0890\3\2\2\2\u08c1\u0899")
        buf.write("\3\2\2\2\u08c2\u00a9\3\2\2\2\u08c3\u08c8\5\u00acW\2\u08c4")
        buf.write("\u08c5\7\6\2\2\u08c5\u08c7\5\u00acW\2\u08c6\u08c4\3\2")
        buf.write("\2\2\u08c7\u08ca\3\2\2\2\u08c8\u08c6\3\2\2\2\u08c8\u08c9")
        buf.write("\3\2\2\2\u08c9\u00ab\3\2\2\2\u08ca\u08c8\3\2\2\2\u08cb")
        buf.write("\u08d0\5\u0100\u0081\2\u08cc\u08cd\7\7\2\2\u08cd\u08cf")
        buf.write("\5\u0100\u0081\2\u08ce\u08cc\3\2\2\2\u08cf\u08d2\3\2\2")
        buf.write("\2\u08d0\u08ce\3\2\2\2\u08d0\u08d1\3\2\2\2\u08d1\u00ad")
        buf.write("\3\2\2\2\u08d2\u08d0\3\2\2\2\u08d3\u08d4\5\u0100\u0081")
        buf.write("\2\u08d4\u08d5\7\7\2\2\u08d5\u08d7\3\2\2\2\u08d6\u08d3")
        buf.write("\3\2\2\2\u08d6\u08d7\3\2\2\2\u08d7\u08d8\3\2\2\2\u08d8")
        buf.write("\u08d9\5\u0100\u0081\2\u08d9\u00af\3\2\2\2\u08da\u08db")
        buf.write("\5\u0100\u0081\2\u08db\u08dc\7\7\2\2\u08dc\u08de\3\2\2")
        buf.write("\2\u08dd\u08da\3\2\2\2\u08dd\u08de\3\2\2\2\u08de\u08df")
        buf.write("\3\2\2\2\u08df\u08e0\5\u0100\u0081\2\u08e0\u00b1\3\2\2")
        buf.write("\2\u08e1\u08e9\5\u00bc_\2\u08e2\u08e4\7\30\2\2\u08e3\u08e2")
        buf.write("\3\2\2\2\u08e3\u08e4\3\2\2\2\u08e4\u08e7\3\2\2\2\u08e5")
        buf.write("\u08e8\5\u0100\u0081\2\u08e6\u08e8\5\u0094K\2\u08e7\u08e5")
        buf.write("\3\2\2\2\u08e7\u08e6\3\2\2\2\u08e8\u08ea\3\2\2\2\u08e9")
        buf.write("\u08e3\3\2\2\2\u08e9\u08ea\3\2\2\2\u08ea\u00b3\3\2\2\2")
        buf.write("\u08eb\u08f0\5\u00b2Z\2\u08ec\u08ed\7\6\2\2\u08ed\u08ef")
        buf.write("\5\u00b2Z\2\u08ee\u08ec\3\2\2\2\u08ef\u08f2\3\2\2\2\u08f0")
        buf.write("\u08ee\3\2\2\2\u08f0\u08f1\3\2\2\2\u08f1\u00b5\3\2\2\2")
        buf.write("\u08f2\u08f0\3\2\2\2\u08f3\u08f4\7\4\2\2\u08f4\u08f9\5")
        buf.write("\u00b8]\2\u08f5\u08f6\7\6\2\2\u08f6\u08f8\5\u00b8]\2\u08f7")
        buf.write("\u08f5\3\2\2\2\u08f8\u08fb\3\2\2\2\u08f9\u08f7\3\2\2\2")
        buf.write("\u08f9\u08fa\3\2\2\2\u08fa\u08fc\3\2\2\2\u08fb\u08f9\3")
        buf.write("\2\2\2\u08fc\u08fd\7\5\2\2\u08fd\u00b7\3\2\2\2\u08fe\u090c")
        buf.write("\5\u00fe\u0080\2\u08ff\u0900\5\u0104\u0083\2\u0900\u0901")
        buf.write("\7\4\2\2\u0901\u0906\5\u00ba^\2\u0902\u0903\7\6\2\2\u0903")
        buf.write("\u0905\5\u00ba^\2\u0904\u0902\3\2\2\2\u0905\u0908\3\2")
        buf.write("\2\2\u0906\u0904\3\2\2\2\u0906\u0907\3\2\2\2\u0907\u0909")
        buf.write("\3\2\2\2\u0908\u0906\3\2\2\2\u0909\u090a\7\5\2\2\u090a")
        buf.write("\u090c\3\2\2\2\u090b\u08fe\3\2\2\2\u090b\u08ff\3\2\2\2")
        buf.write("\u090c\u00b9\3\2\2\2\u090d\u0910\5\u00fe\u0080\2\u090e")
        buf.write("\u0910\5\u00c6d\2\u090f\u090d\3\2\2\2\u090f\u090e\3\2")
        buf.write("\2\2\u0910\u00bb\3\2\2\2\u0911\u0912\5\u00be`\2\u0912")
        buf.write("\u00bd\3\2\2\2\u0913\u0914\b`\1\2\u0914\u0915\7\u009b")
        buf.write("\2\2\u0915\u0920\5\u00be`\7\u0916\u0917\7U\2\2\u0917\u0918")
        buf.write("\7\4\2\2\u0918\u0919\5 \21\2\u0919\u091a\7\5\2\2\u091a")
        buf.write("\u0920\3\2\2\2\u091b\u091d\5\u00c2b\2\u091c\u091e\5\u00c0")
        buf.write("a\2\u091d\u091c\3\2\2\2\u091d\u091e\3\2\2\2\u091e\u0920")
        buf.write("\3\2\2\2\u091f\u0913\3\2\2\2\u091f\u0916\3\2\2\2\u091f")
        buf.write("\u091b\3\2\2\2\u0920\u0929\3\2\2\2\u0921\u0922\f\4\2\2")
        buf.write("\u0922\u0923\7\23\2\2\u0923\u0928\5\u00be`\5\u0924\u0925")
        buf.write("\f\3\2\2\u0925\u0926\7\u00a3\2\2\u0926\u0928\5\u00be`")
        buf.write("\4\u0927\u0921\3\2\2\2\u0927\u0924\3\2\2\2\u0928\u092b")
        buf.write("\3\2\2\2\u0929\u0927\3\2\2\2\u0929\u092a\3\2\2\2\u092a")
        buf.write("\u00bf\3\2\2\2\u092b\u0929\3\2\2\2\u092c\u092e\7\u009b")
        buf.write("\2\2\u092d\u092c\3\2\2\2\u092d\u092e\3\2\2\2\u092e\u092f")
        buf.write("\3\2\2\2\u092f\u0930\7\34\2\2\u0930\u0931\5\u00c2b\2\u0931")
        buf.write("\u0932\7\23\2\2\u0932\u0933\5\u00c2b\2\u0933\u097f\3\2")
        buf.write("\2\2\u0934\u0936\7\u009b\2\2\u0935\u0934\3\2\2\2\u0935")
        buf.write("\u0936\3\2\2\2\u0936\u0937\3\2\2\2\u0937\u0938\7s\2\2")
        buf.write("\u0938\u0939\7\4\2\2\u0939\u093e\5\u00bc_\2\u093a\u093b")
        buf.write("\7\6\2\2\u093b\u093d\5\u00bc_\2\u093c\u093a\3\2\2\2\u093d")
        buf.write("\u0940\3\2\2\2\u093e\u093c\3\2\2\2\u093e\u093f\3\2\2\2")
        buf.write("\u093f\u0941\3\2\2\2\u0940\u093e\3\2\2\2\u0941\u0942\7")
        buf.write("\5\2\2\u0942\u097f\3\2\2\2\u0943\u0945\7\u009b\2\2\u0944")
        buf.write("\u0943\3\2\2\2\u0944\u0945\3\2\2\2\u0945\u0946\3\2\2\2")
        buf.write("\u0946\u0947\7s\2\2\u0947\u0948\7\4\2\2\u0948\u0949\5")
        buf.write(" \21\2\u0949\u094a\7\5\2\2\u094a\u097f\3\2\2\2\u094b\u094d")
        buf.write("\7\u009b\2\2\u094c\u094b\3\2\2\2\u094c\u094d\3\2\2\2\u094d")
        buf.write("\u094e\3\2\2\2\u094e\u094f\7\u00c7\2\2\u094f\u097f\5\u00c2")
        buf.write("b\2\u0950\u0952\7\u009b\2\2\u0951\u0950\3\2\2\2\u0951")
        buf.write("\u0952\3\2\2\2\u0952\u0953\3\2\2\2\u0953\u0954\7\u0086")
        buf.write("\2\2\u0954\u0962\t\25\2\2\u0955\u0956\7\4\2\2\u0956\u0963")
        buf.write("\7\5\2\2\u0957\u0958\7\4\2\2\u0958\u095d\5\u00bc_\2\u0959")
        buf.write("\u095a\7\6\2\2\u095a\u095c\5\u00bc_\2\u095b\u0959\3\2")
        buf.write("\2\2\u095c\u095f\3\2\2\2\u095d\u095b\3\2\2\2\u095d\u095e")
        buf.write("\3\2\2\2\u095e\u0960\3\2\2\2\u095f\u095d\3\2\2\2\u0960")
        buf.write("\u0961\7\5\2\2\u0961\u0963\3\2\2\2\u0962\u0955\3\2\2\2")
        buf.write("\u0962\u0957\3\2\2\2\u0963\u097f\3\2\2\2\u0964\u0966\7")
        buf.write("\u009b\2\2\u0965\u0964\3\2\2\2\u0965\u0966\3\2\2\2\u0966")
        buf.write("\u0967\3\2\2\2\u0967\u0968\7\u0086\2\2\u0968\u096b\5\u00c2")
        buf.write("b\2\u0969\u096a\7Q\2\2\u096a\u096c\7\u011d\2\2\u096b\u0969")
        buf.write("\3\2\2\2\u096b\u096c\3\2\2\2\u096c\u097f\3\2\2\2\u096d")
        buf.write("\u096f\7}\2\2\u096e\u0970\7\u009b\2\2\u096f\u096e\3\2")
        buf.write("\2\2\u096f\u0970\3\2\2\2\u0970\u0971\3\2\2\2\u0971\u097f")
        buf.write("\7\u009c\2\2\u0972\u0974\7}\2\2\u0973\u0975\7\u009b\2")
        buf.write("\2\u0974\u0973\3\2\2\2\u0974\u0975\3\2\2\2\u0975\u0976")
        buf.write("\3\2\2\2\u0976\u097f\t\26\2\2\u0977\u0979\7}\2\2\u0978")
        buf.write("\u097a\7\u009b\2\2\u0979\u0978\3\2\2\2\u0979\u097a\3\2")
        buf.write("\2\2\u097a\u097b\3\2\2\2\u097b\u097c\7L\2\2\u097c\u097d")
        buf.write("\7f\2\2\u097d\u097f\5\u00c2b\2\u097e\u092d\3\2\2\2\u097e")
        buf.write("\u0935\3\2\2\2\u097e\u0944\3\2\2\2\u097e\u094c\3\2\2\2")
        buf.write("\u097e\u0951\3\2\2\2\u097e\u0965\3\2\2\2\u097e\u096d\3")
        buf.write("\2\2\2\u097e\u0972\3\2\2\2\u097e\u0977\3\2\2\2\u097f\u00c1")
        buf.write("\3\2\2\2\u0980\u0981\bb\1\2\u0981\u0985\5\u00c4c\2\u0982")
        buf.write("\u0983\t\27\2\2\u0983\u0985\5\u00c2b\t\u0984\u0980\3\2")
        buf.write("\2\2\u0984\u0982\3\2\2\2\u0985\u099b\3\2\2\2\u0986\u0987")
        buf.write("\f\b\2\2\u0987\u0988\t\30\2\2\u0988\u099a\5\u00c2b\t\u0989")
        buf.write("\u098a\f\7\2\2\u098a\u098b\t\31\2\2\u098b\u099a\5\u00c2")
        buf.write("b\b\u098c\u098d\f\6\2\2\u098d\u098e\7\u0119\2\2\u098e")
        buf.write("\u099a\5\u00c2b\7\u098f\u0990\f\5\2\2\u0990\u0991\7\u011c")
        buf.write("\2\2\u0991\u099a\5\u00c2b\6\u0992\u0993\f\4\2\2\u0993")
        buf.write("\u0994\7\u011a\2\2\u0994\u099a\5\u00c2b\5\u0995\u0996")
        buf.write("\f\3\2\2\u0996\u0997\5\u00c8e\2\u0997\u0998\5\u00c2b\4")
        buf.write("\u0998\u099a\3\2\2\2\u0999\u0986\3\2\2\2\u0999\u0989\3")
        buf.write("\2\2\2\u0999\u098c\3\2\2\2\u0999\u098f\3\2\2\2\u0999\u0992")
        buf.write("\3\2\2\2\u0999\u0995\3\2\2\2\u099a\u099d\3\2\2\2\u099b")
        buf.write("\u0999\3\2\2\2\u099b\u099c\3\2\2\2\u099c\u00c3\3\2\2\2")
        buf.write("\u099d\u099b\3\2\2\2\u099e\u099f\bc\1\2\u099f\u0a57\t")
        buf.write("\32\2\2\u09a0\u09a2\7#\2\2\u09a1\u09a3\5\u00eex\2\u09a2")
        buf.write("\u09a1\3\2\2\2\u09a3\u09a4\3\2\2\2\u09a4\u09a2\3\2\2\2")
        buf.write("\u09a4\u09a5\3\2\2\2\u09a5\u09a8\3\2\2\2\u09a6\u09a7\7")
        buf.write("O\2\2\u09a7\u09a9\5\u00bc_\2\u09a8\u09a6\3\2\2\2\u09a8")
        buf.write("\u09a9\3\2\2\2\u09a9\u09aa\3\2\2\2\u09aa\u09ab\7P\2\2")
        buf.write("\u09ab\u0a57\3\2\2\2\u09ac\u09ad\7#\2\2\u09ad\u09af\5")
        buf.write("\u00bc_\2\u09ae\u09b0\5\u00eex\2\u09af\u09ae\3\2\2\2\u09b0")
        buf.write("\u09b1\3\2\2\2\u09b1\u09af\3\2\2\2\u09b1\u09b2\3\2\2\2")
        buf.write("\u09b2\u09b5\3\2\2\2\u09b3\u09b4\7O\2\2\u09b4\u09b6\5")
        buf.write("\u00bc_\2\u09b5\u09b3\3\2\2\2\u09b5\u09b6\3\2\2\2\u09b6")
        buf.write("\u09b7\3\2\2\2\u09b7\u09b8\7P\2\2\u09b8\u0a57\3\2\2\2")
        buf.write("\u09b9\u09ba\7$\2\2\u09ba\u09bb\7\4\2\2\u09bb\u09bc\5")
        buf.write("\u00bc_\2\u09bc\u09bd\7\30\2\2\u09bd\u09be\5\u00e0q\2")
        buf.write("\u09be\u09bf\7\5\2\2\u09bf\u0a57\3\2\2\2\u09c0\u09c1\7")
        buf.write("\u00e2\2\2\u09c1\u09ca\7\4\2\2\u09c2\u09c7\5\u00b2Z\2")
        buf.write("\u09c3\u09c4\7\6\2\2\u09c4\u09c6\5\u00b2Z\2\u09c5\u09c3")
        buf.write("\3\2\2\2\u09c6\u09c9\3\2\2\2\u09c7\u09c5\3\2\2\2\u09c7")
        buf.write("\u09c8\3\2\2\2\u09c8\u09cb\3\2\2\2\u09c9\u09c7\3\2\2\2")
        buf.write("\u09ca\u09c2\3\2\2\2\u09ca\u09cb\3\2\2\2\u09cb\u09cc\3")
        buf.write("\2\2\2\u09cc\u0a57\7\5\2\2\u09cd\u09ce\7`\2\2\u09ce\u09cf")
        buf.write("\7\4\2\2\u09cf\u09d2\5\u00bc_\2\u09d0\u09d1\7q\2\2\u09d1")
        buf.write("\u09d3\7\u009d\2\2\u09d2\u09d0\3\2\2\2\u09d2\u09d3\3\2")
        buf.write("\2\2\u09d3\u09d4\3\2\2\2\u09d4\u09d5\7\5\2\2\u09d5\u0a57")
        buf.write("\3\2\2\2\u09d6\u09d7\7\u0081\2\2\u09d7\u09d8\7\4\2\2\u09d8")
        buf.write("\u09db\5\u00bc_\2\u09d9\u09da\7q\2\2\u09da\u09dc\7\u009d")
        buf.write("\2\2\u09db\u09d9\3\2\2\2\u09db\u09dc\3\2\2\2\u09dc\u09dd")
        buf.write("\3\2\2\2\u09dd\u09de\7\5\2\2\u09de\u0a57\3\2\2\2\u09df")
        buf.write("\u09e0\7\u00b2\2\2\u09e0\u09e1\7\4\2\2\u09e1\u09e2\5\u00c2")
        buf.write("b\2\u09e2\u09e3\7s\2\2\u09e3\u09e4\5\u00c2b\2\u09e4\u09e5")
        buf.write("\7\5\2\2\u09e5\u0a57\3\2\2\2\u09e6\u0a57\5\u00c6d\2\u09e7")
        buf.write("\u0a57\7\u0114\2\2\u09e8\u09e9\5\u00fe\u0080\2\u09e9\u09ea")
        buf.write("\7\7\2\2\u09ea\u09eb\7\u0114\2\2\u09eb\u0a57\3\2\2\2\u09ec")
        buf.write("\u09ed\7\4\2\2\u09ed\u09f0\5\u00b2Z\2\u09ee\u09ef\7\6")
        buf.write("\2\2\u09ef\u09f1\5\u00b2Z\2\u09f0\u09ee\3\2\2\2\u09f1")
        buf.write("\u09f2\3\2\2\2\u09f2\u09f0\3\2\2\2\u09f2\u09f3\3\2\2\2")
        buf.write("\u09f3\u09f4\3\2\2\2\u09f4\u09f5\7\5\2\2\u09f5\u0a57\3")
        buf.write("\2\2\2\u09f6\u09f7\7\4\2\2\u09f7\u09f8\5 \21\2\u09f8\u09f9")
        buf.write("\7\5\2\2\u09f9\u0a57\3\2\2\2\u09fa\u09fb\5\u00fc\177\2")
        buf.write("\u09fb\u0a07\7\4\2\2\u09fc\u09fe\5\u0086D\2\u09fd\u09fc")
        buf.write("\3\2\2\2\u09fd\u09fe\3\2\2\2\u09fe\u09ff\3\2\2\2\u09ff")
        buf.write("\u0a04\5\u00bc_\2\u0a00\u0a01\7\6\2\2\u0a01\u0a03\5\u00bc")
        buf.write("_\2\u0a02\u0a00\3\2\2\2\u0a03\u0a06\3\2\2\2\u0a04\u0a02")
        buf.write("\3\2\2\2\u0a04\u0a05\3\2\2\2\u0a05\u0a08\3\2\2\2\u0a06")
        buf.write("\u0a04\3\2\2\2\u0a07\u09fd\3\2\2\2\u0a07\u0a08\3\2\2\2")
        buf.write("\u0a08\u0a09\3\2\2\2\u0a09\u0a10\7\5\2\2\u0a0a\u0a0b\7")
        buf.write("^\2\2\u0a0b\u0a0c\7\4\2\2\u0a0c\u0a0d\7\u0106\2\2\u0a0d")
        buf.write("\u0a0e\5\u00be`\2\u0a0e\u0a0f\7\5\2\2\u0a0f\u0a11\3\2")
        buf.write("\2\2\u0a10\u0a0a\3\2\2\2\u0a10\u0a11\3\2\2\2\u0a11\u0a14")
        buf.write("\3\2\2\2\u0a12\u0a13\7\u00a8\2\2\u0a13\u0a15\5\u00f4{")
        buf.write("\2\u0a14\u0a12\3\2\2\2\u0a14\u0a15\3\2\2\2\u0a15\u0a57")
        buf.write("\3\2\2\2\u0a16\u0a17\5\u0104\u0083\2\u0a17\u0a18\7\n\2")
        buf.write("\2\u0a18\u0a19\5\u00bc_\2\u0a19\u0a57\3\2\2\2\u0a1a\u0a1b")
        buf.write("\7\4\2\2\u0a1b\u0a1e\5\u0104\u0083\2\u0a1c\u0a1d\7\6\2")
        buf.write("\2\u0a1d\u0a1f\5\u0104\u0083\2\u0a1e\u0a1c\3\2\2\2\u0a1f")
        buf.write("\u0a20\3\2\2\2\u0a20\u0a1e\3\2\2\2\u0a20\u0a21\3\2\2\2")
        buf.write("\u0a21\u0a22\3\2\2\2\u0a22\u0a23\7\5\2\2\u0a23\u0a24\7")
        buf.write("\n\2\2\u0a24\u0a25\5\u00bc_\2\u0a25\u0a57\3\2\2\2\u0a26")
        buf.write("\u0a57\5\u0104\u0083\2\u0a27\u0a28\7\4\2\2\u0a28\u0a29")
        buf.write("\5\u00bc_\2\u0a29\u0a2a\7\5\2\2\u0a2a\u0a57\3\2\2\2\u0a2b")
        buf.write("\u0a2c\7Z\2\2\u0a2c\u0a2d\7\4\2\2\u0a2d\u0a2e\5\u0104")
        buf.write("\u0083\2\u0a2e\u0a2f\7f\2\2\u0a2f\u0a30\5\u00c2b\2\u0a30")
        buf.write("\u0a31\7\5\2\2\u0a31\u0a57\3\2\2\2\u0a32\u0a33\t\33\2")
        buf.write("\2\u0a33\u0a34\7\4\2\2\u0a34\u0a35\5\u00c2b\2\u0a35\u0a36")
        buf.write("\t\34\2\2\u0a36\u0a39\5\u00c2b\2\u0a37\u0a38\t\35\2\2")
        buf.write("\u0a38\u0a3a\5\u00c2b\2\u0a39\u0a37\3\2\2\2\u0a39\u0a3a")
        buf.write("\3\2\2\2\u0a3a\u0a3b\3\2\2\2\u0a3b\u0a3c\7\5\2\2\u0a3c")
        buf.write("\u0a57\3\2\2\2\u0a3d\u0a3e\7\u00f2\2\2\u0a3e\u0a40\7\4")
        buf.write("\2\2\u0a3f\u0a41\t\36\2\2\u0a40\u0a3f\3\2\2\2\u0a40\u0a41")
        buf.write("\3\2\2\2\u0a41\u0a43\3\2\2\2\u0a42\u0a44\5\u00c2b\2\u0a43")
        buf.write("\u0a42\3\2\2\2\u0a43\u0a44\3\2\2\2\u0a44\u0a45\3\2\2\2")
        buf.write("\u0a45\u0a46\7f\2\2\u0a46\u0a47\5\u00c2b\2\u0a47\u0a48")
        buf.write("\7\5\2\2\u0a48\u0a57\3\2\2\2\u0a49\u0a4a\7\u00aa\2\2\u0a4a")
        buf.write("\u0a4b\7\4\2\2\u0a4b\u0a4c\5\u00c2b\2\u0a4c\u0a4d\7\u00b1")
        buf.write("\2\2\u0a4d\u0a4e\5\u00c2b\2\u0a4e\u0a4f\7f\2\2\u0a4f\u0a52")
        buf.write("\5\u00c2b\2\u0a50\u0a51\7b\2\2\u0a51\u0a53\5\u00c2b\2")
        buf.write("\u0a52\u0a50\3\2\2\2\u0a52\u0a53\3\2\2\2\u0a53\u0a54\3")
        buf.write("\2\2\2\u0a54\u0a55\7\5\2\2\u0a55\u0a57\3\2\2\2\u0a56\u099e")
        buf.write("\3\2\2\2\u0a56\u09a0\3\2\2\2\u0a56\u09ac\3\2\2\2\u0a56")
        buf.write("\u09b9\3\2\2\2\u0a56\u09c0\3\2\2\2\u0a56\u09cd\3\2\2\2")
        buf.write("\u0a56\u09d6\3\2\2\2\u0a56\u09df\3\2\2\2\u0a56\u09e6\3")
        buf.write("\2\2\2\u0a56\u09e7\3\2\2\2\u0a56\u09e8\3\2\2\2\u0a56\u09ec")
        buf.write("\3\2\2\2\u0a56\u09f6\3\2\2\2\u0a56\u09fa\3\2\2\2\u0a56")
        buf.write("\u0a16\3\2\2\2\u0a56\u0a1a\3\2\2\2\u0a56\u0a26\3\2\2\2")
        buf.write("\u0a56\u0a27\3\2\2\2\u0a56\u0a2b\3\2\2\2\u0a56\u0a32\3")
        buf.write("\2\2\2\u0a56\u0a3d\3\2\2\2\u0a56\u0a49\3\2\2\2\u0a57\u0a62")
        buf.write("\3\2\2\2\u0a58\u0a59\f\n\2\2\u0a59\u0a5a\7\13\2\2\u0a5a")
        buf.write("\u0a5b\5\u00c2b\2\u0a5b\u0a5c\7\f\2\2\u0a5c\u0a61\3\2")
        buf.write("\2\2\u0a5d\u0a5e\f\b\2\2\u0a5e\u0a5f\7\7\2\2\u0a5f\u0a61")
        buf.write("\5\u0104\u0083\2\u0a60\u0a58\3\2\2\2\u0a60\u0a5d\3\2\2")
        buf.write("\2\u0a61\u0a64\3\2\2\2\u0a62\u0a60\3\2\2\2\u0a62\u0a63")
        buf.write("\3\2\2\2\u0a63\u00c5\3\2\2\2\u0a64\u0a62\3\2\2\2\u0a65")
        buf.write("\u0a72\7\u009c\2\2\u0a66\u0a72\5\u00d0i\2\u0a67\u0a68")
        buf.write("\5\u0104\u0083\2\u0a68\u0a69\7\u011d\2\2\u0a69\u0a72\3")
        buf.write("\2\2\2\u0a6a\u0a72\5\u010a\u0086\2\u0a6b\u0a72\5\u00ce")
        buf.write("h\2\u0a6c\u0a6e\7\u011d\2\2\u0a6d\u0a6c\3\2\2\2\u0a6e")
        buf.write("\u0a6f\3\2\2\2\u0a6f\u0a6d\3\2\2\2\u0a6f\u0a70\3\2\2\2")
        buf.write("\u0a70\u0a72\3\2\2\2\u0a71\u0a65\3\2\2\2\u0a71\u0a66\3")
        buf.write("\2\2\2\u0a71\u0a67\3\2\2\2\u0a71\u0a6a\3\2\2\2\u0a71\u0a6b")
        buf.write("\3\2\2\2\u0a71\u0a6d\3\2\2\2\u0a72\u00c7\3\2\2\2\u0a73")
        buf.write("\u0a74\t\37\2\2\u0a74\u00c9\3\2\2\2\u0a75\u0a76\t \2\2")
        buf.write("\u0a76\u00cb\3\2\2\2\u0a77\u0a78\t!\2\2\u0a78\u00cd\3")
        buf.write("\2\2\2\u0a79\u0a7a\t\"\2\2\u0a7a\u00cf\3\2\2\2\u0a7b\u0a7e")
        buf.write("\7{\2\2\u0a7c\u0a7f\5\u00d2j\2\u0a7d\u0a7f\5\u00d6l\2")
        buf.write("\u0a7e\u0a7c\3\2\2\2\u0a7e\u0a7d\3\2\2\2\u0a7e\u0a7f\3")
        buf.write("\2\2\2\u0a7f\u00d1\3\2\2\2\u0a80\u0a82\5\u00d4k\2\u0a81")
        buf.write("\u0a83\5\u00d8m\2\u0a82\u0a81\3\2\2\2\u0a82\u0a83\3\2")
        buf.write("\2\2\u0a83\u00d3\3\2\2\2\u0a84\u0a85\5\u00dan\2\u0a85")
        buf.write("\u0a86\5\u00dco\2\u0a86\u0a88\3\2\2\2\u0a87\u0a84\3\2")
        buf.write("\2\2\u0a88\u0a89\3\2\2\2\u0a89\u0a87\3\2\2\2\u0a89\u0a8a")
        buf.write("\3\2\2\2\u0a8a\u00d5\3\2\2\2\u0a8b\u0a8e\5\u00d8m\2\u0a8c")
        buf.write("\u0a8f\5\u00d4k\2\u0a8d\u0a8f\5\u00d8m\2\u0a8e\u0a8c\3")
        buf.write("\2\2\2\u0a8e\u0a8d\3\2\2\2\u0a8e\u0a8f\3\2\2\2\u0a8f\u00d7")
        buf.write("\3\2\2\2\u0a90\u0a91\5\u00dan\2\u0a91\u0a92\5\u00dco\2")
        buf.write("\u0a92\u0a93\7\u00ec\2\2\u0a93\u0a94\5\u00dco\2\u0a94")
        buf.write("\u00d9\3\2\2\2\u0a95\u0a97\t#\2\2\u0a96\u0a95\3\2\2\2")
        buf.write("\u0a96\u0a97\3\2\2\2\u0a97\u0a98\3\2\2\2\u0a98\u0a9b\t")
        buf.write("\24\2\2\u0a99\u0a9b\7\u011d\2\2\u0a9a\u0a96\3\2\2\2\u0a9a")
        buf.write("\u0a99\3\2\2\2\u0a9b\u00db\3\2\2\2\u0a9c\u0aa4\7B\2\2")
        buf.write("\u0a9d\u0aa4\7o\2\2\u0a9e\u0aa4\7\u0094\2\2\u0a9f\u0aa4")
        buf.write("\7\u0095\2\2\u0aa0\u0aa4\7\u00cf\2\2\u0aa1\u0aa4\7\u0109")
        buf.write("\2\2\u0aa2\u0aa4\5\u0104\u0083\2\u0aa3\u0a9c\3\2\2\2\u0aa3")
        buf.write("\u0a9d\3\2\2\2\u0aa3\u0a9e\3\2\2\2\u0aa3\u0a9f\3\2\2\2")
        buf.write("\u0aa3\u0aa0\3\2\2\2\u0aa3\u0aa1\3\2\2\2\u0aa3\u0aa2\3")
        buf.write("\2\2\2\u0aa4\u00dd\3\2\2\2\u0aa5\u0aa9\7`\2\2\u0aa6\u0aa7")
        buf.write("\7\17\2\2\u0aa7\u0aa9\5\u0100\u0081\2\u0aa8\u0aa5\3\2")
        buf.write("\2\2\u0aa8\u0aa6\3\2\2\2\u0aa9\u00df\3\2\2\2\u0aaa\u0aab")
        buf.write("\7\27\2\2\u0aab\u0aac\7\u010e\2\2\u0aac\u0aad\5\u00e0")
        buf.write("q\2\u0aad\u0aae\7\u0110\2\2\u0aae\u0acd\3\2\2\2\u0aaf")
        buf.write("\u0ab0\7\u0091\2\2\u0ab0\u0ab1\7\u010e\2\2\u0ab1\u0ab2")
        buf.write("\5\u00e0q\2\u0ab2\u0ab3\7\6\2\2\u0ab3\u0ab4\5\u00e0q\2")
        buf.write("\u0ab4\u0ab5\7\u0110\2\2\u0ab5\u0acd\3\2\2\2\u0ab6\u0abd")
        buf.write("\7\u00e2\2\2\u0ab7\u0ab9\7\u010e\2\2\u0ab8\u0aba\5\u00ea")
        buf.write("v\2\u0ab9\u0ab8\3\2\2\2\u0ab9\u0aba\3\2\2\2\u0aba\u0abb")
        buf.write("\3\2\2\2\u0abb\u0abe\7\u0110\2\2\u0abc\u0abe\7\u010c\2")
        buf.write("\2\u0abd\u0ab7\3\2\2\2\u0abd\u0abc\3\2\2\2\u0abe\u0acd")
        buf.write("\3\2\2\2\u0abf\u0aca\5\u0104\u0083\2\u0ac0\u0ac1\7\4\2")
        buf.write("\2\u0ac1\u0ac6\7\u0121\2\2\u0ac2\u0ac3\7\6\2\2\u0ac3\u0ac5")
        buf.write("\7\u0121\2\2\u0ac4\u0ac2\3\2\2\2\u0ac5\u0ac8\3\2\2\2\u0ac6")
        buf.write("\u0ac4\3\2\2\2\u0ac6\u0ac7\3\2\2\2\u0ac7\u0ac9\3\2\2\2")
        buf.write("\u0ac8\u0ac6\3\2\2\2\u0ac9\u0acb\7\5\2\2\u0aca\u0ac0\3")
        buf.write("\2\2\2\u0aca\u0acb\3\2\2\2\u0acb\u0acd\3\2\2\2\u0acc\u0aaa")
        buf.write("\3\2\2\2\u0acc\u0aaf\3\2\2\2\u0acc\u0ab6\3\2\2\2\u0acc")
        buf.write("\u0abf\3\2\2\2\u0acd\u00e1\3\2\2\2\u0ace\u0ad3\5\u00e4")
        buf.write("s\2\u0acf\u0ad0\7\6\2\2\u0ad0\u0ad2\5\u00e4s\2\u0ad1\u0acf")
        buf.write("\3\2\2\2\u0ad2\u0ad5\3\2\2\2\u0ad3\u0ad1\3\2\2\2\u0ad3")
        buf.write("\u0ad4\3\2\2\2\u0ad4\u00e3\3\2\2\2\u0ad5\u0ad3\3\2\2\2")
        buf.write("\u0ad6\u0ad7\5\u00acW\2\u0ad7\u0ada\5\u00e0q\2\u0ad8\u0ad9")
        buf.write("\7\u009b\2\2\u0ad9\u0adb\7\u009c\2\2\u0ada\u0ad8\3\2\2")
        buf.write("\2\u0ada\u0adb\3\2\2\2\u0adb\u0add\3\2\2\2\u0adc\u0ade")
        buf.write("\5\36\20\2\u0add\u0adc\3\2\2\2\u0add\u0ade\3\2\2\2\u0ade")
        buf.write("\u0ae0\3\2\2\2\u0adf\u0ae1\5\u00dep\2\u0ae0\u0adf\3\2")
        buf.write("\2\2\u0ae0\u0ae1\3\2\2\2\u0ae1\u00e5\3\2\2\2\u0ae2\u0ae7")
        buf.write("\5\u00e8u\2\u0ae3\u0ae4\7\6\2\2\u0ae4\u0ae6\5\u00e8u\2")
        buf.write("\u0ae5\u0ae3\3\2\2\2\u0ae6\u0ae9\3\2\2\2\u0ae7\u0ae5\3")
        buf.write("\2\2\2\u0ae7\u0ae8\3\2\2\2\u0ae8\u00e7\3\2\2\2\u0ae9\u0ae7")
        buf.write("\3\2\2\2\u0aea\u0aeb\5\u0100\u0081\2\u0aeb\u0aee\5\u00e0")
        buf.write("q\2\u0aec\u0aed\7\u009b\2\2\u0aed\u0aef\7\u009c\2\2\u0aee")
        buf.write("\u0aec\3\2\2\2\u0aee\u0aef\3\2\2\2\u0aef\u0af1\3\2\2\2")
        buf.write("\u0af0\u0af2\5\36\20\2\u0af1\u0af0\3\2\2\2\u0af1\u0af2")
        buf.write("\3\2\2\2\u0af2\u00e9\3\2\2\2\u0af3\u0af8\5\u00ecw\2\u0af4")
        buf.write("\u0af5\7\6\2\2\u0af5\u0af7\5\u00ecw\2\u0af6\u0af4\3\2")
        buf.write("\2\2\u0af7\u0afa\3\2\2\2\u0af8\u0af6\3\2\2\2\u0af8\u0af9")
        buf.write("\3\2\2\2\u0af9\u00eb\3\2\2\2\u0afa\u0af8\3\2\2\2\u0afb")
        buf.write("\u0afc\5\u0104\u0083\2\u0afc\u0afd\7\r\2\2\u0afd\u0b00")
        buf.write("\5\u00e0q\2\u0afe\u0aff\7\u009b\2\2\u0aff\u0b01\7\u009c")
        buf.write("\2\2\u0b00\u0afe\3\2\2\2\u0b00\u0b01\3\2\2\2\u0b01\u0b03")
        buf.write("\3\2\2\2\u0b02\u0b04\5\36\20\2\u0b03\u0b02\3\2\2\2\u0b03")
        buf.write("\u0b04\3\2\2\2\u0b04\u00ed\3\2\2\2\u0b05\u0b06\7\u0105")
        buf.write("\2\2\u0b06\u0b07\5\u00bc_\2\u0b07\u0b08\7\u00eb\2\2\u0b08")
        buf.write("\u0b09\5\u00bc_\2\u0b09\u00ef\3\2\2\2\u0b0a\u0b0b\7\u0107")
        buf.write("\2\2\u0b0b\u0b10\5\u00f2z\2\u0b0c\u0b0d\7\6\2\2\u0b0d")
        buf.write("\u0b0f\5\u00f2z\2\u0b0e\u0b0c\3\2\2\2\u0b0f\u0b12\3\2")
        buf.write("\2\2\u0b10\u0b0e\3\2\2\2\u0b10\u0b11\3\2\2\2\u0b11\u00f1")
        buf.write("\3\2\2\2\u0b12\u0b10\3\2\2\2\u0b13\u0b14\5\u0100\u0081")
        buf.write("\2\u0b14\u0b15\7\30\2\2\u0b15\u0b16\5\u00f4{\2\u0b16\u00f3")
        buf.write("\3\2\2\2\u0b17\u0b46\5\u0100\u0081\2\u0b18\u0b19\7\4\2")
        buf.write("\2\u0b19\u0b1a\5\u0100\u0081\2\u0b1a\u0b1b\7\5\2\2\u0b1b")
        buf.write("\u0b46\3\2\2\2\u0b1c\u0b3f\7\4\2\2\u0b1d\u0b1e\7(\2\2")
        buf.write("\u0b1e\u0b1f\7 \2\2\u0b1f\u0b24\5\u00bc_\2\u0b20\u0b21")
        buf.write("\7\6\2\2\u0b21\u0b23\5\u00bc_\2\u0b22\u0b20\3\2\2\2\u0b23")
        buf.write("\u0b26\3\2\2\2\u0b24\u0b22\3\2\2\2\u0b24\u0b25\3\2\2\2")
        buf.write("\u0b25\u0b40\3\2\2\2\u0b26\u0b24\3\2\2\2\u0b27\u0b28\t")
        buf.write("$\2\2\u0b28\u0b29\7 \2\2\u0b29\u0b2e\5\u00bc_\2\u0b2a")
        buf.write("\u0b2b\7\6\2\2\u0b2b\u0b2d\5\u00bc_\2\u0b2c\u0b2a\3\2")
        buf.write("\2\2\u0b2d\u0b30\3\2\2\2\u0b2e\u0b2c\3\2\2\2\u0b2e\u0b2f")
        buf.write("\3\2\2\2\u0b2f\u0b32\3\2\2\2\u0b30\u0b2e\3\2\2\2\u0b31")
        buf.write("\u0b27\3\2\2\2\u0b31\u0b32\3\2\2\2\u0b32\u0b3d\3\2\2\2")
        buf.write("\u0b33\u0b34\t%\2\2\u0b34\u0b35\7 \2\2\u0b35\u0b3a\5V")
        buf.write(",\2\u0b36\u0b37\7\6\2\2\u0b37\u0b39\5V,\2\u0b38\u0b36")
        buf.write("\3\2\2\2\u0b39\u0b3c\3\2\2\2\u0b3a\u0b38\3\2\2\2\u0b3a")
        buf.write("\u0b3b\3\2\2\2\u0b3b\u0b3e\3\2\2\2\u0b3c\u0b3a\3\2\2\2")
        buf.write("\u0b3d\u0b33\3\2\2\2\u0b3d\u0b3e\3\2\2\2\u0b3e\u0b40\3")
        buf.write("\2\2\2\u0b3f\u0b1d\3\2\2\2\u0b3f\u0b31\3\2\2\2\u0b40\u0b42")
        buf.write("\3\2\2\2\u0b41\u0b43\5\u00f6|\2\u0b42\u0b41\3\2\2\2\u0b42")
        buf.write("\u0b43\3\2\2\2\u0b43\u0b44\3\2\2\2\u0b44\u0b46\7\5\2\2")
        buf.write("\u0b45\u0b17\3\2\2\2\u0b45\u0b18\3\2\2\2\u0b45\u0b1c\3")
        buf.write("\2\2\2\u0b46\u00f5\3\2\2\2\u0b47\u0b48\7\u00b9\2\2\u0b48")
        buf.write("\u0b58\5\u00f8}\2\u0b49\u0b4a\7\u00cd\2\2\u0b4a\u0b58")
        buf.write("\5\u00f8}\2\u0b4b\u0b4c\7\u00b9\2\2\u0b4c\u0b4d\7\34\2")
        buf.write("\2\u0b4d\u0b4e\5\u00f8}\2\u0b4e\u0b4f\7\23\2\2\u0b4f\u0b50")
        buf.write("\5\u00f8}\2\u0b50\u0b58\3\2\2\2\u0b51\u0b52\7\u00cd\2")
        buf.write("\2\u0b52\u0b53\7\34\2\2\u0b53\u0b54\5\u00f8}\2\u0b54\u0b55")
        buf.write("\7\23\2\2\u0b55\u0b56\5\u00f8}\2\u0b56\u0b58\3\2\2\2\u0b57")
        buf.write("\u0b47\3\2\2\2\u0b57\u0b49\3\2\2\2\u0b57\u0b4b\3\2\2\2")
        buf.write("\u0b57\u0b51\3\2\2\2\u0b58\u00f7\3\2\2\2\u0b59\u0b5a\7")
        buf.write("\u00f7\2\2\u0b5a\u0b61\t&\2\2\u0b5b\u0b5c\7:\2\2\u0b5c")
        buf.write("\u0b61\7\u00cc\2\2\u0b5d\u0b5e\5\u00bc_\2\u0b5e\u0b5f")
        buf.write("\t&\2\2\u0b5f\u0b61\3\2\2\2\u0b60\u0b59\3\2\2\2\u0b60")
        buf.write("\u0b5b\3\2\2\2\u0b60\u0b5d\3\2\2\2\u0b61\u00f9\3\2\2\2")
        buf.write("\u0b62\u0b67\5\u00fe\u0080\2\u0b63\u0b64\7\6\2\2\u0b64")
        buf.write("\u0b66\5\u00fe\u0080\2\u0b65\u0b63\3\2\2\2\u0b66\u0b69")
        buf.write("\3\2\2\2\u0b67\u0b65\3\2\2\2\u0b67\u0b68\3\2\2\2\u0b68")
        buf.write("\u00fb\3\2\2\2\u0b69\u0b67\3\2\2\2\u0b6a\u0b6f\5\u00fe")
        buf.write("\u0080\2\u0b6b\u0b6f\7^\2\2\u0b6c\u0b6f\7\u0085\2\2\u0b6d")
        buf.write("\u0b6f\7\u00c6\2\2\u0b6e\u0b6a\3\2\2\2\u0b6e\u0b6b\3\2")
        buf.write("\2\2\u0b6e\u0b6c\3\2\2\2\u0b6e\u0b6d\3\2\2\2\u0b6f\u00fd")
        buf.write("\3\2\2\2\u0b70\u0b75\5\u0104\u0083\2\u0b71\u0b72\7\7\2")
        buf.write("\2\u0b72\u0b74\5\u0104\u0083\2\u0b73\u0b71\3\2\2\2\u0b74")
        buf.write("\u0b77\3\2\2\2\u0b75\u0b73\3\2\2\2\u0b75\u0b76\3\2\2\2")
        buf.write("\u0b76\u00ff\3\2\2\2\u0b77\u0b75\3\2\2\2\u0b78\u0b79\5")
        buf.write("\u0104\u0083\2\u0b79\u0b7a\5\u0102\u0082\2\u0b7a\u0101")
        buf.write("\3\2\2\2\u0b7b\u0b7c\7\u0113\2\2\u0b7c\u0b7e\5\u0104\u0083")
        buf.write("\2\u0b7d\u0b7b\3\2\2\2\u0b7e\u0b7f\3\2\2\2\u0b7f\u0b7d")
        buf.write("\3\2\2\2\u0b7f\u0b80\3\2\2\2\u0b80\u0b83\3\2\2\2\u0b81")
        buf.write("\u0b83\3\2\2\2\u0b82\u0b7d\3\2\2\2\u0b82\u0b81\3\2\2\2")
        buf.write("\u0b83\u0103\3\2\2\2\u0b84\u0b88\5\u0106\u0084\2\u0b85")
        buf.write("\u0b86\6\u0083\24\2\u0b86\u0b88\5\u0110\u0089\2\u0b87")
        buf.write("\u0b84\3\2\2\2\u0b87\u0b85\3\2\2\2\u0b88\u0105\3\2\2\2")
        buf.write("\u0b89\u0b90\7\u0126\2\2\u0b8a\u0b90\5\u0108\u0085\2\u0b8b")
        buf.write("\u0b8c\6\u0084\25\2\u0b8c\u0b90\5\u010e\u0088\2\u0b8d")
        buf.write("\u0b8e\6\u0084\26\2\u0b8e\u0b90\5\u0112\u008a\2\u0b8f")
        buf.write("\u0b89\3\2\2\2\u0b8f\u0b8a\3\2\2\2\u0b8f\u0b8b\3\2\2\2")
        buf.write("\u0b8f\u0b8d\3\2\2\2\u0b90\u0107\3\2\2\2\u0b91\u0b92\7")
        buf.write("\u0127\2\2\u0b92\u0109\3\2\2\2\u0b93\u0b95\6\u0086\27")
        buf.write("\2\u0b94\u0b96\7\u0113\2\2\u0b95\u0b94\3\2\2\2\u0b95\u0b96")
        buf.write("\3\2\2\2\u0b96\u0b97\3\2\2\2\u0b97\u0bbb\7\u0122\2\2\u0b98")
        buf.write("\u0b9a\6\u0086\30\2\u0b99\u0b9b\7\u0113\2\2\u0b9a\u0b99")
        buf.write("\3\2\2\2\u0b9a\u0b9b\3\2\2\2\u0b9b\u0b9c\3\2\2\2\u0b9c")
        buf.write("\u0bbb\7\u0123\2\2\u0b9d\u0b9f\6\u0086\31\2\u0b9e\u0ba0")
        buf.write("\7\u0113\2\2\u0b9f\u0b9e\3\2\2\2\u0b9f\u0ba0\3\2\2\2\u0ba0")
        buf.write("\u0ba1\3\2\2\2\u0ba1\u0bbb\t\'\2\2\u0ba2\u0ba4\7\u0113")
        buf.write("\2\2\u0ba3\u0ba2\3\2\2\2\u0ba3\u0ba4\3\2\2\2\u0ba4\u0ba5")
        buf.write("\3\2\2\2\u0ba5\u0bbb\7\u0121\2\2\u0ba6\u0ba8\7\u0113\2")
        buf.write("\2\u0ba7\u0ba6\3\2\2\2\u0ba7\u0ba8\3\2\2\2\u0ba8\u0ba9")
        buf.write("\3\2\2\2\u0ba9\u0bbb\7\u011e\2\2\u0baa\u0bac\7\u0113\2")
        buf.write("\2\u0bab\u0baa\3\2\2\2\u0bab\u0bac\3\2\2\2\u0bac\u0bad")
        buf.write("\3\2\2\2\u0bad\u0bbb\7\u011f\2\2\u0bae\u0bb0\7\u0113\2")
        buf.write("\2\u0baf\u0bae\3\2\2\2\u0baf\u0bb0\3\2\2\2\u0bb0\u0bb1")
        buf.write("\3\2\2\2\u0bb1\u0bbb\7\u0120\2\2\u0bb2\u0bb4\7\u0113\2")
        buf.write("\2\u0bb3\u0bb2\3\2\2\2\u0bb3\u0bb4\3\2\2\2\u0bb4\u0bb5")
        buf.write("\3\2\2\2\u0bb5\u0bbb\7\u0124\2\2\u0bb6\u0bb8\7\u0113\2")
        buf.write("\2\u0bb7\u0bb6\3\2\2\2\u0bb7\u0bb8\3\2\2\2\u0bb8\u0bb9")
        buf.write("\3\2\2\2\u0bb9\u0bbb\7\u0125\2\2\u0bba\u0b93\3\2\2\2\u0bba")
        buf.write("\u0b98\3\2\2\2\u0bba\u0b9d\3\2\2\2\u0bba\u0ba3\3\2\2\2")
        buf.write("\u0bba\u0ba7\3\2\2\2\u0bba\u0bab\3\2\2\2\u0bba\u0baf\3")
        buf.write("\2\2\2\u0bba\u0bb3\3\2\2\2\u0bba\u0bb7\3\2\2\2\u0bbb\u010b")
        buf.write("\3\2\2\2\u0bbc\u0bbd\7\u00f5\2\2\u0bbd\u0bc4\5\u00e0q")
        buf.write("\2\u0bbe\u0bc4\5\36\20\2\u0bbf\u0bc4\5\u00dep\2\u0bc0")
        buf.write("\u0bc1\t(\2\2\u0bc1\u0bc2\7\u009b\2\2\u0bc2\u0bc4\7\u009c")
        buf.write("\2\2\u0bc3\u0bbc\3\2\2\2\u0bc3\u0bbe\3\2\2\2\u0bc3\u0bbf")
        buf.write("\3\2\2\2\u0bc3\u0bc0\3\2\2\2\u0bc4\u010d\3\2\2\2\u0bc5")
        buf.write("\u0bc6\t)\2\2\u0bc6\u010f\3\2\2\2\u0bc7\u0bc8\t*\2\2\u0bc8")
        buf.write("\u0111\3\2\2\2\u0bc9\u0bca\t+\2\2\u0bca\u0113\3\2\2\2")
        buf.write("\u018c\u0118\u0131\u0136\u013e\u0146\u0148\u015c\u0160")
        buf.write("\u0166\u0169\u016c\u0174\u0177\u017b\u017e\u0186\u018b")
        buf.write("\u018e\u0195\u01a1\u01aa\u01ac\u01b0\u01b3\u01ba\u01c5")
        buf.write("\u01c7\u01cf\u01d4\u01d7\u01dd\u01e8\u0228\u0231\u0235")
        buf.write("\u023b\u023f\u0244\u024a\u0256\u025e\u0264\u0271\u0276")
        buf.write("\u0286\u028d\u0291\u0297\u02a6\u02aa\u02b0\u02b6\u02b9")
        buf.write("\u02bc\u02c2\u02c6\u02ce\u02d0\u02d9\u02dc\u02e5\u02ea")
        buf.write("\u02f0\u02f7\u02fa\u0300\u030b\u030e\u0312\u0317\u031c")
        buf.write("\u0323\u0326\u0329\u0330\u0335\u033e\u0346\u034c\u034f")
        buf.write("\u0352\u0358\u035c\u0360\u0364\u0366\u036e\u0376\u037c")
        buf.write("\u0382\u0385\u0389\u038c\u0390\u03a9\u03ac\u03b0\u03b6")
        buf.write("\u03b9\u03bc\u03c2\u03ca\u03cf\u03d5\u03db\u03e7\u03ea")
        buf.write("\u03f1\u03f8\u0400\u0403\u040b\u040f\u0416\u048a\u0492")
        buf.write("\u049a\u04a3\u04ad\u04b1\u04b4\u04ba\u04c0\u04cc\u04d8")
        buf.write("\u04dd\u04e6\u04ee\u04f5\u04f7\u04fc\u0500\u0505\u050a")
        buf.write("\u050f\u0512\u0517\u051b\u0520\u0522\u0526\u052f\u0537")
        buf.write("\u0540\u0547\u0550\u0555\u0558\u056b\u056d\u0576\u057d")
        buf.write("\u0580\u0587\u058b\u0591\u0599\u05a4\u05af\u05b6\u05bc")
        buf.write("\u05c9\u05d0\u05d7\u05e3\u05eb\u05f1\u05f4\u05fd\u0600")
        buf.write("\u0609\u060c\u0615\u0618\u0621\u0624\u0627\u062c\u062e")
        buf.write("\u063a\u0641\u0648\u064b\u064d\u0659\u065d\u0661\u0667")
        buf.write("\u066b\u0673\u0677\u067a\u067d\u0680\u0684\u0688\u068b")
        buf.write("\u068f\u0694\u0698\u069b\u069e\u06a1\u06a3\u06af\u06b2")
        buf.write("\u06b6\u06c0\u06c4\u06c6\u06c9\u06cd\u06d3\u06d7\u06e2")
        buf.write("\u06ec\u06f8\u0707\u070c\u0713\u0723\u0728\u0735\u073a")
        buf.write("\u0742\u0748\u074c\u0755\u0764\u0769\u0775\u077a\u0782")
        buf.write("\u0785\u0789\u0797\u07a4\u07a9\u07ad\u07b0\u07b5\u07be")
        buf.write("\u07c1\u07c6\u07cd\u07d0\u07d8\u07df\u07e6\u07e9\u07ee")
        buf.write("\u07f1\u07f6\u07fa\u07fd\u0800\u0806\u080b\u0810\u0822")
        buf.write("\u0824\u0827\u0832\u083b\u0842\u084a\u0851\u0855\u085d")
        buf.write("\u0865\u086b\u0873\u087f\u0882\u0888\u088c\u088e\u0897")
        buf.write("\u08a3\u08a5\u08ac\u08b3\u08b9\u08bf\u08c1\u08c8\u08d0")
        buf.write("\u08d6\u08dd\u08e3\u08e7\u08e9\u08f0\u08f9\u0906\u090b")
        buf.write("\u090f\u091d\u091f\u0927\u0929\u092d\u0935\u093e\u0944")
        buf.write("\u094c\u0951\u095d\u0962\u0965\u096b\u096f\u0974\u0979")
        buf.write("\u097e\u0984\u0999\u099b\u09a4\u09a8\u09b1\u09b5\u09c7")
        buf.write("\u09ca\u09d2\u09db\u09f2\u09fd\u0a04\u0a07\u0a10\u0a14")
        buf.write("\u0a20\u0a39\u0a40\u0a43\u0a52\u0a56\u0a60\u0a62\u0a6f")
        buf.write("\u0a71\u0a7e\u0a82\u0a89\u0a8e\u0a96\u0a9a\u0aa3\u0aa8")
        buf.write("\u0ab9\u0abd\u0ac6\u0aca\u0acc\u0ad3\u0ada\u0add\u0ae0")
        buf.write("\u0ae7\u0aee\u0af1\u0af8\u0b00\u0b03\u0b10\u0b24\u0b2e")
        buf.write("\u0b31\u0b3a\u0b3d\u0b3f\u0b42\u0b45\u0b57\u0b60\u0b67")
        buf.write("\u0b6e\u0b75\u0b7f\u0b82\u0b87\u0b8f\u0b95\u0b9a\u0b9f")
        buf.write("\u0ba3\u0ba7\u0bab\u0baf\u0bb3\u0bb7\u0bba\u0bc3")
        return buf.getvalue()


class SqlBaseParser ( Parser ):

    grammarFileName = "SqlBase.g4"

    atn = ATNDeserializer().deserialize(serializedATN())

    decisionsToDFA = [ DFA(ds, i) for i, ds in enumerate(atn.decisionToState) ]

    sharedContextCache = PredictionContextCache()

    literalNames = [ "<INVALID>", "';'", "'('", "')'", "','", "'.'", "'/*+'",
                     "'*/'", "'->'", "'['", "']'", "':'", "'ADD'", "'AFTER'",
                     "'ALL'", "'ALTER'", "'ANALYZE'", "'AND'", "'ANTI'",
                     "'ANY'", "'ARCHIVE'", "'ARRAY'", "'AS'", "'ASC'", "'AT'",
                     "'AUTHORIZATION'", "'BETWEEN'", "'BOTH'", "'BUCKET'",
                     "'BUCKETS'", "'BY'", "'CACHE'", "'CASCADE'", "'CASE'",
                     "'CAST'", "'CHANGE'", "'CHECK'", "'CLEAR'", "'CLUSTER'",
                     "'CLUSTERED'", "'CODEGEN'", "'COLLATE'", "'COLLECTION'",
                     "'COLUMN'", "'COLUMNS'", "'COMMENT'", "'COMMIT'", "'COMPACT'",
                     "'COMPACTIONS'", "'COMPUTE'", "'CONCATENATE'", "'CONSTRAINT'",
                     "'COST'", "'CREATE'", "'CROSS'", "'CUBE'", "'CURRENT'",
                     "'CURRENT_DATE'", "'CURRENT_TIME'", "'CURRENT_TIMESTAMP'",
                     "'CURRENT_USER'", "'DATA'", "'DATABASE'", "<INVALID>",
                     "'DAY'", "'DBPROPERTIES'", "'DEFINED'", "'DELETE'",
                     "'DELIMITED'", "'DESC'", "'DESCRIBE'", "'DFS'", "'DIRECTORIES'",
                     "'DIRECTORY'", "'DISTINCT'", "'DISTRIBUTE'", "'DROP'",
                     "'ELSE'", "'END'", "'ESCAPE'", "'ESCAPED'", "'EXCEPT'",
                     "'EXCHANGE'", "'EXISTS'", "'EXPLAIN'", "'EXPORT'",
                     "'EXTENDED'", "'EXTERNAL'", "'EXTRACT'", "'FALSE'",
                     "'FETCH'", "'FIELDS'", "'FILTER'", "'FILEFORMAT'",
                     "'FIRST'", "'FOLLOWING'", "'FOR'", "'FOREIGN'", "'FORMAT'",
                     "'FORMATTED'", "'FROM'", "'FULL'", "'FUNCTION'", "'FUNCTIONS'",
                     "'GLOBAL'", "'GRANT'", "'GROUP'", "'GROUPING'", "'HAVING'",
                     "'HOUR'", "'IF'", "'IGNORE'", "'IMPORT'", "'IN'", "'INDEX'",
                     "'INDEXES'", "'INNER'", "'INPATH'", "'INPUTFORMAT'",
                     "'INSERT'", "'INTERSECT'", "'INTERVAL'", "'INTO'",
                     "'IS'", "'ITEMS'", "'JOIN'", "'KEYS'", "'LAST'", "'LATERAL'",
                     "'LAZY'", "'LEADING'", "'LEFT'", "'LIKE'", "'LIMIT'",
                     "'LINES'", "'LIST'", "'LOAD'", "'LOCAL'", "'LOCATION'",
                     "'LOCK'", "'LOCKS'", "'LOGICAL'", "'MACRO'", "'MAP'",
                     "'MATCHED'", "'MERGE'", "'MINUTE'", "'MONTH'", "'MSCK'",
                     "'NAMESPACE'", "'NAMESPACES'", "'NATURAL'", "'NO'",
                     "<INVALID>", "'NULL'", "'NULLS'", "'OF'", "'ON'", "'ONLY'",
                     "'OPTION'", "'OPTIONS'", "'OR'", "'ORDER'", "'OUT'",
                     "'OUTER'", "'OUTPUTFORMAT'", "'OVER'", "'OVERLAPS'",
                     "'OVERLAY'", "'OVERWRITE'", "'PARTITION'", "'PARTITIONED'",
                     "'PARTITIONS'", "'PERCENT'", "'PIVOT'", "'PLACING'",
                     "'POSITION'", "'PRECEDING'", "'PRIMARY'", "'PRINCIPALS'",
                     "'PROPERTIES'", "'PURGE'", "'QUERY'", "'RANGE'", "'RECORDREADER'",
                     "'RECORDWRITER'", "'RECOVER'", "'REDUCE'", "'REFERENCES'",
                     "'REFRESH'", "'RENAME'", "'REPAIR'", "'REPLACE'", "'RESET'",
                     "'RESTRICT'", "'REVOKE'", "'RIGHT'", "<INVALID>", "'ROLE'",
                     "'ROLES'", "'ROLLBACK'", "'ROLLUP'", "'ROW'", "'ROWS'",
                     "'SCHEMA'", "'SECOND'", "'SELECT'", "'SEMI'", "'SEPARATED'",
                     "'SERDE'", "'SERDEPROPERTIES'", "'SESSION_USER'", "'SET'",
                     "'MINUS'", "'SETS'", "'SHOW'", "'SKEWED'", "'SOME'",
                     "'SORT'", "'SORTED'", "'START'", "'STATISTICS'", "'STORED'",
                     "'STRATIFY'", "'STRUCT'", "'SUBSTR'", "'SUBSTRING'",
                     "'TABLE'", "'TABLES'", "'TABLESAMPLE'", "'TBLPROPERTIES'",
                     "<INVALID>", "'TERMINATED'", "'THEN'", "'TO'", "'TOUCH'",
                     "'TRAILING'", "'TRANSACTION'", "'TRANSACTIONS'", "'TRANSFORM'",
                     "'TRIM'", "'TRUE'", "'TRUNCATE'", "'TYPE'", "'UNARCHIVE'",
                     "'UNBOUNDED'", "'UNCACHE'", "'UNION'", "'UNIQUE'",
                     "'UNKNOWN'", "'UNLOCK'", "'UNSET'", "'UPDATE'", "'USE'",
                     "'USER'", "'USING'", "'VALUES'", "'VIEW'", "'VIEWS'",
                     "'WHEN'", "'WHERE'", "'WINDOW'", "'WITH'", "'YEAR'",
                     "<INVALID>", "'<=>'", "'<>'", "'!='", "'<'", "<INVALID>",
                     "'>'", "<INVALID>", "'+'", "'-'", "'*'", "'/'", "'%'",
                     "'DIV'", "'~'", "'&'", "'|'", "'||'", "'^'" ]

    symbolicNames = [ "<INVALID>", "<INVALID>", "<INVALID>", "<INVALID>",
                      "<INVALID>", "<INVALID>", "<INVALID>", "<INVALID>",
                      "<INVALID>", "<INVALID>", "<INVALID>", "<INVALID>",
                      "ADD", "AFTER", "ALL", "ALTER", "ANALYZE", "AND",
                      "ANTI", "ANY", "ARCHIVE", "ARRAY", "AS", "ASC", "AT",
                      "AUTHORIZATION", "BETWEEN", "BOTH", "BUCKET", "BUCKETS",
                      "BY", "CACHE", "CASCADE", "CASE", "CAST", "CHANGE",
                      "CHECK", "CLEAR", "CLUSTER", "CLUSTERED", "CODEGEN",
                      "COLLATE", "COLLECTION", "COLUMN", "COLUMNS", "COMMENT",
                      "COMMIT", "COMPACT", "COMPACTIONS", "COMPUTE", "CONCATENATE",
                      "CONSTRAINT", "COST", "CREATE", "CROSS", "CUBE", "CURRENT",
                      "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
                      "CURRENT_USER", "DATA", "DATABASE", "DATABASES", "DAY",
                      "DBPROPERTIES", "DEFINED", "DELETE", "DELIMITED",
                      "DESC", "DESCRIBE", "DFS", "DIRECTORIES", "DIRECTORY",
                      "DISTINCT", "DISTRIBUTE", "DROP", "ELSE", "END", "ESCAPE",
                      "ESCAPED", "EXCEPT", "EXCHANGE", "EXISTS", "EXPLAIN",
                      "EXPORT", "EXTENDED", "EXTERNAL", "EXTRACT", "FALSE",
                      "FETCH", "FIELDS", "FILTER", "FILEFORMAT", "FIRST",
                      "FOLLOWING", "FOR", "FOREIGN", "FORMAT", "FORMATTED",
                      "FROM", "FULL", "FUNCTION", "FUNCTIONS", "GLOBAL",
                      "GRANT", "GROUP", "GROUPING", "HAVING", "HOUR", "IF",
                      "IGNORE", "IMPORT", "IN", "INDEX", "INDEXES", "INNER",
                      "INPATH", "INPUTFORMAT", "INSERT", "INTERSECT", "INTERVAL",
                      "INTO", "IS", "ITEMS", "JOIN", "KEYS", "LAST", "LATERAL",
                      "LAZY", "LEADING", "LEFT", "LIKE", "LIMIT", "LINES",
                      "LIST", "LOAD", "LOCAL", "LOCATION", "LOCK", "LOCKS",
                      "LOGICAL", "MACRO", "MAP", "MATCHED", "MERGE", "MINUTE",
                      "MONTH", "MSCK", "NAMESPACE", "NAMESPACES", "NATURAL",
                      "NO", "NOT", "NULL", "NULLS", "OF", "ON", "ONLY",
                      "OPTION", "OPTIONS", "OR", "ORDER", "OUT", "OUTER",
                      "OUTPUTFORMAT", "OVER", "OVERLAPS", "OVERLAY", "OVERWRITE",
                      "PARTITION", "PARTITIONED", "PARTITIONS", "PERCENTLIT",
                      "PIVOT", "PLACING", "POSITION", "PRECEDING", "PRIMARY",
                      "PRINCIPALS", "PROPERTIES", "PURGE", "QUERY", "RANGE",
                      "RECORDREADER", "RECORDWRITER", "RECOVER", "REDUCE",
                      "REFERENCES", "REFRESH", "RENAME", "REPAIR", "REPLACE",
                      "RESET", "RESTRICT", "REVOKE", "RIGHT", "RLIKE", "ROLE",
                      "ROLES", "ROLLBACK", "ROLLUP", "ROW", "ROWS", "SCHEMA",
                      "SECOND", "SELECT", "SEMI", "SEPARATED", "SERDE",
                      "SERDEPROPERTIES", "SESSION_USER", "SET", "SETMINUS",
                      "SETS", "SHOW", "SKEWED", "SOME", "SORT", "SORTED",
                      "START", "STATISTICS", "STORED", "STRATIFY", "STRUCT",
                      "SUBSTR", "SUBSTRING", "TABLE", "TABLES", "TABLESAMPLE",
                      "TBLPROPERTIES", "TEMPORARY", "TERMINATED", "THEN",
                      "TO", "TOUCH", "TRAILING", "TRANSACTION", "TRANSACTIONS",
                      "TRANSFORM", "TRIM", "TRUE", "TRUNCATE", "TYPE", "UNARCHIVE",
                      "UNBOUNDED", "UNCACHE", "UNION", "UNIQUE", "UNKNOWN",
                      "UNLOCK", "UNSET", "UPDATE", "USE", "USER", "USING",
                      "VALUES", "VIEW", "VIEWS", "WHEN", "WHERE", "WINDOW",
                      "WITH", "YEAR", "EQ", "NSEQ", "NEQ", "NEQJ", "LT",
                      "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH",
                      "PERCENT", "DIV", "TILDE", "AMPERSAND", "PIPE", "CONCAT_PIPE",
                      "HAT", "STRING", "BIGINT_LITERAL", "SMALLINT_LITERAL",
                      "TINYINT_LITERAL", "INTEGER_VALUE", "EXPONENT_VALUE",
                      "DECIMAL_VALUE", "DOUBLE_LITERAL", "BIGDECIMAL_LITERAL",
                      "IDENTIFIER", "BACKQUOTED_IDENTIFIER", "SIMPLE_COMMENT",
                      "BRACKETED_COMMENT", "WS", "UNRECOGNIZED" ]

    RULE_singleStatement = 0
    RULE_singleExpression = 1
    RULE_singleTableIdentifier = 2
    RULE_singleMultipartIdentifier = 3
    RULE_singleFunctionIdentifier = 4
    RULE_singleDataType = 5
    RULE_singleTableSchema = 6
    RULE_statement = 7
    RULE_unsupportedHiveNativeCommands = 8
    RULE_createTableHeader = 9
    RULE_replaceTableHeader = 10
    RULE_bucketSpec = 11
    RULE_skewSpec = 12
    RULE_locationSpec = 13
    RULE_commentSpec = 14
    RULE_query = 15
    RULE_insertInto = 16
    RULE_partitionSpecLocation = 17
    RULE_partitionSpec = 18
    RULE_partitionVal = 19
    RULE_namespace = 20
    RULE_describeFuncName = 21
    RULE_describeColName = 22
    RULE_ctes = 23
    RULE_namedQuery = 24
    RULE_tableProvider = 25
    RULE_createTableClauses = 26
    RULE_tablePropertyList = 27
    RULE_tableProperty = 28
    RULE_tablePropertyKey = 29
    RULE_tablePropertyValue = 30
    RULE_constantList = 31
    RULE_nestedConstantList = 32
    RULE_createFileFormat = 33
    RULE_fileFormat = 34
    RULE_storageHandler = 35
    RULE_resource = 36
    RULE_dmlStatementNoWith = 37
    RULE_queryOrganization = 38
    RULE_multiInsertQueryBody = 39
    RULE_queryTerm = 40
    RULE_queryPrimary = 41
    RULE_sortItem = 42
    RULE_fromStatement = 43
    RULE_fromStatementBody = 44
    RULE_querySpecification = 45
    RULE_transformClause = 46
    RULE_selectClause = 47
    RULE_setClause = 48
    RULE_matchedClause = 49
    RULE_notMatchedClause = 50
    RULE_matchedAction = 51
    RULE_notMatchedAction = 52
    RULE_assignmentList = 53
    RULE_assignment = 54
    RULE_whereClause = 55
    RULE_havingClause = 56
    RULE_hint = 57
    RULE_hintStatement = 58
    RULE_fromClause = 59
    RULE_aggregationClause = 60
    RULE_groupingSet = 61
    RULE_pivotClause = 62
    RULE_pivotColumn = 63
    RULE_pivotValue = 64
    RULE_lateralView = 65
    RULE_setQuantifier = 66
    RULE_relation = 67
    RULE_joinRelation = 68
    RULE_joinType = 69
    RULE_joinCriteria = 70
    RULE_sample = 71
    RULE_sampleMethod = 72
    RULE_identifierList = 73
    RULE_identifierSeq = 74
    RULE_orderedIdentifierList = 75
    RULE_orderedIdentifier = 76
    RULE_identifierCommentList = 77
    RULE_identifierComment = 78
    RULE_relationPrimary = 79
    RULE_inlineTable = 80
    RULE_functionTable = 81
    RULE_tableAlias = 82
    RULE_rowFormat = 83
    RULE_multipartIdentifierList = 84
    RULE_multipartIdentifier = 85
    RULE_tableIdentifier = 86
    RULE_functionIdentifier = 87
    RULE_namedExpression = 88
    RULE_namedExpressionSeq = 89
    RULE_transformList = 90
    RULE_transform = 91
    RULE_transformArgument = 92
    RULE_expression = 93
    RULE_booleanExpression = 94
    RULE_predicate = 95
    RULE_valueExpression = 96
    RULE_primaryExpression = 97
    RULE_constant = 98
    RULE_comparisonOperator = 99
    RULE_arithmeticOperator = 100
    RULE_predicateOperator = 101
    RULE_booleanValue = 102
    RULE_interval = 103
    RULE_errorCapturingMultiUnitsInterval = 104
    RULE_multiUnitsInterval = 105
    RULE_errorCapturingUnitToUnitInterval = 106
    RULE_unitToUnitInterval = 107
    RULE_intervalValue = 108
    RULE_intervalUnit = 109
    RULE_colPosition = 110
    RULE_dataType = 111
    RULE_qualifiedColTypeWithPositionList = 112
    RULE_qualifiedColTypeWithPosition = 113
    RULE_colTypeList = 114
    RULE_colType = 115
    RULE_complexColTypeList = 116
    RULE_complexColType = 117
    RULE_whenClause = 118
    RULE_windowClause = 119
    RULE_namedWindow = 120
    RULE_windowSpec = 121
    RULE_windowFrame = 122
    RULE_frameBound = 123
    RULE_qualifiedNameList = 124
    RULE_functionName = 125
    RULE_qualifiedName = 126
    RULE_errorCapturingIdentifier = 127
    RULE_errorCapturingIdentifierExtra = 128
    RULE_identifier = 129
    RULE_strictIdentifier = 130
    RULE_quotedIdentifier = 131
    RULE_number = 132
    RULE_alterColumnAction = 133
    RULE_ansiNonReserved = 134
    RULE_strictNonReserved = 135
    RULE_nonReserved = 136

    ruleNames =  [ "singleStatement", "singleExpression", "singleTableIdentifier",
                   "singleMultipartIdentifier", "singleFunctionIdentifier",
                   "singleDataType", "singleTableSchema", "statement", "unsupportedHiveNativeCommands",
                   "createTableHeader", "replaceTableHeader", "bucketSpec",
                   "skewSpec", "locationSpec", "commentSpec", "query", "insertInto",
                   "partitionSpecLocation", "partitionSpec", "partitionVal",
                   "namespace", "describeFuncName", "describeColName", "ctes",
                   "namedQuery", "tableProvider", "createTableClauses",
                   "tablePropertyList", "tableProperty", "tablePropertyKey",
                   "tablePropertyValue", "constantList", "nestedConstantList",
                   "createFileFormat", "fileFormat", "storageHandler", "resource",
                   "dmlStatementNoWith", "queryOrganization", "multiInsertQueryBody",
                   "queryTerm", "queryPrimary", "sortItem", "fromStatement",
                   "fromStatementBody", "querySpecification", "transformClause",
                   "selectClause", "setClause", "matchedClause", "notMatchedClause",
                   "matchedAction", "notMatchedAction", "assignmentList",
                   "assignment", "whereClause", "havingClause", "hint",
                   "hintStatement", "fromClause", "aggregationClause", "groupingSet",
                   "pivotClause", "pivotColumn", "pivotValue", "lateralView",
                   "setQuantifier", "relation", "joinRelation", "joinType",
                   "joinCriteria", "sample", "sampleMethod", "identifierList",
                   "identifierSeq", "orderedIdentifierList", "orderedIdentifier",
                   "identifierCommentList", "identifierComment", "relationPrimary",
                   "inlineTable", "functionTable", "tableAlias", "rowFormat",
                   "multipartIdentifierList", "multipartIdentifier", "tableIdentifier",
                   "functionIdentifier", "namedExpression", "namedExpressionSeq",
                   "transformList", "transform", "transformArgument", "expression",
                   "booleanExpression", "predicate", "valueExpression",
                   "primaryExpression", "constant", "comparisonOperator",
                   "arithmeticOperator", "predicateOperator", "booleanValue",
                   "interval", "errorCapturingMultiUnitsInterval", "multiUnitsInterval",
                   "errorCapturingUnitToUnitInterval", "unitToUnitInterval",
                   "intervalValue", "intervalUnit", "colPosition", "dataType",
                   "qualifiedColTypeWithPositionList", "qualifiedColTypeWithPosition",
                   "colTypeList", "colType", "complexColTypeList", "complexColType",
                   "whenClause", "windowClause", "namedWindow", "windowSpec",
                   "windowFrame", "frameBound", "qualifiedNameList", "functionName",
                   "qualifiedName", "errorCapturingIdentifier", "errorCapturingIdentifierExtra",
                   "identifier", "strictIdentifier", "quotedIdentifier",
                   "number", "alterColumnAction", "ansiNonReserved", "strictNonReserved",
                   "nonReserved" ]

    EOF = Token.EOF
    T__0=1
    T__1=2
    T__2=3
    T__3=4
    T__4=5
    T__5=6
    T__6=7
    T__7=8
    T__8=9
    T__9=10
    T__10=11
    ADD=12
    AFTER=13
    ALL=14
    ALTER=15
    ANALYZE=16
    AND=17
    ANTI=18
    ANY=19
    ARCHIVE=20
    ARRAY=21
    AS=22
    ASC=23
    AT=24
    AUTHORIZATION=25
    BETWEEN=26
    BOTH=27
    BUCKET=28
    BUCKETS=29
    BY=30
    CACHE=31
    CASCADE=32
    CASE=33
    CAST=34
    CHANGE=35
    CHECK=36
    CLEAR=37
    CLUSTER=38
    CLUSTERED=39
    CODEGEN=40
    COLLATE=41
    COLLECTION=42
    COLUMN=43
    COLUMNS=44
    COMMENT=45
    COMMIT=46
    COMPACT=47
    COMPACTIONS=48
    COMPUTE=49
    CONCATENATE=50
    CONSTRAINT=51
    COST=52
    CREATE=53
    CROSS=54
    CUBE=55
    CURRENT=56
    CURRENT_DATE=57
    CURRENT_TIME=58
    CURRENT_TIMESTAMP=59
    CURRENT_USER=60
    DATA=61
    DATABASE=62
    DATABASES=63
    DAY=64
    DBPROPERTIES=65
    DEFINED=66
    DELETE=67
    DELIMITED=68
    DESC=69
    DESCRIBE=70
    DFS=71
    DIRECTORIES=72
    DIRECTORY=73
    DISTINCT=74
    DISTRIBUTE=75
    DROP=76
    ELSE=77
    END=78
    ESCAPE=79
    ESCAPED=80
    EXCEPT=81
    EXCHANGE=82
    EXISTS=83
    EXPLAIN=84
    EXPORT=85
    EXTENDED=86
    EXTERNAL=87
    EXTRACT=88
    FALSE=89
    FETCH=90
    FIELDS=91
    FILTER=92
    FILEFORMAT=93
    FIRST=94
    FOLLOWING=95
    FOR=96
    FOREIGN=97
    FORMAT=98
    FORMATTED=99
    FROM=100
    FULL=101
    FUNCTION=102
    FUNCTIONS=103
    GLOBAL=104
    GRANT=105
    GROUP=106
    GROUPING=107
    HAVING=108
    HOUR=109
    IF=110
    IGNORE=111
    IMPORT=112
    IN=113
    INDEX=114
    INDEXES=115
    INNER=116
    INPATH=117
    INPUTFORMAT=118
    INSERT=119
    INTERSECT=120
    INTERVAL=121
    INTO=122
    IS=123
    ITEMS=124
    JOIN=125
    KEYS=126
    LAST=127
    LATERAL=128
    LAZY=129
    LEADING=130
    LEFT=131
    LIKE=132
    LIMIT=133
    LINES=134
    LIST=135
    LOAD=136
    LOCAL=137
    LOCATION=138
    LOCK=139
    LOCKS=140
    LOGICAL=141
    MACRO=142
    MAP=143
    MATCHED=144
    MERGE=145
    MINUTE=146
    MONTH=147
    MSCK=148
    NAMESPACE=149
    NAMESPACES=150
    NATURAL=151
    NO=152
    NOT=153
    NULL=154
    NULLS=155
    OF=156
    ON=157
    ONLY=158
    OPTION=159
    OPTIONS=160
    OR=161
    ORDER=162
    OUT=163
    OUTER=164
    OUTPUTFORMAT=165
    OVER=166
    OVERLAPS=167
    OVERLAY=168
    OVERWRITE=169
    PARTITION=170
    PARTITIONED=171
    PARTITIONS=172
    PERCENTLIT=173
    PIVOT=174
    PLACING=175
    POSITION=176
    PRECEDING=177
    PRIMARY=178
    PRINCIPALS=179
    PROPERTIES=180
    PURGE=181
    QUERY=182
    RANGE=183
    RECORDREADER=184
    RECORDWRITER=185
    RECOVER=186
    REDUCE=187
    REFERENCES=188
    REFRESH=189
    RENAME=190
    REPAIR=191
    REPLACE=192
    RESET=193
    RESTRICT=194
    REVOKE=195
    RIGHT=196
    RLIKE=197
    ROLE=198
    ROLES=199
    ROLLBACK=200
    ROLLUP=201
    ROW=202
    ROWS=203
    SCHEMA=204
    SECOND=205
    SELECT=206
    SEMI=207
    SEPARATED=208
    SERDE=209
    SERDEPROPERTIES=210
    SESSION_USER=211
    SET=212
    SETMINUS=213
    SETS=214
    SHOW=215
    SKEWED=216
    SOME=217
    SORT=218
    SORTED=219
    START=220
    STATISTICS=221
    STORED=222
    STRATIFY=223
    STRUCT=224
    SUBSTR=225
    SUBSTRING=226
    TABLE=227
    TABLES=228
    TABLESAMPLE=229
    TBLPROPERTIES=230
    TEMPORARY=231
    TERMINATED=232
    THEN=233
    TO=234
    TOUCH=235
    TRAILING=236
    TRANSACTION=237
    TRANSACTIONS=238
    TRANSFORM=239
    TRIM=240
    TRUE=241
    TRUNCATE=242
    TYPE=243
    UNARCHIVE=244
    UNBOUNDED=245
    UNCACHE=246
    UNION=247
    UNIQUE=248
    UNKNOWN=249
    UNLOCK=250
    UNSET=251
    UPDATE=252
    USE=253
    USER=254
    USING=255
    VALUES=256
    VIEW=257
    VIEWS=258
    WHEN=259
    WHERE=260
    WINDOW=261
    WITH=262
    YEAR=263
    EQ=264
    NSEQ=265
    NEQ=266
    NEQJ=267
    LT=268
    LTE=269
    GT=270
    GTE=271
    PLUS=272
    MINUS=273
    ASTERISK=274
    SLASH=275
    PERCENT=276
    DIV=277
    TILDE=278
    AMPERSAND=279
    PIPE=280
    CONCAT_PIPE=281
    HAT=282
    STRING=283
    BIGINT_LITERAL=284
    SMALLINT_LITERAL=285
    TINYINT_LITERAL=286
    INTEGER_VALUE=287
    EXPONENT_VALUE=288
    DECIMAL_VALUE=289
    DOUBLE_LITERAL=290
    BIGDECIMAL_LITERAL=291
    IDENTIFIER=292
    BACKQUOTED_IDENTIFIER=293
    SIMPLE_COMMENT=294
    BRACKETED_COMMENT=295
    WS=296
    UNRECOGNIZED=297

    def __init__(self, input:TokenStream, output:TextIO = sys.stdout):
        super().__init__(input, output)
        self.checkVersion("4.7.1")
        self._interp = ParserATNSimulator(self, self.atn, self.decisionsToDFA, self.sharedContextCache)
        self._predicates = None



    """
    When false, INTERSECT is given the greater precedence over the other set
    operations (UNION, EXCEPT and MINUS) as per the SQL standard.
    """
    legacy_setops_precedence_enbled = False

    """
    When false, a literal with an exponent would be converted into
    double type rather than decimal type.
    """
    legacy_exponent_literal_as_decimal_enabled = False

    """
    When false, CREATE TABLE syntax without a provider will use
    the value of spark.sql.sources.default as its provider.
    """
    legacy_create_hive_table_by_default_enabled = False

    """
    When true, the behavior of keywords follows ANSI SQL standard.
    """
    SQL_standard_keyword_behavior = False

    def isValidDecimal(self):
        """
        Verify whether current token is a valid decimal token (which contains dot).
        Returns true if the character that follows the token is not a digit or letter or underscore.

        For example:
        For char stream "2.3", "2." is not a valid decimal token, because it is followed by digit '3'.
        For char stream "2.3_", "2.3" is not a valid decimal token, because it is followed by '_'.
        For char stream "2.3W", "2.3" is not a valid decimal token, because it is followed by 'W'.
        For char stream "12.0D 34.E2+0.12 "  12.0D is a valid decimal token because it is followed
        by a space. 34.E2 is a valid decimal token because it is followed by symbol '+'
        which is not a digit or letter or underscore.
        """
        nextChar = self._input.LA(1)
        if 'A' <= nextChar <= 'Z' or '0' <= nextChar <= '9' or nextChar == '_':
            return False
        else:
            return True

    def isHint(self):
        """
        This method will be called when we see '/*' and try to match it as a bracketed comment.
        If the next character is '+', it should be parsed as hint later, and we cannot match
        it as a bracketed comment.

        Returns true if the next character is '+'.
        """
        nextChar = self._input.LA(1)
        if nextChar == '+':
            return True
        else:
            return False


    class SingleStatementContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def statement(self):
            return self.getTypedRuleContext(SqlBaseParser.StatementContext,0)


        def EOF(self):
            return self.getToken(SqlBaseParser.EOF, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_singleStatement

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSingleStatement" ):
                listener.enterSingleStatement(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSingleStatement" ):
                listener.exitSingleStatement(self)




    def singleStatement(self):

        localctx = SqlBaseParser.SingleStatementContext(self, self._ctx, self.state)
        self.enterRule(localctx, 0, self.RULE_singleStatement)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 274
            self.statement()
            self.state = 278
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while _la==SqlBaseParser.T__0:
                self.state = 275
                self.match(SqlBaseParser.T__0)
                self.state = 280
                self._errHandler.sync(self)
                _la = self._input.LA(1)

            self.state = 281
            self.match(SqlBaseParser.EOF)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class SingleExpressionContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def namedExpression(self):
            return self.getTypedRuleContext(SqlBaseParser.NamedExpressionContext,0)


        def EOF(self):
            return self.getToken(SqlBaseParser.EOF, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_singleExpression

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSingleExpression" ):
                listener.enterSingleExpression(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSingleExpression" ):
                listener.exitSingleExpression(self)




    def singleExpression(self):

        localctx = SqlBaseParser.SingleExpressionContext(self, self._ctx, self.state)
        self.enterRule(localctx, 2, self.RULE_singleExpression)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 283
            self.namedExpression()
            self.state = 284
            self.match(SqlBaseParser.EOF)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class SingleTableIdentifierContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def tableIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.TableIdentifierContext,0)


        def EOF(self):
            return self.getToken(SqlBaseParser.EOF, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_singleTableIdentifier

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSingleTableIdentifier" ):
                listener.enterSingleTableIdentifier(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSingleTableIdentifier" ):
                listener.exitSingleTableIdentifier(self)




    def singleTableIdentifier(self):

        localctx = SqlBaseParser.SingleTableIdentifierContext(self, self._ctx, self.state)
        self.enterRule(localctx, 4, self.RULE_singleTableIdentifier)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 286
            self.tableIdentifier()
            self.state = 287
            self.match(SqlBaseParser.EOF)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class SingleMultipartIdentifierContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)


        def EOF(self):
            return self.getToken(SqlBaseParser.EOF, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_singleMultipartIdentifier

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSingleMultipartIdentifier" ):
                listener.enterSingleMultipartIdentifier(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSingleMultipartIdentifier" ):
                listener.exitSingleMultipartIdentifier(self)




    def singleMultipartIdentifier(self):

        localctx = SqlBaseParser.SingleMultipartIdentifierContext(self, self._ctx, self.state)
        self.enterRule(localctx, 6, self.RULE_singleMultipartIdentifier)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 289
            self.multipartIdentifier()
            self.state = 290
            self.match(SqlBaseParser.EOF)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class SingleFunctionIdentifierContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def functionIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.FunctionIdentifierContext,0)


        def EOF(self):
            return self.getToken(SqlBaseParser.EOF, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_singleFunctionIdentifier

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSingleFunctionIdentifier" ):
                listener.enterSingleFunctionIdentifier(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSingleFunctionIdentifier" ):
                listener.exitSingleFunctionIdentifier(self)




    def singleFunctionIdentifier(self):

        localctx = SqlBaseParser.SingleFunctionIdentifierContext(self, self._ctx, self.state)
        self.enterRule(localctx, 8, self.RULE_singleFunctionIdentifier)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 292
            self.functionIdentifier()
            self.state = 293
            self.match(SqlBaseParser.EOF)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class SingleDataTypeContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def dataType(self):
            return self.getTypedRuleContext(SqlBaseParser.DataTypeContext,0)


        def EOF(self):
            return self.getToken(SqlBaseParser.EOF, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_singleDataType

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSingleDataType" ):
                listener.enterSingleDataType(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSingleDataType" ):
                listener.exitSingleDataType(self)




    def singleDataType(self):

        localctx = SqlBaseParser.SingleDataTypeContext(self, self._ctx, self.state)
        self.enterRule(localctx, 10, self.RULE_singleDataType)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 295
            self.dataType()
            self.state = 296
            self.match(SqlBaseParser.EOF)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class SingleTableSchemaContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def colTypeList(self):
            return self.getTypedRuleContext(SqlBaseParser.ColTypeListContext,0)


        def EOF(self):
            return self.getToken(SqlBaseParser.EOF, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_singleTableSchema

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSingleTableSchema" ):
                listener.enterSingleTableSchema(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSingleTableSchema" ):
                listener.exitSingleTableSchema(self)




    def singleTableSchema(self):

        localctx = SqlBaseParser.SingleTableSchemaContext(self, self._ctx, self.state)
        self.enterRule(localctx, 12, self.RULE_singleTableSchema)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 298
            self.colTypeList()
            self.state = 299
            self.match(SqlBaseParser.EOF)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class StatementContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser


        def getRuleIndex(self):
            return SqlBaseParser.RULE_statement


        def copyFrom(self, ctx:ParserRuleContext):
            super().copyFrom(ctx)



    class ExplainContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def EXPLAIN(self):
            return self.getToken(SqlBaseParser.EXPLAIN, 0)
        def statement(self):
            return self.getTypedRuleContext(SqlBaseParser.StatementContext,0)

        def LOGICAL(self):
            return self.getToken(SqlBaseParser.LOGICAL, 0)
        def FORMATTED(self):
            return self.getToken(SqlBaseParser.FORMATTED, 0)
        def EXTENDED(self):
            return self.getToken(SqlBaseParser.EXTENDED, 0)
        def CODEGEN(self):
            return self.getToken(SqlBaseParser.CODEGEN, 0)
        def COST(self):
            return self.getToken(SqlBaseParser.COST, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterExplain" ):
                listener.enterExplain(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitExplain" ):
                listener.exitExplain(self)


    class ResetConfigurationContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def RESET(self):
            return self.getToken(SqlBaseParser.RESET, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterResetConfiguration" ):
                listener.enterResetConfiguration(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitResetConfiguration" ):
                listener.exitResetConfiguration(self)


    class AlterViewQueryContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def ALTER(self):
            return self.getToken(SqlBaseParser.ALTER, 0)
        def VIEW(self):
            return self.getToken(SqlBaseParser.VIEW, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def query(self):
            return self.getTypedRuleContext(SqlBaseParser.QueryContext,0)

        def AS(self):
            return self.getToken(SqlBaseParser.AS, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterAlterViewQuery" ):
                listener.enterAlterViewQuery(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitAlterViewQuery" ):
                listener.exitAlterViewQuery(self)


    class UseContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def USE(self):
            return self.getToken(SqlBaseParser.USE, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def NAMESPACE(self):
            return self.getToken(SqlBaseParser.NAMESPACE, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterUse" ):
                listener.enterUse(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitUse" ):
                listener.exitUse(self)


    class DropNamespaceContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def DROP(self):
            return self.getToken(SqlBaseParser.DROP, 0)
        def namespace(self):
            return self.getTypedRuleContext(SqlBaseParser.NamespaceContext,0)

        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def IF(self):
            return self.getToken(SqlBaseParser.IF, 0)
        def EXISTS(self):
            return self.getToken(SqlBaseParser.EXISTS, 0)
        def RESTRICT(self):
            return self.getToken(SqlBaseParser.RESTRICT, 0)
        def CASCADE(self):
            return self.getToken(SqlBaseParser.CASCADE, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterDropNamespace" ):
                listener.enterDropNamespace(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitDropNamespace" ):
                listener.exitDropNamespace(self)


    class CreateTempViewUsingContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def CREATE(self):
            return self.getToken(SqlBaseParser.CREATE, 0)
        def TEMPORARY(self):
            return self.getToken(SqlBaseParser.TEMPORARY, 0)
        def VIEW(self):
            return self.getToken(SqlBaseParser.VIEW, 0)
        def tableIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.TableIdentifierContext,0)

        def tableProvider(self):
            return self.getTypedRuleContext(SqlBaseParser.TableProviderContext,0)

        def OR(self):
            return self.getToken(SqlBaseParser.OR, 0)
        def REPLACE(self):
            return self.getToken(SqlBaseParser.REPLACE, 0)
        def GLOBAL(self):
            return self.getToken(SqlBaseParser.GLOBAL, 0)
        def colTypeList(self):
            return self.getTypedRuleContext(SqlBaseParser.ColTypeListContext,0)

        def OPTIONS(self):
            return self.getToken(SqlBaseParser.OPTIONS, 0)
        def tablePropertyList(self):
            return self.getTypedRuleContext(SqlBaseParser.TablePropertyListContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterCreateTempViewUsing" ):
                listener.enterCreateTempViewUsing(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitCreateTempViewUsing" ):
                listener.exitCreateTempViewUsing(self)


    class RenameTableContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.from_ = None # MultipartIdentifierContext
            self.to = None # MultipartIdentifierContext
            self.copyFrom(ctx)

        def ALTER(self):
            return self.getToken(SqlBaseParser.ALTER, 0)
        def RENAME(self):
            return self.getToken(SqlBaseParser.RENAME, 0)
        def TO(self):
            return self.getToken(SqlBaseParser.TO, 0)
        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def VIEW(self):
            return self.getToken(SqlBaseParser.VIEW, 0)
        def multipartIdentifier(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.MultipartIdentifierContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,i)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterRenameTable" ):
                listener.enterRenameTable(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitRenameTable" ):
                listener.exitRenameTable(self)


    class FailNativeCommandContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def SET(self):
            return self.getToken(SqlBaseParser.SET, 0)
        def ROLE(self):
            return self.getToken(SqlBaseParser.ROLE, 0)
        def unsupportedHiveNativeCommands(self):
            return self.getTypedRuleContext(SqlBaseParser.UnsupportedHiveNativeCommandsContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterFailNativeCommand" ):
                listener.enterFailNativeCommand(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitFailNativeCommand" ):
                listener.exitFailNativeCommand(self)


    class ClearCacheContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def CLEAR(self):
            return self.getToken(SqlBaseParser.CLEAR, 0)
        def CACHE(self):
            return self.getToken(SqlBaseParser.CACHE, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterClearCache" ):
                listener.enterClearCache(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitClearCache" ):
                listener.exitClearCache(self)


    class DropViewContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def DROP(self):
            return self.getToken(SqlBaseParser.DROP, 0)
        def VIEW(self):
            return self.getToken(SqlBaseParser.VIEW, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def IF(self):
            return self.getToken(SqlBaseParser.IF, 0)
        def EXISTS(self):
            return self.getToken(SqlBaseParser.EXISTS, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterDropView" ):
                listener.enterDropView(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitDropView" ):
                listener.exitDropView(self)


    class ShowTablesContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.pattern = None # Token
            self.copyFrom(ctx)

        def SHOW(self):
            return self.getToken(SqlBaseParser.SHOW, 0)
        def TABLES(self):
            return self.getToken(SqlBaseParser.TABLES, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def FROM(self):
            return self.getToken(SqlBaseParser.FROM, 0)
        def IN(self):
            return self.getToken(SqlBaseParser.IN, 0)
        def STRING(self):
            return self.getToken(SqlBaseParser.STRING, 0)
        def LIKE(self):
            return self.getToken(SqlBaseParser.LIKE, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterShowTables" ):
                listener.enterShowTables(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitShowTables" ):
                listener.exitShowTables(self)


    class RecoverPartitionsContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def ALTER(self):
            return self.getToken(SqlBaseParser.ALTER, 0)
        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def RECOVER(self):
            return self.getToken(SqlBaseParser.RECOVER, 0)
        def PARTITIONS(self):
            return self.getToken(SqlBaseParser.PARTITIONS, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterRecoverPartitions" ):
                listener.enterRecoverPartitions(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitRecoverPartitions" ):
                listener.exitRecoverPartitions(self)


    class ShowCurrentNamespaceContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def SHOW(self):
            return self.getToken(SqlBaseParser.SHOW, 0)
        def CURRENT(self):
            return self.getToken(SqlBaseParser.CURRENT, 0)
        def NAMESPACE(self):
            return self.getToken(SqlBaseParser.NAMESPACE, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterShowCurrentNamespace" ):
                listener.enterShowCurrentNamespace(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitShowCurrentNamespace" ):
                listener.exitShowCurrentNamespace(self)


    class RenameTablePartitionContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.from_ = None # PartitionSpecContext
            self.to = None # PartitionSpecContext
            self.copyFrom(ctx)

        def ALTER(self):
            return self.getToken(SqlBaseParser.ALTER, 0)
        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def RENAME(self):
            return self.getToken(SqlBaseParser.RENAME, 0)
        def TO(self):
            return self.getToken(SqlBaseParser.TO, 0)
        def partitionSpec(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.PartitionSpecContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.PartitionSpecContext,i)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterRenameTablePartition" ):
                listener.enterRenameTablePartition(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitRenameTablePartition" ):
                listener.exitRenameTablePartition(self)


    class RepairTableContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def MSCK(self):
            return self.getToken(SqlBaseParser.MSCK, 0)
        def REPAIR(self):
            return self.getToken(SqlBaseParser.REPAIR, 0)
        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterRepairTable" ):
                listener.enterRepairTable(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitRepairTable" ):
                listener.exitRepairTable(self)


    class RefreshResourceContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def REFRESH(self):
            return self.getToken(SqlBaseParser.REFRESH, 0)
        def STRING(self):
            return self.getToken(SqlBaseParser.STRING, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterRefreshResource" ):
                listener.enterRefreshResource(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitRefreshResource" ):
                listener.exitRefreshResource(self)


    class ShowCreateTableContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def SHOW(self):
            return self.getToken(SqlBaseParser.SHOW, 0)
        def CREATE(self):
            return self.getToken(SqlBaseParser.CREATE, 0)
        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def AS(self):
            return self.getToken(SqlBaseParser.AS, 0)
        def SERDE(self):
            return self.getToken(SqlBaseParser.SERDE, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterShowCreateTable" ):
                listener.enterShowCreateTable(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitShowCreateTable" ):
                listener.exitShowCreateTable(self)


    class ShowNamespacesContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.pattern = None # Token
            self.copyFrom(ctx)

        def SHOW(self):
            return self.getToken(SqlBaseParser.SHOW, 0)
        def DATABASES(self):
            return self.getToken(SqlBaseParser.DATABASES, 0)
        def NAMESPACES(self):
            return self.getToken(SqlBaseParser.NAMESPACES, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def FROM(self):
            return self.getToken(SqlBaseParser.FROM, 0)
        def IN(self):
            return self.getToken(SqlBaseParser.IN, 0)
        def STRING(self):
            return self.getToken(SqlBaseParser.STRING, 0)
        def LIKE(self):
            return self.getToken(SqlBaseParser.LIKE, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterShowNamespaces" ):
                listener.enterShowNamespaces(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitShowNamespaces" ):
                listener.exitShowNamespaces(self)


    class ShowColumnsContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.table = None # MultipartIdentifierContext
            self.ns = None # MultipartIdentifierContext
            self.copyFrom(ctx)

        def SHOW(self):
            return self.getToken(SqlBaseParser.SHOW, 0)
        def COLUMNS(self):
            return self.getToken(SqlBaseParser.COLUMNS, 0)
        def FROM(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.FROM)
            else:
                return self.getToken(SqlBaseParser.FROM, i)
        def IN(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.IN)
            else:
                return self.getToken(SqlBaseParser.IN, i)
        def multipartIdentifier(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.MultipartIdentifierContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,i)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterShowColumns" ):
                listener.enterShowColumns(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitShowColumns" ):
                listener.exitShowColumns(self)


    class ReplaceTableContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def replaceTableHeader(self):
            return self.getTypedRuleContext(SqlBaseParser.ReplaceTableHeaderContext,0)

        def tableProvider(self):
            return self.getTypedRuleContext(SqlBaseParser.TableProviderContext,0)

        def createTableClauses(self):
            return self.getTypedRuleContext(SqlBaseParser.CreateTableClausesContext,0)

        def colTypeList(self):
            return self.getTypedRuleContext(SqlBaseParser.ColTypeListContext,0)

        def query(self):
            return self.getTypedRuleContext(SqlBaseParser.QueryContext,0)

        def AS(self):
            return self.getToken(SqlBaseParser.AS, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterReplaceTable" ):
                listener.enterReplaceTable(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitReplaceTable" ):
                listener.exitReplaceTable(self)


    class AddTablePartitionContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def ALTER(self):
            return self.getToken(SqlBaseParser.ALTER, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def ADD(self):
            return self.getToken(SqlBaseParser.ADD, 0)
        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def VIEW(self):
            return self.getToken(SqlBaseParser.VIEW, 0)
        def IF(self):
            return self.getToken(SqlBaseParser.IF, 0)
        def NOT(self):
            return self.getToken(SqlBaseParser.NOT, 0)
        def EXISTS(self):
            return self.getToken(SqlBaseParser.EXISTS, 0)
        def partitionSpecLocation(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.PartitionSpecLocationContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.PartitionSpecLocationContext,i)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterAddTablePartition" ):
                listener.enterAddTablePartition(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitAddTablePartition" ):
                listener.exitAddTablePartition(self)


    class SetNamespaceLocationContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def ALTER(self):
            return self.getToken(SqlBaseParser.ALTER, 0)
        def namespace(self):
            return self.getTypedRuleContext(SqlBaseParser.NamespaceContext,0)

        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def SET(self):
            return self.getToken(SqlBaseParser.SET, 0)
        def locationSpec(self):
            return self.getTypedRuleContext(SqlBaseParser.LocationSpecContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSetNamespaceLocation" ):
                listener.enterSetNamespaceLocation(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSetNamespaceLocation" ):
                listener.exitSetNamespaceLocation(self)


    class RefreshTableContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def REFRESH(self):
            return self.getToken(SqlBaseParser.REFRESH, 0)
        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterRefreshTable" ):
                listener.enterRefreshTable(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitRefreshTable" ):
                listener.exitRefreshTable(self)


    class SetNamespacePropertiesContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def ALTER(self):
            return self.getToken(SqlBaseParser.ALTER, 0)
        def namespace(self):
            return self.getTypedRuleContext(SqlBaseParser.NamespaceContext,0)

        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def SET(self):
            return self.getToken(SqlBaseParser.SET, 0)
        def tablePropertyList(self):
            return self.getTypedRuleContext(SqlBaseParser.TablePropertyListContext,0)

        def DBPROPERTIES(self):
            return self.getToken(SqlBaseParser.DBPROPERTIES, 0)
        def PROPERTIES(self):
            return self.getToken(SqlBaseParser.PROPERTIES, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSetNamespaceProperties" ):
                listener.enterSetNamespaceProperties(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSetNamespaceProperties" ):
                listener.exitSetNamespaceProperties(self)


    class ManageResourceContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.op = None # Token
            self.copyFrom(ctx)

        def identifier(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierContext,0)

        def ADD(self):
            return self.getToken(SqlBaseParser.ADD, 0)
        def LIST(self):
            return self.getToken(SqlBaseParser.LIST, 0)
        def STRING(self):
            return self.getToken(SqlBaseParser.STRING, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterManageResource" ):
                listener.enterManageResource(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitManageResource" ):
                listener.exitManageResource(self)


    class AnalyzeContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def ANALYZE(self):
            return self.getToken(SqlBaseParser.ANALYZE, 0)
        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def COMPUTE(self):
            return self.getToken(SqlBaseParser.COMPUTE, 0)
        def STATISTICS(self):
            return self.getToken(SqlBaseParser.STATISTICS, 0)
        def partitionSpec(self):
            return self.getTypedRuleContext(SqlBaseParser.PartitionSpecContext,0)

        def identifier(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierContext,0)

        def FOR(self):
            return self.getToken(SqlBaseParser.FOR, 0)
        def COLUMNS(self):
            return self.getToken(SqlBaseParser.COLUMNS, 0)
        def identifierSeq(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierSeqContext,0)

        def ALL(self):
            return self.getToken(SqlBaseParser.ALL, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterAnalyze" ):
                listener.enterAnalyze(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitAnalyze" ):
                listener.exitAnalyze(self)


    class CreateHiveTableContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.columns = None # ColTypeListContext
            self.partitionColumns = None # ColTypeListContext
            self.partitionColumnNames = None # IdentifierListContext
            self.tableProps = None # TablePropertyListContext
            self.copyFrom(ctx)

        def createTableHeader(self):
            return self.getTypedRuleContext(SqlBaseParser.CreateTableHeaderContext,0)

        def commentSpec(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.CommentSpecContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.CommentSpecContext,i)

        def bucketSpec(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.BucketSpecContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.BucketSpecContext,i)

        def skewSpec(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.SkewSpecContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.SkewSpecContext,i)

        def rowFormat(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.RowFormatContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.RowFormatContext,i)

        def createFileFormat(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.CreateFileFormatContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.CreateFileFormatContext,i)

        def locationSpec(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.LocationSpecContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.LocationSpecContext,i)

        def query(self):
            return self.getTypedRuleContext(SqlBaseParser.QueryContext,0)

        def colTypeList(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ColTypeListContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ColTypeListContext,i)

        def PARTITIONED(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.PARTITIONED)
            else:
                return self.getToken(SqlBaseParser.PARTITIONED, i)
        def BY(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.BY)
            else:
                return self.getToken(SqlBaseParser.BY, i)
        def TBLPROPERTIES(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.TBLPROPERTIES)
            else:
                return self.getToken(SqlBaseParser.TBLPROPERTIES, i)
        def identifierList(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.IdentifierListContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.IdentifierListContext,i)

        def tablePropertyList(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.TablePropertyListContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.TablePropertyListContext,i)

        def AS(self):
            return self.getToken(SqlBaseParser.AS, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterCreateHiveTable" ):
                listener.enterCreateHiveTable(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitCreateHiveTable" ):
                listener.exitCreateHiveTable(self)


    class CreateFunctionContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.className = None # Token
            self.copyFrom(ctx)

        def CREATE(self):
            return self.getToken(SqlBaseParser.CREATE, 0)
        def FUNCTION(self):
            return self.getToken(SqlBaseParser.FUNCTION, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def AS(self):
            return self.getToken(SqlBaseParser.AS, 0)
        def STRING(self):
            return self.getToken(SqlBaseParser.STRING, 0)
        def OR(self):
            return self.getToken(SqlBaseParser.OR, 0)
        def REPLACE(self):
            return self.getToken(SqlBaseParser.REPLACE, 0)
        def TEMPORARY(self):
            return self.getToken(SqlBaseParser.TEMPORARY, 0)
        def IF(self):
            return self.getToken(SqlBaseParser.IF, 0)
        def NOT(self):
            return self.getToken(SqlBaseParser.NOT, 0)
        def EXISTS(self):
            return self.getToken(SqlBaseParser.EXISTS, 0)
        def USING(self):
            return self.getToken(SqlBaseParser.USING, 0)
        def resource(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ResourceContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ResourceContext,i)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterCreateFunction" ):
                listener.enterCreateFunction(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitCreateFunction" ):
                listener.exitCreateFunction(self)


    class ShowTableContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.ns = None # MultipartIdentifierContext
            self.pattern = None # Token
            self.copyFrom(ctx)

        def SHOW(self):
            return self.getToken(SqlBaseParser.SHOW, 0)
        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def EXTENDED(self):
            return self.getToken(SqlBaseParser.EXTENDED, 0)
        def LIKE(self):
            return self.getToken(SqlBaseParser.LIKE, 0)
        def STRING(self):
            return self.getToken(SqlBaseParser.STRING, 0)
        def partitionSpec(self):
            return self.getTypedRuleContext(SqlBaseParser.PartitionSpecContext,0)

        def FROM(self):
            return self.getToken(SqlBaseParser.FROM, 0)
        def IN(self):
            return self.getToken(SqlBaseParser.IN, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterShowTable" ):
                listener.enterShowTable(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitShowTable" ):
                listener.exitShowTable(self)


    class HiveReplaceColumnsContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.table = None # MultipartIdentifierContext
            self.columns = None # QualifiedColTypeWithPositionListContext
            self.copyFrom(ctx)

        def ALTER(self):
            return self.getToken(SqlBaseParser.ALTER, 0)
        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def REPLACE(self):
            return self.getToken(SqlBaseParser.REPLACE, 0)
        def COLUMNS(self):
            return self.getToken(SqlBaseParser.COLUMNS, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def qualifiedColTypeWithPositionList(self):
            return self.getTypedRuleContext(SqlBaseParser.QualifiedColTypeWithPositionListContext,0)

        def partitionSpec(self):
            return self.getTypedRuleContext(SqlBaseParser.PartitionSpecContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterHiveReplaceColumns" ):
                listener.enterHiveReplaceColumns(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitHiveReplaceColumns" ):
                listener.exitHiveReplaceColumns(self)


    class CommentNamespaceContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.comment = None # Token
            self.copyFrom(ctx)

        def COMMENT(self):
            return self.getToken(SqlBaseParser.COMMENT, 0)
        def ON(self):
            return self.getToken(SqlBaseParser.ON, 0)
        def namespace(self):
            return self.getTypedRuleContext(SqlBaseParser.NamespaceContext,0)

        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def IS(self):
            return self.getToken(SqlBaseParser.IS, 0)
        def STRING(self):
            return self.getToken(SqlBaseParser.STRING, 0)
        def NULL(self):
            return self.getToken(SqlBaseParser.NULL, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterCommentNamespace" ):
                listener.enterCommentNamespace(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitCommentNamespace" ):
                listener.exitCommentNamespace(self)


    class CreateTableContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def createTableHeader(self):
            return self.getTypedRuleContext(SqlBaseParser.CreateTableHeaderContext,0)

        def createTableClauses(self):
            return self.getTypedRuleContext(SqlBaseParser.CreateTableClausesContext,0)

        def colTypeList(self):
            return self.getTypedRuleContext(SqlBaseParser.ColTypeListContext,0)

        def tableProvider(self):
            return self.getTypedRuleContext(SqlBaseParser.TableProviderContext,0)

        def query(self):
            return self.getTypedRuleContext(SqlBaseParser.QueryContext,0)

        def AS(self):
            return self.getToken(SqlBaseParser.AS, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterCreateTable" ):
                listener.enterCreateTable(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitCreateTable" ):
                listener.exitCreateTable(self)


    class DmlStatementContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def dmlStatementNoWith(self):
            return self.getTypedRuleContext(SqlBaseParser.DmlStatementNoWithContext,0)

        def ctes(self):
            return self.getTypedRuleContext(SqlBaseParser.CtesContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterDmlStatement" ):
                listener.enterDmlStatement(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitDmlStatement" ):
                listener.exitDmlStatement(self)


    class CreateTableLikeContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.target = None # TableIdentifierContext
            self.source = None # TableIdentifierContext
            self.tableProps = None # TablePropertyListContext
            self.copyFrom(ctx)

        def CREATE(self):
            return self.getToken(SqlBaseParser.CREATE, 0)
        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def LIKE(self):
            return self.getToken(SqlBaseParser.LIKE, 0)
        def tableIdentifier(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.TableIdentifierContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.TableIdentifierContext,i)

        def IF(self):
            return self.getToken(SqlBaseParser.IF, 0)
        def NOT(self):
            return self.getToken(SqlBaseParser.NOT, 0)
        def EXISTS(self):
            return self.getToken(SqlBaseParser.EXISTS, 0)
        def tableProvider(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.TableProviderContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.TableProviderContext,i)

        def rowFormat(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.RowFormatContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.RowFormatContext,i)

        def createFileFormat(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.CreateFileFormatContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.CreateFileFormatContext,i)

        def locationSpec(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.LocationSpecContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.LocationSpecContext,i)

        def TBLPROPERTIES(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.TBLPROPERTIES)
            else:
                return self.getToken(SqlBaseParser.TBLPROPERTIES, i)
        def tablePropertyList(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.TablePropertyListContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.TablePropertyListContext,i)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterCreateTableLike" ):
                listener.enterCreateTableLike(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitCreateTableLike" ):
                listener.exitCreateTableLike(self)


    class UncacheTableContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def UNCACHE(self):
            return self.getToken(SqlBaseParser.UNCACHE, 0)
        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def IF(self):
            return self.getToken(SqlBaseParser.IF, 0)
        def EXISTS(self):
            return self.getToken(SqlBaseParser.EXISTS, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterUncacheTable" ):
                listener.enterUncacheTable(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitUncacheTable" ):
                listener.exitUncacheTable(self)


    class DropFunctionContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def DROP(self):
            return self.getToken(SqlBaseParser.DROP, 0)
        def FUNCTION(self):
            return self.getToken(SqlBaseParser.FUNCTION, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def TEMPORARY(self):
            return self.getToken(SqlBaseParser.TEMPORARY, 0)
        def IF(self):
            return self.getToken(SqlBaseParser.IF, 0)
        def EXISTS(self):
            return self.getToken(SqlBaseParser.EXISTS, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterDropFunction" ):
                listener.enterDropFunction(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitDropFunction" ):
                listener.exitDropFunction(self)


    class DescribeRelationContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.option = None # Token
            self.copyFrom(ctx)

        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def DESC(self):
            return self.getToken(SqlBaseParser.DESC, 0)
        def DESCRIBE(self):
            return self.getToken(SqlBaseParser.DESCRIBE, 0)
        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def partitionSpec(self):
            return self.getTypedRuleContext(SqlBaseParser.PartitionSpecContext,0)

        def describeColName(self):
            return self.getTypedRuleContext(SqlBaseParser.DescribeColNameContext,0)

        def EXTENDED(self):
            return self.getToken(SqlBaseParser.EXTENDED, 0)
        def FORMATTED(self):
            return self.getToken(SqlBaseParser.FORMATTED, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterDescribeRelation" ):
                listener.enterDescribeRelation(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitDescribeRelation" ):
                listener.exitDescribeRelation(self)


    class LoadDataContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.path = None # Token
            self.copyFrom(ctx)

        def LOAD(self):
            return self.getToken(SqlBaseParser.LOAD, 0)
        def DATA(self):
            return self.getToken(SqlBaseParser.DATA, 0)
        def INPATH(self):
            return self.getToken(SqlBaseParser.INPATH, 0)
        def INTO(self):
            return self.getToken(SqlBaseParser.INTO, 0)
        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def STRING(self):
            return self.getToken(SqlBaseParser.STRING, 0)
        def LOCAL(self):
            return self.getToken(SqlBaseParser.LOCAL, 0)
        def OVERWRITE(self):
            return self.getToken(SqlBaseParser.OVERWRITE, 0)
        def partitionSpec(self):
            return self.getTypedRuleContext(SqlBaseParser.PartitionSpecContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterLoadData" ):
                listener.enterLoadData(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitLoadData" ):
                listener.exitLoadData(self)


    class ShowPartitionsContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def SHOW(self):
            return self.getToken(SqlBaseParser.SHOW, 0)
        def PARTITIONS(self):
            return self.getToken(SqlBaseParser.PARTITIONS, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def partitionSpec(self):
            return self.getTypedRuleContext(SqlBaseParser.PartitionSpecContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterShowPartitions" ):
                listener.enterShowPartitions(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitShowPartitions" ):
                listener.exitShowPartitions(self)


    class DescribeFunctionContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def FUNCTION(self):
            return self.getToken(SqlBaseParser.FUNCTION, 0)
        def describeFuncName(self):
            return self.getTypedRuleContext(SqlBaseParser.DescribeFuncNameContext,0)

        def DESC(self):
            return self.getToken(SqlBaseParser.DESC, 0)
        def DESCRIBE(self):
            return self.getToken(SqlBaseParser.DESCRIBE, 0)
        def EXTENDED(self):
            return self.getToken(SqlBaseParser.EXTENDED, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterDescribeFunction" ):
                listener.enterDescribeFunction(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitDescribeFunction" ):
                listener.exitDescribeFunction(self)


    class RenameTableColumnContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.table = None # MultipartIdentifierContext
            self.from_ = None # MultipartIdentifierContext
            self.to = None # ErrorCapturingIdentifierContext
            self.copyFrom(ctx)

        def ALTER(self):
            return self.getToken(SqlBaseParser.ALTER, 0)
        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def RENAME(self):
            return self.getToken(SqlBaseParser.RENAME, 0)
        def COLUMN(self):
            return self.getToken(SqlBaseParser.COLUMN, 0)
        def TO(self):
            return self.getToken(SqlBaseParser.TO, 0)
        def multipartIdentifier(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.MultipartIdentifierContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,i)

        def errorCapturingIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.ErrorCapturingIdentifierContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterRenameTableColumn" ):
                listener.enterRenameTableColumn(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitRenameTableColumn" ):
                listener.exitRenameTableColumn(self)


    class StatementDefaultContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def query(self):
            return self.getTypedRuleContext(SqlBaseParser.QueryContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterStatementDefault" ):
                listener.enterStatementDefault(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitStatementDefault" ):
                listener.exitStatementDefault(self)


    class HiveChangeColumnContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.table = None # MultipartIdentifierContext
            self.colName = None # MultipartIdentifierContext
            self.copyFrom(ctx)

        def ALTER(self):
            return self.getToken(SqlBaseParser.ALTER, 0)
        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def CHANGE(self):
            return self.getToken(SqlBaseParser.CHANGE, 0)
        def colType(self):
            return self.getTypedRuleContext(SqlBaseParser.ColTypeContext,0)

        def multipartIdentifier(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.MultipartIdentifierContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,i)

        def partitionSpec(self):
            return self.getTypedRuleContext(SqlBaseParser.PartitionSpecContext,0)

        def COLUMN(self):
            return self.getToken(SqlBaseParser.COLUMN, 0)
        def colPosition(self):
            return self.getTypedRuleContext(SqlBaseParser.ColPositionContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterHiveChangeColumn" ):
                listener.enterHiveChangeColumn(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitHiveChangeColumn" ):
                listener.exitHiveChangeColumn(self)


    class DescribeQueryContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def query(self):
            return self.getTypedRuleContext(SqlBaseParser.QueryContext,0)

        def DESC(self):
            return self.getToken(SqlBaseParser.DESC, 0)
        def DESCRIBE(self):
            return self.getToken(SqlBaseParser.DESCRIBE, 0)
        def QUERY(self):
            return self.getToken(SqlBaseParser.QUERY, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterDescribeQuery" ):
                listener.enterDescribeQuery(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitDescribeQuery" ):
                listener.exitDescribeQuery(self)


    class TruncateTableContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def TRUNCATE(self):
            return self.getToken(SqlBaseParser.TRUNCATE, 0)
        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def partitionSpec(self):
            return self.getTypedRuleContext(SqlBaseParser.PartitionSpecContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterTruncateTable" ):
                listener.enterTruncateTable(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitTruncateTable" ):
                listener.exitTruncateTable(self)


    class SetTableSerDeContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def ALTER(self):
            return self.getToken(SqlBaseParser.ALTER, 0)
        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def SET(self):
            return self.getToken(SqlBaseParser.SET, 0)
        def SERDE(self):
            return self.getToken(SqlBaseParser.SERDE, 0)
        def STRING(self):
            return self.getToken(SqlBaseParser.STRING, 0)
        def partitionSpec(self):
            return self.getTypedRuleContext(SqlBaseParser.PartitionSpecContext,0)

        def WITH(self):
            return self.getToken(SqlBaseParser.WITH, 0)
        def SERDEPROPERTIES(self):
            return self.getToken(SqlBaseParser.SERDEPROPERTIES, 0)
        def tablePropertyList(self):
            return self.getTypedRuleContext(SqlBaseParser.TablePropertyListContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSetTableSerDe" ):
                listener.enterSetTableSerDe(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSetTableSerDe" ):
                listener.exitSetTableSerDe(self)


    class CreateViewContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def CREATE(self):
            return self.getToken(SqlBaseParser.CREATE, 0)
        def VIEW(self):
            return self.getToken(SqlBaseParser.VIEW, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def AS(self):
            return self.getToken(SqlBaseParser.AS, 0)
        def query(self):
            return self.getTypedRuleContext(SqlBaseParser.QueryContext,0)

        def OR(self):
            return self.getToken(SqlBaseParser.OR, 0)
        def REPLACE(self):
            return self.getToken(SqlBaseParser.REPLACE, 0)
        def TEMPORARY(self):
            return self.getToken(SqlBaseParser.TEMPORARY, 0)
        def IF(self):
            return self.getToken(SqlBaseParser.IF, 0)
        def NOT(self):
            return self.getToken(SqlBaseParser.NOT, 0)
        def EXISTS(self):
            return self.getToken(SqlBaseParser.EXISTS, 0)
        def identifierCommentList(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierCommentListContext,0)

        def commentSpec(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.CommentSpecContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.CommentSpecContext,i)

        def PARTITIONED(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.PARTITIONED)
            else:
                return self.getToken(SqlBaseParser.PARTITIONED, i)
        def ON(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.ON)
            else:
                return self.getToken(SqlBaseParser.ON, i)
        def identifierList(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.IdentifierListContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.IdentifierListContext,i)

        def TBLPROPERTIES(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.TBLPROPERTIES)
            else:
                return self.getToken(SqlBaseParser.TBLPROPERTIES, i)
        def tablePropertyList(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.TablePropertyListContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.TablePropertyListContext,i)

        def GLOBAL(self):
            return self.getToken(SqlBaseParser.GLOBAL, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterCreateView" ):
                listener.enterCreateView(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitCreateView" ):
                listener.exitCreateView(self)


    class DropTablePartitionsContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def ALTER(self):
            return self.getToken(SqlBaseParser.ALTER, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def DROP(self):
            return self.getToken(SqlBaseParser.DROP, 0)
        def partitionSpec(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.PartitionSpecContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.PartitionSpecContext,i)

        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def VIEW(self):
            return self.getToken(SqlBaseParser.VIEW, 0)
        def IF(self):
            return self.getToken(SqlBaseParser.IF, 0)
        def EXISTS(self):
            return self.getToken(SqlBaseParser.EXISTS, 0)
        def PURGE(self):
            return self.getToken(SqlBaseParser.PURGE, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterDropTablePartitions" ):
                listener.enterDropTablePartitions(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitDropTablePartitions" ):
                listener.exitDropTablePartitions(self)


    class SetConfigurationContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def SET(self):
            return self.getToken(SqlBaseParser.SET, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSetConfiguration" ):
                listener.enterSetConfiguration(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSetConfiguration" ):
                listener.exitSetConfiguration(self)


    class DropTableContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def DROP(self):
            return self.getToken(SqlBaseParser.DROP, 0)
        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def IF(self):
            return self.getToken(SqlBaseParser.IF, 0)
        def EXISTS(self):
            return self.getToken(SqlBaseParser.EXISTS, 0)
        def PURGE(self):
            return self.getToken(SqlBaseParser.PURGE, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterDropTable" ):
                listener.enterDropTable(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitDropTable" ):
                listener.exitDropTable(self)


    class DescribeNamespaceContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def namespace(self):
            return self.getTypedRuleContext(SqlBaseParser.NamespaceContext,0)

        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def DESC(self):
            return self.getToken(SqlBaseParser.DESC, 0)
        def DESCRIBE(self):
            return self.getToken(SqlBaseParser.DESCRIBE, 0)
        def EXTENDED(self):
            return self.getToken(SqlBaseParser.EXTENDED, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterDescribeNamespace" ):
                listener.enterDescribeNamespace(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitDescribeNamespace" ):
                listener.exitDescribeNamespace(self)


    class AlterTableAlterColumnContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.table = None # MultipartIdentifierContext
            self.column = None # MultipartIdentifierContext
            self.copyFrom(ctx)

        def ALTER(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.ALTER)
            else:
                return self.getToken(SqlBaseParser.ALTER, i)
        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def multipartIdentifier(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.MultipartIdentifierContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,i)

        def CHANGE(self):
            return self.getToken(SqlBaseParser.CHANGE, 0)
        def COLUMN(self):
            return self.getToken(SqlBaseParser.COLUMN, 0)
        def alterColumnAction(self):
            return self.getTypedRuleContext(SqlBaseParser.AlterColumnActionContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterAlterTableAlterColumn" ):
                listener.enterAlterTableAlterColumn(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitAlterTableAlterColumn" ):
                listener.exitAlterTableAlterColumn(self)


    class CommentTableContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.comment = None # Token
            self.copyFrom(ctx)

        def COMMENT(self):
            return self.getToken(SqlBaseParser.COMMENT, 0)
        def ON(self):
            return self.getToken(SqlBaseParser.ON, 0)
        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def IS(self):
            return self.getToken(SqlBaseParser.IS, 0)
        def STRING(self):
            return self.getToken(SqlBaseParser.STRING, 0)
        def NULL(self):
            return self.getToken(SqlBaseParser.NULL, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterCommentTable" ):
                listener.enterCommentTable(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitCommentTable" ):
                listener.exitCommentTable(self)


    class CreateNamespaceContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def CREATE(self):
            return self.getToken(SqlBaseParser.CREATE, 0)
        def namespace(self):
            return self.getTypedRuleContext(SqlBaseParser.NamespaceContext,0)

        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def IF(self):
            return self.getToken(SqlBaseParser.IF, 0)
        def NOT(self):
            return self.getToken(SqlBaseParser.NOT, 0)
        def EXISTS(self):
            return self.getToken(SqlBaseParser.EXISTS, 0)
        def commentSpec(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.CommentSpecContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.CommentSpecContext,i)

        def locationSpec(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.LocationSpecContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.LocationSpecContext,i)

        def WITH(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.WITH)
            else:
                return self.getToken(SqlBaseParser.WITH, i)
        def tablePropertyList(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.TablePropertyListContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.TablePropertyListContext,i)

        def DBPROPERTIES(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.DBPROPERTIES)
            else:
                return self.getToken(SqlBaseParser.DBPROPERTIES, i)
        def PROPERTIES(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.PROPERTIES)
            else:
                return self.getToken(SqlBaseParser.PROPERTIES, i)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterCreateNamespace" ):
                listener.enterCreateNamespace(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitCreateNamespace" ):
                listener.exitCreateNamespace(self)


    class ShowTblPropertiesContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.table = None # MultipartIdentifierContext
            self.key = None # TablePropertyKeyContext
            self.copyFrom(ctx)

        def SHOW(self):
            return self.getToken(SqlBaseParser.SHOW, 0)
        def TBLPROPERTIES(self):
            return self.getToken(SqlBaseParser.TBLPROPERTIES, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def tablePropertyKey(self):
            return self.getTypedRuleContext(SqlBaseParser.TablePropertyKeyContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterShowTblProperties" ):
                listener.enterShowTblProperties(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitShowTblProperties" ):
                listener.exitShowTblProperties(self)


    class UnsetTablePropertiesContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def ALTER(self):
            return self.getToken(SqlBaseParser.ALTER, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def UNSET(self):
            return self.getToken(SqlBaseParser.UNSET, 0)
        def TBLPROPERTIES(self):
            return self.getToken(SqlBaseParser.TBLPROPERTIES, 0)
        def tablePropertyList(self):
            return self.getTypedRuleContext(SqlBaseParser.TablePropertyListContext,0)

        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def VIEW(self):
            return self.getToken(SqlBaseParser.VIEW, 0)
        def IF(self):
            return self.getToken(SqlBaseParser.IF, 0)
        def EXISTS(self):
            return self.getToken(SqlBaseParser.EXISTS, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterUnsetTableProperties" ):
                listener.enterUnsetTableProperties(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitUnsetTableProperties" ):
                listener.exitUnsetTableProperties(self)


    class SetTableLocationContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def ALTER(self):
            return self.getToken(SqlBaseParser.ALTER, 0)
        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def SET(self):
            return self.getToken(SqlBaseParser.SET, 0)
        def locationSpec(self):
            return self.getTypedRuleContext(SqlBaseParser.LocationSpecContext,0)

        def partitionSpec(self):
            return self.getTypedRuleContext(SqlBaseParser.PartitionSpecContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSetTableLocation" ):
                listener.enterSetTableLocation(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSetTableLocation" ):
                listener.exitSetTableLocation(self)


    class DropTableColumnsContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.columns = None # MultipartIdentifierListContext
            self.copyFrom(ctx)

        def ALTER(self):
            return self.getToken(SqlBaseParser.ALTER, 0)
        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def DROP(self):
            return self.getToken(SqlBaseParser.DROP, 0)
        def COLUMN(self):
            return self.getToken(SqlBaseParser.COLUMN, 0)
        def COLUMNS(self):
            return self.getToken(SqlBaseParser.COLUMNS, 0)
        def multipartIdentifierList(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierListContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterDropTableColumns" ):
                listener.enterDropTableColumns(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitDropTableColumns" ):
                listener.exitDropTableColumns(self)


    class ShowViewsContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.pattern = None # Token
            self.copyFrom(ctx)

        def SHOW(self):
            return self.getToken(SqlBaseParser.SHOW, 0)
        def VIEWS(self):
            return self.getToken(SqlBaseParser.VIEWS, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def FROM(self):
            return self.getToken(SqlBaseParser.FROM, 0)
        def IN(self):
            return self.getToken(SqlBaseParser.IN, 0)
        def STRING(self):
            return self.getToken(SqlBaseParser.STRING, 0)
        def LIKE(self):
            return self.getToken(SqlBaseParser.LIKE, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterShowViews" ):
                listener.enterShowViews(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitShowViews" ):
                listener.exitShowViews(self)


    class ShowFunctionsContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.pattern = None # Token
            self.copyFrom(ctx)

        def SHOW(self):
            return self.getToken(SqlBaseParser.SHOW, 0)
        def FUNCTIONS(self):
            return self.getToken(SqlBaseParser.FUNCTIONS, 0)
        def identifier(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierContext,0)

        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def LIKE(self):
            return self.getToken(SqlBaseParser.LIKE, 0)
        def STRING(self):
            return self.getToken(SqlBaseParser.STRING, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterShowFunctions" ):
                listener.enterShowFunctions(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitShowFunctions" ):
                listener.exitShowFunctions(self)


    class CacheTableContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.options = None # TablePropertyListContext
            self.copyFrom(ctx)

        def CACHE(self):
            return self.getToken(SqlBaseParser.CACHE, 0)
        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def LAZY(self):
            return self.getToken(SqlBaseParser.LAZY, 0)
        def OPTIONS(self):
            return self.getToken(SqlBaseParser.OPTIONS, 0)
        def query(self):
            return self.getTypedRuleContext(SqlBaseParser.QueryContext,0)

        def tablePropertyList(self):
            return self.getTypedRuleContext(SqlBaseParser.TablePropertyListContext,0)

        def AS(self):
            return self.getToken(SqlBaseParser.AS, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterCacheTable" ):
                listener.enterCacheTable(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitCacheTable" ):
                listener.exitCacheTable(self)


    class AddTableColumnsContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.columns = None # QualifiedColTypeWithPositionListContext
            self.copyFrom(ctx)

        def ALTER(self):
            return self.getToken(SqlBaseParser.ALTER, 0)
        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def ADD(self):
            return self.getToken(SqlBaseParser.ADD, 0)
        def COLUMN(self):
            return self.getToken(SqlBaseParser.COLUMN, 0)
        def COLUMNS(self):
            return self.getToken(SqlBaseParser.COLUMNS, 0)
        def qualifiedColTypeWithPositionList(self):
            return self.getTypedRuleContext(SqlBaseParser.QualifiedColTypeWithPositionListContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterAddTableColumns" ):
                listener.enterAddTableColumns(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitAddTableColumns" ):
                listener.exitAddTableColumns(self)


    class SetTablePropertiesContext(StatementContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StatementContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def ALTER(self):
            return self.getToken(SqlBaseParser.ALTER, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def SET(self):
            return self.getToken(SqlBaseParser.SET, 0)
        def TBLPROPERTIES(self):
            return self.getToken(SqlBaseParser.TBLPROPERTIES, 0)
        def tablePropertyList(self):
            return self.getTypedRuleContext(SqlBaseParser.TablePropertyListContext,0)

        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def VIEW(self):
            return self.getToken(SqlBaseParser.VIEW, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSetTableProperties" ):
                listener.enterSetTableProperties(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSetTableProperties" ):
                listener.exitSetTableProperties(self)



    def statement(self):

        localctx = SqlBaseParser.StatementContext(self, self._ctx, self.state)
        self.enterRule(localctx, 14, self.RULE_statement)
        self._la = 0 # Token type
        try:
            self.state = 1025
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,110,self._ctx)
            if la_ == 1:
                localctx = SqlBaseParser.StatementDefaultContext(self, localctx)
                self.enterOuterAlt(localctx, 1)
                self.state = 301
                self.query()
                pass

            elif la_ == 2:
                localctx = SqlBaseParser.DmlStatementContext(self, localctx)
                self.enterOuterAlt(localctx, 2)
                self.state = 303
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.WITH:
                    self.state = 302
                    self.ctes()


                self.state = 305
                self.dmlStatementNoWith()
                pass

            elif la_ == 3:
                localctx = SqlBaseParser.UseContext(self, localctx)
                self.enterOuterAlt(localctx, 3)
                self.state = 306
                self.match(SqlBaseParser.USE)
                self.state = 308
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,2,self._ctx)
                if la_ == 1:
                    self.state = 307
                    self.match(SqlBaseParser.NAMESPACE)


                self.state = 310
                self.multipartIdentifier()
                pass

            elif la_ == 4:
                localctx = SqlBaseParser.CreateNamespaceContext(self, localctx)
                self.enterOuterAlt(localctx, 4)
                self.state = 311
                self.match(SqlBaseParser.CREATE)
                self.state = 312
                self.namespace()
                self.state = 316
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,3,self._ctx)
                if la_ == 1:
                    self.state = 313
                    self.match(SqlBaseParser.IF)
                    self.state = 314
                    self.match(SqlBaseParser.NOT)
                    self.state = 315
                    self.match(SqlBaseParser.EXISTS)


                self.state = 318
                self.multipartIdentifier()
                self.state = 326
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while _la==SqlBaseParser.COMMENT or _la==SqlBaseParser.LOCATION or _la==SqlBaseParser.WITH:
                    self.state = 324
                    self._errHandler.sync(self)
                    token = self._input.LA(1)
                    if token in [SqlBaseParser.COMMENT]:
                        self.state = 319
                        self.commentSpec()
                        pass
                    elif token in [SqlBaseParser.LOCATION]:
                        self.state = 320
                        self.locationSpec()
                        pass
                    elif token in [SqlBaseParser.WITH]:
                        self.state = 321
                        self.match(SqlBaseParser.WITH)
                        self.state = 322
                        _la = self._input.LA(1)
                        if not(_la==SqlBaseParser.DBPROPERTIES or _la==SqlBaseParser.PROPERTIES):
                            self._errHandler.recoverInline(self)
                        else:
                            self._errHandler.reportMatch(self)
                            self.consume()
                        self.state = 323
                        self.tablePropertyList()
                        pass
                    else:
                        raise NoViableAltException(self)

                    self.state = 328
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)

                pass

            elif la_ == 5:
                localctx = SqlBaseParser.SetNamespacePropertiesContext(self, localctx)
                self.enterOuterAlt(localctx, 5)
                self.state = 329
                self.match(SqlBaseParser.ALTER)
                self.state = 330
                self.namespace()
                self.state = 331
                self.multipartIdentifier()
                self.state = 332
                self.match(SqlBaseParser.SET)
                self.state = 333
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.DBPROPERTIES or _la==SqlBaseParser.PROPERTIES):
                    self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                self.state = 334
                self.tablePropertyList()
                pass

            elif la_ == 6:
                localctx = SqlBaseParser.SetNamespaceLocationContext(self, localctx)
                self.enterOuterAlt(localctx, 6)
                self.state = 336
                self.match(SqlBaseParser.ALTER)
                self.state = 337
                self.namespace()
                self.state = 338
                self.multipartIdentifier()
                self.state = 339
                self.match(SqlBaseParser.SET)
                self.state = 340
                self.locationSpec()
                pass

            elif la_ == 7:
                localctx = SqlBaseParser.DropNamespaceContext(self, localctx)
                self.enterOuterAlt(localctx, 7)
                self.state = 342
                self.match(SqlBaseParser.DROP)
                self.state = 343
                self.namespace()
                self.state = 346
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,6,self._ctx)
                if la_ == 1:
                    self.state = 344
                    self.match(SqlBaseParser.IF)
                    self.state = 345
                    self.match(SqlBaseParser.EXISTS)


                self.state = 348
                self.multipartIdentifier()
                self.state = 350
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.CASCADE or _la==SqlBaseParser.RESTRICT:
                    self.state = 349
                    _la = self._input.LA(1)
                    if not(_la==SqlBaseParser.CASCADE or _la==SqlBaseParser.RESTRICT):
                        self._errHandler.recoverInline(self)
                    else:
                        self._errHandler.reportMatch(self)
                        self.consume()


                pass

            elif la_ == 8:
                localctx = SqlBaseParser.ShowNamespacesContext(self, localctx)
                self.enterOuterAlt(localctx, 8)
                self.state = 352
                self.match(SqlBaseParser.SHOW)
                self.state = 353
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.DATABASES or _la==SqlBaseParser.NAMESPACES):
                    self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                self.state = 356
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.FROM or _la==SqlBaseParser.IN:
                    self.state = 354
                    _la = self._input.LA(1)
                    if not(_la==SqlBaseParser.FROM or _la==SqlBaseParser.IN):
                        self._errHandler.recoverInline(self)
                    else:
                        self._errHandler.reportMatch(self)
                        self.consume()
                    self.state = 355
                    self.multipartIdentifier()


                self.state = 362
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.LIKE or _la==SqlBaseParser.STRING:
                    self.state = 359
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    if _la==SqlBaseParser.LIKE:
                        self.state = 358
                        self.match(SqlBaseParser.LIKE)


                    self.state = 361
                    localctx.pattern = self.match(SqlBaseParser.STRING)


                pass

            elif la_ == 9:
                localctx = SqlBaseParser.CreateTableContext(self, localctx)
                self.enterOuterAlt(localctx, 9)
                self.state = 364
                if not not self.legacy_create_hive_table_by_default_enabled:
                    from antlr4.error.Errors import FailedPredicateException
                    raise FailedPredicateException(self, "not self.legacy_create_hive_table_by_default_enabled")
                self.state = 365
                self.createTableHeader()
                self.state = 370
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,11,self._ctx)
                if la_ == 1:
                    self.state = 366
                    self.match(SqlBaseParser.T__1)
                    self.state = 367
                    self.colTypeList()
                    self.state = 368
                    self.match(SqlBaseParser.T__2)


                self.state = 373
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.USING:
                    self.state = 372
                    self.tableProvider()


                self.state = 375
                self.createTableClauses()
                self.state = 380
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.T__1 or _la==SqlBaseParser.AS or _la==SqlBaseParser.FROM or _la==SqlBaseParser.MAP or ((((_la - 187)) & ~0x3f) == 0 and ((1 << (_la - 187)) & ((1 << (SqlBaseParser.REDUCE - 187)) | (1 << (SqlBaseParser.SELECT - 187)) | (1 << (SqlBaseParser.TABLE - 187)))) != 0) or _la==SqlBaseParser.VALUES or _la==SqlBaseParser.WITH:
                    self.state = 377
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    if _la==SqlBaseParser.AS:
                        self.state = 376
                        self.match(SqlBaseParser.AS)


                    self.state = 379
                    self.query()


                pass

            elif la_ == 10:
                localctx = SqlBaseParser.CreateTableContext(self, localctx)
                self.enterOuterAlt(localctx, 10)
                self.state = 382
                if not self.legacy_create_hive_table_by_default_enabled:
                    from antlr4.error.Errors import FailedPredicateException
                    raise FailedPredicateException(self, "self.legacy_create_hive_table_by_default_enabled")
                self.state = 383
                self.createTableHeader()
                self.state = 388
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.T__1:
                    self.state = 384
                    self.match(SqlBaseParser.T__1)
                    self.state = 385
                    self.colTypeList()
                    self.state = 386
                    self.match(SqlBaseParser.T__2)


                self.state = 390
                self.tableProvider()
                self.state = 391
                self.createTableClauses()
                self.state = 396
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.T__1 or _la==SqlBaseParser.AS or _la==SqlBaseParser.FROM or _la==SqlBaseParser.MAP or ((((_la - 187)) & ~0x3f) == 0 and ((1 << (_la - 187)) & ((1 << (SqlBaseParser.REDUCE - 187)) | (1 << (SqlBaseParser.SELECT - 187)) | (1 << (SqlBaseParser.TABLE - 187)))) != 0) or _la==SqlBaseParser.VALUES or _la==SqlBaseParser.WITH:
                    self.state = 393
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    if _la==SqlBaseParser.AS:
                        self.state = 392
                        self.match(SqlBaseParser.AS)


                    self.state = 395
                    self.query()


                pass

            elif la_ == 11:
                localctx = SqlBaseParser.CreateHiveTableContext(self, localctx)
                self.enterOuterAlt(localctx, 11)
                self.state = 398
                self.createTableHeader()
                self.state = 403
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,18,self._ctx)
                if la_ == 1:
                    self.state = 399
                    self.match(SqlBaseParser.T__1)
                    self.state = 400
                    localctx.columns = self.colTypeList()
                    self.state = 401
                    self.match(SqlBaseParser.T__2)


                self.state = 426
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while _la==SqlBaseParser.CLUSTERED or _la==SqlBaseParser.COMMENT or _la==SqlBaseParser.LOCATION or _la==SqlBaseParser.PARTITIONED or ((((_la - 202)) & ~0x3f) == 0 and ((1 << (_la - 202)) & ((1 << (SqlBaseParser.ROW - 202)) | (1 << (SqlBaseParser.SKEWED - 202)) | (1 << (SqlBaseParser.STORED - 202)) | (1 << (SqlBaseParser.TBLPROPERTIES - 202)))) != 0):
                    self.state = 424
                    self._errHandler.sync(self)
                    token = self._input.LA(1)
                    if token in [SqlBaseParser.COMMENT]:
                        self.state = 405
                        self.commentSpec()
                        pass
                    elif token in [SqlBaseParser.PARTITIONED]:
                        self.state = 415
                        self._errHandler.sync(self)
                        la_ = self._interp.adaptivePredict(self._input,19,self._ctx)
                        if la_ == 1:
                            self.state = 406
                            self.match(SqlBaseParser.PARTITIONED)
                            self.state = 407
                            self.match(SqlBaseParser.BY)
                            self.state = 408
                            self.match(SqlBaseParser.T__1)
                            self.state = 409
                            localctx.partitionColumns = self.colTypeList()
                            self.state = 410
                            self.match(SqlBaseParser.T__2)
                            pass

                        elif la_ == 2:
                            self.state = 412
                            self.match(SqlBaseParser.PARTITIONED)
                            self.state = 413
                            self.match(SqlBaseParser.BY)
                            self.state = 414
                            localctx.partitionColumnNames = self.identifierList()
                            pass


                        pass
                    elif token in [SqlBaseParser.CLUSTERED]:
                        self.state = 417
                        self.bucketSpec()
                        pass
                    elif token in [SqlBaseParser.SKEWED]:
                        self.state = 418
                        self.skewSpec()
                        pass
                    elif token in [SqlBaseParser.ROW]:
                        self.state = 419
                        self.rowFormat()
                        pass
                    elif token in [SqlBaseParser.STORED]:
                        self.state = 420
                        self.createFileFormat()
                        pass
                    elif token in [SqlBaseParser.LOCATION]:
                        self.state = 421
                        self.locationSpec()
                        pass
                    elif token in [SqlBaseParser.TBLPROPERTIES]:
                        self.state = 422
                        self.match(SqlBaseParser.TBLPROPERTIES)
                        self.state = 423
                        localctx.tableProps = self.tablePropertyList()
                        pass
                    else:
                        raise NoViableAltException(self)

                    self.state = 428
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)

                self.state = 433
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.T__1 or _la==SqlBaseParser.AS or _la==SqlBaseParser.FROM or _la==SqlBaseParser.MAP or ((((_la - 187)) & ~0x3f) == 0 and ((1 << (_la - 187)) & ((1 << (SqlBaseParser.REDUCE - 187)) | (1 << (SqlBaseParser.SELECT - 187)) | (1 << (SqlBaseParser.TABLE - 187)))) != 0) or _la==SqlBaseParser.VALUES or _la==SqlBaseParser.WITH:
                    self.state = 430
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    if _la==SqlBaseParser.AS:
                        self.state = 429
                        self.match(SqlBaseParser.AS)


                    self.state = 432
                    self.query()


                pass

            elif la_ == 12:
                localctx = SqlBaseParser.CreateTableLikeContext(self, localctx)
                self.enterOuterAlt(localctx, 12)
                self.state = 435
                self.match(SqlBaseParser.CREATE)
                self.state = 436
                self.match(SqlBaseParser.TABLE)
                self.state = 440
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,24,self._ctx)
                if la_ == 1:
                    self.state = 437
                    self.match(SqlBaseParser.IF)
                    self.state = 438
                    self.match(SqlBaseParser.NOT)
                    self.state = 439
                    self.match(SqlBaseParser.EXISTS)


                self.state = 442
                localctx.target = self.tableIdentifier()
                self.state = 443
                self.match(SqlBaseParser.LIKE)
                self.state = 444
                localctx.source = self.tableIdentifier()
                self.state = 453
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while _la==SqlBaseParser.LOCATION or ((((_la - 202)) & ~0x3f) == 0 and ((1 << (_la - 202)) & ((1 << (SqlBaseParser.ROW - 202)) | (1 << (SqlBaseParser.STORED - 202)) | (1 << (SqlBaseParser.TBLPROPERTIES - 202)) | (1 << (SqlBaseParser.USING - 202)))) != 0):
                    self.state = 451
                    self._errHandler.sync(self)
                    token = self._input.LA(1)
                    if token in [SqlBaseParser.USING]:
                        self.state = 445
                        self.tableProvider()
                        pass
                    elif token in [SqlBaseParser.ROW]:
                        self.state = 446
                        self.rowFormat()
                        pass
                    elif token in [SqlBaseParser.STORED]:
                        self.state = 447
                        self.createFileFormat()
                        pass
                    elif token in [SqlBaseParser.LOCATION]:
                        self.state = 448
                        self.locationSpec()
                        pass
                    elif token in [SqlBaseParser.TBLPROPERTIES]:
                        self.state = 449
                        self.match(SqlBaseParser.TBLPROPERTIES)
                        self.state = 450
                        localctx.tableProps = self.tablePropertyList()
                        pass
                    else:
                        raise NoViableAltException(self)

                    self.state = 455
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)

                pass

            elif la_ == 13:
                localctx = SqlBaseParser.ReplaceTableContext(self, localctx)
                self.enterOuterAlt(localctx, 13)
                self.state = 456
                self.replaceTableHeader()
                self.state = 461
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.T__1:
                    self.state = 457
                    self.match(SqlBaseParser.T__1)
                    self.state = 458
                    self.colTypeList()
                    self.state = 459
                    self.match(SqlBaseParser.T__2)


                self.state = 463
                self.tableProvider()
                self.state = 464
                self.createTableClauses()
                self.state = 469
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.T__1 or _la==SqlBaseParser.AS or _la==SqlBaseParser.FROM or _la==SqlBaseParser.MAP or ((((_la - 187)) & ~0x3f) == 0 and ((1 << (_la - 187)) & ((1 << (SqlBaseParser.REDUCE - 187)) | (1 << (SqlBaseParser.SELECT - 187)) | (1 << (SqlBaseParser.TABLE - 187)))) != 0) or _la==SqlBaseParser.VALUES or _la==SqlBaseParser.WITH:
                    self.state = 466
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    if _la==SqlBaseParser.AS:
                        self.state = 465
                        self.match(SqlBaseParser.AS)


                    self.state = 468
                    self.query()


                pass

            elif la_ == 14:
                localctx = SqlBaseParser.AnalyzeContext(self, localctx)
                self.enterOuterAlt(localctx, 14)
                self.state = 471
                self.match(SqlBaseParser.ANALYZE)
                self.state = 472
                self.match(SqlBaseParser.TABLE)
                self.state = 473
                self.multipartIdentifier()
                self.state = 475
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.PARTITION:
                    self.state = 474
                    self.partitionSpec()


                self.state = 477
                self.match(SqlBaseParser.COMPUTE)
                self.state = 478
                self.match(SqlBaseParser.STATISTICS)
                self.state = 486
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,31,self._ctx)
                if la_ == 1:
                    self.state = 479
                    self.identifier()

                elif la_ == 2:
                    self.state = 480
                    self.match(SqlBaseParser.FOR)
                    self.state = 481
                    self.match(SqlBaseParser.COLUMNS)
                    self.state = 482
                    self.identifierSeq()

                elif la_ == 3:
                    self.state = 483
                    self.match(SqlBaseParser.FOR)
                    self.state = 484
                    self.match(SqlBaseParser.ALL)
                    self.state = 485
                    self.match(SqlBaseParser.COLUMNS)


                pass

            elif la_ == 15:
                localctx = SqlBaseParser.AddTableColumnsContext(self, localctx)
                self.enterOuterAlt(localctx, 15)
                self.state = 488
                self.match(SqlBaseParser.ALTER)
                self.state = 489
                self.match(SqlBaseParser.TABLE)
                self.state = 490
                self.multipartIdentifier()
                self.state = 491
                self.match(SqlBaseParser.ADD)
                self.state = 492
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.COLUMN or _la==SqlBaseParser.COLUMNS):
                    self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                self.state = 493
                localctx.columns = self.qualifiedColTypeWithPositionList()
                pass

            elif la_ == 16:
                localctx = SqlBaseParser.AddTableColumnsContext(self, localctx)
                self.enterOuterAlt(localctx, 16)
                self.state = 495
                self.match(SqlBaseParser.ALTER)
                self.state = 496
                self.match(SqlBaseParser.TABLE)
                self.state = 497
                self.multipartIdentifier()
                self.state = 498
                self.match(SqlBaseParser.ADD)
                self.state = 499
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.COLUMN or _la==SqlBaseParser.COLUMNS):
                    self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                self.state = 500
                self.match(SqlBaseParser.T__1)
                self.state = 501
                localctx.columns = self.qualifiedColTypeWithPositionList()
                self.state = 502
                self.match(SqlBaseParser.T__2)
                pass

            elif la_ == 17:
                localctx = SqlBaseParser.RenameTableColumnContext(self, localctx)
                self.enterOuterAlt(localctx, 17)
                self.state = 504
                self.match(SqlBaseParser.ALTER)
                self.state = 505
                self.match(SqlBaseParser.TABLE)
                self.state = 506
                localctx.table = self.multipartIdentifier()
                self.state = 507
                self.match(SqlBaseParser.RENAME)
                self.state = 508
                self.match(SqlBaseParser.COLUMN)
                self.state = 509
                localctx.from_ = self.multipartIdentifier()
                self.state = 510
                self.match(SqlBaseParser.TO)
                self.state = 511
                localctx.to = self.errorCapturingIdentifier()
                pass

            elif la_ == 18:
                localctx = SqlBaseParser.DropTableColumnsContext(self, localctx)
                self.enterOuterAlt(localctx, 18)
                self.state = 513
                self.match(SqlBaseParser.ALTER)
                self.state = 514
                self.match(SqlBaseParser.TABLE)
                self.state = 515
                self.multipartIdentifier()
                self.state = 516
                self.match(SqlBaseParser.DROP)
                self.state = 517
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.COLUMN or _la==SqlBaseParser.COLUMNS):
                    self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                self.state = 518
                self.match(SqlBaseParser.T__1)
                self.state = 519
                localctx.columns = self.multipartIdentifierList()
                self.state = 520
                self.match(SqlBaseParser.T__2)
                pass

            elif la_ == 19:
                localctx = SqlBaseParser.DropTableColumnsContext(self, localctx)
                self.enterOuterAlt(localctx, 19)
                self.state = 522
                self.match(SqlBaseParser.ALTER)
                self.state = 523
                self.match(SqlBaseParser.TABLE)
                self.state = 524
                self.multipartIdentifier()
                self.state = 525
                self.match(SqlBaseParser.DROP)
                self.state = 526
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.COLUMN or _la==SqlBaseParser.COLUMNS):
                    self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                self.state = 527
                localctx.columns = self.multipartIdentifierList()
                pass

            elif la_ == 20:
                localctx = SqlBaseParser.RenameTableContext(self, localctx)
                self.enterOuterAlt(localctx, 20)
                self.state = 529
                self.match(SqlBaseParser.ALTER)
                self.state = 530
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.TABLE or _la==SqlBaseParser.VIEW):
                    self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                self.state = 531
                localctx.from_ = self.multipartIdentifier()
                self.state = 532
                self.match(SqlBaseParser.RENAME)
                self.state = 533
                self.match(SqlBaseParser.TO)
                self.state = 534
                localctx.to = self.multipartIdentifier()
                pass

            elif la_ == 21:
                localctx = SqlBaseParser.SetTablePropertiesContext(self, localctx)
                self.enterOuterAlt(localctx, 21)
                self.state = 536
                self.match(SqlBaseParser.ALTER)
                self.state = 537
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.TABLE or _la==SqlBaseParser.VIEW):
                    self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                self.state = 538
                self.multipartIdentifier()
                self.state = 539
                self.match(SqlBaseParser.SET)
                self.state = 540
                self.match(SqlBaseParser.TBLPROPERTIES)
                self.state = 541
                self.tablePropertyList()
                pass

            elif la_ == 22:
                localctx = SqlBaseParser.UnsetTablePropertiesContext(self, localctx)
                self.enterOuterAlt(localctx, 22)
                self.state = 543
                self.match(SqlBaseParser.ALTER)
                self.state = 544
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.TABLE or _la==SqlBaseParser.VIEW):
                    self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                self.state = 545
                self.multipartIdentifier()
                self.state = 546
                self.match(SqlBaseParser.UNSET)
                self.state = 547
                self.match(SqlBaseParser.TBLPROPERTIES)
                self.state = 550
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.IF:
                    self.state = 548
                    self.match(SqlBaseParser.IF)
                    self.state = 549
                    self.match(SqlBaseParser.EXISTS)


                self.state = 552
                self.tablePropertyList()
                pass

            elif la_ == 23:
                localctx = SqlBaseParser.AlterTableAlterColumnContext(self, localctx)
                self.enterOuterAlt(localctx, 23)
                self.state = 554
                self.match(SqlBaseParser.ALTER)
                self.state = 555
                self.match(SqlBaseParser.TABLE)
                self.state = 556
                localctx.table = self.multipartIdentifier()
                self.state = 557
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.ALTER or _la==SqlBaseParser.CHANGE):
                    self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                self.state = 559
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,33,self._ctx)
                if la_ == 1:
                    self.state = 558
                    self.match(SqlBaseParser.COLUMN)


                self.state = 561
                localctx.column = self.multipartIdentifier()
                self.state = 563
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.AFTER or _la==SqlBaseParser.COMMENT or _la==SqlBaseParser.DROP or _la==SqlBaseParser.FIRST or _la==SqlBaseParser.SET or _la==SqlBaseParser.TYPE:
                    self.state = 562
                    self.alterColumnAction()


                pass

            elif la_ == 24:
                localctx = SqlBaseParser.HiveChangeColumnContext(self, localctx)
                self.enterOuterAlt(localctx, 24)
                self.state = 565
                self.match(SqlBaseParser.ALTER)
                self.state = 566
                self.match(SqlBaseParser.TABLE)
                self.state = 567
                localctx.table = self.multipartIdentifier()
                self.state = 569
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.PARTITION:
                    self.state = 568
                    self.partitionSpec()


                self.state = 571
                self.match(SqlBaseParser.CHANGE)
                self.state = 573
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,36,self._ctx)
                if la_ == 1:
                    self.state = 572
                    self.match(SqlBaseParser.COLUMN)


                self.state = 575
                localctx.colName = self.multipartIdentifier()
                self.state = 576
                self.colType()
                self.state = 578
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.AFTER or _la==SqlBaseParser.FIRST:
                    self.state = 577
                    self.colPosition()


                pass

            elif la_ == 25:
                localctx = SqlBaseParser.HiveReplaceColumnsContext(self, localctx)
                self.enterOuterAlt(localctx, 25)
                self.state = 580
                self.match(SqlBaseParser.ALTER)
                self.state = 581
                self.match(SqlBaseParser.TABLE)
                self.state = 582
                localctx.table = self.multipartIdentifier()
                self.state = 584
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.PARTITION:
                    self.state = 583
                    self.partitionSpec()


                self.state = 586
                self.match(SqlBaseParser.REPLACE)
                self.state = 587
                self.match(SqlBaseParser.COLUMNS)
                self.state = 588
                self.match(SqlBaseParser.T__1)
                self.state = 589
                localctx.columns = self.qualifiedColTypeWithPositionList()
                self.state = 590
                self.match(SqlBaseParser.T__2)
                pass

            elif la_ == 26:
                localctx = SqlBaseParser.SetTableSerDeContext(self, localctx)
                self.enterOuterAlt(localctx, 26)
                self.state = 592
                self.match(SqlBaseParser.ALTER)
                self.state = 593
                self.match(SqlBaseParser.TABLE)
                self.state = 594
                self.multipartIdentifier()
                self.state = 596
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.PARTITION:
                    self.state = 595
                    self.partitionSpec()


                self.state = 598
                self.match(SqlBaseParser.SET)
                self.state = 599
                self.match(SqlBaseParser.SERDE)
                self.state = 600
                self.match(SqlBaseParser.STRING)
                self.state = 604
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.WITH:
                    self.state = 601
                    self.match(SqlBaseParser.WITH)
                    self.state = 602
                    self.match(SqlBaseParser.SERDEPROPERTIES)
                    self.state = 603
                    self.tablePropertyList()


                pass

            elif la_ == 27:
                localctx = SqlBaseParser.SetTableSerDeContext(self, localctx)
                self.enterOuterAlt(localctx, 27)
                self.state = 606
                self.match(SqlBaseParser.ALTER)
                self.state = 607
                self.match(SqlBaseParser.TABLE)
                self.state = 608
                self.multipartIdentifier()
                self.state = 610
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.PARTITION:
                    self.state = 609
                    self.partitionSpec()


                self.state = 612
                self.match(SqlBaseParser.SET)
                self.state = 613
                self.match(SqlBaseParser.SERDEPROPERTIES)
                self.state = 614
                self.tablePropertyList()
                pass

            elif la_ == 28:
                localctx = SqlBaseParser.AddTablePartitionContext(self, localctx)
                self.enterOuterAlt(localctx, 28)
                self.state = 616
                self.match(SqlBaseParser.ALTER)
                self.state = 617
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.TABLE or _la==SqlBaseParser.VIEW):
                    self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                self.state = 618
                self.multipartIdentifier()
                self.state = 619
                self.match(SqlBaseParser.ADD)
                self.state = 623
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.IF:
                    self.state = 620
                    self.match(SqlBaseParser.IF)
                    self.state = 621
                    self.match(SqlBaseParser.NOT)
                    self.state = 622
                    self.match(SqlBaseParser.EXISTS)


                self.state = 626
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while True:
                    self.state = 625
                    self.partitionSpecLocation()
                    self.state = 628
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    if not (_la==SqlBaseParser.PARTITION):
                        break

                pass

            elif la_ == 29:
                localctx = SqlBaseParser.RenameTablePartitionContext(self, localctx)
                self.enterOuterAlt(localctx, 29)
                self.state = 630
                self.match(SqlBaseParser.ALTER)
                self.state = 631
                self.match(SqlBaseParser.TABLE)
                self.state = 632
                self.multipartIdentifier()
                self.state = 633
                localctx.from_ = self.partitionSpec()
                self.state = 634
                self.match(SqlBaseParser.RENAME)
                self.state = 635
                self.match(SqlBaseParser.TO)
                self.state = 636
                localctx.to = self.partitionSpec()
                pass

            elif la_ == 30:
                localctx = SqlBaseParser.DropTablePartitionsContext(self, localctx)
                self.enterOuterAlt(localctx, 30)
                self.state = 638
                self.match(SqlBaseParser.ALTER)
                self.state = 639
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.TABLE or _la==SqlBaseParser.VIEW):
                    self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                self.state = 640
                self.multipartIdentifier()
                self.state = 641
                self.match(SqlBaseParser.DROP)
                self.state = 644
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.IF:
                    self.state = 642
                    self.match(SqlBaseParser.IF)
                    self.state = 643
                    self.match(SqlBaseParser.EXISTS)


                self.state = 646
                self.partitionSpec()
                self.state = 651
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while _la==SqlBaseParser.T__3:
                    self.state = 647
                    self.match(SqlBaseParser.T__3)
                    self.state = 648
                    self.partitionSpec()
                    self.state = 653
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)

                self.state = 655
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.PURGE:
                    self.state = 654
                    self.match(SqlBaseParser.PURGE)


                pass

            elif la_ == 31:
                localctx = SqlBaseParser.SetTableLocationContext(self, localctx)
                self.enterOuterAlt(localctx, 31)
                self.state = 657
                self.match(SqlBaseParser.ALTER)
                self.state = 658
                self.match(SqlBaseParser.TABLE)
                self.state = 659
                self.multipartIdentifier()
                self.state = 661
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.PARTITION:
                    self.state = 660
                    self.partitionSpec()


                self.state = 663
                self.match(SqlBaseParser.SET)
                self.state = 664
                self.locationSpec()
                pass

            elif la_ == 32:
                localctx = SqlBaseParser.RecoverPartitionsContext(self, localctx)
                self.enterOuterAlt(localctx, 32)
                self.state = 666
                self.match(SqlBaseParser.ALTER)
                self.state = 667
                self.match(SqlBaseParser.TABLE)
                self.state = 668
                self.multipartIdentifier()
                self.state = 669
                self.match(SqlBaseParser.RECOVER)
                self.state = 670
                self.match(SqlBaseParser.PARTITIONS)
                pass

            elif la_ == 33:
                localctx = SqlBaseParser.DropTableContext(self, localctx)
                self.enterOuterAlt(localctx, 33)
                self.state = 672
                self.match(SqlBaseParser.DROP)
                self.state = 673
                self.match(SqlBaseParser.TABLE)
                self.state = 676
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,48,self._ctx)
                if la_ == 1:
                    self.state = 674
                    self.match(SqlBaseParser.IF)
                    self.state = 675
                    self.match(SqlBaseParser.EXISTS)


                self.state = 678
                self.multipartIdentifier()
                self.state = 680
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.PURGE:
                    self.state = 679
                    self.match(SqlBaseParser.PURGE)


                pass

            elif la_ == 34:
                localctx = SqlBaseParser.DropViewContext(self, localctx)
                self.enterOuterAlt(localctx, 34)
                self.state = 682
                self.match(SqlBaseParser.DROP)
                self.state = 683
                self.match(SqlBaseParser.VIEW)
                self.state = 686
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,50,self._ctx)
                if la_ == 1:
                    self.state = 684
                    self.match(SqlBaseParser.IF)
                    self.state = 685
                    self.match(SqlBaseParser.EXISTS)


                self.state = 688
                self.multipartIdentifier()
                pass

            elif la_ == 35:
                localctx = SqlBaseParser.CreateViewContext(self, localctx)
                self.enterOuterAlt(localctx, 35)
                self.state = 689
                self.match(SqlBaseParser.CREATE)
                self.state = 692
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.OR:
                    self.state = 690
                    self.match(SqlBaseParser.OR)
                    self.state = 691
                    self.match(SqlBaseParser.REPLACE)


                self.state = 698
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.GLOBAL or _la==SqlBaseParser.TEMPORARY:
                    self.state = 695
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    if _la==SqlBaseParser.GLOBAL:
                        self.state = 694
                        self.match(SqlBaseParser.GLOBAL)


                    self.state = 697
                    self.match(SqlBaseParser.TEMPORARY)


                self.state = 700
                self.match(SqlBaseParser.VIEW)
                self.state = 704
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,54,self._ctx)
                if la_ == 1:
                    self.state = 701
                    self.match(SqlBaseParser.IF)
                    self.state = 702
                    self.match(SqlBaseParser.NOT)
                    self.state = 703
                    self.match(SqlBaseParser.EXISTS)


                self.state = 706
                self.multipartIdentifier()
                self.state = 708
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.T__1:
                    self.state = 707
                    self.identifierCommentList()


                self.state = 718
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while _la==SqlBaseParser.COMMENT or _la==SqlBaseParser.PARTITIONED or _la==SqlBaseParser.TBLPROPERTIES:
                    self.state = 716
                    self._errHandler.sync(self)
                    token = self._input.LA(1)
                    if token in [SqlBaseParser.COMMENT]:
                        self.state = 710
                        self.commentSpec()
                        pass
                    elif token in [SqlBaseParser.PARTITIONED]:
                        self.state = 711
                        self.match(SqlBaseParser.PARTITIONED)
                        self.state = 712
                        self.match(SqlBaseParser.ON)
                        self.state = 713
                        self.identifierList()
                        pass
                    elif token in [SqlBaseParser.TBLPROPERTIES]:
                        self.state = 714
                        self.match(SqlBaseParser.TBLPROPERTIES)
                        self.state = 715
                        self.tablePropertyList()
                        pass
                    else:
                        raise NoViableAltException(self)

                    self.state = 720
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)

                self.state = 721
                self.match(SqlBaseParser.AS)
                self.state = 722
                self.query()
                pass

            elif la_ == 36:
                localctx = SqlBaseParser.CreateTempViewUsingContext(self, localctx)
                self.enterOuterAlt(localctx, 36)
                self.state = 724
                self.match(SqlBaseParser.CREATE)
                self.state = 727
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.OR:
                    self.state = 725
                    self.match(SqlBaseParser.OR)
                    self.state = 726
                    self.match(SqlBaseParser.REPLACE)


                self.state = 730
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.GLOBAL:
                    self.state = 729
                    self.match(SqlBaseParser.GLOBAL)


                self.state = 732
                self.match(SqlBaseParser.TEMPORARY)
                self.state = 733
                self.match(SqlBaseParser.VIEW)
                self.state = 734
                self.tableIdentifier()
                self.state = 739
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.T__1:
                    self.state = 735
                    self.match(SqlBaseParser.T__1)
                    self.state = 736
                    self.colTypeList()
                    self.state = 737
                    self.match(SqlBaseParser.T__2)


                self.state = 741
                self.tableProvider()
                self.state = 744
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.OPTIONS:
                    self.state = 742
                    self.match(SqlBaseParser.OPTIONS)
                    self.state = 743
                    self.tablePropertyList()


                pass

            elif la_ == 37:
                localctx = SqlBaseParser.AlterViewQueryContext(self, localctx)
                self.enterOuterAlt(localctx, 37)
                self.state = 746
                self.match(SqlBaseParser.ALTER)
                self.state = 747
                self.match(SqlBaseParser.VIEW)
                self.state = 748
                self.multipartIdentifier()
                self.state = 750
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.AS:
                    self.state = 749
                    self.match(SqlBaseParser.AS)


                self.state = 752
                self.query()
                pass

            elif la_ == 38:
                localctx = SqlBaseParser.CreateFunctionContext(self, localctx)
                self.enterOuterAlt(localctx, 38)
                self.state = 754
                self.match(SqlBaseParser.CREATE)
                self.state = 757
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.OR:
                    self.state = 755
                    self.match(SqlBaseParser.OR)
                    self.state = 756
                    self.match(SqlBaseParser.REPLACE)


                self.state = 760
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.TEMPORARY:
                    self.state = 759
                    self.match(SqlBaseParser.TEMPORARY)


                self.state = 762
                self.match(SqlBaseParser.FUNCTION)
                self.state = 766
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,65,self._ctx)
                if la_ == 1:
                    self.state = 763
                    self.match(SqlBaseParser.IF)
                    self.state = 764
                    self.match(SqlBaseParser.NOT)
                    self.state = 765
                    self.match(SqlBaseParser.EXISTS)


                self.state = 768
                self.multipartIdentifier()
                self.state = 769
                self.match(SqlBaseParser.AS)
                self.state = 770
                localctx.className = self.match(SqlBaseParser.STRING)
                self.state = 780
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.USING:
                    self.state = 771
                    self.match(SqlBaseParser.USING)
                    self.state = 772
                    self.resource()
                    self.state = 777
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    while _la==SqlBaseParser.T__3:
                        self.state = 773
                        self.match(SqlBaseParser.T__3)
                        self.state = 774
                        self.resource()
                        self.state = 779
                        self._errHandler.sync(self)
                        _la = self._input.LA(1)



                pass

            elif la_ == 39:
                localctx = SqlBaseParser.DropFunctionContext(self, localctx)
                self.enterOuterAlt(localctx, 39)
                self.state = 782
                self.match(SqlBaseParser.DROP)
                self.state = 784
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.TEMPORARY:
                    self.state = 783
                    self.match(SqlBaseParser.TEMPORARY)


                self.state = 786
                self.match(SqlBaseParser.FUNCTION)
                self.state = 789
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,69,self._ctx)
                if la_ == 1:
                    self.state = 787
                    self.match(SqlBaseParser.IF)
                    self.state = 788
                    self.match(SqlBaseParser.EXISTS)


                self.state = 791
                self.multipartIdentifier()
                pass

            elif la_ == 40:
                localctx = SqlBaseParser.ExplainContext(self, localctx)
                self.enterOuterAlt(localctx, 40)
                self.state = 792
                self.match(SqlBaseParser.EXPLAIN)
                self.state = 794
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,70,self._ctx)
                if la_ == 1:
                    self.state = 793
                    _la = self._input.LA(1)
                    if not(_la==SqlBaseParser.CODEGEN or _la==SqlBaseParser.COST or ((((_la - 86)) & ~0x3f) == 0 and ((1 << (_la - 86)) & ((1 << (SqlBaseParser.EXTENDED - 86)) | (1 << (SqlBaseParser.FORMATTED - 86)) | (1 << (SqlBaseParser.LOGICAL - 86)))) != 0)):
                        self._errHandler.recoverInline(self)
                    else:
                        self._errHandler.reportMatch(self)
                        self.consume()


                self.state = 796
                self.statement()
                pass

            elif la_ == 41:
                localctx = SqlBaseParser.ShowTablesContext(self, localctx)
                self.enterOuterAlt(localctx, 41)
                self.state = 797
                self.match(SqlBaseParser.SHOW)
                self.state = 798
                self.match(SqlBaseParser.TABLES)
                self.state = 801
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.FROM or _la==SqlBaseParser.IN:
                    self.state = 799
                    _la = self._input.LA(1)
                    if not(_la==SqlBaseParser.FROM or _la==SqlBaseParser.IN):
                        self._errHandler.recoverInline(self)
                    else:
                        self._errHandler.reportMatch(self)
                        self.consume()
                    self.state = 800
                    self.multipartIdentifier()


                self.state = 807
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.LIKE or _la==SqlBaseParser.STRING:
                    self.state = 804
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    if _la==SqlBaseParser.LIKE:
                        self.state = 803
                        self.match(SqlBaseParser.LIKE)


                    self.state = 806
                    localctx.pattern = self.match(SqlBaseParser.STRING)


                pass

            elif la_ == 42:
                localctx = SqlBaseParser.ShowTableContext(self, localctx)
                self.enterOuterAlt(localctx, 42)
                self.state = 809
                self.match(SqlBaseParser.SHOW)
                self.state = 810
                self.match(SqlBaseParser.TABLE)
                self.state = 811
                self.match(SqlBaseParser.EXTENDED)
                self.state = 814
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.FROM or _la==SqlBaseParser.IN:
                    self.state = 812
                    _la = self._input.LA(1)
                    if not(_la==SqlBaseParser.FROM or _la==SqlBaseParser.IN):
                        self._errHandler.recoverInline(self)
                    else:
                        self._errHandler.reportMatch(self)
                        self.consume()
                    self.state = 813
                    localctx.ns = self.multipartIdentifier()


                self.state = 816
                self.match(SqlBaseParser.LIKE)
                self.state = 817
                localctx.pattern = self.match(SqlBaseParser.STRING)
                self.state = 819
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.PARTITION:
                    self.state = 818
                    self.partitionSpec()


                pass

            elif la_ == 43:
                localctx = SqlBaseParser.ShowTblPropertiesContext(self, localctx)
                self.enterOuterAlt(localctx, 43)
                self.state = 821
                self.match(SqlBaseParser.SHOW)
                self.state = 822
                self.match(SqlBaseParser.TBLPROPERTIES)
                self.state = 823
                localctx.table = self.multipartIdentifier()
                self.state = 828
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.T__1:
                    self.state = 824
                    self.match(SqlBaseParser.T__1)
                    self.state = 825
                    localctx.key = self.tablePropertyKey()
                    self.state = 826
                    self.match(SqlBaseParser.T__2)


                pass

            elif la_ == 44:
                localctx = SqlBaseParser.ShowColumnsContext(self, localctx)
                self.enterOuterAlt(localctx, 44)
                self.state = 830
                self.match(SqlBaseParser.SHOW)
                self.state = 831
                self.match(SqlBaseParser.COLUMNS)
                self.state = 832
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.FROM or _la==SqlBaseParser.IN):
                    self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                self.state = 833
                localctx.table = self.multipartIdentifier()
                self.state = 836
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.FROM or _la==SqlBaseParser.IN:
                    self.state = 834
                    _la = self._input.LA(1)
                    if not(_la==SqlBaseParser.FROM or _la==SqlBaseParser.IN):
                        self._errHandler.recoverInline(self)
                    else:
                        self._errHandler.reportMatch(self)
                        self.consume()
                    self.state = 835
                    localctx.ns = self.multipartIdentifier()


                pass

            elif la_ == 45:
                localctx = SqlBaseParser.ShowViewsContext(self, localctx)
                self.enterOuterAlt(localctx, 45)
                self.state = 838
                self.match(SqlBaseParser.SHOW)
                self.state = 839
                self.match(SqlBaseParser.VIEWS)
                self.state = 842
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.FROM or _la==SqlBaseParser.IN:
                    self.state = 840
                    _la = self._input.LA(1)
                    if not(_la==SqlBaseParser.FROM or _la==SqlBaseParser.IN):
                        self._errHandler.recoverInline(self)
                    else:
                        self._errHandler.reportMatch(self)
                        self.consume()
                    self.state = 841
                    self.multipartIdentifier()


                self.state = 848
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.LIKE or _la==SqlBaseParser.STRING:
                    self.state = 845
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    if _la==SqlBaseParser.LIKE:
                        self.state = 844
                        self.match(SqlBaseParser.LIKE)


                    self.state = 847
                    localctx.pattern = self.match(SqlBaseParser.STRING)


                pass

            elif la_ == 46:
                localctx = SqlBaseParser.ShowPartitionsContext(self, localctx)
                self.enterOuterAlt(localctx, 46)
                self.state = 850
                self.match(SqlBaseParser.SHOW)
                self.state = 851
                self.match(SqlBaseParser.PARTITIONS)
                self.state = 852
                self.multipartIdentifier()
                self.state = 854
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.PARTITION:
                    self.state = 853
                    self.partitionSpec()


                pass

            elif la_ == 47:
                localctx = SqlBaseParser.ShowFunctionsContext(self, localctx)
                self.enterOuterAlt(localctx, 47)
                self.state = 856
                self.match(SqlBaseParser.SHOW)
                self.state = 858
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,82,self._ctx)
                if la_ == 1:
                    self.state = 857
                    self.identifier()


                self.state = 860
                self.match(SqlBaseParser.FUNCTIONS)
                self.state = 868
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,85,self._ctx)
                if la_ == 1:
                    self.state = 862
                    self._errHandler.sync(self)
                    la_ = self._interp.adaptivePredict(self._input,83,self._ctx)
                    if la_ == 1:
                        self.state = 861
                        self.match(SqlBaseParser.LIKE)


                    self.state = 866
                    self._errHandler.sync(self)
                    la_ = self._interp.adaptivePredict(self._input,84,self._ctx)
                    if la_ == 1:
                        self.state = 864
                        self.multipartIdentifier()
                        pass

                    elif la_ == 2:
                        self.state = 865
                        localctx.pattern = self.match(SqlBaseParser.STRING)
                        pass




                pass

            elif la_ == 48:
                localctx = SqlBaseParser.ShowCreateTableContext(self, localctx)
                self.enterOuterAlt(localctx, 48)
                self.state = 870
                self.match(SqlBaseParser.SHOW)
                self.state = 871
                self.match(SqlBaseParser.CREATE)
                self.state = 872
                self.match(SqlBaseParser.TABLE)
                self.state = 873
                self.multipartIdentifier()
                self.state = 876
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.AS:
                    self.state = 874
                    self.match(SqlBaseParser.AS)
                    self.state = 875
                    self.match(SqlBaseParser.SERDE)


                pass

            elif la_ == 49:
                localctx = SqlBaseParser.ShowCurrentNamespaceContext(self, localctx)
                self.enterOuterAlt(localctx, 49)
                self.state = 878
                self.match(SqlBaseParser.SHOW)
                self.state = 879
                self.match(SqlBaseParser.CURRENT)
                self.state = 880
                self.match(SqlBaseParser.NAMESPACE)
                pass

            elif la_ == 50:
                localctx = SqlBaseParser.DescribeFunctionContext(self, localctx)
                self.enterOuterAlt(localctx, 50)
                self.state = 881
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.DESC or _la==SqlBaseParser.DESCRIBE):
                    self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                self.state = 882
                self.match(SqlBaseParser.FUNCTION)
                self.state = 884
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,87,self._ctx)
                if la_ == 1:
                    self.state = 883
                    self.match(SqlBaseParser.EXTENDED)


                self.state = 886
                self.describeFuncName()
                pass

            elif la_ == 51:
                localctx = SqlBaseParser.DescribeNamespaceContext(self, localctx)
                self.enterOuterAlt(localctx, 51)
                self.state = 887
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.DESC or _la==SqlBaseParser.DESCRIBE):
                    self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                self.state = 888
                self.namespace()
                self.state = 890
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,88,self._ctx)
                if la_ == 1:
                    self.state = 889
                    self.match(SqlBaseParser.EXTENDED)


                self.state = 892
                self.multipartIdentifier()
                pass

            elif la_ == 52:
                localctx = SqlBaseParser.DescribeRelationContext(self, localctx)
                self.enterOuterAlt(localctx, 52)
                self.state = 894
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.DESC or _la==SqlBaseParser.DESCRIBE):
                    self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                self.state = 896
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,89,self._ctx)
                if la_ == 1:
                    self.state = 895
                    self.match(SqlBaseParser.TABLE)


                self.state = 899
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,90,self._ctx)
                if la_ == 1:
                    self.state = 898
                    localctx.option = self._input.LT(1)
                    _la = self._input.LA(1)
                    if not(_la==SqlBaseParser.EXTENDED or _la==SqlBaseParser.FORMATTED):
                        localctx.option = self._errHandler.recoverInline(self)
                    else:
                        self._errHandler.reportMatch(self)
                        self.consume()


                self.state = 901
                self.multipartIdentifier()
                self.state = 903
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,91,self._ctx)
                if la_ == 1:
                    self.state = 902
                    self.partitionSpec()


                self.state = 906
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,92,self._ctx)
                if la_ == 1:
                    self.state = 905
                    self.describeColName()


                pass

            elif la_ == 53:
                localctx = SqlBaseParser.DescribeQueryContext(self, localctx)
                self.enterOuterAlt(localctx, 53)
                self.state = 908
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.DESC or _la==SqlBaseParser.DESCRIBE):
                    self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                self.state = 910
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.QUERY:
                    self.state = 909
                    self.match(SqlBaseParser.QUERY)


                self.state = 912
                self.query()
                pass

            elif la_ == 54:
                localctx = SqlBaseParser.CommentNamespaceContext(self, localctx)
                self.enterOuterAlt(localctx, 54)
                self.state = 913
                self.match(SqlBaseParser.COMMENT)
                self.state = 914
                self.match(SqlBaseParser.ON)
                self.state = 915
                self.namespace()
                self.state = 916
                self.multipartIdentifier()
                self.state = 917
                self.match(SqlBaseParser.IS)
                self.state = 918
                localctx.comment = self._input.LT(1)
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.NULL or _la==SqlBaseParser.STRING):
                    localctx.comment = self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                pass

            elif la_ == 55:
                localctx = SqlBaseParser.CommentTableContext(self, localctx)
                self.enterOuterAlt(localctx, 55)
                self.state = 920
                self.match(SqlBaseParser.COMMENT)
                self.state = 921
                self.match(SqlBaseParser.ON)
                self.state = 922
                self.match(SqlBaseParser.TABLE)
                self.state = 923
                self.multipartIdentifier()
                self.state = 924
                self.match(SqlBaseParser.IS)
                self.state = 925
                localctx.comment = self._input.LT(1)
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.NULL or _la==SqlBaseParser.STRING):
                    localctx.comment = self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                pass

            elif la_ == 56:
                localctx = SqlBaseParser.RefreshTableContext(self, localctx)
                self.enterOuterAlt(localctx, 56)
                self.state = 927
                self.match(SqlBaseParser.REFRESH)
                self.state = 928
                self.match(SqlBaseParser.TABLE)
                self.state = 929
                self.multipartIdentifier()
                pass

            elif la_ == 57:
                localctx = SqlBaseParser.RefreshResourceContext(self, localctx)
                self.enterOuterAlt(localctx, 57)
                self.state = 930
                self.match(SqlBaseParser.REFRESH)
                self.state = 938
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,95,self._ctx)
                if la_ == 1:
                    self.state = 931
                    self.match(SqlBaseParser.STRING)
                    pass

                elif la_ == 2:
                    self.state = 935
                    self._errHandler.sync(self)
                    _alt = self._interp.adaptivePredict(self._input,94,self._ctx)
                    while _alt!=1 and _alt!=ATN.INVALID_ALT_NUMBER:
                        if _alt==1+1:
                            self.state = 932
                            self.matchWildcard()
                        self.state = 937
                        self._errHandler.sync(self)
                        _alt = self._interp.adaptivePredict(self._input,94,self._ctx)

                    pass


                pass

            elif la_ == 58:
                localctx = SqlBaseParser.CacheTableContext(self, localctx)
                self.enterOuterAlt(localctx, 58)
                self.state = 940
                self.match(SqlBaseParser.CACHE)
                self.state = 942
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.LAZY:
                    self.state = 941
                    self.match(SqlBaseParser.LAZY)


                self.state = 944
                self.match(SqlBaseParser.TABLE)
                self.state = 945
                self.multipartIdentifier()
                self.state = 948
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.OPTIONS:
                    self.state = 946
                    self.match(SqlBaseParser.OPTIONS)
                    self.state = 947
                    localctx.options = self.tablePropertyList()


                self.state = 954
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.T__1 or _la==SqlBaseParser.AS or _la==SqlBaseParser.FROM or _la==SqlBaseParser.MAP or ((((_la - 187)) & ~0x3f) == 0 and ((1 << (_la - 187)) & ((1 << (SqlBaseParser.REDUCE - 187)) | (1 << (SqlBaseParser.SELECT - 187)) | (1 << (SqlBaseParser.TABLE - 187)))) != 0) or _la==SqlBaseParser.VALUES or _la==SqlBaseParser.WITH:
                    self.state = 951
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    if _la==SqlBaseParser.AS:
                        self.state = 950
                        self.match(SqlBaseParser.AS)


                    self.state = 953
                    self.query()


                pass

            elif la_ == 59:
                localctx = SqlBaseParser.UncacheTableContext(self, localctx)
                self.enterOuterAlt(localctx, 59)
                self.state = 956
                self.match(SqlBaseParser.UNCACHE)
                self.state = 957
                self.match(SqlBaseParser.TABLE)
                self.state = 960
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,100,self._ctx)
                if la_ == 1:
                    self.state = 958
                    self.match(SqlBaseParser.IF)
                    self.state = 959
                    self.match(SqlBaseParser.EXISTS)


                self.state = 962
                self.multipartIdentifier()
                pass

            elif la_ == 60:
                localctx = SqlBaseParser.ClearCacheContext(self, localctx)
                self.enterOuterAlt(localctx, 60)
                self.state = 963
                self.match(SqlBaseParser.CLEAR)
                self.state = 964
                self.match(SqlBaseParser.CACHE)
                pass

            elif la_ == 61:
                localctx = SqlBaseParser.LoadDataContext(self, localctx)
                self.enterOuterAlt(localctx, 61)
                self.state = 965
                self.match(SqlBaseParser.LOAD)
                self.state = 966
                self.match(SqlBaseParser.DATA)
                self.state = 968
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.LOCAL:
                    self.state = 967
                    self.match(SqlBaseParser.LOCAL)


                self.state = 970
                self.match(SqlBaseParser.INPATH)
                self.state = 971
                localctx.path = self.match(SqlBaseParser.STRING)
                self.state = 973
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.OVERWRITE:
                    self.state = 972
                    self.match(SqlBaseParser.OVERWRITE)


                self.state = 975
                self.match(SqlBaseParser.INTO)
                self.state = 976
                self.match(SqlBaseParser.TABLE)
                self.state = 977
                self.multipartIdentifier()
                self.state = 979
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.PARTITION:
                    self.state = 978
                    self.partitionSpec()


                pass

            elif la_ == 62:
                localctx = SqlBaseParser.TruncateTableContext(self, localctx)
                self.enterOuterAlt(localctx, 62)
                self.state = 981
                self.match(SqlBaseParser.TRUNCATE)
                self.state = 982
                self.match(SqlBaseParser.TABLE)
                self.state = 983
                self.multipartIdentifier()
                self.state = 985
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.PARTITION:
                    self.state = 984
                    self.partitionSpec()


                pass

            elif la_ == 63:
                localctx = SqlBaseParser.RepairTableContext(self, localctx)
                self.enterOuterAlt(localctx, 63)
                self.state = 987
                self.match(SqlBaseParser.MSCK)
                self.state = 988
                self.match(SqlBaseParser.REPAIR)
                self.state = 989
                self.match(SqlBaseParser.TABLE)
                self.state = 990
                self.multipartIdentifier()
                pass

            elif la_ == 64:
                localctx = SqlBaseParser.ManageResourceContext(self, localctx)
                self.enterOuterAlt(localctx, 64)
                self.state = 991
                localctx.op = self._input.LT(1)
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.ADD or _la==SqlBaseParser.LIST):
                    localctx.op = self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                self.state = 992
                self.identifier()
                self.state = 1000
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,106,self._ctx)
                if la_ == 1:
                    self.state = 993
                    self.match(SqlBaseParser.STRING)
                    pass

                elif la_ == 2:
                    self.state = 997
                    self._errHandler.sync(self)
                    _alt = self._interp.adaptivePredict(self._input,105,self._ctx)
                    while _alt!=1 and _alt!=ATN.INVALID_ALT_NUMBER:
                        if _alt==1+1:
                            self.state = 994
                            self.matchWildcard()
                        self.state = 999
                        self._errHandler.sync(self)
                        _alt = self._interp.adaptivePredict(self._input,105,self._ctx)

                    pass


                pass

            elif la_ == 65:
                localctx = SqlBaseParser.FailNativeCommandContext(self, localctx)
                self.enterOuterAlt(localctx, 65)
                self.state = 1002
                self.match(SqlBaseParser.SET)
                self.state = 1003
                self.match(SqlBaseParser.ROLE)
                self.state = 1007
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,107,self._ctx)
                while _alt!=1 and _alt!=ATN.INVALID_ALT_NUMBER:
                    if _alt==1+1:
                        self.state = 1004
                        self.matchWildcard()
                    self.state = 1009
                    self._errHandler.sync(self)
                    _alt = self._interp.adaptivePredict(self._input,107,self._ctx)

                pass

            elif la_ == 66:
                localctx = SqlBaseParser.SetConfigurationContext(self, localctx)
                self.enterOuterAlt(localctx, 66)
                self.state = 1010
                self.match(SqlBaseParser.SET)
                self.state = 1014
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,108,self._ctx)
                while _alt!=1 and _alt!=ATN.INVALID_ALT_NUMBER:
                    if _alt==1+1:
                        self.state = 1011
                        self.matchWildcard()
                    self.state = 1016
                    self._errHandler.sync(self)
                    _alt = self._interp.adaptivePredict(self._input,108,self._ctx)

                pass

            elif la_ == 67:
                localctx = SqlBaseParser.ResetConfigurationContext(self, localctx)
                self.enterOuterAlt(localctx, 67)
                self.state = 1017
                self.match(SqlBaseParser.RESET)
                pass

            elif la_ == 68:
                localctx = SqlBaseParser.FailNativeCommandContext(self, localctx)
                self.enterOuterAlt(localctx, 68)
                self.state = 1018
                self.unsupportedHiveNativeCommands()
                self.state = 1022
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,109,self._ctx)
                while _alt!=1 and _alt!=ATN.INVALID_ALT_NUMBER:
                    if _alt==1+1:
                        self.state = 1019
                        self.matchWildcard()
                    self.state = 1024
                    self._errHandler.sync(self)
                    _alt = self._interp.adaptivePredict(self._input,109,self._ctx)

                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class UnsupportedHiveNativeCommandsContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.kw1 = None # Token
            self.kw2 = None # Token
            self.kw3 = None # Token
            self.kw4 = None # Token
            self.kw5 = None # Token
            self.kw6 = None # Token

        def CREATE(self):
            return self.getToken(SqlBaseParser.CREATE, 0)

        def ROLE(self):
            return self.getToken(SqlBaseParser.ROLE, 0)

        def DROP(self):
            return self.getToken(SqlBaseParser.DROP, 0)

        def GRANT(self):
            return self.getToken(SqlBaseParser.GRANT, 0)

        def REVOKE(self):
            return self.getToken(SqlBaseParser.REVOKE, 0)

        def SHOW(self):
            return self.getToken(SqlBaseParser.SHOW, 0)

        def PRINCIPALS(self):
            return self.getToken(SqlBaseParser.PRINCIPALS, 0)

        def ROLES(self):
            return self.getToken(SqlBaseParser.ROLES, 0)

        def CURRENT(self):
            return self.getToken(SqlBaseParser.CURRENT, 0)

        def EXPORT(self):
            return self.getToken(SqlBaseParser.EXPORT, 0)

        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)

        def IMPORT(self):
            return self.getToken(SqlBaseParser.IMPORT, 0)

        def COMPACTIONS(self):
            return self.getToken(SqlBaseParser.COMPACTIONS, 0)

        def TRANSACTIONS(self):
            return self.getToken(SqlBaseParser.TRANSACTIONS, 0)

        def INDEXES(self):
            return self.getToken(SqlBaseParser.INDEXES, 0)

        def LOCKS(self):
            return self.getToken(SqlBaseParser.LOCKS, 0)

        def INDEX(self):
            return self.getToken(SqlBaseParser.INDEX, 0)

        def ALTER(self):
            return self.getToken(SqlBaseParser.ALTER, 0)

        def LOCK(self):
            return self.getToken(SqlBaseParser.LOCK, 0)

        def DATABASE(self):
            return self.getToken(SqlBaseParser.DATABASE, 0)

        def UNLOCK(self):
            return self.getToken(SqlBaseParser.UNLOCK, 0)

        def TEMPORARY(self):
            return self.getToken(SqlBaseParser.TEMPORARY, 0)

        def MACRO(self):
            return self.getToken(SqlBaseParser.MACRO, 0)

        def tableIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.TableIdentifierContext,0)


        def NOT(self):
            return self.getToken(SqlBaseParser.NOT, 0)

        def CLUSTERED(self):
            return self.getToken(SqlBaseParser.CLUSTERED, 0)

        def BY(self):
            return self.getToken(SqlBaseParser.BY, 0)

        def SORTED(self):
            return self.getToken(SqlBaseParser.SORTED, 0)

        def SKEWED(self):
            return self.getToken(SqlBaseParser.SKEWED, 0)

        def STORED(self):
            return self.getToken(SqlBaseParser.STORED, 0)

        def AS(self):
            return self.getToken(SqlBaseParser.AS, 0)

        def DIRECTORIES(self):
            return self.getToken(SqlBaseParser.DIRECTORIES, 0)

        def SET(self):
            return self.getToken(SqlBaseParser.SET, 0)

        def LOCATION(self):
            return self.getToken(SqlBaseParser.LOCATION, 0)

        def EXCHANGE(self):
            return self.getToken(SqlBaseParser.EXCHANGE, 0)

        def PARTITION(self):
            return self.getToken(SqlBaseParser.PARTITION, 0)

        def ARCHIVE(self):
            return self.getToken(SqlBaseParser.ARCHIVE, 0)

        def UNARCHIVE(self):
            return self.getToken(SqlBaseParser.UNARCHIVE, 0)

        def TOUCH(self):
            return self.getToken(SqlBaseParser.TOUCH, 0)

        def COMPACT(self):
            return self.getToken(SqlBaseParser.COMPACT, 0)

        def partitionSpec(self):
            return self.getTypedRuleContext(SqlBaseParser.PartitionSpecContext,0)


        def CONCATENATE(self):
            return self.getToken(SqlBaseParser.CONCATENATE, 0)

        def FILEFORMAT(self):
            return self.getToken(SqlBaseParser.FILEFORMAT, 0)

        def REPLACE(self):
            return self.getToken(SqlBaseParser.REPLACE, 0)

        def COLUMNS(self):
            return self.getToken(SqlBaseParser.COLUMNS, 0)

        def START(self):
            return self.getToken(SqlBaseParser.START, 0)

        def TRANSACTION(self):
            return self.getToken(SqlBaseParser.TRANSACTION, 0)

        def COMMIT(self):
            return self.getToken(SqlBaseParser.COMMIT, 0)

        def ROLLBACK(self):
            return self.getToken(SqlBaseParser.ROLLBACK, 0)

        def DFS(self):
            return self.getToken(SqlBaseParser.DFS, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_unsupportedHiveNativeCommands

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterUnsupportedHiveNativeCommands" ):
                listener.enterUnsupportedHiveNativeCommands(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitUnsupportedHiveNativeCommands" ):
                listener.exitUnsupportedHiveNativeCommands(self)




    def unsupportedHiveNativeCommands(self):

        localctx = SqlBaseParser.UnsupportedHiveNativeCommandsContext(self, self._ctx, self.state)
        self.enterRule(localctx, 16, self.RULE_unsupportedHiveNativeCommands)
        self._la = 0 # Token type
        try:
            self.state = 1195
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,118,self._ctx)
            if la_ == 1:
                self.enterOuterAlt(localctx, 1)
                self.state = 1027
                localctx.kw1 = self.match(SqlBaseParser.CREATE)
                self.state = 1028
                localctx.kw2 = self.match(SqlBaseParser.ROLE)
                pass

            elif la_ == 2:
                self.enterOuterAlt(localctx, 2)
                self.state = 1029
                localctx.kw1 = self.match(SqlBaseParser.DROP)
                self.state = 1030
                localctx.kw2 = self.match(SqlBaseParser.ROLE)
                pass

            elif la_ == 3:
                self.enterOuterAlt(localctx, 3)
                self.state = 1031
                localctx.kw1 = self.match(SqlBaseParser.GRANT)
                self.state = 1033
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,111,self._ctx)
                if la_ == 1:
                    self.state = 1032
                    localctx.kw2 = self.match(SqlBaseParser.ROLE)


                pass

            elif la_ == 4:
                self.enterOuterAlt(localctx, 4)
                self.state = 1035
                localctx.kw1 = self.match(SqlBaseParser.REVOKE)
                self.state = 1037
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,112,self._ctx)
                if la_ == 1:
                    self.state = 1036
                    localctx.kw2 = self.match(SqlBaseParser.ROLE)


                pass

            elif la_ == 5:
                self.enterOuterAlt(localctx, 5)
                self.state = 1039
                localctx.kw1 = self.match(SqlBaseParser.SHOW)
                self.state = 1040
                localctx.kw2 = self.match(SqlBaseParser.GRANT)
                pass

            elif la_ == 6:
                self.enterOuterAlt(localctx, 6)
                self.state = 1041
                localctx.kw1 = self.match(SqlBaseParser.SHOW)
                self.state = 1042
                localctx.kw2 = self.match(SqlBaseParser.ROLE)
                self.state = 1044
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,113,self._ctx)
                if la_ == 1:
                    self.state = 1043
                    localctx.kw3 = self.match(SqlBaseParser.GRANT)


                pass

            elif la_ == 7:
                self.enterOuterAlt(localctx, 7)
                self.state = 1046
                localctx.kw1 = self.match(SqlBaseParser.SHOW)
                self.state = 1047
                localctx.kw2 = self.match(SqlBaseParser.PRINCIPALS)
                pass

            elif la_ == 8:
                self.enterOuterAlt(localctx, 8)
                self.state = 1048
                localctx.kw1 = self.match(SqlBaseParser.SHOW)
                self.state = 1049
                localctx.kw2 = self.match(SqlBaseParser.ROLES)
                pass

            elif la_ == 9:
                self.enterOuterAlt(localctx, 9)
                self.state = 1050
                localctx.kw1 = self.match(SqlBaseParser.SHOW)
                self.state = 1051
                localctx.kw2 = self.match(SqlBaseParser.CURRENT)
                self.state = 1052
                localctx.kw3 = self.match(SqlBaseParser.ROLES)
                pass

            elif la_ == 10:
                self.enterOuterAlt(localctx, 10)
                self.state = 1053
                localctx.kw1 = self.match(SqlBaseParser.EXPORT)
                self.state = 1054
                localctx.kw2 = self.match(SqlBaseParser.TABLE)
                pass

            elif la_ == 11:
                self.enterOuterAlt(localctx, 11)
                self.state = 1055
                localctx.kw1 = self.match(SqlBaseParser.IMPORT)
                self.state = 1056
                localctx.kw2 = self.match(SqlBaseParser.TABLE)
                pass

            elif la_ == 12:
                self.enterOuterAlt(localctx, 12)
                self.state = 1057
                localctx.kw1 = self.match(SqlBaseParser.SHOW)
                self.state = 1058
                localctx.kw2 = self.match(SqlBaseParser.COMPACTIONS)
                pass

            elif la_ == 13:
                self.enterOuterAlt(localctx, 13)
                self.state = 1059
                localctx.kw1 = self.match(SqlBaseParser.SHOW)
                self.state = 1060
                localctx.kw2 = self.match(SqlBaseParser.CREATE)
                self.state = 1061
                localctx.kw3 = self.match(SqlBaseParser.TABLE)
                pass

            elif la_ == 14:
                self.enterOuterAlt(localctx, 14)
                self.state = 1062
                localctx.kw1 = self.match(SqlBaseParser.SHOW)
                self.state = 1063
                localctx.kw2 = self.match(SqlBaseParser.TRANSACTIONS)
                pass

            elif la_ == 15:
                self.enterOuterAlt(localctx, 15)
                self.state = 1064
                localctx.kw1 = self.match(SqlBaseParser.SHOW)
                self.state = 1065
                localctx.kw2 = self.match(SqlBaseParser.INDEXES)
                pass

            elif la_ == 16:
                self.enterOuterAlt(localctx, 16)
                self.state = 1066
                localctx.kw1 = self.match(SqlBaseParser.SHOW)
                self.state = 1067
                localctx.kw2 = self.match(SqlBaseParser.LOCKS)
                pass

            elif la_ == 17:
                self.enterOuterAlt(localctx, 17)
                self.state = 1068
                localctx.kw1 = self.match(SqlBaseParser.CREATE)
                self.state = 1069
                localctx.kw2 = self.match(SqlBaseParser.INDEX)
                pass

            elif la_ == 18:
                self.enterOuterAlt(localctx, 18)
                self.state = 1070
                localctx.kw1 = self.match(SqlBaseParser.DROP)
                self.state = 1071
                localctx.kw2 = self.match(SqlBaseParser.INDEX)
                pass

            elif la_ == 19:
                self.enterOuterAlt(localctx, 19)
                self.state = 1072
                localctx.kw1 = self.match(SqlBaseParser.ALTER)
                self.state = 1073
                localctx.kw2 = self.match(SqlBaseParser.INDEX)
                pass

            elif la_ == 20:
                self.enterOuterAlt(localctx, 20)
                self.state = 1074
                localctx.kw1 = self.match(SqlBaseParser.LOCK)
                self.state = 1075
                localctx.kw2 = self.match(SqlBaseParser.TABLE)
                pass

            elif la_ == 21:
                self.enterOuterAlt(localctx, 21)
                self.state = 1076
                localctx.kw1 = self.match(SqlBaseParser.LOCK)
                self.state = 1077
                localctx.kw2 = self.match(SqlBaseParser.DATABASE)
                pass

            elif la_ == 22:
                self.enterOuterAlt(localctx, 22)
                self.state = 1078
                localctx.kw1 = self.match(SqlBaseParser.UNLOCK)
                self.state = 1079
                localctx.kw2 = self.match(SqlBaseParser.TABLE)
                pass

            elif la_ == 23:
                self.enterOuterAlt(localctx, 23)
                self.state = 1080
                localctx.kw1 = self.match(SqlBaseParser.UNLOCK)
                self.state = 1081
                localctx.kw2 = self.match(SqlBaseParser.DATABASE)
                pass

            elif la_ == 24:
                self.enterOuterAlt(localctx, 24)
                self.state = 1082
                localctx.kw1 = self.match(SqlBaseParser.CREATE)
                self.state = 1083
                localctx.kw2 = self.match(SqlBaseParser.TEMPORARY)
                self.state = 1084
                localctx.kw3 = self.match(SqlBaseParser.MACRO)
                pass

            elif la_ == 25:
                self.enterOuterAlt(localctx, 25)
                self.state = 1085
                localctx.kw1 = self.match(SqlBaseParser.DROP)
                self.state = 1086
                localctx.kw2 = self.match(SqlBaseParser.TEMPORARY)
                self.state = 1087
                localctx.kw3 = self.match(SqlBaseParser.MACRO)
                pass

            elif la_ == 26:
                self.enterOuterAlt(localctx, 26)
                self.state = 1088
                localctx.kw1 = self.match(SqlBaseParser.ALTER)
                self.state = 1089
                localctx.kw2 = self.match(SqlBaseParser.TABLE)
                self.state = 1090
                self.tableIdentifier()
                self.state = 1091
                localctx.kw3 = self.match(SqlBaseParser.NOT)
                self.state = 1092
                localctx.kw4 = self.match(SqlBaseParser.CLUSTERED)
                pass

            elif la_ == 27:
                self.enterOuterAlt(localctx, 27)
                self.state = 1094
                localctx.kw1 = self.match(SqlBaseParser.ALTER)
                self.state = 1095
                localctx.kw2 = self.match(SqlBaseParser.TABLE)
                self.state = 1096
                self.tableIdentifier()
                self.state = 1097
                localctx.kw3 = self.match(SqlBaseParser.CLUSTERED)
                self.state = 1098
                localctx.kw4 = self.match(SqlBaseParser.BY)
                pass

            elif la_ == 28:
                self.enterOuterAlt(localctx, 28)
                self.state = 1100
                localctx.kw1 = self.match(SqlBaseParser.ALTER)
                self.state = 1101
                localctx.kw2 = self.match(SqlBaseParser.TABLE)
                self.state = 1102
                self.tableIdentifier()
                self.state = 1103
                localctx.kw3 = self.match(SqlBaseParser.NOT)
                self.state = 1104
                localctx.kw4 = self.match(SqlBaseParser.SORTED)
                pass

            elif la_ == 29:
                self.enterOuterAlt(localctx, 29)
                self.state = 1106
                localctx.kw1 = self.match(SqlBaseParser.ALTER)
                self.state = 1107
                localctx.kw2 = self.match(SqlBaseParser.TABLE)
                self.state = 1108
                self.tableIdentifier()
                self.state = 1109
                localctx.kw3 = self.match(SqlBaseParser.SKEWED)
                self.state = 1110
                localctx.kw4 = self.match(SqlBaseParser.BY)
                pass

            elif la_ == 30:
                self.enterOuterAlt(localctx, 30)
                self.state = 1112
                localctx.kw1 = self.match(SqlBaseParser.ALTER)
                self.state = 1113
                localctx.kw2 = self.match(SqlBaseParser.TABLE)
                self.state = 1114
                self.tableIdentifier()
                self.state = 1115
                localctx.kw3 = self.match(SqlBaseParser.NOT)
                self.state = 1116
                localctx.kw4 = self.match(SqlBaseParser.SKEWED)
                pass

            elif la_ == 31:
                self.enterOuterAlt(localctx, 31)
                self.state = 1118
                localctx.kw1 = self.match(SqlBaseParser.ALTER)
                self.state = 1119
                localctx.kw2 = self.match(SqlBaseParser.TABLE)
                self.state = 1120
                self.tableIdentifier()
                self.state = 1121
                localctx.kw3 = self.match(SqlBaseParser.NOT)
                self.state = 1122
                localctx.kw4 = self.match(SqlBaseParser.STORED)
                self.state = 1123
                localctx.kw5 = self.match(SqlBaseParser.AS)
                self.state = 1124
                localctx.kw6 = self.match(SqlBaseParser.DIRECTORIES)
                pass

            elif la_ == 32:
                self.enterOuterAlt(localctx, 32)
                self.state = 1126
                localctx.kw1 = self.match(SqlBaseParser.ALTER)
                self.state = 1127
                localctx.kw2 = self.match(SqlBaseParser.TABLE)
                self.state = 1128
                self.tableIdentifier()
                self.state = 1129
                localctx.kw3 = self.match(SqlBaseParser.SET)
                self.state = 1130
                localctx.kw4 = self.match(SqlBaseParser.SKEWED)
                self.state = 1131
                localctx.kw5 = self.match(SqlBaseParser.LOCATION)
                pass

            elif la_ == 33:
                self.enterOuterAlt(localctx, 33)
                self.state = 1133
                localctx.kw1 = self.match(SqlBaseParser.ALTER)
                self.state = 1134
                localctx.kw2 = self.match(SqlBaseParser.TABLE)
                self.state = 1135
                self.tableIdentifier()
                self.state = 1136
                localctx.kw3 = self.match(SqlBaseParser.EXCHANGE)
                self.state = 1137
                localctx.kw4 = self.match(SqlBaseParser.PARTITION)
                pass

            elif la_ == 34:
                self.enterOuterAlt(localctx, 34)
                self.state = 1139
                localctx.kw1 = self.match(SqlBaseParser.ALTER)
                self.state = 1140
                localctx.kw2 = self.match(SqlBaseParser.TABLE)
                self.state = 1141
                self.tableIdentifier()
                self.state = 1142
                localctx.kw3 = self.match(SqlBaseParser.ARCHIVE)
                self.state = 1143
                localctx.kw4 = self.match(SqlBaseParser.PARTITION)
                pass

            elif la_ == 35:
                self.enterOuterAlt(localctx, 35)
                self.state = 1145
                localctx.kw1 = self.match(SqlBaseParser.ALTER)
                self.state = 1146
                localctx.kw2 = self.match(SqlBaseParser.TABLE)
                self.state = 1147
                self.tableIdentifier()
                self.state = 1148
                localctx.kw3 = self.match(SqlBaseParser.UNARCHIVE)
                self.state = 1149
                localctx.kw4 = self.match(SqlBaseParser.PARTITION)
                pass

            elif la_ == 36:
                self.enterOuterAlt(localctx, 36)
                self.state = 1151
                localctx.kw1 = self.match(SqlBaseParser.ALTER)
                self.state = 1152
                localctx.kw2 = self.match(SqlBaseParser.TABLE)
                self.state = 1153
                self.tableIdentifier()
                self.state = 1154
                localctx.kw3 = self.match(SqlBaseParser.TOUCH)
                pass

            elif la_ == 37:
                self.enterOuterAlt(localctx, 37)
                self.state = 1156
                localctx.kw1 = self.match(SqlBaseParser.ALTER)
                self.state = 1157
                localctx.kw2 = self.match(SqlBaseParser.TABLE)
                self.state = 1158
                self.tableIdentifier()
                self.state = 1160
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.PARTITION:
                    self.state = 1159
                    self.partitionSpec()


                self.state = 1162
                localctx.kw3 = self.match(SqlBaseParser.COMPACT)
                pass

            elif la_ == 38:
                self.enterOuterAlt(localctx, 38)
                self.state = 1164
                localctx.kw1 = self.match(SqlBaseParser.ALTER)
                self.state = 1165
                localctx.kw2 = self.match(SqlBaseParser.TABLE)
                self.state = 1166
                self.tableIdentifier()
                self.state = 1168
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.PARTITION:
                    self.state = 1167
                    self.partitionSpec()


                self.state = 1170
                localctx.kw3 = self.match(SqlBaseParser.CONCATENATE)
                pass

            elif la_ == 39:
                self.enterOuterAlt(localctx, 39)
                self.state = 1172
                localctx.kw1 = self.match(SqlBaseParser.ALTER)
                self.state = 1173
                localctx.kw2 = self.match(SqlBaseParser.TABLE)
                self.state = 1174
                self.tableIdentifier()
                self.state = 1176
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.PARTITION:
                    self.state = 1175
                    self.partitionSpec()


                self.state = 1178
                localctx.kw3 = self.match(SqlBaseParser.SET)
                self.state = 1179
                localctx.kw4 = self.match(SqlBaseParser.FILEFORMAT)
                pass

            elif la_ == 40:
                self.enterOuterAlt(localctx, 40)
                self.state = 1181
                localctx.kw1 = self.match(SqlBaseParser.ALTER)
                self.state = 1182
                localctx.kw2 = self.match(SqlBaseParser.TABLE)
                self.state = 1183
                self.tableIdentifier()
                self.state = 1185
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.PARTITION:
                    self.state = 1184
                    self.partitionSpec()


                self.state = 1187
                localctx.kw3 = self.match(SqlBaseParser.REPLACE)
                self.state = 1188
                localctx.kw4 = self.match(SqlBaseParser.COLUMNS)
                pass

            elif la_ == 41:
                self.enterOuterAlt(localctx, 41)
                self.state = 1190
                localctx.kw1 = self.match(SqlBaseParser.START)
                self.state = 1191
                localctx.kw2 = self.match(SqlBaseParser.TRANSACTION)
                pass

            elif la_ == 42:
                self.enterOuterAlt(localctx, 42)
                self.state = 1192
                localctx.kw1 = self.match(SqlBaseParser.COMMIT)
                pass

            elif la_ == 43:
                self.enterOuterAlt(localctx, 43)
                self.state = 1193
                localctx.kw1 = self.match(SqlBaseParser.ROLLBACK)
                pass

            elif la_ == 44:
                self.enterOuterAlt(localctx, 44)
                self.state = 1194
                localctx.kw1 = self.match(SqlBaseParser.DFS)
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class CreateTableHeaderContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def CREATE(self):
            return self.getToken(SqlBaseParser.CREATE, 0)

        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)

        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)


        def TEMPORARY(self):
            return self.getToken(SqlBaseParser.TEMPORARY, 0)

        def EXTERNAL(self):
            return self.getToken(SqlBaseParser.EXTERNAL, 0)

        def IF(self):
            return self.getToken(SqlBaseParser.IF, 0)

        def NOT(self):
            return self.getToken(SqlBaseParser.NOT, 0)

        def EXISTS(self):
            return self.getToken(SqlBaseParser.EXISTS, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_createTableHeader

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterCreateTableHeader" ):
                listener.enterCreateTableHeader(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitCreateTableHeader" ):
                listener.exitCreateTableHeader(self)




    def createTableHeader(self):

        localctx = SqlBaseParser.CreateTableHeaderContext(self, self._ctx, self.state)
        self.enterRule(localctx, 18, self.RULE_createTableHeader)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1197
            self.match(SqlBaseParser.CREATE)
            self.state = 1199
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==SqlBaseParser.TEMPORARY:
                self.state = 1198
                self.match(SqlBaseParser.TEMPORARY)


            self.state = 1202
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==SqlBaseParser.EXTERNAL:
                self.state = 1201
                self.match(SqlBaseParser.EXTERNAL)


            self.state = 1204
            self.match(SqlBaseParser.TABLE)
            self.state = 1208
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,121,self._ctx)
            if la_ == 1:
                self.state = 1205
                self.match(SqlBaseParser.IF)
                self.state = 1206
                self.match(SqlBaseParser.NOT)
                self.state = 1207
                self.match(SqlBaseParser.EXISTS)


            self.state = 1210
            self.multipartIdentifier()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class ReplaceTableHeaderContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def REPLACE(self):
            return self.getToken(SqlBaseParser.REPLACE, 0)

        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)

        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)


        def CREATE(self):
            return self.getToken(SqlBaseParser.CREATE, 0)

        def OR(self):
            return self.getToken(SqlBaseParser.OR, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_replaceTableHeader

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterReplaceTableHeader" ):
                listener.enterReplaceTableHeader(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitReplaceTableHeader" ):
                listener.exitReplaceTableHeader(self)




    def replaceTableHeader(self):

        localctx = SqlBaseParser.ReplaceTableHeaderContext(self, self._ctx, self.state)
        self.enterRule(localctx, 20, self.RULE_replaceTableHeader)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1214
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==SqlBaseParser.CREATE:
                self.state = 1212
                self.match(SqlBaseParser.CREATE)
                self.state = 1213
                self.match(SqlBaseParser.OR)


            self.state = 1216
            self.match(SqlBaseParser.REPLACE)
            self.state = 1217
            self.match(SqlBaseParser.TABLE)
            self.state = 1218
            self.multipartIdentifier()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class BucketSpecContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def CLUSTERED(self):
            return self.getToken(SqlBaseParser.CLUSTERED, 0)

        def BY(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.BY)
            else:
                return self.getToken(SqlBaseParser.BY, i)

        def identifierList(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierListContext,0)


        def INTO(self):
            return self.getToken(SqlBaseParser.INTO, 0)

        def INTEGER_VALUE(self):
            return self.getToken(SqlBaseParser.INTEGER_VALUE, 0)

        def BUCKETS(self):
            return self.getToken(SqlBaseParser.BUCKETS, 0)

        def SORTED(self):
            return self.getToken(SqlBaseParser.SORTED, 0)

        def orderedIdentifierList(self):
            return self.getTypedRuleContext(SqlBaseParser.OrderedIdentifierListContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_bucketSpec

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterBucketSpec" ):
                listener.enterBucketSpec(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitBucketSpec" ):
                listener.exitBucketSpec(self)




    def bucketSpec(self):

        localctx = SqlBaseParser.BucketSpecContext(self, self._ctx, self.state)
        self.enterRule(localctx, 22, self.RULE_bucketSpec)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1220
            self.match(SqlBaseParser.CLUSTERED)
            self.state = 1221
            self.match(SqlBaseParser.BY)
            self.state = 1222
            self.identifierList()
            self.state = 1226
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==SqlBaseParser.SORTED:
                self.state = 1223
                self.match(SqlBaseParser.SORTED)
                self.state = 1224
                self.match(SqlBaseParser.BY)
                self.state = 1225
                self.orderedIdentifierList()


            self.state = 1228
            self.match(SqlBaseParser.INTO)
            self.state = 1229
            self.match(SqlBaseParser.INTEGER_VALUE)
            self.state = 1230
            self.match(SqlBaseParser.BUCKETS)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class SkewSpecContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def SKEWED(self):
            return self.getToken(SqlBaseParser.SKEWED, 0)

        def BY(self):
            return self.getToken(SqlBaseParser.BY, 0)

        def identifierList(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierListContext,0)


        def ON(self):
            return self.getToken(SqlBaseParser.ON, 0)

        def constantList(self):
            return self.getTypedRuleContext(SqlBaseParser.ConstantListContext,0)


        def nestedConstantList(self):
            return self.getTypedRuleContext(SqlBaseParser.NestedConstantListContext,0)


        def STORED(self):
            return self.getToken(SqlBaseParser.STORED, 0)

        def AS(self):
            return self.getToken(SqlBaseParser.AS, 0)

        def DIRECTORIES(self):
            return self.getToken(SqlBaseParser.DIRECTORIES, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_skewSpec

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSkewSpec" ):
                listener.enterSkewSpec(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSkewSpec" ):
                listener.exitSkewSpec(self)




    def skewSpec(self):

        localctx = SqlBaseParser.SkewSpecContext(self, self._ctx, self.state)
        self.enterRule(localctx, 24, self.RULE_skewSpec)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1232
            self.match(SqlBaseParser.SKEWED)
            self.state = 1233
            self.match(SqlBaseParser.BY)
            self.state = 1234
            self.identifierList()
            self.state = 1235
            self.match(SqlBaseParser.ON)
            self.state = 1238
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,124,self._ctx)
            if la_ == 1:
                self.state = 1236
                self.constantList()
                pass

            elif la_ == 2:
                self.state = 1237
                self.nestedConstantList()
                pass


            self.state = 1243
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,125,self._ctx)
            if la_ == 1:
                self.state = 1240
                self.match(SqlBaseParser.STORED)
                self.state = 1241
                self.match(SqlBaseParser.AS)
                self.state = 1242
                self.match(SqlBaseParser.DIRECTORIES)


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class LocationSpecContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def LOCATION(self):
            return self.getToken(SqlBaseParser.LOCATION, 0)

        def STRING(self):
            return self.getToken(SqlBaseParser.STRING, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_locationSpec

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterLocationSpec" ):
                listener.enterLocationSpec(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitLocationSpec" ):
                listener.exitLocationSpec(self)




    def locationSpec(self):

        localctx = SqlBaseParser.LocationSpecContext(self, self._ctx, self.state)
        self.enterRule(localctx, 26, self.RULE_locationSpec)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1245
            self.match(SqlBaseParser.LOCATION)
            self.state = 1246
            self.match(SqlBaseParser.STRING)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class CommentSpecContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def COMMENT(self):
            return self.getToken(SqlBaseParser.COMMENT, 0)

        def STRING(self):
            return self.getToken(SqlBaseParser.STRING, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_commentSpec

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterCommentSpec" ):
                listener.enterCommentSpec(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitCommentSpec" ):
                listener.exitCommentSpec(self)




    def commentSpec(self):

        localctx = SqlBaseParser.CommentSpecContext(self, self._ctx, self.state)
        self.enterRule(localctx, 28, self.RULE_commentSpec)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1248
            self.match(SqlBaseParser.COMMENT)
            self.state = 1249
            self.match(SqlBaseParser.STRING)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class QueryContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def queryTerm(self):
            return self.getTypedRuleContext(SqlBaseParser.QueryTermContext,0)


        def queryOrganization(self):
            return self.getTypedRuleContext(SqlBaseParser.QueryOrganizationContext,0)


        def ctes(self):
            return self.getTypedRuleContext(SqlBaseParser.CtesContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_query

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterQuery" ):
                listener.enterQuery(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitQuery" ):
                listener.exitQuery(self)




    def query(self):

        localctx = SqlBaseParser.QueryContext(self, self._ctx, self.state)
        self.enterRule(localctx, 30, self.RULE_query)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1252
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==SqlBaseParser.WITH:
                self.state = 1251
                self.ctes()


            self.state = 1254
            self.queryTerm(0)
            self.state = 1255
            self.queryOrganization()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class InsertIntoContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser


        def getRuleIndex(self):
            return SqlBaseParser.RULE_insertInto


        def copyFrom(self, ctx:ParserRuleContext):
            super().copyFrom(ctx)



    class InsertOverwriteHiveDirContext(InsertIntoContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.InsertIntoContext
            super().__init__(parser)
            self.path = None # Token
            self.copyFrom(ctx)

        def INSERT(self):
            return self.getToken(SqlBaseParser.INSERT, 0)
        def OVERWRITE(self):
            return self.getToken(SqlBaseParser.OVERWRITE, 0)
        def DIRECTORY(self):
            return self.getToken(SqlBaseParser.DIRECTORY, 0)
        def STRING(self):
            return self.getToken(SqlBaseParser.STRING, 0)
        def LOCAL(self):
            return self.getToken(SqlBaseParser.LOCAL, 0)
        def rowFormat(self):
            return self.getTypedRuleContext(SqlBaseParser.RowFormatContext,0)

        def createFileFormat(self):
            return self.getTypedRuleContext(SqlBaseParser.CreateFileFormatContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterInsertOverwriteHiveDir" ):
                listener.enterInsertOverwriteHiveDir(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitInsertOverwriteHiveDir" ):
                listener.exitInsertOverwriteHiveDir(self)


    class InsertOverwriteDirContext(InsertIntoContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.InsertIntoContext
            super().__init__(parser)
            self.path = None # Token
            self.options = None # TablePropertyListContext
            self.copyFrom(ctx)

        def INSERT(self):
            return self.getToken(SqlBaseParser.INSERT, 0)
        def OVERWRITE(self):
            return self.getToken(SqlBaseParser.OVERWRITE, 0)
        def DIRECTORY(self):
            return self.getToken(SqlBaseParser.DIRECTORY, 0)
        def tableProvider(self):
            return self.getTypedRuleContext(SqlBaseParser.TableProviderContext,0)

        def LOCAL(self):
            return self.getToken(SqlBaseParser.LOCAL, 0)
        def OPTIONS(self):
            return self.getToken(SqlBaseParser.OPTIONS, 0)
        def STRING(self):
            return self.getToken(SqlBaseParser.STRING, 0)
        def tablePropertyList(self):
            return self.getTypedRuleContext(SqlBaseParser.TablePropertyListContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterInsertOverwriteDir" ):
                listener.enterInsertOverwriteDir(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitInsertOverwriteDir" ):
                listener.exitInsertOverwriteDir(self)


    class InsertOverwriteTableContext(InsertIntoContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.InsertIntoContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def INSERT(self):
            return self.getToken(SqlBaseParser.INSERT, 0)
        def OVERWRITE(self):
            return self.getToken(SqlBaseParser.OVERWRITE, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def partitionSpec(self):
            return self.getTypedRuleContext(SqlBaseParser.PartitionSpecContext,0)

        def IF(self):
            return self.getToken(SqlBaseParser.IF, 0)
        def NOT(self):
            return self.getToken(SqlBaseParser.NOT, 0)
        def EXISTS(self):
            return self.getToken(SqlBaseParser.EXISTS, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterInsertOverwriteTable" ):
                listener.enterInsertOverwriteTable(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitInsertOverwriteTable" ):
                listener.exitInsertOverwriteTable(self)


    class InsertIntoTableContext(InsertIntoContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.InsertIntoContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def INSERT(self):
            return self.getToken(SqlBaseParser.INSERT, 0)
        def INTO(self):
            return self.getToken(SqlBaseParser.INTO, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def partitionSpec(self):
            return self.getTypedRuleContext(SqlBaseParser.PartitionSpecContext,0)

        def IF(self):
            return self.getToken(SqlBaseParser.IF, 0)
        def NOT(self):
            return self.getToken(SqlBaseParser.NOT, 0)
        def EXISTS(self):
            return self.getToken(SqlBaseParser.EXISTS, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterInsertIntoTable" ):
                listener.enterInsertIntoTable(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitInsertIntoTable" ):
                listener.exitInsertIntoTable(self)



    def insertInto(self):

        localctx = SqlBaseParser.InsertIntoContext(self, self._ctx, self.state)
        self.enterRule(localctx, 32, self.RULE_insertInto)
        self._la = 0 # Token type
        try:
            self.state = 1312
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,139,self._ctx)
            if la_ == 1:
                localctx = SqlBaseParser.InsertOverwriteTableContext(self, localctx)
                self.enterOuterAlt(localctx, 1)
                self.state = 1257
                self.match(SqlBaseParser.INSERT)
                self.state = 1258
                self.match(SqlBaseParser.OVERWRITE)
                self.state = 1260
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,127,self._ctx)
                if la_ == 1:
                    self.state = 1259
                    self.match(SqlBaseParser.TABLE)


                self.state = 1262
                self.multipartIdentifier()
                self.state = 1269
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.PARTITION:
                    self.state = 1263
                    self.partitionSpec()
                    self.state = 1267
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    if _la==SqlBaseParser.IF:
                        self.state = 1264
                        self.match(SqlBaseParser.IF)
                        self.state = 1265
                        self.match(SqlBaseParser.NOT)
                        self.state = 1266
                        self.match(SqlBaseParser.EXISTS)




                pass

            elif la_ == 2:
                localctx = SqlBaseParser.InsertIntoTableContext(self, localctx)
                self.enterOuterAlt(localctx, 2)
                self.state = 1271
                self.match(SqlBaseParser.INSERT)
                self.state = 1272
                self.match(SqlBaseParser.INTO)
                self.state = 1274
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,130,self._ctx)
                if la_ == 1:
                    self.state = 1273
                    self.match(SqlBaseParser.TABLE)


                self.state = 1276
                self.multipartIdentifier()
                self.state = 1278
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.PARTITION:
                    self.state = 1277
                    self.partitionSpec()


                self.state = 1283
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.IF:
                    self.state = 1280
                    self.match(SqlBaseParser.IF)
                    self.state = 1281
                    self.match(SqlBaseParser.NOT)
                    self.state = 1282
                    self.match(SqlBaseParser.EXISTS)


                pass

            elif la_ == 3:
                localctx = SqlBaseParser.InsertOverwriteHiveDirContext(self, localctx)
                self.enterOuterAlt(localctx, 3)
                self.state = 1285
                self.match(SqlBaseParser.INSERT)
                self.state = 1286
                self.match(SqlBaseParser.OVERWRITE)
                self.state = 1288
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.LOCAL:
                    self.state = 1287
                    self.match(SqlBaseParser.LOCAL)


                self.state = 1290
                self.match(SqlBaseParser.DIRECTORY)
                self.state = 1291
                localctx.path = self.match(SqlBaseParser.STRING)
                self.state = 1293
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.ROW:
                    self.state = 1292
                    self.rowFormat()


                self.state = 1296
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.STORED:
                    self.state = 1295
                    self.createFileFormat()


                pass

            elif la_ == 4:
                localctx = SqlBaseParser.InsertOverwriteDirContext(self, localctx)
                self.enterOuterAlt(localctx, 4)
                self.state = 1298
                self.match(SqlBaseParser.INSERT)
                self.state = 1299
                self.match(SqlBaseParser.OVERWRITE)
                self.state = 1301
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.LOCAL:
                    self.state = 1300
                    self.match(SqlBaseParser.LOCAL)


                self.state = 1303
                self.match(SqlBaseParser.DIRECTORY)
                self.state = 1305
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.STRING:
                    self.state = 1304
                    localctx.path = self.match(SqlBaseParser.STRING)


                self.state = 1307
                self.tableProvider()
                self.state = 1310
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.OPTIONS:
                    self.state = 1308
                    self.match(SqlBaseParser.OPTIONS)
                    self.state = 1309
                    localctx.options = self.tablePropertyList()


                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class PartitionSpecLocationContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def partitionSpec(self):
            return self.getTypedRuleContext(SqlBaseParser.PartitionSpecContext,0)


        def locationSpec(self):
            return self.getTypedRuleContext(SqlBaseParser.LocationSpecContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_partitionSpecLocation

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterPartitionSpecLocation" ):
                listener.enterPartitionSpecLocation(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitPartitionSpecLocation" ):
                listener.exitPartitionSpecLocation(self)




    def partitionSpecLocation(self):

        localctx = SqlBaseParser.PartitionSpecLocationContext(self, self._ctx, self.state)
        self.enterRule(localctx, 34, self.RULE_partitionSpecLocation)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1314
            self.partitionSpec()
            self.state = 1316
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==SqlBaseParser.LOCATION:
                self.state = 1315
                self.locationSpec()


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class PartitionSpecContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def PARTITION(self):
            return self.getToken(SqlBaseParser.PARTITION, 0)

        def partitionVal(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.PartitionValContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.PartitionValContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_partitionSpec

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterPartitionSpec" ):
                listener.enterPartitionSpec(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitPartitionSpec" ):
                listener.exitPartitionSpec(self)




    def partitionSpec(self):

        localctx = SqlBaseParser.PartitionSpecContext(self, self._ctx, self.state)
        self.enterRule(localctx, 36, self.RULE_partitionSpec)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1318
            self.match(SqlBaseParser.PARTITION)
            self.state = 1319
            self.match(SqlBaseParser.T__1)
            self.state = 1320
            self.partitionVal()
            self.state = 1325
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while _la==SqlBaseParser.T__3:
                self.state = 1321
                self.match(SqlBaseParser.T__3)
                self.state = 1322
                self.partitionVal()
                self.state = 1327
                self._errHandler.sync(self)
                _la = self._input.LA(1)

            self.state = 1328
            self.match(SqlBaseParser.T__2)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class PartitionValContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def identifier(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierContext,0)


        def EQ(self):
            return self.getToken(SqlBaseParser.EQ, 0)

        def constant(self):
            return self.getTypedRuleContext(SqlBaseParser.ConstantContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_partitionVal

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterPartitionVal" ):
                listener.enterPartitionVal(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitPartitionVal" ):
                listener.exitPartitionVal(self)




    def partitionVal(self):

        localctx = SqlBaseParser.PartitionValContext(self, self._ctx, self.state)
        self.enterRule(localctx, 38, self.RULE_partitionVal)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1330
            self.identifier()
            self.state = 1333
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==SqlBaseParser.EQ:
                self.state = 1331
                self.match(SqlBaseParser.EQ)
                self.state = 1332
                self.constant()


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class NamespaceContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def NAMESPACE(self):
            return self.getToken(SqlBaseParser.NAMESPACE, 0)

        def DATABASE(self):
            return self.getToken(SqlBaseParser.DATABASE, 0)

        def SCHEMA(self):
            return self.getToken(SqlBaseParser.SCHEMA, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_namespace

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterNamespace" ):
                listener.enterNamespace(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitNamespace" ):
                listener.exitNamespace(self)




    def namespace(self):

        localctx = SqlBaseParser.NamespaceContext(self, self._ctx, self.state)
        self.enterRule(localctx, 40, self.RULE_namespace)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1335
            _la = self._input.LA(1)
            if not(_la==SqlBaseParser.DATABASE or _la==SqlBaseParser.NAMESPACE or _la==SqlBaseParser.SCHEMA):
                self._errHandler.recoverInline(self)
            else:
                self._errHandler.reportMatch(self)
                self.consume()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class DescribeFuncNameContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def qualifiedName(self):
            return self.getTypedRuleContext(SqlBaseParser.QualifiedNameContext,0)


        def STRING(self):
            return self.getToken(SqlBaseParser.STRING, 0)

        def comparisonOperator(self):
            return self.getTypedRuleContext(SqlBaseParser.ComparisonOperatorContext,0)


        def arithmeticOperator(self):
            return self.getTypedRuleContext(SqlBaseParser.ArithmeticOperatorContext,0)


        def predicateOperator(self):
            return self.getTypedRuleContext(SqlBaseParser.PredicateOperatorContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_describeFuncName

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterDescribeFuncName" ):
                listener.enterDescribeFuncName(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitDescribeFuncName" ):
                listener.exitDescribeFuncName(self)




    def describeFuncName(self):

        localctx = SqlBaseParser.DescribeFuncNameContext(self, self._ctx, self.state)
        self.enterRule(localctx, 42, self.RULE_describeFuncName)
        try:
            self.state = 1342
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,143,self._ctx)
            if la_ == 1:
                self.enterOuterAlt(localctx, 1)
                self.state = 1337
                self.qualifiedName()
                pass

            elif la_ == 2:
                self.enterOuterAlt(localctx, 2)
                self.state = 1338
                self.match(SqlBaseParser.STRING)
                pass

            elif la_ == 3:
                self.enterOuterAlt(localctx, 3)
                self.state = 1339
                self.comparisonOperator()
                pass

            elif la_ == 4:
                self.enterOuterAlt(localctx, 4)
                self.state = 1340
                self.arithmeticOperator()
                pass

            elif la_ == 5:
                self.enterOuterAlt(localctx, 5)
                self.state = 1341
                self.predicateOperator()
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class DescribeColNameContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self._identifier = None # IdentifierContext
            self.nameParts = list() # of IdentifierContexts

        def identifier(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.IdentifierContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.IdentifierContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_describeColName

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterDescribeColName" ):
                listener.enterDescribeColName(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitDescribeColName" ):
                listener.exitDescribeColName(self)




    def describeColName(self):

        localctx = SqlBaseParser.DescribeColNameContext(self, self._ctx, self.state)
        self.enterRule(localctx, 44, self.RULE_describeColName)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1344
            localctx._identifier = self.identifier()
            localctx.nameParts.append(localctx._identifier)
            self.state = 1349
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while _la==SqlBaseParser.T__4:
                self.state = 1345
                self.match(SqlBaseParser.T__4)
                self.state = 1346
                localctx._identifier = self.identifier()
                localctx.nameParts.append(localctx._identifier)
                self.state = 1351
                self._errHandler.sync(self)
                _la = self._input.LA(1)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class CtesContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def WITH(self):
            return self.getToken(SqlBaseParser.WITH, 0)

        def namedQuery(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.NamedQueryContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.NamedQueryContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_ctes

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterCtes" ):
                listener.enterCtes(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitCtes" ):
                listener.exitCtes(self)




    def ctes(self):

        localctx = SqlBaseParser.CtesContext(self, self._ctx, self.state)
        self.enterRule(localctx, 46, self.RULE_ctes)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1352
            self.match(SqlBaseParser.WITH)
            self.state = 1353
            self.namedQuery()
            self.state = 1358
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while _la==SqlBaseParser.T__3:
                self.state = 1354
                self.match(SqlBaseParser.T__3)
                self.state = 1355
                self.namedQuery()
                self.state = 1360
                self._errHandler.sync(self)
                _la = self._input.LA(1)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class NamedQueryContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.name = None # ErrorCapturingIdentifierContext
            self.columnAliases = None # IdentifierListContext

        def query(self):
            return self.getTypedRuleContext(SqlBaseParser.QueryContext,0)


        def errorCapturingIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.ErrorCapturingIdentifierContext,0)


        def AS(self):
            return self.getToken(SqlBaseParser.AS, 0)

        def identifierList(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierListContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_namedQuery

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterNamedQuery" ):
                listener.enterNamedQuery(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitNamedQuery" ):
                listener.exitNamedQuery(self)




    def namedQuery(self):

        localctx = SqlBaseParser.NamedQueryContext(self, self._ctx, self.state)
        self.enterRule(localctx, 48, self.RULE_namedQuery)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1361
            localctx.name = self.errorCapturingIdentifier()
            self.state = 1363
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,146,self._ctx)
            if la_ == 1:
                self.state = 1362
                localctx.columnAliases = self.identifierList()


            self.state = 1366
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==SqlBaseParser.AS:
                self.state = 1365
                self.match(SqlBaseParser.AS)


            self.state = 1368
            self.match(SqlBaseParser.T__1)
            self.state = 1369
            self.query()
            self.state = 1370
            self.match(SqlBaseParser.T__2)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class TableProviderContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def USING(self):
            return self.getToken(SqlBaseParser.USING, 0)

        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_tableProvider

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterTableProvider" ):
                listener.enterTableProvider(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitTableProvider" ):
                listener.exitTableProvider(self)




    def tableProvider(self):

        localctx = SqlBaseParser.TableProviderContext(self, self._ctx, self.state)
        self.enterRule(localctx, 50, self.RULE_tableProvider)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1372
            self.match(SqlBaseParser.USING)
            self.state = 1373
            self.multipartIdentifier()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class CreateTableClausesContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.options = None # TablePropertyListContext
            self.partitioning = None # TransformListContext
            self.tableProps = None # TablePropertyListContext

        def bucketSpec(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.BucketSpecContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.BucketSpecContext,i)


        def locationSpec(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.LocationSpecContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.LocationSpecContext,i)


        def commentSpec(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.CommentSpecContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.CommentSpecContext,i)


        def OPTIONS(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.OPTIONS)
            else:
                return self.getToken(SqlBaseParser.OPTIONS, i)

        def PARTITIONED(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.PARTITIONED)
            else:
                return self.getToken(SqlBaseParser.PARTITIONED, i)

        def BY(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.BY)
            else:
                return self.getToken(SqlBaseParser.BY, i)

        def TBLPROPERTIES(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.TBLPROPERTIES)
            else:
                return self.getToken(SqlBaseParser.TBLPROPERTIES, i)

        def tablePropertyList(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.TablePropertyListContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.TablePropertyListContext,i)


        def transformList(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.TransformListContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.TransformListContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_createTableClauses

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterCreateTableClauses" ):
                listener.enterCreateTableClauses(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitCreateTableClauses" ):
                listener.exitCreateTableClauses(self)




    def createTableClauses(self):

        localctx = SqlBaseParser.CreateTableClausesContext(self, self._ctx, self.state)
        self.enterRule(localctx, 52, self.RULE_createTableClauses)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1387
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while _la==SqlBaseParser.CLUSTERED or _la==SqlBaseParser.COMMENT or ((((_la - 138)) & ~0x3f) == 0 and ((1 << (_la - 138)) & ((1 << (SqlBaseParser.LOCATION - 138)) | (1 << (SqlBaseParser.OPTIONS - 138)) | (1 << (SqlBaseParser.PARTITIONED - 138)))) != 0) or _la==SqlBaseParser.TBLPROPERTIES:
                self.state = 1385
                self._errHandler.sync(self)
                token = self._input.LA(1)
                if token in [SqlBaseParser.OPTIONS]:
                    self.state = 1375
                    self.match(SqlBaseParser.OPTIONS)
                    self.state = 1376
                    localctx.options = self.tablePropertyList()
                    pass
                elif token in [SqlBaseParser.PARTITIONED]:
                    self.state = 1377
                    self.match(SqlBaseParser.PARTITIONED)
                    self.state = 1378
                    self.match(SqlBaseParser.BY)
                    self.state = 1379
                    localctx.partitioning = self.transformList()
                    pass
                elif token in [SqlBaseParser.CLUSTERED]:
                    self.state = 1380
                    self.bucketSpec()
                    pass
                elif token in [SqlBaseParser.LOCATION]:
                    self.state = 1381
                    self.locationSpec()
                    pass
                elif token in [SqlBaseParser.COMMENT]:
                    self.state = 1382
                    self.commentSpec()
                    pass
                elif token in [SqlBaseParser.TBLPROPERTIES]:
                    self.state = 1383
                    self.match(SqlBaseParser.TBLPROPERTIES)
                    self.state = 1384
                    localctx.tableProps = self.tablePropertyList()
                    pass
                else:
                    raise NoViableAltException(self)

                self.state = 1389
                self._errHandler.sync(self)
                _la = self._input.LA(1)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class TablePropertyListContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def tableProperty(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.TablePropertyContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.TablePropertyContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_tablePropertyList

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterTablePropertyList" ):
                listener.enterTablePropertyList(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitTablePropertyList" ):
                listener.exitTablePropertyList(self)




    def tablePropertyList(self):

        localctx = SqlBaseParser.TablePropertyListContext(self, self._ctx, self.state)
        self.enterRule(localctx, 54, self.RULE_tablePropertyList)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1390
            self.match(SqlBaseParser.T__1)
            self.state = 1391
            self.tableProperty()
            self.state = 1396
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while _la==SqlBaseParser.T__3:
                self.state = 1392
                self.match(SqlBaseParser.T__3)
                self.state = 1393
                self.tableProperty()
                self.state = 1398
                self._errHandler.sync(self)
                _la = self._input.LA(1)

            self.state = 1399
            self.match(SqlBaseParser.T__2)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class TablePropertyContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.key = None # TablePropertyKeyContext
            self.value = None # TablePropertyValueContext

        def tablePropertyKey(self):
            return self.getTypedRuleContext(SqlBaseParser.TablePropertyKeyContext,0)


        def tablePropertyValue(self):
            return self.getTypedRuleContext(SqlBaseParser.TablePropertyValueContext,0)


        def EQ(self):
            return self.getToken(SqlBaseParser.EQ, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_tableProperty

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterTableProperty" ):
                listener.enterTableProperty(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitTableProperty" ):
                listener.exitTableProperty(self)




    def tableProperty(self):

        localctx = SqlBaseParser.TablePropertyContext(self, self._ctx, self.state)
        self.enterRule(localctx, 56, self.RULE_tableProperty)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1401
            localctx.key = self.tablePropertyKey()
            self.state = 1406
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==SqlBaseParser.FALSE or ((((_la - 241)) & ~0x3f) == 0 and ((1 << (_la - 241)) & ((1 << (SqlBaseParser.TRUE - 241)) | (1 << (SqlBaseParser.EQ - 241)) | (1 << (SqlBaseParser.STRING - 241)) | (1 << (SqlBaseParser.INTEGER_VALUE - 241)) | (1 << (SqlBaseParser.DECIMAL_VALUE - 241)))) != 0):
                self.state = 1403
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.EQ:
                    self.state = 1402
                    self.match(SqlBaseParser.EQ)


                self.state = 1405
                localctx.value = self.tablePropertyValue()


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class TablePropertyKeyContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def identifier(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.IdentifierContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.IdentifierContext,i)


        def STRING(self):
            return self.getToken(SqlBaseParser.STRING, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_tablePropertyKey

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterTablePropertyKey" ):
                listener.enterTablePropertyKey(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitTablePropertyKey" ):
                listener.exitTablePropertyKey(self)




    def tablePropertyKey(self):

        localctx = SqlBaseParser.TablePropertyKeyContext(self, self._ctx, self.state)
        self.enterRule(localctx, 58, self.RULE_tablePropertyKey)
        self._la = 0 # Token type
        try:
            self.state = 1417
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,154,self._ctx)
            if la_ == 1:
                self.enterOuterAlt(localctx, 1)
                self.state = 1408
                self.identifier()
                self.state = 1413
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while _la==SqlBaseParser.T__4:
                    self.state = 1409
                    self.match(SqlBaseParser.T__4)
                    self.state = 1410
                    self.identifier()
                    self.state = 1415
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)

                pass

            elif la_ == 2:
                self.enterOuterAlt(localctx, 2)
                self.state = 1416
                self.match(SqlBaseParser.STRING)
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class TablePropertyValueContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def INTEGER_VALUE(self):
            return self.getToken(SqlBaseParser.INTEGER_VALUE, 0)

        def DECIMAL_VALUE(self):
            return self.getToken(SqlBaseParser.DECIMAL_VALUE, 0)

        def booleanValue(self):
            return self.getTypedRuleContext(SqlBaseParser.BooleanValueContext,0)


        def STRING(self):
            return self.getToken(SqlBaseParser.STRING, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_tablePropertyValue

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterTablePropertyValue" ):
                listener.enterTablePropertyValue(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitTablePropertyValue" ):
                listener.exitTablePropertyValue(self)




    def tablePropertyValue(self):

        localctx = SqlBaseParser.TablePropertyValueContext(self, self._ctx, self.state)
        self.enterRule(localctx, 60, self.RULE_tablePropertyValue)
        try:
            self.state = 1423
            self._errHandler.sync(self)
            token = self._input.LA(1)
            if token in [SqlBaseParser.INTEGER_VALUE]:
                self.enterOuterAlt(localctx, 1)
                self.state = 1419
                self.match(SqlBaseParser.INTEGER_VALUE)
                pass
            elif token in [SqlBaseParser.DECIMAL_VALUE]:
                self.enterOuterAlt(localctx, 2)
                self.state = 1420
                self.match(SqlBaseParser.DECIMAL_VALUE)
                pass
            elif token in [SqlBaseParser.FALSE, SqlBaseParser.TRUE]:
                self.enterOuterAlt(localctx, 3)
                self.state = 1421
                self.booleanValue()
                pass
            elif token in [SqlBaseParser.STRING]:
                self.enterOuterAlt(localctx, 4)
                self.state = 1422
                self.match(SqlBaseParser.STRING)
                pass
            else:
                raise NoViableAltException(self)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class ConstantListContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def constant(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ConstantContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ConstantContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_constantList

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterConstantList" ):
                listener.enterConstantList(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitConstantList" ):
                listener.exitConstantList(self)




    def constantList(self):

        localctx = SqlBaseParser.ConstantListContext(self, self._ctx, self.state)
        self.enterRule(localctx, 62, self.RULE_constantList)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1425
            self.match(SqlBaseParser.T__1)
            self.state = 1426
            self.constant()
            self.state = 1431
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while _la==SqlBaseParser.T__3:
                self.state = 1427
                self.match(SqlBaseParser.T__3)
                self.state = 1428
                self.constant()
                self.state = 1433
                self._errHandler.sync(self)
                _la = self._input.LA(1)

            self.state = 1434
            self.match(SqlBaseParser.T__2)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class NestedConstantListContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def constantList(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ConstantListContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ConstantListContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_nestedConstantList

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterNestedConstantList" ):
                listener.enterNestedConstantList(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitNestedConstantList" ):
                listener.exitNestedConstantList(self)




    def nestedConstantList(self):

        localctx = SqlBaseParser.NestedConstantListContext(self, self._ctx, self.state)
        self.enterRule(localctx, 64, self.RULE_nestedConstantList)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1436
            self.match(SqlBaseParser.T__1)
            self.state = 1437
            self.constantList()
            self.state = 1442
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while _la==SqlBaseParser.T__3:
                self.state = 1438
                self.match(SqlBaseParser.T__3)
                self.state = 1439
                self.constantList()
                self.state = 1444
                self._errHandler.sync(self)
                _la = self._input.LA(1)

            self.state = 1445
            self.match(SqlBaseParser.T__2)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class CreateFileFormatContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def STORED(self):
            return self.getToken(SqlBaseParser.STORED, 0)

        def AS(self):
            return self.getToken(SqlBaseParser.AS, 0)

        def fileFormat(self):
            return self.getTypedRuleContext(SqlBaseParser.FileFormatContext,0)


        def BY(self):
            return self.getToken(SqlBaseParser.BY, 0)

        def storageHandler(self):
            return self.getTypedRuleContext(SqlBaseParser.StorageHandlerContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_createFileFormat

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterCreateFileFormat" ):
                listener.enterCreateFileFormat(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitCreateFileFormat" ):
                listener.exitCreateFileFormat(self)




    def createFileFormat(self):

        localctx = SqlBaseParser.CreateFileFormatContext(self, self._ctx, self.state)
        self.enterRule(localctx, 66, self.RULE_createFileFormat)
        try:
            self.state = 1453
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,158,self._ctx)
            if la_ == 1:
                self.enterOuterAlt(localctx, 1)
                self.state = 1447
                self.match(SqlBaseParser.STORED)
                self.state = 1448
                self.match(SqlBaseParser.AS)
                self.state = 1449
                self.fileFormat()
                pass

            elif la_ == 2:
                self.enterOuterAlt(localctx, 2)
                self.state = 1450
                self.match(SqlBaseParser.STORED)
                self.state = 1451
                self.match(SqlBaseParser.BY)
                self.state = 1452
                self.storageHandler()
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class FileFormatContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser


        def getRuleIndex(self):
            return SqlBaseParser.RULE_fileFormat


        def copyFrom(self, ctx:ParserRuleContext):
            super().copyFrom(ctx)



    class TableFileFormatContext(FileFormatContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.FileFormatContext
            super().__init__(parser)
            self.inFmt = None # Token
            self.outFmt = None # Token
            self.copyFrom(ctx)

        def INPUTFORMAT(self):
            return self.getToken(SqlBaseParser.INPUTFORMAT, 0)
        def OUTPUTFORMAT(self):
            return self.getToken(SqlBaseParser.OUTPUTFORMAT, 0)
        def STRING(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.STRING)
            else:
                return self.getToken(SqlBaseParser.STRING, i)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterTableFileFormat" ):
                listener.enterTableFileFormat(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitTableFileFormat" ):
                listener.exitTableFileFormat(self)


    class GenericFileFormatContext(FileFormatContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.FileFormatContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def identifier(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterGenericFileFormat" ):
                listener.enterGenericFileFormat(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitGenericFileFormat" ):
                listener.exitGenericFileFormat(self)



    def fileFormat(self):

        localctx = SqlBaseParser.FileFormatContext(self, self._ctx, self.state)
        self.enterRule(localctx, 68, self.RULE_fileFormat)
        try:
            self.state = 1460
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,159,self._ctx)
            if la_ == 1:
                localctx = SqlBaseParser.TableFileFormatContext(self, localctx)
                self.enterOuterAlt(localctx, 1)
                self.state = 1455
                self.match(SqlBaseParser.INPUTFORMAT)
                self.state = 1456
                localctx.inFmt = self.match(SqlBaseParser.STRING)
                self.state = 1457
                self.match(SqlBaseParser.OUTPUTFORMAT)
                self.state = 1458
                localctx.outFmt = self.match(SqlBaseParser.STRING)
                pass

            elif la_ == 2:
                localctx = SqlBaseParser.GenericFileFormatContext(self, localctx)
                self.enterOuterAlt(localctx, 2)
                self.state = 1459
                self.identifier()
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class StorageHandlerContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def STRING(self):
            return self.getToken(SqlBaseParser.STRING, 0)

        def WITH(self):
            return self.getToken(SqlBaseParser.WITH, 0)

        def SERDEPROPERTIES(self):
            return self.getToken(SqlBaseParser.SERDEPROPERTIES, 0)

        def tablePropertyList(self):
            return self.getTypedRuleContext(SqlBaseParser.TablePropertyListContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_storageHandler

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterStorageHandler" ):
                listener.enterStorageHandler(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitStorageHandler" ):
                listener.exitStorageHandler(self)




    def storageHandler(self):

        localctx = SqlBaseParser.StorageHandlerContext(self, self._ctx, self.state)
        self.enterRule(localctx, 70, self.RULE_storageHandler)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1462
            self.match(SqlBaseParser.STRING)
            self.state = 1466
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,160,self._ctx)
            if la_ == 1:
                self.state = 1463
                self.match(SqlBaseParser.WITH)
                self.state = 1464
                self.match(SqlBaseParser.SERDEPROPERTIES)
                self.state = 1465
                self.tablePropertyList()


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class ResourceContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def identifier(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierContext,0)


        def STRING(self):
            return self.getToken(SqlBaseParser.STRING, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_resource

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterResource" ):
                listener.enterResource(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitResource" ):
                listener.exitResource(self)




    def resource(self):

        localctx = SqlBaseParser.ResourceContext(self, self._ctx, self.state)
        self.enterRule(localctx, 72, self.RULE_resource)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1468
            self.identifier()
            self.state = 1469
            self.match(SqlBaseParser.STRING)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class DmlStatementNoWithContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser


        def getRuleIndex(self):
            return SqlBaseParser.RULE_dmlStatementNoWith


        def copyFrom(self, ctx:ParserRuleContext):
            super().copyFrom(ctx)



    class DeleteFromTableContext(DmlStatementNoWithContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.DmlStatementNoWithContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def DELETE(self):
            return self.getToken(SqlBaseParser.DELETE, 0)
        def FROM(self):
            return self.getToken(SqlBaseParser.FROM, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def tableAlias(self):
            return self.getTypedRuleContext(SqlBaseParser.TableAliasContext,0)

        def whereClause(self):
            return self.getTypedRuleContext(SqlBaseParser.WhereClauseContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterDeleteFromTable" ):
                listener.enterDeleteFromTable(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitDeleteFromTable" ):
                listener.exitDeleteFromTable(self)


    class SingleInsertQueryContext(DmlStatementNoWithContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.DmlStatementNoWithContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def insertInto(self):
            return self.getTypedRuleContext(SqlBaseParser.InsertIntoContext,0)

        def queryTerm(self):
            return self.getTypedRuleContext(SqlBaseParser.QueryTermContext,0)

        def queryOrganization(self):
            return self.getTypedRuleContext(SqlBaseParser.QueryOrganizationContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSingleInsertQuery" ):
                listener.enterSingleInsertQuery(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSingleInsertQuery" ):
                listener.exitSingleInsertQuery(self)


    class MultiInsertQueryContext(DmlStatementNoWithContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.DmlStatementNoWithContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def fromClause(self):
            return self.getTypedRuleContext(SqlBaseParser.FromClauseContext,0)

        def multiInsertQueryBody(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.MultiInsertQueryBodyContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.MultiInsertQueryBodyContext,i)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterMultiInsertQuery" ):
                listener.enterMultiInsertQuery(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitMultiInsertQuery" ):
                listener.exitMultiInsertQuery(self)


    class UpdateTableContext(DmlStatementNoWithContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.DmlStatementNoWithContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def UPDATE(self):
            return self.getToken(SqlBaseParser.UPDATE, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def tableAlias(self):
            return self.getTypedRuleContext(SqlBaseParser.TableAliasContext,0)

        def setClause(self):
            return self.getTypedRuleContext(SqlBaseParser.SetClauseContext,0)

        def whereClause(self):
            return self.getTypedRuleContext(SqlBaseParser.WhereClauseContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterUpdateTable" ):
                listener.enterUpdateTable(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitUpdateTable" ):
                listener.exitUpdateTable(self)


    class MergeIntoTableContext(DmlStatementNoWithContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.DmlStatementNoWithContext
            super().__init__(parser)
            self.target = None # MultipartIdentifierContext
            self.targetAlias = None # TableAliasContext
            self.source = None # MultipartIdentifierContext
            self.sourceQuery = None # QueryContext
            self.sourceAlias = None # TableAliasContext
            self.mergeCondition = None # BooleanExpressionContext
            self.copyFrom(ctx)

        def MERGE(self):
            return self.getToken(SqlBaseParser.MERGE, 0)
        def INTO(self):
            return self.getToken(SqlBaseParser.INTO, 0)
        def USING(self):
            return self.getToken(SqlBaseParser.USING, 0)
        def ON(self):
            return self.getToken(SqlBaseParser.ON, 0)
        def multipartIdentifier(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.MultipartIdentifierContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,i)

        def tableAlias(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.TableAliasContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.TableAliasContext,i)

        def booleanExpression(self):
            return self.getTypedRuleContext(SqlBaseParser.BooleanExpressionContext,0)

        def query(self):
            return self.getTypedRuleContext(SqlBaseParser.QueryContext,0)

        def matchedClause(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.MatchedClauseContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.MatchedClauseContext,i)

        def notMatchedClause(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.NotMatchedClauseContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.NotMatchedClauseContext,i)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterMergeIntoTable" ):
                listener.enterMergeIntoTable(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitMergeIntoTable" ):
                listener.exitMergeIntoTable(self)



    def dmlStatementNoWith(self):

        localctx = SqlBaseParser.DmlStatementNoWithContext(self, self._ctx, self.state)
        self.enterRule(localctx, 74, self.RULE_dmlStatementNoWith)
        self._la = 0 # Token type
        try:
            self.state = 1522
            self._errHandler.sync(self)
            token = self._input.LA(1)
            if token in [SqlBaseParser.INSERT]:
                localctx = SqlBaseParser.SingleInsertQueryContext(self, localctx)
                self.enterOuterAlt(localctx, 1)
                self.state = 1471
                self.insertInto()
                self.state = 1472
                self.queryTerm(0)
                self.state = 1473
                self.queryOrganization()
                pass
            elif token in [SqlBaseParser.FROM]:
                localctx = SqlBaseParser.MultiInsertQueryContext(self, localctx)
                self.enterOuterAlt(localctx, 2)
                self.state = 1475
                self.fromClause()
                self.state = 1477
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while True:
                    self.state = 1476
                    self.multiInsertQueryBody()
                    self.state = 1479
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    if not (_la==SqlBaseParser.INSERT):
                        break

                pass
            elif token in [SqlBaseParser.DELETE]:
                localctx = SqlBaseParser.DeleteFromTableContext(self, localctx)
                self.enterOuterAlt(localctx, 3)
                self.state = 1481
                self.match(SqlBaseParser.DELETE)
                self.state = 1482
                self.match(SqlBaseParser.FROM)
                self.state = 1483
                self.multipartIdentifier()
                self.state = 1484
                self.tableAlias()
                self.state = 1486
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.WHERE:
                    self.state = 1485
                    self.whereClause()


                pass
            elif token in [SqlBaseParser.UPDATE]:
                localctx = SqlBaseParser.UpdateTableContext(self, localctx)
                self.enterOuterAlt(localctx, 4)
                self.state = 1488
                self.match(SqlBaseParser.UPDATE)
                self.state = 1489
                self.multipartIdentifier()
                self.state = 1490
                self.tableAlias()
                self.state = 1491
                self.setClause()
                self.state = 1493
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.WHERE:
                    self.state = 1492
                    self.whereClause()


                pass
            elif token in [SqlBaseParser.MERGE]:
                localctx = SqlBaseParser.MergeIntoTableContext(self, localctx)
                self.enterOuterAlt(localctx, 5)
                self.state = 1495
                self.match(SqlBaseParser.MERGE)
                self.state = 1496
                self.match(SqlBaseParser.INTO)
                self.state = 1497
                localctx.target = self.multipartIdentifier()
                self.state = 1498
                localctx.targetAlias = self.tableAlias()
                self.state = 1499
                self.match(SqlBaseParser.USING)
                self.state = 1505
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,164,self._ctx)
                if la_ == 1:
                    self.state = 1500
                    localctx.source = self.multipartIdentifier()
                    pass

                elif la_ == 2:
                    self.state = 1501
                    self.match(SqlBaseParser.T__1)
                    self.state = 1502
                    localctx.sourceQuery = self.query()
                    self.state = 1503
                    self.match(SqlBaseParser.T__2)
                    pass


                self.state = 1507
                localctx.sourceAlias = self.tableAlias()
                self.state = 1508
                self.match(SqlBaseParser.ON)
                self.state = 1509
                localctx.mergeCondition = self.booleanExpression(0)
                self.state = 1513
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,165,self._ctx)
                while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                    if _alt==1:
                        self.state = 1510
                        self.matchedClause()
                    self.state = 1515
                    self._errHandler.sync(self)
                    _alt = self._interp.adaptivePredict(self._input,165,self._ctx)

                self.state = 1519
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while _la==SqlBaseParser.WHEN:
                    self.state = 1516
                    self.notMatchedClause()
                    self.state = 1521
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)

                pass
            else:
                raise NoViableAltException(self)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class QueryOrganizationContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self._sortItem = None # SortItemContext
            self.order = list() # of SortItemContexts
            self._expression = None # ExpressionContext
            self.clusterBy = list() # of ExpressionContexts
            self.distributeBy = list() # of ExpressionContexts
            self.sort = list() # of SortItemContexts
            self.limit = None # ExpressionContext

        def ORDER(self):
            return self.getToken(SqlBaseParser.ORDER, 0)

        def BY(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.BY)
            else:
                return self.getToken(SqlBaseParser.BY, i)

        def CLUSTER(self):
            return self.getToken(SqlBaseParser.CLUSTER, 0)

        def DISTRIBUTE(self):
            return self.getToken(SqlBaseParser.DISTRIBUTE, 0)

        def SORT(self):
            return self.getToken(SqlBaseParser.SORT, 0)

        def windowClause(self):
            return self.getTypedRuleContext(SqlBaseParser.WindowClauseContext,0)


        def LIMIT(self):
            return self.getToken(SqlBaseParser.LIMIT, 0)

        def sortItem(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.SortItemContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.SortItemContext,i)


        def expression(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ExpressionContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ExpressionContext,i)


        def ALL(self):
            return self.getToken(SqlBaseParser.ALL, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_queryOrganization

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterQueryOrganization" ):
                listener.enterQueryOrganization(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitQueryOrganization" ):
                listener.exitQueryOrganization(self)




    def queryOrganization(self):

        localctx = SqlBaseParser.QueryOrganizationContext(self, self._ctx, self.state)
        self.enterRule(localctx, 76, self.RULE_queryOrganization)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1534
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,169,self._ctx)
            if la_ == 1:
                self.state = 1524
                self.match(SqlBaseParser.ORDER)
                self.state = 1525
                self.match(SqlBaseParser.BY)
                self.state = 1526
                localctx._sortItem = self.sortItem()
                localctx.order.append(localctx._sortItem)
                self.state = 1531
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,168,self._ctx)
                while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                    if _alt==1:
                        self.state = 1527
                        self.match(SqlBaseParser.T__3)
                        self.state = 1528
                        localctx._sortItem = self.sortItem()
                        localctx.order.append(localctx._sortItem)
                    self.state = 1533
                    self._errHandler.sync(self)
                    _alt = self._interp.adaptivePredict(self._input,168,self._ctx)



            self.state = 1546
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,171,self._ctx)
            if la_ == 1:
                self.state = 1536
                self.match(SqlBaseParser.CLUSTER)
                self.state = 1537
                self.match(SqlBaseParser.BY)
                self.state = 1538
                localctx._expression = self.expression()
                localctx.clusterBy.append(localctx._expression)
                self.state = 1543
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,170,self._ctx)
                while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                    if _alt==1:
                        self.state = 1539
                        self.match(SqlBaseParser.T__3)
                        self.state = 1540
                        localctx._expression = self.expression()
                        localctx.clusterBy.append(localctx._expression)
                    self.state = 1545
                    self._errHandler.sync(self)
                    _alt = self._interp.adaptivePredict(self._input,170,self._ctx)



            self.state = 1558
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,173,self._ctx)
            if la_ == 1:
                self.state = 1548
                self.match(SqlBaseParser.DISTRIBUTE)
                self.state = 1549
                self.match(SqlBaseParser.BY)
                self.state = 1550
                localctx._expression = self.expression()
                localctx.distributeBy.append(localctx._expression)
                self.state = 1555
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,172,self._ctx)
                while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                    if _alt==1:
                        self.state = 1551
                        self.match(SqlBaseParser.T__3)
                        self.state = 1552
                        localctx._expression = self.expression()
                        localctx.distributeBy.append(localctx._expression)
                    self.state = 1557
                    self._errHandler.sync(self)
                    _alt = self._interp.adaptivePredict(self._input,172,self._ctx)



            self.state = 1570
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,175,self._ctx)
            if la_ == 1:
                self.state = 1560
                self.match(SqlBaseParser.SORT)
                self.state = 1561
                self.match(SqlBaseParser.BY)
                self.state = 1562
                localctx._sortItem = self.sortItem()
                localctx.sort.append(localctx._sortItem)
                self.state = 1567
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,174,self._ctx)
                while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                    if _alt==1:
                        self.state = 1563
                        self.match(SqlBaseParser.T__3)
                        self.state = 1564
                        localctx._sortItem = self.sortItem()
                        localctx.sort.append(localctx._sortItem)
                    self.state = 1569
                    self._errHandler.sync(self)
                    _alt = self._interp.adaptivePredict(self._input,174,self._ctx)



            self.state = 1573
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,176,self._ctx)
            if la_ == 1:
                self.state = 1572
                self.windowClause()


            self.state = 1580
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,178,self._ctx)
            if la_ == 1:
                self.state = 1575
                self.match(SqlBaseParser.LIMIT)
                self.state = 1578
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,177,self._ctx)
                if la_ == 1:
                    self.state = 1576
                    self.match(SqlBaseParser.ALL)
                    pass

                elif la_ == 2:
                    self.state = 1577
                    localctx.limit = self.expression()
                    pass




        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class MultiInsertQueryBodyContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def insertInto(self):
            return self.getTypedRuleContext(SqlBaseParser.InsertIntoContext,0)


        def fromStatementBody(self):
            return self.getTypedRuleContext(SqlBaseParser.FromStatementBodyContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_multiInsertQueryBody

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterMultiInsertQueryBody" ):
                listener.enterMultiInsertQueryBody(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitMultiInsertQueryBody" ):
                listener.exitMultiInsertQueryBody(self)




    def multiInsertQueryBody(self):

        localctx = SqlBaseParser.MultiInsertQueryBodyContext(self, self._ctx, self.state)
        self.enterRule(localctx, 78, self.RULE_multiInsertQueryBody)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1582
            self.insertInto()
            self.state = 1583
            self.fromStatementBody()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class QueryTermContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser


        def getRuleIndex(self):
            return SqlBaseParser.RULE_queryTerm


        def copyFrom(self, ctx:ParserRuleContext):
            super().copyFrom(ctx)


    class QueryTermDefaultContext(QueryTermContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.QueryTermContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def queryPrimary(self):
            return self.getTypedRuleContext(SqlBaseParser.QueryPrimaryContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterQueryTermDefault" ):
                listener.enterQueryTermDefault(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitQueryTermDefault" ):
                listener.exitQueryTermDefault(self)


    class SetOperationContext(QueryTermContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.QueryTermContext
            super().__init__(parser)
            self.left = None # QueryTermContext
            self.operator = None # Token
            self.right = None # QueryTermContext
            self.copyFrom(ctx)

        def queryTerm(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.QueryTermContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.QueryTermContext,i)

        def INTERSECT(self):
            return self.getToken(SqlBaseParser.INTERSECT, 0)
        def UNION(self):
            return self.getToken(SqlBaseParser.UNION, 0)
        def EXCEPT(self):
            return self.getToken(SqlBaseParser.EXCEPT, 0)
        def SETMINUS(self):
            return self.getToken(SqlBaseParser.SETMINUS, 0)
        def setQuantifier(self):
            return self.getTypedRuleContext(SqlBaseParser.SetQuantifierContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSetOperation" ):
                listener.enterSetOperation(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSetOperation" ):
                listener.exitSetOperation(self)



    def queryTerm(self, _p:int=0):
        _parentctx = self._ctx
        _parentState = self.state
        localctx = SqlBaseParser.QueryTermContext(self, self._ctx, _parentState)
        _prevctx = localctx
        _startState = 80
        self.enterRecursionRule(localctx, 80, self.RULE_queryTerm, _p)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            localctx = SqlBaseParser.QueryTermDefaultContext(self, localctx)
            self._ctx = localctx
            _prevctx = localctx

            self.state = 1586
            self.queryPrimary()
            self._ctx.stop = self._input.LT(-1)
            self.state = 1611
            self._errHandler.sync(self)
            _alt = self._interp.adaptivePredict(self._input,183,self._ctx)
            while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                if _alt==1:
                    if self._parseListeners is not None:
                        self.triggerExitRuleEvent()
                    _prevctx = localctx
                    self.state = 1609
                    self._errHandler.sync(self)
                    la_ = self._interp.adaptivePredict(self._input,182,self._ctx)
                    if la_ == 1:
                        localctx = SqlBaseParser.SetOperationContext(self, SqlBaseParser.QueryTermContext(self, _parentctx, _parentState))
                        localctx.left = _prevctx
                        self.pushNewRecursionContext(localctx, _startState, self.RULE_queryTerm)
                        self.state = 1588
                        if not self.precpred(self._ctx, 3):
                            from antlr4.error.Errors import FailedPredicateException
                            raise FailedPredicateException(self, "self.precpred(self._ctx, 3)")
                        self.state = 1589
                        if not self.legacy_setops_precedence_enbled:
                            from antlr4.error.Errors import FailedPredicateException
                            raise FailedPredicateException(self, "self.legacy_setops_precedence_enbled")
                        self.state = 1590
                        localctx.operator = self._input.LT(1)
                        _la = self._input.LA(1)
                        if not(_la==SqlBaseParser.EXCEPT or _la==SqlBaseParser.INTERSECT or _la==SqlBaseParser.SETMINUS or _la==SqlBaseParser.UNION):
                            localctx.operator = self._errHandler.recoverInline(self)
                        else:
                            self._errHandler.reportMatch(self)
                            self.consume()
                        self.state = 1592
                        self._errHandler.sync(self)
                        _la = self._input.LA(1)
                        if _la==SqlBaseParser.ALL or _la==SqlBaseParser.DISTINCT:
                            self.state = 1591
                            self.setQuantifier()


                        self.state = 1594
                        localctx.right = self.queryTerm(4)
                        pass

                    elif la_ == 2:
                        localctx = SqlBaseParser.SetOperationContext(self, SqlBaseParser.QueryTermContext(self, _parentctx, _parentState))
                        localctx.left = _prevctx
                        self.pushNewRecursionContext(localctx, _startState, self.RULE_queryTerm)
                        self.state = 1595
                        if not self.precpred(self._ctx, 2):
                            from antlr4.error.Errors import FailedPredicateException
                            raise FailedPredicateException(self, "self.precpred(self._ctx, 2)")
                        self.state = 1596
                        if not not self.legacy_setops_precedence_enbled:
                            from antlr4.error.Errors import FailedPredicateException
                            raise FailedPredicateException(self, "not self.legacy_setops_precedence_enbled")
                        self.state = 1597
                        localctx.operator = self.match(SqlBaseParser.INTERSECT)
                        self.state = 1599
                        self._errHandler.sync(self)
                        _la = self._input.LA(1)
                        if _la==SqlBaseParser.ALL or _la==SqlBaseParser.DISTINCT:
                            self.state = 1598
                            self.setQuantifier()


                        self.state = 1601
                        localctx.right = self.queryTerm(3)
                        pass

                    elif la_ == 3:
                        localctx = SqlBaseParser.SetOperationContext(self, SqlBaseParser.QueryTermContext(self, _parentctx, _parentState))
                        localctx.left = _prevctx
                        self.pushNewRecursionContext(localctx, _startState, self.RULE_queryTerm)
                        self.state = 1602
                        if not self.precpred(self._ctx, 1):
                            from antlr4.error.Errors import FailedPredicateException
                            raise FailedPredicateException(self, "self.precpred(self._ctx, 1)")
                        self.state = 1603
                        if not not self.legacy_setops_precedence_enbled:
                            from antlr4.error.Errors import FailedPredicateException
                            raise FailedPredicateException(self, "not self.legacy_setops_precedence_enbled")
                        self.state = 1604
                        localctx.operator = self._input.LT(1)
                        _la = self._input.LA(1)
                        if not(_la==SqlBaseParser.EXCEPT or _la==SqlBaseParser.SETMINUS or _la==SqlBaseParser.UNION):
                            localctx.operator = self._errHandler.recoverInline(self)
                        else:
                            self._errHandler.reportMatch(self)
                            self.consume()
                        self.state = 1606
                        self._errHandler.sync(self)
                        _la = self._input.LA(1)
                        if _la==SqlBaseParser.ALL or _la==SqlBaseParser.DISTINCT:
                            self.state = 1605
                            self.setQuantifier()


                        self.state = 1608
                        localctx.right = self.queryTerm(2)
                        pass


                self.state = 1613
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,183,self._ctx)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.unrollRecursionContexts(_parentctx)
        return localctx

    class QueryPrimaryContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser


        def getRuleIndex(self):
            return SqlBaseParser.RULE_queryPrimary


        def copyFrom(self, ctx:ParserRuleContext):
            super().copyFrom(ctx)



    class SubqueryContext(QueryPrimaryContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.QueryPrimaryContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def query(self):
            return self.getTypedRuleContext(SqlBaseParser.QueryContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSubquery" ):
                listener.enterSubquery(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSubquery" ):
                listener.exitSubquery(self)


    class QueryPrimaryDefaultContext(QueryPrimaryContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.QueryPrimaryContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def querySpecification(self):
            return self.getTypedRuleContext(SqlBaseParser.QuerySpecificationContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterQueryPrimaryDefault" ):
                listener.enterQueryPrimaryDefault(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitQueryPrimaryDefault" ):
                listener.exitQueryPrimaryDefault(self)


    class InlineTableDefault1Context(QueryPrimaryContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.QueryPrimaryContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def inlineTable(self):
            return self.getTypedRuleContext(SqlBaseParser.InlineTableContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterInlineTableDefault1" ):
                listener.enterInlineTableDefault1(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitInlineTableDefault1" ):
                listener.exitInlineTableDefault1(self)


    class FromStmtContext(QueryPrimaryContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.QueryPrimaryContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def fromStatement(self):
            return self.getTypedRuleContext(SqlBaseParser.FromStatementContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterFromStmt" ):
                listener.enterFromStmt(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitFromStmt" ):
                listener.exitFromStmt(self)


    class TableContext(QueryPrimaryContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.QueryPrimaryContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)
        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterTable" ):
                listener.enterTable(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitTable" ):
                listener.exitTable(self)



    def queryPrimary(self):

        localctx = SqlBaseParser.QueryPrimaryContext(self, self._ctx, self.state)
        self.enterRule(localctx, 82, self.RULE_queryPrimary)
        try:
            self.state = 1623
            self._errHandler.sync(self)
            token = self._input.LA(1)
            if token in [SqlBaseParser.MAP, SqlBaseParser.REDUCE, SqlBaseParser.SELECT]:
                localctx = SqlBaseParser.QueryPrimaryDefaultContext(self, localctx)
                self.enterOuterAlt(localctx, 1)
                self.state = 1614
                self.querySpecification()
                pass
            elif token in [SqlBaseParser.FROM]:
                localctx = SqlBaseParser.FromStmtContext(self, localctx)
                self.enterOuterAlt(localctx, 2)
                self.state = 1615
                self.fromStatement()
                pass
            elif token in [SqlBaseParser.TABLE]:
                localctx = SqlBaseParser.TableContext(self, localctx)
                self.enterOuterAlt(localctx, 3)
                self.state = 1616
                self.match(SqlBaseParser.TABLE)
                self.state = 1617
                self.multipartIdentifier()
                pass
            elif token in [SqlBaseParser.VALUES]:
                localctx = SqlBaseParser.InlineTableDefault1Context(self, localctx)
                self.enterOuterAlt(localctx, 4)
                self.state = 1618
                self.inlineTable()
                pass
            elif token in [SqlBaseParser.T__1]:
                localctx = SqlBaseParser.SubqueryContext(self, localctx)
                self.enterOuterAlt(localctx, 5)
                self.state = 1619
                self.match(SqlBaseParser.T__1)
                self.state = 1620
                self.query()
                self.state = 1621
                self.match(SqlBaseParser.T__2)
                pass
            else:
                raise NoViableAltException(self)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class SortItemContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.ordering = None # Token
            self.nullOrder = None # Token

        def expression(self):
            return self.getTypedRuleContext(SqlBaseParser.ExpressionContext,0)


        def NULLS(self):
            return self.getToken(SqlBaseParser.NULLS, 0)

        def ASC(self):
            return self.getToken(SqlBaseParser.ASC, 0)

        def DESC(self):
            return self.getToken(SqlBaseParser.DESC, 0)

        def LAST(self):
            return self.getToken(SqlBaseParser.LAST, 0)

        def FIRST(self):
            return self.getToken(SqlBaseParser.FIRST, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_sortItem

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSortItem" ):
                listener.enterSortItem(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSortItem" ):
                listener.exitSortItem(self)




    def sortItem(self):

        localctx = SqlBaseParser.SortItemContext(self, self._ctx, self.state)
        self.enterRule(localctx, 84, self.RULE_sortItem)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1625
            self.expression()
            self.state = 1627
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,185,self._ctx)
            if la_ == 1:
                self.state = 1626
                localctx.ordering = self._input.LT(1)
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.ASC or _la==SqlBaseParser.DESC):
                    localctx.ordering = self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()


            self.state = 1631
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,186,self._ctx)
            if la_ == 1:
                self.state = 1629
                self.match(SqlBaseParser.NULLS)
                self.state = 1630
                localctx.nullOrder = self._input.LT(1)
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.FIRST or _la==SqlBaseParser.LAST):
                    localctx.nullOrder = self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class FromStatementContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def fromClause(self):
            return self.getTypedRuleContext(SqlBaseParser.FromClauseContext,0)


        def fromStatementBody(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.FromStatementBodyContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.FromStatementBodyContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_fromStatement

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterFromStatement" ):
                listener.enterFromStatement(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitFromStatement" ):
                listener.exitFromStatement(self)




    def fromStatement(self):

        localctx = SqlBaseParser.FromStatementContext(self, self._ctx, self.state)
        self.enterRule(localctx, 86, self.RULE_fromStatement)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1633
            self.fromClause()
            self.state = 1635
            self._errHandler.sync(self)
            _alt = 1
            while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                if _alt == 1:
                    self.state = 1634
                    self.fromStatementBody()

                else:
                    raise NoViableAltException(self)
                self.state = 1637
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,187,self._ctx)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class FromStatementBodyContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def transformClause(self):
            return self.getTypedRuleContext(SqlBaseParser.TransformClauseContext,0)


        def queryOrganization(self):
            return self.getTypedRuleContext(SqlBaseParser.QueryOrganizationContext,0)


        def whereClause(self):
            return self.getTypedRuleContext(SqlBaseParser.WhereClauseContext,0)


        def selectClause(self):
            return self.getTypedRuleContext(SqlBaseParser.SelectClauseContext,0)


        def lateralView(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.LateralViewContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.LateralViewContext,i)


        def aggregationClause(self):
            return self.getTypedRuleContext(SqlBaseParser.AggregationClauseContext,0)


        def havingClause(self):
            return self.getTypedRuleContext(SqlBaseParser.HavingClauseContext,0)


        def windowClause(self):
            return self.getTypedRuleContext(SqlBaseParser.WindowClauseContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_fromStatementBody

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterFromStatementBody" ):
                listener.enterFromStatementBody(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitFromStatementBody" ):
                listener.exitFromStatementBody(self)




    def fromStatementBody(self):

        localctx = SqlBaseParser.FromStatementBodyContext(self, self._ctx, self.state)
        self.enterRule(localctx, 88, self.RULE_fromStatementBody)
        try:
            self.state = 1666
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,194,self._ctx)
            if la_ == 1:
                self.enterOuterAlt(localctx, 1)
                self.state = 1639
                self.transformClause()
                self.state = 1641
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,188,self._ctx)
                if la_ == 1:
                    self.state = 1640
                    self.whereClause()


                self.state = 1643
                self.queryOrganization()
                pass

            elif la_ == 2:
                self.enterOuterAlt(localctx, 2)
                self.state = 1645
                self.selectClause()
                self.state = 1649
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,189,self._ctx)
                while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                    if _alt==1:
                        self.state = 1646
                        self.lateralView()
                    self.state = 1651
                    self._errHandler.sync(self)
                    _alt = self._interp.adaptivePredict(self._input,189,self._ctx)

                self.state = 1653
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,190,self._ctx)
                if la_ == 1:
                    self.state = 1652
                    self.whereClause()


                self.state = 1656
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,191,self._ctx)
                if la_ == 1:
                    self.state = 1655
                    self.aggregationClause()


                self.state = 1659
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,192,self._ctx)
                if la_ == 1:
                    self.state = 1658
                    self.havingClause()


                self.state = 1662
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,193,self._ctx)
                if la_ == 1:
                    self.state = 1661
                    self.windowClause()


                self.state = 1664
                self.queryOrganization()
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class QuerySpecificationContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser


        def getRuleIndex(self):
            return SqlBaseParser.RULE_querySpecification


        def copyFrom(self, ctx:ParserRuleContext):
            super().copyFrom(ctx)



    class RegularQuerySpecificationContext(QuerySpecificationContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.QuerySpecificationContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def selectClause(self):
            return self.getTypedRuleContext(SqlBaseParser.SelectClauseContext,0)

        def fromClause(self):
            return self.getTypedRuleContext(SqlBaseParser.FromClauseContext,0)

        def lateralView(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.LateralViewContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.LateralViewContext,i)

        def whereClause(self):
            return self.getTypedRuleContext(SqlBaseParser.WhereClauseContext,0)

        def aggregationClause(self):
            return self.getTypedRuleContext(SqlBaseParser.AggregationClauseContext,0)

        def havingClause(self):
            return self.getTypedRuleContext(SqlBaseParser.HavingClauseContext,0)

        def windowClause(self):
            return self.getTypedRuleContext(SqlBaseParser.WindowClauseContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterRegularQuerySpecification" ):
                listener.enterRegularQuerySpecification(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitRegularQuerySpecification" ):
                listener.exitRegularQuerySpecification(self)


    class TransformQuerySpecificationContext(QuerySpecificationContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.QuerySpecificationContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def transformClause(self):
            return self.getTypedRuleContext(SqlBaseParser.TransformClauseContext,0)

        def fromClause(self):
            return self.getTypedRuleContext(SqlBaseParser.FromClauseContext,0)

        def whereClause(self):
            return self.getTypedRuleContext(SqlBaseParser.WhereClauseContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterTransformQuerySpecification" ):
                listener.enterTransformQuerySpecification(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitTransformQuerySpecification" ):
                listener.exitTransformQuerySpecification(self)



    def querySpecification(self):

        localctx = SqlBaseParser.QuerySpecificationContext(self, self._ctx, self.state)
        self.enterRule(localctx, 90, self.RULE_querySpecification)
        try:
            self.state = 1697
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,203,self._ctx)
            if la_ == 1:
                localctx = SqlBaseParser.TransformQuerySpecificationContext(self, localctx)
                self.enterOuterAlt(localctx, 1)
                self.state = 1668
                self.transformClause()
                self.state = 1670
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,195,self._ctx)
                if la_ == 1:
                    self.state = 1669
                    self.fromClause()


                self.state = 1673
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,196,self._ctx)
                if la_ == 1:
                    self.state = 1672
                    self.whereClause()


                pass

            elif la_ == 2:
                localctx = SqlBaseParser.RegularQuerySpecificationContext(self, localctx)
                self.enterOuterAlt(localctx, 2)
                self.state = 1675
                self.selectClause()
                self.state = 1677
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,197,self._ctx)
                if la_ == 1:
                    self.state = 1676
                    self.fromClause()


                self.state = 1682
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,198,self._ctx)
                while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                    if _alt==1:
                        self.state = 1679
                        self.lateralView()
                    self.state = 1684
                    self._errHandler.sync(self)
                    _alt = self._interp.adaptivePredict(self._input,198,self._ctx)

                self.state = 1686
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,199,self._ctx)
                if la_ == 1:
                    self.state = 1685
                    self.whereClause()


                self.state = 1689
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,200,self._ctx)
                if la_ == 1:
                    self.state = 1688
                    self.aggregationClause()


                self.state = 1692
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,201,self._ctx)
                if la_ == 1:
                    self.state = 1691
                    self.havingClause()


                self.state = 1695
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,202,self._ctx)
                if la_ == 1:
                    self.state = 1694
                    self.windowClause()


                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class TransformClauseContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.kind = None # Token
            self.inRowFormat = None # RowFormatContext
            self.recordWriter = None # Token
            self.script = None # Token
            self.outRowFormat = None # RowFormatContext
            self.recordReader = None # Token

        def USING(self):
            return self.getToken(SqlBaseParser.USING, 0)

        def STRING(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.STRING)
            else:
                return self.getToken(SqlBaseParser.STRING, i)

        def SELECT(self):
            return self.getToken(SqlBaseParser.SELECT, 0)

        def namedExpressionSeq(self):
            return self.getTypedRuleContext(SqlBaseParser.NamedExpressionSeqContext,0)


        def TRANSFORM(self):
            return self.getToken(SqlBaseParser.TRANSFORM, 0)

        def MAP(self):
            return self.getToken(SqlBaseParser.MAP, 0)

        def REDUCE(self):
            return self.getToken(SqlBaseParser.REDUCE, 0)

        def RECORDWRITER(self):
            return self.getToken(SqlBaseParser.RECORDWRITER, 0)

        def AS(self):
            return self.getToken(SqlBaseParser.AS, 0)

        def RECORDREADER(self):
            return self.getToken(SqlBaseParser.RECORDREADER, 0)

        def rowFormat(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.RowFormatContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.RowFormatContext,i)


        def identifierSeq(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierSeqContext,0)


        def colTypeList(self):
            return self.getTypedRuleContext(SqlBaseParser.ColTypeListContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_transformClause

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterTransformClause" ):
                listener.enterTransformClause(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitTransformClause" ):
                listener.exitTransformClause(self)




    def transformClause(self):

        localctx = SqlBaseParser.TransformClauseContext(self, self._ctx, self.state)
        self.enterRule(localctx, 92, self.RULE_transformClause)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1709
            self._errHandler.sync(self)
            token = self._input.LA(1)
            if token in [SqlBaseParser.SELECT]:
                self.state = 1699
                self.match(SqlBaseParser.SELECT)
                self.state = 1700
                localctx.kind = self.match(SqlBaseParser.TRANSFORM)
                self.state = 1701
                self.match(SqlBaseParser.T__1)
                self.state = 1702
                self.namedExpressionSeq()
                self.state = 1703
                self.match(SqlBaseParser.T__2)
                pass
            elif token in [SqlBaseParser.MAP]:
                self.state = 1705
                localctx.kind = self.match(SqlBaseParser.MAP)
                self.state = 1706
                self.namedExpressionSeq()
                pass
            elif token in [SqlBaseParser.REDUCE]:
                self.state = 1707
                localctx.kind = self.match(SqlBaseParser.REDUCE)
                self.state = 1708
                self.namedExpressionSeq()
                pass
            else:
                raise NoViableAltException(self)

            self.state = 1712
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==SqlBaseParser.ROW:
                self.state = 1711
                localctx.inRowFormat = self.rowFormat()


            self.state = 1716
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==SqlBaseParser.RECORDWRITER:
                self.state = 1714
                self.match(SqlBaseParser.RECORDWRITER)
                self.state = 1715
                localctx.recordWriter = self.match(SqlBaseParser.STRING)


            self.state = 1718
            self.match(SqlBaseParser.USING)
            self.state = 1719
            localctx.script = self.match(SqlBaseParser.STRING)
            self.state = 1732
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,209,self._ctx)
            if la_ == 1:
                self.state = 1720
                self.match(SqlBaseParser.AS)
                self.state = 1730
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,208,self._ctx)
                if la_ == 1:
                    self.state = 1721
                    self.identifierSeq()
                    pass

                elif la_ == 2:
                    self.state = 1722
                    self.colTypeList()
                    pass

                elif la_ == 3:
                    self.state = 1723
                    self.match(SqlBaseParser.T__1)
                    self.state = 1726
                    self._errHandler.sync(self)
                    la_ = self._interp.adaptivePredict(self._input,207,self._ctx)
                    if la_ == 1:
                        self.state = 1724
                        self.identifierSeq()
                        pass

                    elif la_ == 2:
                        self.state = 1725
                        self.colTypeList()
                        pass


                    self.state = 1728
                    self.match(SqlBaseParser.T__2)
                    pass




            self.state = 1735
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,210,self._ctx)
            if la_ == 1:
                self.state = 1734
                localctx.outRowFormat = self.rowFormat()


            self.state = 1739
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,211,self._ctx)
            if la_ == 1:
                self.state = 1737
                self.match(SqlBaseParser.RECORDREADER)
                self.state = 1738
                localctx.recordReader = self.match(SqlBaseParser.STRING)


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class SelectClauseContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self._hint = None # HintContext
            self.hints = list() # of HintContexts

        def SELECT(self):
            return self.getToken(SqlBaseParser.SELECT, 0)

        def namedExpressionSeq(self):
            return self.getTypedRuleContext(SqlBaseParser.NamedExpressionSeqContext,0)


        def setQuantifier(self):
            return self.getTypedRuleContext(SqlBaseParser.SetQuantifierContext,0)


        def hint(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.HintContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.HintContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_selectClause

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSelectClause" ):
                listener.enterSelectClause(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSelectClause" ):
                listener.exitSelectClause(self)




    def selectClause(self):

        localctx = SqlBaseParser.SelectClauseContext(self, self._ctx, self.state)
        self.enterRule(localctx, 94, self.RULE_selectClause)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1741
            self.match(SqlBaseParser.SELECT)
            self.state = 1745
            self._errHandler.sync(self)
            _alt = self._interp.adaptivePredict(self._input,212,self._ctx)
            while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                if _alt==1:
                    self.state = 1742
                    localctx._hint = self.hint()
                    localctx.hints.append(localctx._hint)
                self.state = 1747
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,212,self._ctx)

            self.state = 1749
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,213,self._ctx)
            if la_ == 1:
                self.state = 1748
                self.setQuantifier()


            self.state = 1751
            self.namedExpressionSeq()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class SetClauseContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def SET(self):
            return self.getToken(SqlBaseParser.SET, 0)

        def assignmentList(self):
            return self.getTypedRuleContext(SqlBaseParser.AssignmentListContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_setClause

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSetClause" ):
                listener.enterSetClause(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSetClause" ):
                listener.exitSetClause(self)




    def setClause(self):

        localctx = SqlBaseParser.SetClauseContext(self, self._ctx, self.state)
        self.enterRule(localctx, 96, self.RULE_setClause)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1753
            self.match(SqlBaseParser.SET)
            self.state = 1754
            self.assignmentList()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class MatchedClauseContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.matchedCond = None # BooleanExpressionContext

        def WHEN(self):
            return self.getToken(SqlBaseParser.WHEN, 0)

        def MATCHED(self):
            return self.getToken(SqlBaseParser.MATCHED, 0)

        def THEN(self):
            return self.getToken(SqlBaseParser.THEN, 0)

        def matchedAction(self):
            return self.getTypedRuleContext(SqlBaseParser.MatchedActionContext,0)


        def AND(self):
            return self.getToken(SqlBaseParser.AND, 0)

        def booleanExpression(self):
            return self.getTypedRuleContext(SqlBaseParser.BooleanExpressionContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_matchedClause

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterMatchedClause" ):
                listener.enterMatchedClause(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitMatchedClause" ):
                listener.exitMatchedClause(self)




    def matchedClause(self):

        localctx = SqlBaseParser.MatchedClauseContext(self, self._ctx, self.state)
        self.enterRule(localctx, 98, self.RULE_matchedClause)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1756
            self.match(SqlBaseParser.WHEN)
            self.state = 1757
            self.match(SqlBaseParser.MATCHED)
            self.state = 1760
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==SqlBaseParser.AND:
                self.state = 1758
                self.match(SqlBaseParser.AND)
                self.state = 1759
                localctx.matchedCond = self.booleanExpression(0)


            self.state = 1762
            self.match(SqlBaseParser.THEN)
            self.state = 1763
            self.matchedAction()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class NotMatchedClauseContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.notMatchedCond = None # BooleanExpressionContext

        def WHEN(self):
            return self.getToken(SqlBaseParser.WHEN, 0)

        def NOT(self):
            return self.getToken(SqlBaseParser.NOT, 0)

        def MATCHED(self):
            return self.getToken(SqlBaseParser.MATCHED, 0)

        def THEN(self):
            return self.getToken(SqlBaseParser.THEN, 0)

        def notMatchedAction(self):
            return self.getTypedRuleContext(SqlBaseParser.NotMatchedActionContext,0)


        def AND(self):
            return self.getToken(SqlBaseParser.AND, 0)

        def booleanExpression(self):
            return self.getTypedRuleContext(SqlBaseParser.BooleanExpressionContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_notMatchedClause

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterNotMatchedClause" ):
                listener.enterNotMatchedClause(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitNotMatchedClause" ):
                listener.exitNotMatchedClause(self)




    def notMatchedClause(self):

        localctx = SqlBaseParser.NotMatchedClauseContext(self, self._ctx, self.state)
        self.enterRule(localctx, 100, self.RULE_notMatchedClause)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1765
            self.match(SqlBaseParser.WHEN)
            self.state = 1766
            self.match(SqlBaseParser.NOT)
            self.state = 1767
            self.match(SqlBaseParser.MATCHED)
            self.state = 1770
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==SqlBaseParser.AND:
                self.state = 1768
                self.match(SqlBaseParser.AND)
                self.state = 1769
                localctx.notMatchedCond = self.booleanExpression(0)


            self.state = 1772
            self.match(SqlBaseParser.THEN)
            self.state = 1773
            self.notMatchedAction()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class MatchedActionContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def DELETE(self):
            return self.getToken(SqlBaseParser.DELETE, 0)

        def UPDATE(self):
            return self.getToken(SqlBaseParser.UPDATE, 0)

        def SET(self):
            return self.getToken(SqlBaseParser.SET, 0)

        def ASTERISK(self):
            return self.getToken(SqlBaseParser.ASTERISK, 0)

        def assignmentList(self):
            return self.getTypedRuleContext(SqlBaseParser.AssignmentListContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_matchedAction

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterMatchedAction" ):
                listener.enterMatchedAction(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitMatchedAction" ):
                listener.exitMatchedAction(self)




    def matchedAction(self):

        localctx = SqlBaseParser.MatchedActionContext(self, self._ctx, self.state)
        self.enterRule(localctx, 102, self.RULE_matchedAction)
        try:
            self.state = 1782
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,216,self._ctx)
            if la_ == 1:
                self.enterOuterAlt(localctx, 1)
                self.state = 1775
                self.match(SqlBaseParser.DELETE)
                pass

            elif la_ == 2:
                self.enterOuterAlt(localctx, 2)
                self.state = 1776
                self.match(SqlBaseParser.UPDATE)
                self.state = 1777
                self.match(SqlBaseParser.SET)
                self.state = 1778
                self.match(SqlBaseParser.ASTERISK)
                pass

            elif la_ == 3:
                self.enterOuterAlt(localctx, 3)
                self.state = 1779
                self.match(SqlBaseParser.UPDATE)
                self.state = 1780
                self.match(SqlBaseParser.SET)
                self.state = 1781
                self.assignmentList()
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class NotMatchedActionContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.columns = None # MultipartIdentifierListContext

        def INSERT(self):
            return self.getToken(SqlBaseParser.INSERT, 0)

        def ASTERISK(self):
            return self.getToken(SqlBaseParser.ASTERISK, 0)

        def VALUES(self):
            return self.getToken(SqlBaseParser.VALUES, 0)

        def expression(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ExpressionContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ExpressionContext,i)


        def multipartIdentifierList(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierListContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_notMatchedAction

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterNotMatchedAction" ):
                listener.enterNotMatchedAction(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitNotMatchedAction" ):
                listener.exitNotMatchedAction(self)




    def notMatchedAction(self):

        localctx = SqlBaseParser.NotMatchedActionContext(self, self._ctx, self.state)
        self.enterRule(localctx, 104, self.RULE_notMatchedAction)
        self._la = 0 # Token type
        try:
            self.state = 1802
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,218,self._ctx)
            if la_ == 1:
                self.enterOuterAlt(localctx, 1)
                self.state = 1784
                self.match(SqlBaseParser.INSERT)
                self.state = 1785
                self.match(SqlBaseParser.ASTERISK)
                pass

            elif la_ == 2:
                self.enterOuterAlt(localctx, 2)
                self.state = 1786
                self.match(SqlBaseParser.INSERT)
                self.state = 1787
                self.match(SqlBaseParser.T__1)
                self.state = 1788
                localctx.columns = self.multipartIdentifierList()
                self.state = 1789
                self.match(SqlBaseParser.T__2)
                self.state = 1790
                self.match(SqlBaseParser.VALUES)
                self.state = 1791
                self.match(SqlBaseParser.T__1)
                self.state = 1792
                self.expression()
                self.state = 1797
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while _la==SqlBaseParser.T__3:
                    self.state = 1793
                    self.match(SqlBaseParser.T__3)
                    self.state = 1794
                    self.expression()
                    self.state = 1799
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)

                self.state = 1800
                self.match(SqlBaseParser.T__2)
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class AssignmentListContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def assignment(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.AssignmentContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.AssignmentContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_assignmentList

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterAssignmentList" ):
                listener.enterAssignmentList(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitAssignmentList" ):
                listener.exitAssignmentList(self)




    def assignmentList(self):

        localctx = SqlBaseParser.AssignmentListContext(self, self._ctx, self.state)
        self.enterRule(localctx, 106, self.RULE_assignmentList)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1804
            self.assignment()
            self.state = 1809
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while _la==SqlBaseParser.T__3:
                self.state = 1805
                self.match(SqlBaseParser.T__3)
                self.state = 1806
                self.assignment()
                self.state = 1811
                self._errHandler.sync(self)
                _la = self._input.LA(1)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class AssignmentContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.key = None # MultipartIdentifierContext
            self.value = None # ExpressionContext

        def EQ(self):
            return self.getToken(SqlBaseParser.EQ, 0)

        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)


        def expression(self):
            return self.getTypedRuleContext(SqlBaseParser.ExpressionContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_assignment

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterAssignment" ):
                listener.enterAssignment(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitAssignment" ):
                listener.exitAssignment(self)




    def assignment(self):

        localctx = SqlBaseParser.AssignmentContext(self, self._ctx, self.state)
        self.enterRule(localctx, 108, self.RULE_assignment)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1812
            localctx.key = self.multipartIdentifier()
            self.state = 1813
            self.match(SqlBaseParser.EQ)
            self.state = 1814
            localctx.value = self.expression()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class WhereClauseContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def WHERE(self):
            return self.getToken(SqlBaseParser.WHERE, 0)

        def booleanExpression(self):
            return self.getTypedRuleContext(SqlBaseParser.BooleanExpressionContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_whereClause

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterWhereClause" ):
                listener.enterWhereClause(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitWhereClause" ):
                listener.exitWhereClause(self)




    def whereClause(self):

        localctx = SqlBaseParser.WhereClauseContext(self, self._ctx, self.state)
        self.enterRule(localctx, 110, self.RULE_whereClause)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1816
            self.match(SqlBaseParser.WHERE)
            self.state = 1817
            self.booleanExpression(0)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class HavingClauseContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def HAVING(self):
            return self.getToken(SqlBaseParser.HAVING, 0)

        def booleanExpression(self):
            return self.getTypedRuleContext(SqlBaseParser.BooleanExpressionContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_havingClause

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterHavingClause" ):
                listener.enterHavingClause(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitHavingClause" ):
                listener.exitHavingClause(self)




    def havingClause(self):

        localctx = SqlBaseParser.HavingClauseContext(self, self._ctx, self.state)
        self.enterRule(localctx, 112, self.RULE_havingClause)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1819
            self.match(SqlBaseParser.HAVING)
            self.state = 1820
            self.booleanExpression(0)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class HintContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self._hintStatement = None # HintStatementContext
            self.hintStatements = list() # of HintStatementContexts

        def hintStatement(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.HintStatementContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.HintStatementContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_hint

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterHint" ):
                listener.enterHint(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitHint" ):
                listener.exitHint(self)




    def hint(self):

        localctx = SqlBaseParser.HintContext(self, self._ctx, self.state)
        self.enterRule(localctx, 114, self.RULE_hint)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1822
            self.match(SqlBaseParser.T__5)
            self.state = 1823
            localctx._hintStatement = self.hintStatement()
            localctx.hintStatements.append(localctx._hintStatement)
            self.state = 1830
            self._errHandler.sync(self)
            _alt = self._interp.adaptivePredict(self._input,221,self._ctx)
            while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                if _alt==1:
                    self.state = 1825
                    self._errHandler.sync(self)
                    la_ = self._interp.adaptivePredict(self._input,220,self._ctx)
                    if la_ == 1:
                        self.state = 1824
                        self.match(SqlBaseParser.T__3)


                    self.state = 1827
                    localctx._hintStatement = self.hintStatement()
                    localctx.hintStatements.append(localctx._hintStatement)
                self.state = 1832
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,221,self._ctx)

            self.state = 1833
            self.match(SqlBaseParser.T__6)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class HintStatementContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.hintName = None # IdentifierContext
            self._primaryExpression = None # PrimaryExpressionContext
            self.parameters = list() # of PrimaryExpressionContexts

        def identifier(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierContext,0)


        def primaryExpression(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.PrimaryExpressionContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.PrimaryExpressionContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_hintStatement

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterHintStatement" ):
                listener.enterHintStatement(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitHintStatement" ):
                listener.exitHintStatement(self)




    def hintStatement(self):

        localctx = SqlBaseParser.HintStatementContext(self, self._ctx, self.state)
        self.enterRule(localctx, 116, self.RULE_hintStatement)
        self._la = 0 # Token type
        try:
            self.state = 1848
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,223,self._ctx)
            if la_ == 1:
                self.enterOuterAlt(localctx, 1)
                self.state = 1835
                localctx.hintName = self.identifier()
                pass

            elif la_ == 2:
                self.enterOuterAlt(localctx, 2)
                self.state = 1836
                localctx.hintName = self.identifier()
                self.state = 1837
                self.match(SqlBaseParser.T__1)
                self.state = 1838
                localctx._primaryExpression = self.primaryExpression(0)
                localctx.parameters.append(localctx._primaryExpression)
                self.state = 1843
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while _la==SqlBaseParser.T__3:
                    self.state = 1839
                    self.match(SqlBaseParser.T__3)
                    self.state = 1840
                    localctx._primaryExpression = self.primaryExpression(0)
                    localctx.parameters.append(localctx._primaryExpression)
                    self.state = 1845
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)

                self.state = 1846
                self.match(SqlBaseParser.T__2)
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class FromClauseContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def FROM(self):
            return self.getToken(SqlBaseParser.FROM, 0)

        def relation(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.RelationContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.RelationContext,i)


        def lateralView(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.LateralViewContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.LateralViewContext,i)


        def pivotClause(self):
            return self.getTypedRuleContext(SqlBaseParser.PivotClauseContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_fromClause

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterFromClause" ):
                listener.enterFromClause(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitFromClause" ):
                listener.exitFromClause(self)




    def fromClause(self):

        localctx = SqlBaseParser.FromClauseContext(self, self._ctx, self.state)
        self.enterRule(localctx, 118, self.RULE_fromClause)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1850
            self.match(SqlBaseParser.FROM)
            self.state = 1851
            self.relation()
            self.state = 1856
            self._errHandler.sync(self)
            _alt = self._interp.adaptivePredict(self._input,224,self._ctx)
            while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                if _alt==1:
                    self.state = 1852
                    self.match(SqlBaseParser.T__3)
                    self.state = 1853
                    self.relation()
                self.state = 1858
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,224,self._ctx)

            self.state = 1862
            self._errHandler.sync(self)
            _alt = self._interp.adaptivePredict(self._input,225,self._ctx)
            while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                if _alt==1:
                    self.state = 1859
                    self.lateralView()
                self.state = 1864
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,225,self._ctx)

            self.state = 1866
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,226,self._ctx)
            if la_ == 1:
                self.state = 1865
                self.pivotClause()


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class AggregationClauseContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self._expression = None # ExpressionContext
            self.groupingExpressions = list() # of ExpressionContexts
            self.kind = None # Token

        def GROUP(self):
            return self.getToken(SqlBaseParser.GROUP, 0)

        def BY(self):
            return self.getToken(SqlBaseParser.BY, 0)

        def expression(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ExpressionContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ExpressionContext,i)


        def WITH(self):
            return self.getToken(SqlBaseParser.WITH, 0)

        def SETS(self):
            return self.getToken(SqlBaseParser.SETS, 0)

        def groupingSet(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.GroupingSetContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.GroupingSetContext,i)


        def ROLLUP(self):
            return self.getToken(SqlBaseParser.ROLLUP, 0)

        def CUBE(self):
            return self.getToken(SqlBaseParser.CUBE, 0)

        def GROUPING(self):
            return self.getToken(SqlBaseParser.GROUPING, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_aggregationClause

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterAggregationClause" ):
                listener.enterAggregationClause(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitAggregationClause" ):
                listener.exitAggregationClause(self)




    def aggregationClause(self):

        localctx = SqlBaseParser.AggregationClauseContext(self, self._ctx, self.state)
        self.enterRule(localctx, 120, self.RULE_aggregationClause)
        self._la = 0 # Token type
        try:
            self.state = 1912
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,231,self._ctx)
            if la_ == 1:
                self.enterOuterAlt(localctx, 1)
                self.state = 1868
                self.match(SqlBaseParser.GROUP)
                self.state = 1869
                self.match(SqlBaseParser.BY)
                self.state = 1870
                localctx._expression = self.expression()
                localctx.groupingExpressions.append(localctx._expression)
                self.state = 1875
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,227,self._ctx)
                while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                    if _alt==1:
                        self.state = 1871
                        self.match(SqlBaseParser.T__3)
                        self.state = 1872
                        localctx._expression = self.expression()
                        localctx.groupingExpressions.append(localctx._expression)
                    self.state = 1877
                    self._errHandler.sync(self)
                    _alt = self._interp.adaptivePredict(self._input,227,self._ctx)

                self.state = 1895
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,229,self._ctx)
                if la_ == 1:
                    self.state = 1878
                    self.match(SqlBaseParser.WITH)
                    self.state = 1879
                    localctx.kind = self.match(SqlBaseParser.ROLLUP)

                elif la_ == 2:
                    self.state = 1880
                    self.match(SqlBaseParser.WITH)
                    self.state = 1881
                    localctx.kind = self.match(SqlBaseParser.CUBE)

                elif la_ == 3:
                    self.state = 1882
                    localctx.kind = self.match(SqlBaseParser.GROUPING)
                    self.state = 1883
                    self.match(SqlBaseParser.SETS)
                    self.state = 1884
                    self.match(SqlBaseParser.T__1)
                    self.state = 1885
                    self.groupingSet()
                    self.state = 1890
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    while _la==SqlBaseParser.T__3:
                        self.state = 1886
                        self.match(SqlBaseParser.T__3)
                        self.state = 1887
                        self.groupingSet()
                        self.state = 1892
                        self._errHandler.sync(self)
                        _la = self._input.LA(1)

                    self.state = 1893
                    self.match(SqlBaseParser.T__2)


                pass

            elif la_ == 2:
                self.enterOuterAlt(localctx, 2)
                self.state = 1897
                self.match(SqlBaseParser.GROUP)
                self.state = 1898
                self.match(SqlBaseParser.BY)
                self.state = 1899
                localctx.kind = self.match(SqlBaseParser.GROUPING)
                self.state = 1900
                self.match(SqlBaseParser.SETS)
                self.state = 1901
                self.match(SqlBaseParser.T__1)
                self.state = 1902
                self.groupingSet()
                self.state = 1907
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while _la==SqlBaseParser.T__3:
                    self.state = 1903
                    self.match(SqlBaseParser.T__3)
                    self.state = 1904
                    self.groupingSet()
                    self.state = 1909
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)

                self.state = 1910
                self.match(SqlBaseParser.T__2)
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class GroupingSetContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def expression(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ExpressionContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ExpressionContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_groupingSet

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterGroupingSet" ):
                listener.enterGroupingSet(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitGroupingSet" ):
                listener.exitGroupingSet(self)




    def groupingSet(self):

        localctx = SqlBaseParser.GroupingSetContext(self, self._ctx, self.state)
        self.enterRule(localctx, 122, self.RULE_groupingSet)
        self._la = 0 # Token type
        try:
            self.state = 1927
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,234,self._ctx)
            if la_ == 1:
                self.enterOuterAlt(localctx, 1)
                self.state = 1914
                self.match(SqlBaseParser.T__1)
                self.state = 1923
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,233,self._ctx)
                if la_ == 1:
                    self.state = 1915
                    self.expression()
                    self.state = 1920
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    while _la==SqlBaseParser.T__3:
                        self.state = 1916
                        self.match(SqlBaseParser.T__3)
                        self.state = 1917
                        self.expression()
                        self.state = 1922
                        self._errHandler.sync(self)
                        _la = self._input.LA(1)



                self.state = 1925
                self.match(SqlBaseParser.T__2)
                pass

            elif la_ == 2:
                self.enterOuterAlt(localctx, 2)
                self.state = 1926
                self.expression()
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class PivotClauseContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.aggregates = None # NamedExpressionSeqContext
            self._pivotValue = None # PivotValueContext
            self.pivotValues = list() # of PivotValueContexts

        def PIVOT(self):
            return self.getToken(SqlBaseParser.PIVOT, 0)

        def FOR(self):
            return self.getToken(SqlBaseParser.FOR, 0)

        def pivotColumn(self):
            return self.getTypedRuleContext(SqlBaseParser.PivotColumnContext,0)


        def IN(self):
            return self.getToken(SqlBaseParser.IN, 0)

        def namedExpressionSeq(self):
            return self.getTypedRuleContext(SqlBaseParser.NamedExpressionSeqContext,0)


        def pivotValue(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.PivotValueContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.PivotValueContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_pivotClause

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterPivotClause" ):
                listener.enterPivotClause(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitPivotClause" ):
                listener.exitPivotClause(self)




    def pivotClause(self):

        localctx = SqlBaseParser.PivotClauseContext(self, self._ctx, self.state)
        self.enterRule(localctx, 124, self.RULE_pivotClause)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1929
            self.match(SqlBaseParser.PIVOT)
            self.state = 1930
            self.match(SqlBaseParser.T__1)
            self.state = 1931
            localctx.aggregates = self.namedExpressionSeq()
            self.state = 1932
            self.match(SqlBaseParser.FOR)
            self.state = 1933
            self.pivotColumn()
            self.state = 1934
            self.match(SqlBaseParser.IN)
            self.state = 1935
            self.match(SqlBaseParser.T__1)
            self.state = 1936
            localctx._pivotValue = self.pivotValue()
            localctx.pivotValues.append(localctx._pivotValue)
            self.state = 1941
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while _la==SqlBaseParser.T__3:
                self.state = 1937
                self.match(SqlBaseParser.T__3)
                self.state = 1938
                localctx._pivotValue = self.pivotValue()
                localctx.pivotValues.append(localctx._pivotValue)
                self.state = 1943
                self._errHandler.sync(self)
                _la = self._input.LA(1)

            self.state = 1944
            self.match(SqlBaseParser.T__2)
            self.state = 1945
            self.match(SqlBaseParser.T__2)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class PivotColumnContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self._identifier = None # IdentifierContext
            self.identifiers = list() # of IdentifierContexts

        def identifier(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.IdentifierContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.IdentifierContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_pivotColumn

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterPivotColumn" ):
                listener.enterPivotColumn(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitPivotColumn" ):
                listener.exitPivotColumn(self)




    def pivotColumn(self):

        localctx = SqlBaseParser.PivotColumnContext(self, self._ctx, self.state)
        self.enterRule(localctx, 126, self.RULE_pivotColumn)
        self._la = 0 # Token type
        try:
            self.state = 1959
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,237,self._ctx)
            if la_ == 1:
                self.enterOuterAlt(localctx, 1)
                self.state = 1947
                localctx._identifier = self.identifier()
                localctx.identifiers.append(localctx._identifier)
                pass

            elif la_ == 2:
                self.enterOuterAlt(localctx, 2)
                self.state = 1948
                self.match(SqlBaseParser.T__1)
                self.state = 1949
                localctx._identifier = self.identifier()
                localctx.identifiers.append(localctx._identifier)
                self.state = 1954
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while _la==SqlBaseParser.T__3:
                    self.state = 1950
                    self.match(SqlBaseParser.T__3)
                    self.state = 1951
                    localctx._identifier = self.identifier()
                    localctx.identifiers.append(localctx._identifier)
                    self.state = 1956
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)

                self.state = 1957
                self.match(SqlBaseParser.T__2)
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class PivotValueContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def expression(self):
            return self.getTypedRuleContext(SqlBaseParser.ExpressionContext,0)


        def identifier(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierContext,0)


        def AS(self):
            return self.getToken(SqlBaseParser.AS, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_pivotValue

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterPivotValue" ):
                listener.enterPivotValue(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitPivotValue" ):
                listener.exitPivotValue(self)




    def pivotValue(self):

        localctx = SqlBaseParser.PivotValueContext(self, self._ctx, self.state)
        self.enterRule(localctx, 128, self.RULE_pivotValue)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1961
            self.expression()
            self.state = 1966
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,239,self._ctx)
            if la_ == 1:
                self.state = 1963
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,238,self._ctx)
                if la_ == 1:
                    self.state = 1962
                    self.match(SqlBaseParser.AS)


                self.state = 1965
                self.identifier()


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class LateralViewContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.tblName = None # IdentifierContext
            self._identifier = None # IdentifierContext
            self.colName = list() # of IdentifierContexts

        def LATERAL(self):
            return self.getToken(SqlBaseParser.LATERAL, 0)

        def VIEW(self):
            return self.getToken(SqlBaseParser.VIEW, 0)

        def qualifiedName(self):
            return self.getTypedRuleContext(SqlBaseParser.QualifiedNameContext,0)


        def identifier(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.IdentifierContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.IdentifierContext,i)


        def OUTER(self):
            return self.getToken(SqlBaseParser.OUTER, 0)

        def expression(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ExpressionContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ExpressionContext,i)


        def AS(self):
            return self.getToken(SqlBaseParser.AS, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_lateralView

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterLateralView" ):
                listener.enterLateralView(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitLateralView" ):
                listener.exitLateralView(self)




    def lateralView(self):

        localctx = SqlBaseParser.LateralViewContext(self, self._ctx, self.state)
        self.enterRule(localctx, 130, self.RULE_lateralView)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 1968
            self.match(SqlBaseParser.LATERAL)
            self.state = 1969
            self.match(SqlBaseParser.VIEW)
            self.state = 1971
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,240,self._ctx)
            if la_ == 1:
                self.state = 1970
                self.match(SqlBaseParser.OUTER)


            self.state = 1973
            self.qualifiedName()
            self.state = 1974
            self.match(SqlBaseParser.T__1)
            self.state = 1983
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,242,self._ctx)
            if la_ == 1:
                self.state = 1975
                self.expression()
                self.state = 1980
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while _la==SqlBaseParser.T__3:
                    self.state = 1976
                    self.match(SqlBaseParser.T__3)
                    self.state = 1977
                    self.expression()
                    self.state = 1982
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)



            self.state = 1985
            self.match(SqlBaseParser.T__2)
            self.state = 1986
            localctx.tblName = self.identifier()
            self.state = 1998
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,245,self._ctx)
            if la_ == 1:
                self.state = 1988
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,243,self._ctx)
                if la_ == 1:
                    self.state = 1987
                    self.match(SqlBaseParser.AS)


                self.state = 1990
                localctx._identifier = self.identifier()
                localctx.colName.append(localctx._identifier)
                self.state = 1995
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,244,self._ctx)
                while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                    if _alt==1:
                        self.state = 1991
                        self.match(SqlBaseParser.T__3)
                        self.state = 1992
                        localctx._identifier = self.identifier()
                        localctx.colName.append(localctx._identifier)
                    self.state = 1997
                    self._errHandler.sync(self)
                    _alt = self._interp.adaptivePredict(self._input,244,self._ctx)



        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class SetQuantifierContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def DISTINCT(self):
            return self.getToken(SqlBaseParser.DISTINCT, 0)

        def ALL(self):
            return self.getToken(SqlBaseParser.ALL, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_setQuantifier

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSetQuantifier" ):
                listener.enterSetQuantifier(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSetQuantifier" ):
                listener.exitSetQuantifier(self)




    def setQuantifier(self):

        localctx = SqlBaseParser.SetQuantifierContext(self, self._ctx, self.state)
        self.enterRule(localctx, 132, self.RULE_setQuantifier)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2000
            _la = self._input.LA(1)
            if not(_la==SqlBaseParser.ALL or _la==SqlBaseParser.DISTINCT):
                self._errHandler.recoverInline(self)
            else:
                self._errHandler.reportMatch(self)
                self.consume()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class RelationContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def relationPrimary(self):
            return self.getTypedRuleContext(SqlBaseParser.RelationPrimaryContext,0)


        def joinRelation(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.JoinRelationContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.JoinRelationContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_relation

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterRelation" ):
                listener.enterRelation(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitRelation" ):
                listener.exitRelation(self)




    def relation(self):

        localctx = SqlBaseParser.RelationContext(self, self._ctx, self.state)
        self.enterRule(localctx, 134, self.RULE_relation)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2002
            self.relationPrimary()
            self.state = 2006
            self._errHandler.sync(self)
            _alt = self._interp.adaptivePredict(self._input,246,self._ctx)
            while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                if _alt==1:
                    self.state = 2003
                    self.joinRelation()
                self.state = 2008
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,246,self._ctx)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class JoinRelationContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.right = None # RelationPrimaryContext

        def JOIN(self):
            return self.getToken(SqlBaseParser.JOIN, 0)

        def relationPrimary(self):
            return self.getTypedRuleContext(SqlBaseParser.RelationPrimaryContext,0)


        def joinType(self):
            return self.getTypedRuleContext(SqlBaseParser.JoinTypeContext,0)


        def joinCriteria(self):
            return self.getTypedRuleContext(SqlBaseParser.JoinCriteriaContext,0)


        def NATURAL(self):
            return self.getToken(SqlBaseParser.NATURAL, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_joinRelation

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterJoinRelation" ):
                listener.enterJoinRelation(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitJoinRelation" ):
                listener.exitJoinRelation(self)




    def joinRelation(self):

        localctx = SqlBaseParser.JoinRelationContext(self, self._ctx, self.state)
        self.enterRule(localctx, 136, self.RULE_joinRelation)
        try:
            self.state = 2020
            self._errHandler.sync(self)
            token = self._input.LA(1)
            if token in [SqlBaseParser.ANTI, SqlBaseParser.CROSS, SqlBaseParser.FULL, SqlBaseParser.INNER, SqlBaseParser.JOIN, SqlBaseParser.LEFT, SqlBaseParser.RIGHT, SqlBaseParser.SEMI]:
                self.enterOuterAlt(localctx, 1)
                self.state = 2009
                self.joinType()
                self.state = 2010
                self.match(SqlBaseParser.JOIN)
                self.state = 2011
                localctx.right = self.relationPrimary()
                self.state = 2013
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,247,self._ctx)
                if la_ == 1:
                    self.state = 2012
                    self.joinCriteria()


                pass
            elif token in [SqlBaseParser.NATURAL]:
                self.enterOuterAlt(localctx, 2)
                self.state = 2015
                self.match(SqlBaseParser.NATURAL)
                self.state = 2016
                self.joinType()
                self.state = 2017
                self.match(SqlBaseParser.JOIN)
                self.state = 2018
                localctx.right = self.relationPrimary()
                pass
            else:
                raise NoViableAltException(self)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class JoinTypeContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def INNER(self):
            return self.getToken(SqlBaseParser.INNER, 0)

        def CROSS(self):
            return self.getToken(SqlBaseParser.CROSS, 0)

        def LEFT(self):
            return self.getToken(SqlBaseParser.LEFT, 0)

        def OUTER(self):
            return self.getToken(SqlBaseParser.OUTER, 0)

        def SEMI(self):
            return self.getToken(SqlBaseParser.SEMI, 0)

        def RIGHT(self):
            return self.getToken(SqlBaseParser.RIGHT, 0)

        def FULL(self):
            return self.getToken(SqlBaseParser.FULL, 0)

        def ANTI(self):
            return self.getToken(SqlBaseParser.ANTI, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_joinType

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterJoinType" ):
                listener.enterJoinType(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitJoinType" ):
                listener.exitJoinType(self)




    def joinType(self):

        localctx = SqlBaseParser.JoinTypeContext(self, self._ctx, self.state)
        self.enterRule(localctx, 138, self.RULE_joinType)
        self._la = 0 # Token type
        try:
            self.state = 2046
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,255,self._ctx)
            if la_ == 1:
                self.enterOuterAlt(localctx, 1)
                self.state = 2023
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.INNER:
                    self.state = 2022
                    self.match(SqlBaseParser.INNER)


                pass

            elif la_ == 2:
                self.enterOuterAlt(localctx, 2)
                self.state = 2025
                self.match(SqlBaseParser.CROSS)
                pass

            elif la_ == 3:
                self.enterOuterAlt(localctx, 3)
                self.state = 2026
                self.match(SqlBaseParser.LEFT)
                self.state = 2028
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.OUTER:
                    self.state = 2027
                    self.match(SqlBaseParser.OUTER)


                pass

            elif la_ == 4:
                self.enterOuterAlt(localctx, 4)
                self.state = 2031
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.LEFT:
                    self.state = 2030
                    self.match(SqlBaseParser.LEFT)


                self.state = 2033
                self.match(SqlBaseParser.SEMI)
                pass

            elif la_ == 5:
                self.enterOuterAlt(localctx, 5)
                self.state = 2034
                self.match(SqlBaseParser.RIGHT)
                self.state = 2036
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.OUTER:
                    self.state = 2035
                    self.match(SqlBaseParser.OUTER)


                pass

            elif la_ == 6:
                self.enterOuterAlt(localctx, 6)
                self.state = 2038
                self.match(SqlBaseParser.FULL)
                self.state = 2040
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.OUTER:
                    self.state = 2039
                    self.match(SqlBaseParser.OUTER)


                pass

            elif la_ == 7:
                self.enterOuterAlt(localctx, 7)
                self.state = 2043
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.LEFT:
                    self.state = 2042
                    self.match(SqlBaseParser.LEFT)


                self.state = 2045
                self.match(SqlBaseParser.ANTI)
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class JoinCriteriaContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def ON(self):
            return self.getToken(SqlBaseParser.ON, 0)

        def booleanExpression(self):
            return self.getTypedRuleContext(SqlBaseParser.BooleanExpressionContext,0)


        def USING(self):
            return self.getToken(SqlBaseParser.USING, 0)

        def identifierList(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierListContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_joinCriteria

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterJoinCriteria" ):
                listener.enterJoinCriteria(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitJoinCriteria" ):
                listener.exitJoinCriteria(self)




    def joinCriteria(self):

        localctx = SqlBaseParser.JoinCriteriaContext(self, self._ctx, self.state)
        self.enterRule(localctx, 140, self.RULE_joinCriteria)
        try:
            self.state = 2052
            self._errHandler.sync(self)
            token = self._input.LA(1)
            if token in [SqlBaseParser.ON]:
                self.enterOuterAlt(localctx, 1)
                self.state = 2048
                self.match(SqlBaseParser.ON)
                self.state = 2049
                self.booleanExpression(0)
                pass
            elif token in [SqlBaseParser.USING]:
                self.enterOuterAlt(localctx, 2)
                self.state = 2050
                self.match(SqlBaseParser.USING)
                self.state = 2051
                self.identifierList()
                pass
            else:
                raise NoViableAltException(self)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class SampleContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def TABLESAMPLE(self):
            return self.getToken(SqlBaseParser.TABLESAMPLE, 0)

        def sampleMethod(self):
            return self.getTypedRuleContext(SqlBaseParser.SampleMethodContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_sample

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSample" ):
                listener.enterSample(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSample" ):
                listener.exitSample(self)




    def sample(self):

        localctx = SqlBaseParser.SampleContext(self, self._ctx, self.state)
        self.enterRule(localctx, 142, self.RULE_sample)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2054
            self.match(SqlBaseParser.TABLESAMPLE)
            self.state = 2055
            self.match(SqlBaseParser.T__1)
            self.state = 2057
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,257,self._ctx)
            if la_ == 1:
                self.state = 2056
                self.sampleMethod()


            self.state = 2059
            self.match(SqlBaseParser.T__2)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class SampleMethodContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser


        def getRuleIndex(self):
            return SqlBaseParser.RULE_sampleMethod


        def copyFrom(self, ctx:ParserRuleContext):
            super().copyFrom(ctx)



    class SampleByRowsContext(SampleMethodContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.SampleMethodContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def expression(self):
            return self.getTypedRuleContext(SqlBaseParser.ExpressionContext,0)

        def ROWS(self):
            return self.getToken(SqlBaseParser.ROWS, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSampleByRows" ):
                listener.enterSampleByRows(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSampleByRows" ):
                listener.exitSampleByRows(self)


    class SampleByPercentileContext(SampleMethodContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.SampleMethodContext
            super().__init__(parser)
            self.negativeSign = None # Token
            self.percentage = None # Token
            self.copyFrom(ctx)

        def PERCENTLIT(self):
            return self.getToken(SqlBaseParser.PERCENTLIT, 0)
        def INTEGER_VALUE(self):
            return self.getToken(SqlBaseParser.INTEGER_VALUE, 0)
        def DECIMAL_VALUE(self):
            return self.getToken(SqlBaseParser.DECIMAL_VALUE, 0)
        def MINUS(self):
            return self.getToken(SqlBaseParser.MINUS, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSampleByPercentile" ):
                listener.enterSampleByPercentile(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSampleByPercentile" ):
                listener.exitSampleByPercentile(self)


    class SampleByBucketContext(SampleMethodContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.SampleMethodContext
            super().__init__(parser)
            self.sampleType = None # Token
            self.numerator = None # Token
            self.denominator = None # Token
            self.copyFrom(ctx)

        def OUT(self):
            return self.getToken(SqlBaseParser.OUT, 0)
        def OF(self):
            return self.getToken(SqlBaseParser.OF, 0)
        def BUCKET(self):
            return self.getToken(SqlBaseParser.BUCKET, 0)
        def INTEGER_VALUE(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.INTEGER_VALUE)
            else:
                return self.getToken(SqlBaseParser.INTEGER_VALUE, i)
        def ON(self):
            return self.getToken(SqlBaseParser.ON, 0)
        def identifier(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierContext,0)

        def qualifiedName(self):
            return self.getTypedRuleContext(SqlBaseParser.QualifiedNameContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSampleByBucket" ):
                listener.enterSampleByBucket(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSampleByBucket" ):
                listener.exitSampleByBucket(self)


    class SampleByBytesContext(SampleMethodContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.SampleMethodContext
            super().__init__(parser)
            self.bytes = None # ExpressionContext
            self.copyFrom(ctx)

        def expression(self):
            return self.getTypedRuleContext(SqlBaseParser.ExpressionContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSampleByBytes" ):
                listener.enterSampleByBytes(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSampleByBytes" ):
                listener.exitSampleByBytes(self)



    def sampleMethod(self):

        localctx = SqlBaseParser.SampleMethodContext(self, self._ctx, self.state)
        self.enterRule(localctx, 144, self.RULE_sampleMethod)
        self._la = 0 # Token type
        try:
            self.state = 2085
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,261,self._ctx)
            if la_ == 1:
                localctx = SqlBaseParser.SampleByPercentileContext(self, localctx)
                self.enterOuterAlt(localctx, 1)
                self.state = 2062
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.MINUS:
                    self.state = 2061
                    localctx.negativeSign = self.match(SqlBaseParser.MINUS)


                self.state = 2064
                localctx.percentage = self._input.LT(1)
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.INTEGER_VALUE or _la==SqlBaseParser.DECIMAL_VALUE):
                    localctx.percentage = self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                self.state = 2065
                self.match(SqlBaseParser.PERCENTLIT)
                pass

            elif la_ == 2:
                localctx = SqlBaseParser.SampleByRowsContext(self, localctx)
                self.enterOuterAlt(localctx, 2)
                self.state = 2066
                self.expression()
                self.state = 2067
                self.match(SqlBaseParser.ROWS)
                pass

            elif la_ == 3:
                localctx = SqlBaseParser.SampleByBucketContext(self, localctx)
                self.enterOuterAlt(localctx, 3)
                self.state = 2069
                localctx.sampleType = self.match(SqlBaseParser.BUCKET)
                self.state = 2070
                localctx.numerator = self.match(SqlBaseParser.INTEGER_VALUE)
                self.state = 2071
                self.match(SqlBaseParser.OUT)
                self.state = 2072
                self.match(SqlBaseParser.OF)
                self.state = 2073
                localctx.denominator = self.match(SqlBaseParser.INTEGER_VALUE)
                self.state = 2082
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.ON:
                    self.state = 2074
                    self.match(SqlBaseParser.ON)
                    self.state = 2080
                    self._errHandler.sync(self)
                    la_ = self._interp.adaptivePredict(self._input,259,self._ctx)
                    if la_ == 1:
                        self.state = 2075
                        self.identifier()
                        pass

                    elif la_ == 2:
                        self.state = 2076
                        self.qualifiedName()
                        self.state = 2077
                        self.match(SqlBaseParser.T__1)
                        self.state = 2078
                        self.match(SqlBaseParser.T__2)
                        pass




                pass

            elif la_ == 4:
                localctx = SqlBaseParser.SampleByBytesContext(self, localctx)
                self.enterOuterAlt(localctx, 4)
                self.state = 2084
                localctx.bytes = self.expression()
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class IdentifierListContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def identifierSeq(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierSeqContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_identifierList

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterIdentifierList" ):
                listener.enterIdentifierList(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitIdentifierList" ):
                listener.exitIdentifierList(self)




    def identifierList(self):

        localctx = SqlBaseParser.IdentifierListContext(self, self._ctx, self.state)
        self.enterRule(localctx, 146, self.RULE_identifierList)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2087
            self.match(SqlBaseParser.T__1)
            self.state = 2088
            self.identifierSeq()
            self.state = 2089
            self.match(SqlBaseParser.T__2)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class IdentifierSeqContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self._errorCapturingIdentifier = None # ErrorCapturingIdentifierContext
            self.ident = list() # of ErrorCapturingIdentifierContexts

        def errorCapturingIdentifier(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ErrorCapturingIdentifierContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ErrorCapturingIdentifierContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_identifierSeq

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterIdentifierSeq" ):
                listener.enterIdentifierSeq(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitIdentifierSeq" ):
                listener.exitIdentifierSeq(self)




    def identifierSeq(self):

        localctx = SqlBaseParser.IdentifierSeqContext(self, self._ctx, self.state)
        self.enterRule(localctx, 148, self.RULE_identifierSeq)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2091
            localctx._errorCapturingIdentifier = self.errorCapturingIdentifier()
            localctx.ident.append(localctx._errorCapturingIdentifier)
            self.state = 2096
            self._errHandler.sync(self)
            _alt = self._interp.adaptivePredict(self._input,262,self._ctx)
            while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                if _alt==1:
                    self.state = 2092
                    self.match(SqlBaseParser.T__3)
                    self.state = 2093
                    localctx._errorCapturingIdentifier = self.errorCapturingIdentifier()
                    localctx.ident.append(localctx._errorCapturingIdentifier)
                self.state = 2098
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,262,self._ctx)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class OrderedIdentifierListContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def orderedIdentifier(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.OrderedIdentifierContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.OrderedIdentifierContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_orderedIdentifierList

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterOrderedIdentifierList" ):
                listener.enterOrderedIdentifierList(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitOrderedIdentifierList" ):
                listener.exitOrderedIdentifierList(self)




    def orderedIdentifierList(self):

        localctx = SqlBaseParser.OrderedIdentifierListContext(self, self._ctx, self.state)
        self.enterRule(localctx, 150, self.RULE_orderedIdentifierList)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2099
            self.match(SqlBaseParser.T__1)
            self.state = 2100
            self.orderedIdentifier()
            self.state = 2105
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while _la==SqlBaseParser.T__3:
                self.state = 2101
                self.match(SqlBaseParser.T__3)
                self.state = 2102
                self.orderedIdentifier()
                self.state = 2107
                self._errHandler.sync(self)
                _la = self._input.LA(1)

            self.state = 2108
            self.match(SqlBaseParser.T__2)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class OrderedIdentifierContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.ident = None # ErrorCapturingIdentifierContext
            self.ordering = None # Token

        def errorCapturingIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.ErrorCapturingIdentifierContext,0)


        def ASC(self):
            return self.getToken(SqlBaseParser.ASC, 0)

        def DESC(self):
            return self.getToken(SqlBaseParser.DESC, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_orderedIdentifier

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterOrderedIdentifier" ):
                listener.enterOrderedIdentifier(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitOrderedIdentifier" ):
                listener.exitOrderedIdentifier(self)




    def orderedIdentifier(self):

        localctx = SqlBaseParser.OrderedIdentifierContext(self, self._ctx, self.state)
        self.enterRule(localctx, 152, self.RULE_orderedIdentifier)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2110
            localctx.ident = self.errorCapturingIdentifier()
            self.state = 2112
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==SqlBaseParser.ASC or _la==SqlBaseParser.DESC:
                self.state = 2111
                localctx.ordering = self._input.LT(1)
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.ASC or _la==SqlBaseParser.DESC):
                    localctx.ordering = self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class IdentifierCommentListContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def identifierComment(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.IdentifierCommentContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.IdentifierCommentContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_identifierCommentList

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterIdentifierCommentList" ):
                listener.enterIdentifierCommentList(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitIdentifierCommentList" ):
                listener.exitIdentifierCommentList(self)




    def identifierCommentList(self):

        localctx = SqlBaseParser.IdentifierCommentListContext(self, self._ctx, self.state)
        self.enterRule(localctx, 154, self.RULE_identifierCommentList)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2114
            self.match(SqlBaseParser.T__1)
            self.state = 2115
            self.identifierComment()
            self.state = 2120
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while _la==SqlBaseParser.T__3:
                self.state = 2116
                self.match(SqlBaseParser.T__3)
                self.state = 2117
                self.identifierComment()
                self.state = 2122
                self._errHandler.sync(self)
                _la = self._input.LA(1)

            self.state = 2123
            self.match(SqlBaseParser.T__2)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class IdentifierCommentContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def identifier(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierContext,0)


        def commentSpec(self):
            return self.getTypedRuleContext(SqlBaseParser.CommentSpecContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_identifierComment

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterIdentifierComment" ):
                listener.enterIdentifierComment(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitIdentifierComment" ):
                listener.exitIdentifierComment(self)




    def identifierComment(self):

        localctx = SqlBaseParser.IdentifierCommentContext(self, self._ctx, self.state)
        self.enterRule(localctx, 156, self.RULE_identifierComment)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2125
            self.identifier()
            self.state = 2127
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==SqlBaseParser.COMMENT:
                self.state = 2126
                self.commentSpec()


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class RelationPrimaryContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser


        def getRuleIndex(self):
            return SqlBaseParser.RULE_relationPrimary


        def copyFrom(self, ctx:ParserRuleContext):
            super().copyFrom(ctx)



    class TableValuedFunctionContext(RelationPrimaryContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.RelationPrimaryContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def functionTable(self):
            return self.getTypedRuleContext(SqlBaseParser.FunctionTableContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterTableValuedFunction" ):
                listener.enterTableValuedFunction(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitTableValuedFunction" ):
                listener.exitTableValuedFunction(self)


    class InlineTableDefault2Context(RelationPrimaryContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.RelationPrimaryContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def inlineTable(self):
            return self.getTypedRuleContext(SqlBaseParser.InlineTableContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterInlineTableDefault2" ):
                listener.enterInlineTableDefault2(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitInlineTableDefault2" ):
                listener.exitInlineTableDefault2(self)


    class AliasedRelationContext(RelationPrimaryContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.RelationPrimaryContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def relation(self):
            return self.getTypedRuleContext(SqlBaseParser.RelationContext,0)

        def tableAlias(self):
            return self.getTypedRuleContext(SqlBaseParser.TableAliasContext,0)

        def sample(self):
            return self.getTypedRuleContext(SqlBaseParser.SampleContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterAliasedRelation" ):
                listener.enterAliasedRelation(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitAliasedRelation" ):
                listener.exitAliasedRelation(self)


    class AliasedQueryContext(RelationPrimaryContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.RelationPrimaryContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def query(self):
            return self.getTypedRuleContext(SqlBaseParser.QueryContext,0)

        def tableAlias(self):
            return self.getTypedRuleContext(SqlBaseParser.TableAliasContext,0)

        def sample(self):
            return self.getTypedRuleContext(SqlBaseParser.SampleContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterAliasedQuery" ):
                listener.enterAliasedQuery(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitAliasedQuery" ):
                listener.exitAliasedQuery(self)


    class TableNameContext(RelationPrimaryContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.RelationPrimaryContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)

        def tableAlias(self):
            return self.getTypedRuleContext(SqlBaseParser.TableAliasContext,0)

        def sample(self):
            return self.getTypedRuleContext(SqlBaseParser.SampleContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterTableName" ):
                listener.enterTableName(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitTableName" ):
                listener.exitTableName(self)



    def relationPrimary(self):

        localctx = SqlBaseParser.RelationPrimaryContext(self, self._ctx, self.state)
        self.enterRule(localctx, 158, self.RULE_relationPrimary)
        try:
            self.state = 2153
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,270,self._ctx)
            if la_ == 1:
                localctx = SqlBaseParser.TableNameContext(self, localctx)
                self.enterOuterAlt(localctx, 1)
                self.state = 2129
                self.multipartIdentifier()
                self.state = 2131
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,267,self._ctx)
                if la_ == 1:
                    self.state = 2130
                    self.sample()


                self.state = 2133
                self.tableAlias()
                pass

            elif la_ == 2:
                localctx = SqlBaseParser.AliasedQueryContext(self, localctx)
                self.enterOuterAlt(localctx, 2)
                self.state = 2135
                self.match(SqlBaseParser.T__1)
                self.state = 2136
                self.query()
                self.state = 2137
                self.match(SqlBaseParser.T__2)
                self.state = 2139
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,268,self._ctx)
                if la_ == 1:
                    self.state = 2138
                    self.sample()


                self.state = 2141
                self.tableAlias()
                pass

            elif la_ == 3:
                localctx = SqlBaseParser.AliasedRelationContext(self, localctx)
                self.enterOuterAlt(localctx, 3)
                self.state = 2143
                self.match(SqlBaseParser.T__1)
                self.state = 2144
                self.relation()
                self.state = 2145
                self.match(SqlBaseParser.T__2)
                self.state = 2147
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,269,self._ctx)
                if la_ == 1:
                    self.state = 2146
                    self.sample()


                self.state = 2149
                self.tableAlias()
                pass

            elif la_ == 4:
                localctx = SqlBaseParser.InlineTableDefault2Context(self, localctx)
                self.enterOuterAlt(localctx, 4)
                self.state = 2151
                self.inlineTable()
                pass

            elif la_ == 5:
                localctx = SqlBaseParser.TableValuedFunctionContext(self, localctx)
                self.enterOuterAlt(localctx, 5)
                self.state = 2152
                self.functionTable()
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class InlineTableContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def VALUES(self):
            return self.getToken(SqlBaseParser.VALUES, 0)

        def expression(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ExpressionContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ExpressionContext,i)


        def tableAlias(self):
            return self.getTypedRuleContext(SqlBaseParser.TableAliasContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_inlineTable

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterInlineTable" ):
                listener.enterInlineTable(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitInlineTable" ):
                listener.exitInlineTable(self)




    def inlineTable(self):

        localctx = SqlBaseParser.InlineTableContext(self, self._ctx, self.state)
        self.enterRule(localctx, 160, self.RULE_inlineTable)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2155
            self.match(SqlBaseParser.VALUES)
            self.state = 2156
            self.expression()
            self.state = 2161
            self._errHandler.sync(self)
            _alt = self._interp.adaptivePredict(self._input,271,self._ctx)
            while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                if _alt==1:
                    self.state = 2157
                    self.match(SqlBaseParser.T__3)
                    self.state = 2158
                    self.expression()
                self.state = 2163
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,271,self._ctx)

            self.state = 2164
            self.tableAlias()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class FunctionTableContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.funcName = None # ErrorCapturingIdentifierContext

        def tableAlias(self):
            return self.getTypedRuleContext(SqlBaseParser.TableAliasContext,0)


        def errorCapturingIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.ErrorCapturingIdentifierContext,0)


        def expression(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ExpressionContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ExpressionContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_functionTable

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterFunctionTable" ):
                listener.enterFunctionTable(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitFunctionTable" ):
                listener.exitFunctionTable(self)




    def functionTable(self):

        localctx = SqlBaseParser.FunctionTableContext(self, self._ctx, self.state)
        self.enterRule(localctx, 162, self.RULE_functionTable)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2166
            localctx.funcName = self.errorCapturingIdentifier()
            self.state = 2167
            self.match(SqlBaseParser.T__1)
            self.state = 2176
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,273,self._ctx)
            if la_ == 1:
                self.state = 2168
                self.expression()
                self.state = 2173
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while _la==SqlBaseParser.T__3:
                    self.state = 2169
                    self.match(SqlBaseParser.T__3)
                    self.state = 2170
                    self.expression()
                    self.state = 2175
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)



            self.state = 2178
            self.match(SqlBaseParser.T__2)
            self.state = 2179
            self.tableAlias()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class TableAliasContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def strictIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.StrictIdentifierContext,0)


        def AS(self):
            return self.getToken(SqlBaseParser.AS, 0)

        def identifierList(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierListContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_tableAlias

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterTableAlias" ):
                listener.enterTableAlias(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitTableAlias" ):
                listener.exitTableAlias(self)




    def tableAlias(self):

        localctx = SqlBaseParser.TableAliasContext(self, self._ctx, self.state)
        self.enterRule(localctx, 164, self.RULE_tableAlias)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2188
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,276,self._ctx)
            if la_ == 1:
                self.state = 2182
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,274,self._ctx)
                if la_ == 1:
                    self.state = 2181
                    self.match(SqlBaseParser.AS)


                self.state = 2184
                self.strictIdentifier()
                self.state = 2186
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,275,self._ctx)
                if la_ == 1:
                    self.state = 2185
                    self.identifierList()




        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class RowFormatContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser


        def getRuleIndex(self):
            return SqlBaseParser.RULE_rowFormat


        def copyFrom(self, ctx:ParserRuleContext):
            super().copyFrom(ctx)



    class RowFormatSerdeContext(RowFormatContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.RowFormatContext
            super().__init__(parser)
            self.name = None # Token
            self.props = None # TablePropertyListContext
            self.copyFrom(ctx)

        def ROW(self):
            return self.getToken(SqlBaseParser.ROW, 0)
        def FORMAT(self):
            return self.getToken(SqlBaseParser.FORMAT, 0)
        def SERDE(self):
            return self.getToken(SqlBaseParser.SERDE, 0)
        def STRING(self):
            return self.getToken(SqlBaseParser.STRING, 0)
        def WITH(self):
            return self.getToken(SqlBaseParser.WITH, 0)
        def SERDEPROPERTIES(self):
            return self.getToken(SqlBaseParser.SERDEPROPERTIES, 0)
        def tablePropertyList(self):
            return self.getTypedRuleContext(SqlBaseParser.TablePropertyListContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterRowFormatSerde" ):
                listener.enterRowFormatSerde(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitRowFormatSerde" ):
                listener.exitRowFormatSerde(self)


    class RowFormatDelimitedContext(RowFormatContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.RowFormatContext
            super().__init__(parser)
            self.fieldsTerminatedBy = None # Token
            self.escapedBy = None # Token
            self.collectionItemsTerminatedBy = None # Token
            self.keysTerminatedBy = None # Token
            self.linesSeparatedBy = None # Token
            self.nullDefinedAs = None # Token
            self.copyFrom(ctx)

        def ROW(self):
            return self.getToken(SqlBaseParser.ROW, 0)
        def FORMAT(self):
            return self.getToken(SqlBaseParser.FORMAT, 0)
        def DELIMITED(self):
            return self.getToken(SqlBaseParser.DELIMITED, 0)
        def FIELDS(self):
            return self.getToken(SqlBaseParser.FIELDS, 0)
        def TERMINATED(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.TERMINATED)
            else:
                return self.getToken(SqlBaseParser.TERMINATED, i)
        def BY(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.BY)
            else:
                return self.getToken(SqlBaseParser.BY, i)
        def COLLECTION(self):
            return self.getToken(SqlBaseParser.COLLECTION, 0)
        def ITEMS(self):
            return self.getToken(SqlBaseParser.ITEMS, 0)
        def MAP(self):
            return self.getToken(SqlBaseParser.MAP, 0)
        def KEYS(self):
            return self.getToken(SqlBaseParser.KEYS, 0)
        def LINES(self):
            return self.getToken(SqlBaseParser.LINES, 0)
        def NULL(self):
            return self.getToken(SqlBaseParser.NULL, 0)
        def DEFINED(self):
            return self.getToken(SqlBaseParser.DEFINED, 0)
        def AS(self):
            return self.getToken(SqlBaseParser.AS, 0)
        def STRING(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.STRING)
            else:
                return self.getToken(SqlBaseParser.STRING, i)
        def ESCAPED(self):
            return self.getToken(SqlBaseParser.ESCAPED, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterRowFormatDelimited" ):
                listener.enterRowFormatDelimited(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitRowFormatDelimited" ):
                listener.exitRowFormatDelimited(self)



    def rowFormat(self):

        localctx = SqlBaseParser.RowFormatContext(self, self._ctx, self.state)
        self.enterRule(localctx, 166, self.RULE_rowFormat)
        try:
            self.state = 2239
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,284,self._ctx)
            if la_ == 1:
                localctx = SqlBaseParser.RowFormatSerdeContext(self, localctx)
                self.enterOuterAlt(localctx, 1)
                self.state = 2190
                self.match(SqlBaseParser.ROW)
                self.state = 2191
                self.match(SqlBaseParser.FORMAT)
                self.state = 2192
                self.match(SqlBaseParser.SERDE)
                self.state = 2193
                localctx.name = self.match(SqlBaseParser.STRING)
                self.state = 2197
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,277,self._ctx)
                if la_ == 1:
                    self.state = 2194
                    self.match(SqlBaseParser.WITH)
                    self.state = 2195
                    self.match(SqlBaseParser.SERDEPROPERTIES)
                    self.state = 2196
                    localctx.props = self.tablePropertyList()


                pass

            elif la_ == 2:
                localctx = SqlBaseParser.RowFormatDelimitedContext(self, localctx)
                self.enterOuterAlt(localctx, 2)
                self.state = 2199
                self.match(SqlBaseParser.ROW)
                self.state = 2200
                self.match(SqlBaseParser.FORMAT)
                self.state = 2201
                self.match(SqlBaseParser.DELIMITED)
                self.state = 2211
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,279,self._ctx)
                if la_ == 1:
                    self.state = 2202
                    self.match(SqlBaseParser.FIELDS)
                    self.state = 2203
                    self.match(SqlBaseParser.TERMINATED)
                    self.state = 2204
                    self.match(SqlBaseParser.BY)
                    self.state = 2205
                    localctx.fieldsTerminatedBy = self.match(SqlBaseParser.STRING)
                    self.state = 2209
                    self._errHandler.sync(self)
                    la_ = self._interp.adaptivePredict(self._input,278,self._ctx)
                    if la_ == 1:
                        self.state = 2206
                        self.match(SqlBaseParser.ESCAPED)
                        self.state = 2207
                        self.match(SqlBaseParser.BY)
                        self.state = 2208
                        localctx.escapedBy = self.match(SqlBaseParser.STRING)




                self.state = 2218
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,280,self._ctx)
                if la_ == 1:
                    self.state = 2213
                    self.match(SqlBaseParser.COLLECTION)
                    self.state = 2214
                    self.match(SqlBaseParser.ITEMS)
                    self.state = 2215
                    self.match(SqlBaseParser.TERMINATED)
                    self.state = 2216
                    self.match(SqlBaseParser.BY)
                    self.state = 2217
                    localctx.collectionItemsTerminatedBy = self.match(SqlBaseParser.STRING)


                self.state = 2225
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,281,self._ctx)
                if la_ == 1:
                    self.state = 2220
                    self.match(SqlBaseParser.MAP)
                    self.state = 2221
                    self.match(SqlBaseParser.KEYS)
                    self.state = 2222
                    self.match(SqlBaseParser.TERMINATED)
                    self.state = 2223
                    self.match(SqlBaseParser.BY)
                    self.state = 2224
                    localctx.keysTerminatedBy = self.match(SqlBaseParser.STRING)


                self.state = 2231
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,282,self._ctx)
                if la_ == 1:
                    self.state = 2227
                    self.match(SqlBaseParser.LINES)
                    self.state = 2228
                    self.match(SqlBaseParser.TERMINATED)
                    self.state = 2229
                    self.match(SqlBaseParser.BY)
                    self.state = 2230
                    localctx.linesSeparatedBy = self.match(SqlBaseParser.STRING)


                self.state = 2237
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,283,self._ctx)
                if la_ == 1:
                    self.state = 2233
                    self.match(SqlBaseParser.NULL)
                    self.state = 2234
                    self.match(SqlBaseParser.DEFINED)
                    self.state = 2235
                    self.match(SqlBaseParser.AS)
                    self.state = 2236
                    localctx.nullDefinedAs = self.match(SqlBaseParser.STRING)


                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class MultipartIdentifierListContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def multipartIdentifier(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.MultipartIdentifierContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_multipartIdentifierList

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterMultipartIdentifierList" ):
                listener.enterMultipartIdentifierList(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitMultipartIdentifierList" ):
                listener.exitMultipartIdentifierList(self)




    def multipartIdentifierList(self):

        localctx = SqlBaseParser.MultipartIdentifierListContext(self, self._ctx, self.state)
        self.enterRule(localctx, 168, self.RULE_multipartIdentifierList)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2241
            self.multipartIdentifier()
            self.state = 2246
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while _la==SqlBaseParser.T__3:
                self.state = 2242
                self.match(SqlBaseParser.T__3)
                self.state = 2243
                self.multipartIdentifier()
                self.state = 2248
                self._errHandler.sync(self)
                _la = self._input.LA(1)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class MultipartIdentifierContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self._errorCapturingIdentifier = None # ErrorCapturingIdentifierContext
            self.parts = list() # of ErrorCapturingIdentifierContexts

        def errorCapturingIdentifier(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ErrorCapturingIdentifierContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ErrorCapturingIdentifierContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_multipartIdentifier

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterMultipartIdentifier" ):
                listener.enterMultipartIdentifier(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitMultipartIdentifier" ):
                listener.exitMultipartIdentifier(self)




    def multipartIdentifier(self):

        localctx = SqlBaseParser.MultipartIdentifierContext(self, self._ctx, self.state)
        self.enterRule(localctx, 170, self.RULE_multipartIdentifier)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2249
            localctx._errorCapturingIdentifier = self.errorCapturingIdentifier()
            localctx.parts.append(localctx._errorCapturingIdentifier)
            self.state = 2254
            self._errHandler.sync(self)
            _alt = self._interp.adaptivePredict(self._input,286,self._ctx)
            while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                if _alt==1:
                    self.state = 2250
                    self.match(SqlBaseParser.T__4)
                    self.state = 2251
                    localctx._errorCapturingIdentifier = self.errorCapturingIdentifier()
                    localctx.parts.append(localctx._errorCapturingIdentifier)
                self.state = 2256
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,286,self._ctx)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class TableIdentifierContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.db = None # ErrorCapturingIdentifierContext
            self.table = None # ErrorCapturingIdentifierContext

        def errorCapturingIdentifier(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ErrorCapturingIdentifierContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ErrorCapturingIdentifierContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_tableIdentifier

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterTableIdentifier" ):
                listener.enterTableIdentifier(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitTableIdentifier" ):
                listener.exitTableIdentifier(self)




    def tableIdentifier(self):

        localctx = SqlBaseParser.TableIdentifierContext(self, self._ctx, self.state)
        self.enterRule(localctx, 172, self.RULE_tableIdentifier)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2260
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,287,self._ctx)
            if la_ == 1:
                self.state = 2257
                localctx.db = self.errorCapturingIdentifier()
                self.state = 2258
                self.match(SqlBaseParser.T__4)


            self.state = 2262
            localctx.table = self.errorCapturingIdentifier()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class FunctionIdentifierContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.db = None # ErrorCapturingIdentifierContext
            self.function = None # ErrorCapturingIdentifierContext

        def errorCapturingIdentifier(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ErrorCapturingIdentifierContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ErrorCapturingIdentifierContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_functionIdentifier

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterFunctionIdentifier" ):
                listener.enterFunctionIdentifier(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitFunctionIdentifier" ):
                listener.exitFunctionIdentifier(self)




    def functionIdentifier(self):

        localctx = SqlBaseParser.FunctionIdentifierContext(self, self._ctx, self.state)
        self.enterRule(localctx, 174, self.RULE_functionIdentifier)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2267
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,288,self._ctx)
            if la_ == 1:
                self.state = 2264
                localctx.db = self.errorCapturingIdentifier()
                self.state = 2265
                self.match(SqlBaseParser.T__4)


            self.state = 2269
            localctx.function = self.errorCapturingIdentifier()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class NamedExpressionContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.name = None # ErrorCapturingIdentifierContext

        def expression(self):
            return self.getTypedRuleContext(SqlBaseParser.ExpressionContext,0)


        def identifierList(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierListContext,0)


        def AS(self):
            return self.getToken(SqlBaseParser.AS, 0)

        def errorCapturingIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.ErrorCapturingIdentifierContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_namedExpression

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterNamedExpression" ):
                listener.enterNamedExpression(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitNamedExpression" ):
                listener.exitNamedExpression(self)




    def namedExpression(self):

        localctx = SqlBaseParser.NamedExpressionContext(self, self._ctx, self.state)
        self.enterRule(localctx, 176, self.RULE_namedExpression)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2271
            self.expression()
            self.state = 2279
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,291,self._ctx)
            if la_ == 1:
                self.state = 2273
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,289,self._ctx)
                if la_ == 1:
                    self.state = 2272
                    self.match(SqlBaseParser.AS)


                self.state = 2277
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,290,self._ctx)
                if la_ == 1:
                    self.state = 2275
                    localctx.name = self.errorCapturingIdentifier()
                    pass

                elif la_ == 2:
                    self.state = 2276
                    self.identifierList()
                    pass




        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class NamedExpressionSeqContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def namedExpression(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.NamedExpressionContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.NamedExpressionContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_namedExpressionSeq

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterNamedExpressionSeq" ):
                listener.enterNamedExpressionSeq(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitNamedExpressionSeq" ):
                listener.exitNamedExpressionSeq(self)




    def namedExpressionSeq(self):

        localctx = SqlBaseParser.NamedExpressionSeqContext(self, self._ctx, self.state)
        self.enterRule(localctx, 178, self.RULE_namedExpressionSeq)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2281
            self.namedExpression()
            self.state = 2286
            self._errHandler.sync(self)
            _alt = self._interp.adaptivePredict(self._input,292,self._ctx)
            while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                if _alt==1:
                    self.state = 2282
                    self.match(SqlBaseParser.T__3)
                    self.state = 2283
                    self.namedExpression()
                self.state = 2288
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,292,self._ctx)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class TransformListContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self._transform = None # TransformContext
            self.transforms = list() # of TransformContexts

        def transform(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.TransformContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.TransformContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_transformList

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterTransformList" ):
                listener.enterTransformList(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitTransformList" ):
                listener.exitTransformList(self)




    def transformList(self):

        localctx = SqlBaseParser.TransformListContext(self, self._ctx, self.state)
        self.enterRule(localctx, 180, self.RULE_transformList)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2289
            self.match(SqlBaseParser.T__1)
            self.state = 2290
            localctx._transform = self.transform()
            localctx.transforms.append(localctx._transform)
            self.state = 2295
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while _la==SqlBaseParser.T__3:
                self.state = 2291
                self.match(SqlBaseParser.T__3)
                self.state = 2292
                localctx._transform = self.transform()
                localctx.transforms.append(localctx._transform)
                self.state = 2297
                self._errHandler.sync(self)
                _la = self._input.LA(1)

            self.state = 2298
            self.match(SqlBaseParser.T__2)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class TransformContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser


        def getRuleIndex(self):
            return SqlBaseParser.RULE_transform


        def copyFrom(self, ctx:ParserRuleContext):
            super().copyFrom(ctx)



    class IdentityTransformContext(TransformContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.TransformContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def qualifiedName(self):
            return self.getTypedRuleContext(SqlBaseParser.QualifiedNameContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterIdentityTransform" ):
                listener.enterIdentityTransform(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitIdentityTransform" ):
                listener.exitIdentityTransform(self)


    class ApplyTransformContext(TransformContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.TransformContext
            super().__init__(parser)
            self.transformName = None # IdentifierContext
            self._transformArgument = None # TransformArgumentContext
            self.argument = list() # of TransformArgumentContexts
            self.copyFrom(ctx)

        def identifier(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierContext,0)

        def transformArgument(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.TransformArgumentContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.TransformArgumentContext,i)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterApplyTransform" ):
                listener.enterApplyTransform(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitApplyTransform" ):
                listener.exitApplyTransform(self)



    def transform(self):

        localctx = SqlBaseParser.TransformContext(self, self._ctx, self.state)
        self.enterRule(localctx, 182, self.RULE_transform)
        self._la = 0 # Token type
        try:
            self.state = 2313
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,295,self._ctx)
            if la_ == 1:
                localctx = SqlBaseParser.IdentityTransformContext(self, localctx)
                self.enterOuterAlt(localctx, 1)
                self.state = 2300
                self.qualifiedName()
                pass

            elif la_ == 2:
                localctx = SqlBaseParser.ApplyTransformContext(self, localctx)
                self.enterOuterAlt(localctx, 2)
                self.state = 2301
                localctx.transformName = self.identifier()
                self.state = 2302
                self.match(SqlBaseParser.T__1)
                self.state = 2303
                localctx._transformArgument = self.transformArgument()
                localctx.argument.append(localctx._transformArgument)
                self.state = 2308
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while _la==SqlBaseParser.T__3:
                    self.state = 2304
                    self.match(SqlBaseParser.T__3)
                    self.state = 2305
                    localctx._transformArgument = self.transformArgument()
                    localctx.argument.append(localctx._transformArgument)
                    self.state = 2310
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)

                self.state = 2311
                self.match(SqlBaseParser.T__2)
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class TransformArgumentContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def qualifiedName(self):
            return self.getTypedRuleContext(SqlBaseParser.QualifiedNameContext,0)


        def constant(self):
            return self.getTypedRuleContext(SqlBaseParser.ConstantContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_transformArgument

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterTransformArgument" ):
                listener.enterTransformArgument(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitTransformArgument" ):
                listener.exitTransformArgument(self)




    def transformArgument(self):

        localctx = SqlBaseParser.TransformArgumentContext(self, self._ctx, self.state)
        self.enterRule(localctx, 184, self.RULE_transformArgument)
        try:
            self.state = 2317
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,296,self._ctx)
            if la_ == 1:
                self.enterOuterAlt(localctx, 1)
                self.state = 2315
                self.qualifiedName()
                pass

            elif la_ == 2:
                self.enterOuterAlt(localctx, 2)
                self.state = 2316
                self.constant()
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class ExpressionContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def booleanExpression(self):
            return self.getTypedRuleContext(SqlBaseParser.BooleanExpressionContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_expression

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterExpression" ):
                listener.enterExpression(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitExpression" ):
                listener.exitExpression(self)




    def expression(self):

        localctx = SqlBaseParser.ExpressionContext(self, self._ctx, self.state)
        self.enterRule(localctx, 186, self.RULE_expression)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2319
            self.booleanExpression(0)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class BooleanExpressionContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser


        def getRuleIndex(self):
            return SqlBaseParser.RULE_booleanExpression


        def copyFrom(self, ctx:ParserRuleContext):
            super().copyFrom(ctx)


    class LogicalNotContext(BooleanExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.BooleanExpressionContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def NOT(self):
            return self.getToken(SqlBaseParser.NOT, 0)
        def booleanExpression(self):
            return self.getTypedRuleContext(SqlBaseParser.BooleanExpressionContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterLogicalNot" ):
                listener.enterLogicalNot(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitLogicalNot" ):
                listener.exitLogicalNot(self)


    class PredicatedContext(BooleanExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.BooleanExpressionContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def valueExpression(self):
            return self.getTypedRuleContext(SqlBaseParser.ValueExpressionContext,0)

        def predicate(self):
            return self.getTypedRuleContext(SqlBaseParser.PredicateContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterPredicated" ):
                listener.enterPredicated(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitPredicated" ):
                listener.exitPredicated(self)


    class ExistsContext(BooleanExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.BooleanExpressionContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def EXISTS(self):
            return self.getToken(SqlBaseParser.EXISTS, 0)
        def query(self):
            return self.getTypedRuleContext(SqlBaseParser.QueryContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterExists" ):
                listener.enterExists(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitExists" ):
                listener.exitExists(self)


    class LogicalBinaryContext(BooleanExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.BooleanExpressionContext
            super().__init__(parser)
            self.left = None # BooleanExpressionContext
            self.operator = None # Token
            self.right = None # BooleanExpressionContext
            self.copyFrom(ctx)

        def booleanExpression(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.BooleanExpressionContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.BooleanExpressionContext,i)

        def AND(self):
            return self.getToken(SqlBaseParser.AND, 0)
        def OR(self):
            return self.getToken(SqlBaseParser.OR, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterLogicalBinary" ):
                listener.enterLogicalBinary(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitLogicalBinary" ):
                listener.exitLogicalBinary(self)



    def booleanExpression(self, _p:int=0):
        _parentctx = self._ctx
        _parentState = self.state
        localctx = SqlBaseParser.BooleanExpressionContext(self, self._ctx, _parentState)
        _prevctx = localctx
        _startState = 188
        self.enterRecursionRule(localctx, 188, self.RULE_booleanExpression, _p)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2333
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,298,self._ctx)
            if la_ == 1:
                localctx = SqlBaseParser.LogicalNotContext(self, localctx)
                self._ctx = localctx
                _prevctx = localctx

                self.state = 2322
                self.match(SqlBaseParser.NOT)
                self.state = 2323
                self.booleanExpression(5)
                pass

            elif la_ == 2:
                localctx = SqlBaseParser.ExistsContext(self, localctx)
                self._ctx = localctx
                _prevctx = localctx
                self.state = 2324
                self.match(SqlBaseParser.EXISTS)
                self.state = 2325
                self.match(SqlBaseParser.T__1)
                self.state = 2326
                self.query()
                self.state = 2327
                self.match(SqlBaseParser.T__2)
                pass

            elif la_ == 3:
                localctx = SqlBaseParser.PredicatedContext(self, localctx)
                self._ctx = localctx
                _prevctx = localctx
                self.state = 2329
                self.valueExpression(0)
                self.state = 2331
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,297,self._ctx)
                if la_ == 1:
                    self.state = 2330
                    self.predicate()


                pass


            self._ctx.stop = self._input.LT(-1)
            self.state = 2343
            self._errHandler.sync(self)
            _alt = self._interp.adaptivePredict(self._input,300,self._ctx)
            while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                if _alt==1:
                    if self._parseListeners is not None:
                        self.triggerExitRuleEvent()
                    _prevctx = localctx
                    self.state = 2341
                    self._errHandler.sync(self)
                    la_ = self._interp.adaptivePredict(self._input,299,self._ctx)
                    if la_ == 1:
                        localctx = SqlBaseParser.LogicalBinaryContext(self, SqlBaseParser.BooleanExpressionContext(self, _parentctx, _parentState))
                        localctx.left = _prevctx
                        self.pushNewRecursionContext(localctx, _startState, self.RULE_booleanExpression)
                        self.state = 2335
                        if not self.precpred(self._ctx, 2):
                            from antlr4.error.Errors import FailedPredicateException
                            raise FailedPredicateException(self, "self.precpred(self._ctx, 2)")
                        self.state = 2336
                        localctx.operator = self.match(SqlBaseParser.AND)
                        self.state = 2337
                        localctx.right = self.booleanExpression(3)
                        pass

                    elif la_ == 2:
                        localctx = SqlBaseParser.LogicalBinaryContext(self, SqlBaseParser.BooleanExpressionContext(self, _parentctx, _parentState))
                        localctx.left = _prevctx
                        self.pushNewRecursionContext(localctx, _startState, self.RULE_booleanExpression)
                        self.state = 2338
                        if not self.precpred(self._ctx, 1):
                            from antlr4.error.Errors import FailedPredicateException
                            raise FailedPredicateException(self, "self.precpred(self._ctx, 1)")
                        self.state = 2339
                        localctx.operator = self.match(SqlBaseParser.OR)
                        self.state = 2340
                        localctx.right = self.booleanExpression(2)
                        pass


                self.state = 2345
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,300,self._ctx)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.unrollRecursionContexts(_parentctx)
        return localctx

    class PredicateContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.kind = None # Token
            self.lower = None # ValueExpressionContext
            self.upper = None # ValueExpressionContext
            self.pattern = None # ValueExpressionContext
            self.quantifier = None # Token
            self.escapeChar = None # Token
            self.right = None # ValueExpressionContext

        def AND(self):
            return self.getToken(SqlBaseParser.AND, 0)

        def BETWEEN(self):
            return self.getToken(SqlBaseParser.BETWEEN, 0)

        def valueExpression(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ValueExpressionContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ValueExpressionContext,i)


        def NOT(self):
            return self.getToken(SqlBaseParser.NOT, 0)

        def expression(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ExpressionContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ExpressionContext,i)


        def IN(self):
            return self.getToken(SqlBaseParser.IN, 0)

        def query(self):
            return self.getTypedRuleContext(SqlBaseParser.QueryContext,0)


        def RLIKE(self):
            return self.getToken(SqlBaseParser.RLIKE, 0)

        def LIKE(self):
            return self.getToken(SqlBaseParser.LIKE, 0)

        def ANY(self):
            return self.getToken(SqlBaseParser.ANY, 0)

        def SOME(self):
            return self.getToken(SqlBaseParser.SOME, 0)

        def ALL(self):
            return self.getToken(SqlBaseParser.ALL, 0)

        def ESCAPE(self):
            return self.getToken(SqlBaseParser.ESCAPE, 0)

        def STRING(self):
            return self.getToken(SqlBaseParser.STRING, 0)

        def IS(self):
            return self.getToken(SqlBaseParser.IS, 0)

        def NULL(self):
            return self.getToken(SqlBaseParser.NULL, 0)

        def TRUE(self):
            return self.getToken(SqlBaseParser.TRUE, 0)

        def FALSE(self):
            return self.getToken(SqlBaseParser.FALSE, 0)

        def UNKNOWN(self):
            return self.getToken(SqlBaseParser.UNKNOWN, 0)

        def FROM(self):
            return self.getToken(SqlBaseParser.FROM, 0)

        def DISTINCT(self):
            return self.getToken(SqlBaseParser.DISTINCT, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_predicate

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterPredicate" ):
                listener.enterPredicate(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitPredicate" ):
                listener.exitPredicate(self)




    def predicate(self):

        localctx = SqlBaseParser.PredicateContext(self, self._ctx, self.state)
        self.enterRule(localctx, 190, self.RULE_predicate)
        self._la = 0 # Token type
        try:
            self.state = 2428
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,314,self._ctx)
            if la_ == 1:
                self.enterOuterAlt(localctx, 1)
                self.state = 2347
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.NOT:
                    self.state = 2346
                    self.match(SqlBaseParser.NOT)


                self.state = 2349
                localctx.kind = self.match(SqlBaseParser.BETWEEN)
                self.state = 2350
                localctx.lower = self.valueExpression(0)
                self.state = 2351
                self.match(SqlBaseParser.AND)
                self.state = 2352
                localctx.upper = self.valueExpression(0)
                pass

            elif la_ == 2:
                self.enterOuterAlt(localctx, 2)
                self.state = 2355
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.NOT:
                    self.state = 2354
                    self.match(SqlBaseParser.NOT)


                self.state = 2357
                localctx.kind = self.match(SqlBaseParser.IN)
                self.state = 2358
                self.match(SqlBaseParser.T__1)
                self.state = 2359
                self.expression()
                self.state = 2364
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while _la==SqlBaseParser.T__3:
                    self.state = 2360
                    self.match(SqlBaseParser.T__3)
                    self.state = 2361
                    self.expression()
                    self.state = 2366
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)

                self.state = 2367
                self.match(SqlBaseParser.T__2)
                pass

            elif la_ == 3:
                self.enterOuterAlt(localctx, 3)
                self.state = 2370
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.NOT:
                    self.state = 2369
                    self.match(SqlBaseParser.NOT)


                self.state = 2372
                localctx.kind = self.match(SqlBaseParser.IN)
                self.state = 2373
                self.match(SqlBaseParser.T__1)
                self.state = 2374
                self.query()
                self.state = 2375
                self.match(SqlBaseParser.T__2)
                pass

            elif la_ == 4:
                self.enterOuterAlt(localctx, 4)
                self.state = 2378
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.NOT:
                    self.state = 2377
                    self.match(SqlBaseParser.NOT)


                self.state = 2380
                localctx.kind = self.match(SqlBaseParser.RLIKE)
                self.state = 2381
                localctx.pattern = self.valueExpression(0)
                pass

            elif la_ == 5:
                self.enterOuterAlt(localctx, 5)
                self.state = 2383
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.NOT:
                    self.state = 2382
                    self.match(SqlBaseParser.NOT)


                self.state = 2385
                localctx.kind = self.match(SqlBaseParser.LIKE)
                self.state = 2386
                localctx.quantifier = self._input.LT(1)
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.ALL or _la==SqlBaseParser.ANY or _la==SqlBaseParser.SOME):
                    localctx.quantifier = self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                self.state = 2400
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,308,self._ctx)
                if la_ == 1:
                    self.state = 2387
                    self.match(SqlBaseParser.T__1)
                    self.state = 2388
                    self.match(SqlBaseParser.T__2)
                    pass

                elif la_ == 2:
                    self.state = 2389
                    self.match(SqlBaseParser.T__1)
                    self.state = 2390
                    self.expression()
                    self.state = 2395
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    while _la==SqlBaseParser.T__3:
                        self.state = 2391
                        self.match(SqlBaseParser.T__3)
                        self.state = 2392
                        self.expression()
                        self.state = 2397
                        self._errHandler.sync(self)
                        _la = self._input.LA(1)

                    self.state = 2398
                    self.match(SqlBaseParser.T__2)
                    pass


                pass

            elif la_ == 6:
                self.enterOuterAlt(localctx, 6)
                self.state = 2403
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.NOT:
                    self.state = 2402
                    self.match(SqlBaseParser.NOT)


                self.state = 2405
                localctx.kind = self.match(SqlBaseParser.LIKE)
                self.state = 2406
                localctx.pattern = self.valueExpression(0)
                self.state = 2409
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,310,self._ctx)
                if la_ == 1:
                    self.state = 2407
                    self.match(SqlBaseParser.ESCAPE)
                    self.state = 2408
                    localctx.escapeChar = self.match(SqlBaseParser.STRING)


                pass

            elif la_ == 7:
                self.enterOuterAlt(localctx, 7)
                self.state = 2411
                self.match(SqlBaseParser.IS)
                self.state = 2413
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.NOT:
                    self.state = 2412
                    self.match(SqlBaseParser.NOT)


                self.state = 2415
                localctx.kind = self.match(SqlBaseParser.NULL)
                pass

            elif la_ == 8:
                self.enterOuterAlt(localctx, 8)
                self.state = 2416
                self.match(SqlBaseParser.IS)
                self.state = 2418
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.NOT:
                    self.state = 2417
                    self.match(SqlBaseParser.NOT)


                self.state = 2420
                localctx.kind = self._input.LT(1)
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.FALSE or _la==SqlBaseParser.TRUE or _la==SqlBaseParser.UNKNOWN):
                    localctx.kind = self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                pass

            elif la_ == 9:
                self.enterOuterAlt(localctx, 9)
                self.state = 2421
                self.match(SqlBaseParser.IS)
                self.state = 2423
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.NOT:
                    self.state = 2422
                    self.match(SqlBaseParser.NOT)


                self.state = 2425
                localctx.kind = self.match(SqlBaseParser.DISTINCT)
                self.state = 2426
                self.match(SqlBaseParser.FROM)
                self.state = 2427
                localctx.right = self.valueExpression(0)
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class ValueExpressionContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser


        def getRuleIndex(self):
            return SqlBaseParser.RULE_valueExpression


        def copyFrom(self, ctx:ParserRuleContext):
            super().copyFrom(ctx)


    class ValueExpressionDefaultContext(ValueExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.ValueExpressionContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def primaryExpression(self):
            return self.getTypedRuleContext(SqlBaseParser.PrimaryExpressionContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterValueExpressionDefault" ):
                listener.enterValueExpressionDefault(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitValueExpressionDefault" ):
                listener.exitValueExpressionDefault(self)


    class ComparisonContext(ValueExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.ValueExpressionContext
            super().__init__(parser)
            self.left = None # ValueExpressionContext
            self.right = None # ValueExpressionContext
            self.copyFrom(ctx)

        def comparisonOperator(self):
            return self.getTypedRuleContext(SqlBaseParser.ComparisonOperatorContext,0)

        def valueExpression(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ValueExpressionContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ValueExpressionContext,i)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterComparison" ):
                listener.enterComparison(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitComparison" ):
                listener.exitComparison(self)


    class ArithmeticBinaryContext(ValueExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.ValueExpressionContext
            super().__init__(parser)
            self.left = None # ValueExpressionContext
            self.operator = None # Token
            self.right = None # ValueExpressionContext
            self.copyFrom(ctx)

        def valueExpression(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ValueExpressionContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ValueExpressionContext,i)

        def ASTERISK(self):
            return self.getToken(SqlBaseParser.ASTERISK, 0)
        def SLASH(self):
            return self.getToken(SqlBaseParser.SLASH, 0)
        def PERCENT(self):
            return self.getToken(SqlBaseParser.PERCENT, 0)
        def DIV(self):
            return self.getToken(SqlBaseParser.DIV, 0)
        def PLUS(self):
            return self.getToken(SqlBaseParser.PLUS, 0)
        def MINUS(self):
            return self.getToken(SqlBaseParser.MINUS, 0)
        def CONCAT_PIPE(self):
            return self.getToken(SqlBaseParser.CONCAT_PIPE, 0)
        def AMPERSAND(self):
            return self.getToken(SqlBaseParser.AMPERSAND, 0)
        def HAT(self):
            return self.getToken(SqlBaseParser.HAT, 0)
        def PIPE(self):
            return self.getToken(SqlBaseParser.PIPE, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterArithmeticBinary" ):
                listener.enterArithmeticBinary(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitArithmeticBinary" ):
                listener.exitArithmeticBinary(self)


    class ArithmeticUnaryContext(ValueExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.ValueExpressionContext
            super().__init__(parser)
            self.operator = None # Token
            self.copyFrom(ctx)

        def valueExpression(self):
            return self.getTypedRuleContext(SqlBaseParser.ValueExpressionContext,0)

        def MINUS(self):
            return self.getToken(SqlBaseParser.MINUS, 0)
        def PLUS(self):
            return self.getToken(SqlBaseParser.PLUS, 0)
        def TILDE(self):
            return self.getToken(SqlBaseParser.TILDE, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterArithmeticUnary" ):
                listener.enterArithmeticUnary(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitArithmeticUnary" ):
                listener.exitArithmeticUnary(self)



    def valueExpression(self, _p:int=0):
        _parentctx = self._ctx
        _parentState = self.state
        localctx = SqlBaseParser.ValueExpressionContext(self, self._ctx, _parentState)
        _prevctx = localctx
        _startState = 192
        self.enterRecursionRule(localctx, 192, self.RULE_valueExpression, _p)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2434
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,315,self._ctx)
            if la_ == 1:
                localctx = SqlBaseParser.ValueExpressionDefaultContext(self, localctx)
                self._ctx = localctx
                _prevctx = localctx

                self.state = 2431
                self.primaryExpression(0)
                pass

            elif la_ == 2:
                localctx = SqlBaseParser.ArithmeticUnaryContext(self, localctx)
                self._ctx = localctx
                _prevctx = localctx
                self.state = 2432
                localctx.operator = self._input.LT(1)
                _la = self._input.LA(1)
                if not(((((_la - 272)) & ~0x3f) == 0 and ((1 << (_la - 272)) & ((1 << (SqlBaseParser.PLUS - 272)) | (1 << (SqlBaseParser.MINUS - 272)) | (1 << (SqlBaseParser.TILDE - 272)))) != 0)):
                    localctx.operator = self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                self.state = 2433
                self.valueExpression(7)
                pass


            self._ctx.stop = self._input.LT(-1)
            self.state = 2457
            self._errHandler.sync(self)
            _alt = self._interp.adaptivePredict(self._input,317,self._ctx)
            while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                if _alt==1:
                    if self._parseListeners is not None:
                        self.triggerExitRuleEvent()
                    _prevctx = localctx
                    self.state = 2455
                    self._errHandler.sync(self)
                    la_ = self._interp.adaptivePredict(self._input,316,self._ctx)
                    if la_ == 1:
                        localctx = SqlBaseParser.ArithmeticBinaryContext(self, SqlBaseParser.ValueExpressionContext(self, _parentctx, _parentState))
                        localctx.left = _prevctx
                        self.pushNewRecursionContext(localctx, _startState, self.RULE_valueExpression)
                        self.state = 2436
                        if not self.precpred(self._ctx, 6):
                            from antlr4.error.Errors import FailedPredicateException
                            raise FailedPredicateException(self, "self.precpred(self._ctx, 6)")
                        self.state = 2437
                        localctx.operator = self._input.LT(1)
                        _la = self._input.LA(1)
                        if not(((((_la - 274)) & ~0x3f) == 0 and ((1 << (_la - 274)) & ((1 << (SqlBaseParser.ASTERISK - 274)) | (1 << (SqlBaseParser.SLASH - 274)) | (1 << (SqlBaseParser.PERCENT - 274)) | (1 << (SqlBaseParser.DIV - 274)))) != 0)):
                            localctx.operator = self._errHandler.recoverInline(self)
                        else:
                            self._errHandler.reportMatch(self)
                            self.consume()
                        self.state = 2438
                        localctx.right = self.valueExpression(7)
                        pass

                    elif la_ == 2:
                        localctx = SqlBaseParser.ArithmeticBinaryContext(self, SqlBaseParser.ValueExpressionContext(self, _parentctx, _parentState))
                        localctx.left = _prevctx
                        self.pushNewRecursionContext(localctx, _startState, self.RULE_valueExpression)
                        self.state = 2439
                        if not self.precpred(self._ctx, 5):
                            from antlr4.error.Errors import FailedPredicateException
                            raise FailedPredicateException(self, "self.precpred(self._ctx, 5)")
                        self.state = 2440
                        localctx.operator = self._input.LT(1)
                        _la = self._input.LA(1)
                        if not(((((_la - 272)) & ~0x3f) == 0 and ((1 << (_la - 272)) & ((1 << (SqlBaseParser.PLUS - 272)) | (1 << (SqlBaseParser.MINUS - 272)) | (1 << (SqlBaseParser.CONCAT_PIPE - 272)))) != 0)):
                            localctx.operator = self._errHandler.recoverInline(self)
                        else:
                            self._errHandler.reportMatch(self)
                            self.consume()
                        self.state = 2441
                        localctx.right = self.valueExpression(6)
                        pass

                    elif la_ == 3:
                        localctx = SqlBaseParser.ArithmeticBinaryContext(self, SqlBaseParser.ValueExpressionContext(self, _parentctx, _parentState))
                        localctx.left = _prevctx
                        self.pushNewRecursionContext(localctx, _startState, self.RULE_valueExpression)
                        self.state = 2442
                        if not self.precpred(self._ctx, 4):
                            from antlr4.error.Errors import FailedPredicateException
                            raise FailedPredicateException(self, "self.precpred(self._ctx, 4)")
                        self.state = 2443
                        localctx.operator = self.match(SqlBaseParser.AMPERSAND)
                        self.state = 2444
                        localctx.right = self.valueExpression(5)
                        pass

                    elif la_ == 4:
                        localctx = SqlBaseParser.ArithmeticBinaryContext(self, SqlBaseParser.ValueExpressionContext(self, _parentctx, _parentState))
                        localctx.left = _prevctx
                        self.pushNewRecursionContext(localctx, _startState, self.RULE_valueExpression)
                        self.state = 2445
                        if not self.precpred(self._ctx, 3):
                            from antlr4.error.Errors import FailedPredicateException
                            raise FailedPredicateException(self, "self.precpred(self._ctx, 3)")
                        self.state = 2446
                        localctx.operator = self.match(SqlBaseParser.HAT)
                        self.state = 2447
                        localctx.right = self.valueExpression(4)
                        pass

                    elif la_ == 5:
                        localctx = SqlBaseParser.ArithmeticBinaryContext(self, SqlBaseParser.ValueExpressionContext(self, _parentctx, _parentState))
                        localctx.left = _prevctx
                        self.pushNewRecursionContext(localctx, _startState, self.RULE_valueExpression)
                        self.state = 2448
                        if not self.precpred(self._ctx, 2):
                            from antlr4.error.Errors import FailedPredicateException
                            raise FailedPredicateException(self, "self.precpred(self._ctx, 2)")
                        self.state = 2449
                        localctx.operator = self.match(SqlBaseParser.PIPE)
                        self.state = 2450
                        localctx.right = self.valueExpression(3)
                        pass

                    elif la_ == 6:
                        localctx = SqlBaseParser.ComparisonContext(self, SqlBaseParser.ValueExpressionContext(self, _parentctx, _parentState))
                        localctx.left = _prevctx
                        self.pushNewRecursionContext(localctx, _startState, self.RULE_valueExpression)
                        self.state = 2451
                        if not self.precpred(self._ctx, 1):
                            from antlr4.error.Errors import FailedPredicateException
                            raise FailedPredicateException(self, "self.precpred(self._ctx, 1)")
                        self.state = 2452
                        self.comparisonOperator()
                        self.state = 2453
                        localctx.right = self.valueExpression(2)
                        pass


                self.state = 2459
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,317,self._ctx)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.unrollRecursionContexts(_parentctx)
        return localctx

    class PrimaryExpressionContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser


        def getRuleIndex(self):
            return SqlBaseParser.RULE_primaryExpression


        def copyFrom(self, ctx:ParserRuleContext):
            super().copyFrom(ctx)


    class StructContext(PrimaryExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.PrimaryExpressionContext
            super().__init__(parser)
            self._namedExpression = None # NamedExpressionContext
            self.argument = list() # of NamedExpressionContexts
            self.copyFrom(ctx)

        def STRUCT(self):
            return self.getToken(SqlBaseParser.STRUCT, 0)
        def namedExpression(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.NamedExpressionContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.NamedExpressionContext,i)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterStruct" ):
                listener.enterStruct(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitStruct" ):
                listener.exitStruct(self)


    class DereferenceContext(PrimaryExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.PrimaryExpressionContext
            super().__init__(parser)
            self.base = None # PrimaryExpressionContext
            self.fieldName = None # IdentifierContext
            self.copyFrom(ctx)

        def primaryExpression(self):
            return self.getTypedRuleContext(SqlBaseParser.PrimaryExpressionContext,0)

        def identifier(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterDereference" ):
                listener.enterDereference(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitDereference" ):
                listener.exitDereference(self)


    class SimpleCaseContext(PrimaryExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.PrimaryExpressionContext
            super().__init__(parser)
            self.value = None # ExpressionContext
            self.elseExpression = None # ExpressionContext
            self.copyFrom(ctx)

        def CASE(self):
            return self.getToken(SqlBaseParser.CASE, 0)
        def END(self):
            return self.getToken(SqlBaseParser.END, 0)
        def expression(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ExpressionContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ExpressionContext,i)

        def whenClause(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.WhenClauseContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.WhenClauseContext,i)

        def ELSE(self):
            return self.getToken(SqlBaseParser.ELSE, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSimpleCase" ):
                listener.enterSimpleCase(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSimpleCase" ):
                listener.exitSimpleCase(self)


    class ColumnReferenceContext(PrimaryExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.PrimaryExpressionContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def identifier(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterColumnReference" ):
                listener.enterColumnReference(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitColumnReference" ):
                listener.exitColumnReference(self)


    class RowConstructorContext(PrimaryExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.PrimaryExpressionContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def namedExpression(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.NamedExpressionContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.NamedExpressionContext,i)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterRowConstructor" ):
                listener.enterRowConstructor(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitRowConstructor" ):
                listener.exitRowConstructor(self)


    class LastContext(PrimaryExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.PrimaryExpressionContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def LAST(self):
            return self.getToken(SqlBaseParser.LAST, 0)
        def expression(self):
            return self.getTypedRuleContext(SqlBaseParser.ExpressionContext,0)

        def IGNORE(self):
            return self.getToken(SqlBaseParser.IGNORE, 0)
        def NULLS(self):
            return self.getToken(SqlBaseParser.NULLS, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterLast" ):
                listener.enterLast(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitLast" ):
                listener.exitLast(self)


    class StarContext(PrimaryExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.PrimaryExpressionContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def ASTERISK(self):
            return self.getToken(SqlBaseParser.ASTERISK, 0)
        def qualifiedName(self):
            return self.getTypedRuleContext(SqlBaseParser.QualifiedNameContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterStar" ):
                listener.enterStar(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitStar" ):
                listener.exitStar(self)


    class OverlayContext(PrimaryExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.PrimaryExpressionContext
            super().__init__(parser)
            self.input = None # ValueExpressionContext
            self.replace = None # ValueExpressionContext
            self.position = None # ValueExpressionContext
            self.length = None # ValueExpressionContext
            self.copyFrom(ctx)

        def OVERLAY(self):
            return self.getToken(SqlBaseParser.OVERLAY, 0)
        def PLACING(self):
            return self.getToken(SqlBaseParser.PLACING, 0)
        def FROM(self):
            return self.getToken(SqlBaseParser.FROM, 0)
        def valueExpression(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ValueExpressionContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ValueExpressionContext,i)

        def FOR(self):
            return self.getToken(SqlBaseParser.FOR, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterOverlay" ):
                listener.enterOverlay(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitOverlay" ):
                listener.exitOverlay(self)


    class SubscriptContext(PrimaryExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.PrimaryExpressionContext
            super().__init__(parser)
            self.value = None # PrimaryExpressionContext
            self.index = None # ValueExpressionContext
            self.copyFrom(ctx)

        def primaryExpression(self):
            return self.getTypedRuleContext(SqlBaseParser.PrimaryExpressionContext,0)

        def valueExpression(self):
            return self.getTypedRuleContext(SqlBaseParser.ValueExpressionContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSubscript" ):
                listener.enterSubscript(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSubscript" ):
                listener.exitSubscript(self)


    class SubqueryExpressionContext(PrimaryExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.PrimaryExpressionContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def query(self):
            return self.getTypedRuleContext(SqlBaseParser.QueryContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSubqueryExpression" ):
                listener.enterSubqueryExpression(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSubqueryExpression" ):
                listener.exitSubqueryExpression(self)


    class SubstringContext(PrimaryExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.PrimaryExpressionContext
            super().__init__(parser)
            self.str = None # ValueExpressionContext
            self.pos = None # ValueExpressionContext
            self.len = None # ValueExpressionContext
            self.copyFrom(ctx)

        def SUBSTR(self):
            return self.getToken(SqlBaseParser.SUBSTR, 0)
        def SUBSTRING(self):
            return self.getToken(SqlBaseParser.SUBSTRING, 0)
        def valueExpression(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ValueExpressionContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ValueExpressionContext,i)

        def FROM(self):
            return self.getToken(SqlBaseParser.FROM, 0)
        def FOR(self):
            return self.getToken(SqlBaseParser.FOR, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSubstring" ):
                listener.enterSubstring(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSubstring" ):
                listener.exitSubstring(self)


    class CurrentDatetimeContext(PrimaryExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.PrimaryExpressionContext
            super().__init__(parser)
            self.name = None # Token
            self.copyFrom(ctx)

        def CURRENT_DATE(self):
            return self.getToken(SqlBaseParser.CURRENT_DATE, 0)
        def CURRENT_TIMESTAMP(self):
            return self.getToken(SqlBaseParser.CURRENT_TIMESTAMP, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterCurrentDatetime" ):
                listener.enterCurrentDatetime(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitCurrentDatetime" ):
                listener.exitCurrentDatetime(self)


    class CastContext(PrimaryExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.PrimaryExpressionContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def CAST(self):
            return self.getToken(SqlBaseParser.CAST, 0)
        def expression(self):
            return self.getTypedRuleContext(SqlBaseParser.ExpressionContext,0)

        def AS(self):
            return self.getToken(SqlBaseParser.AS, 0)
        def dataType(self):
            return self.getTypedRuleContext(SqlBaseParser.DataTypeContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterCast" ):
                listener.enterCast(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitCast" ):
                listener.exitCast(self)


    class ConstantDefaultContext(PrimaryExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.PrimaryExpressionContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def constant(self):
            return self.getTypedRuleContext(SqlBaseParser.ConstantContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterConstantDefault" ):
                listener.enterConstantDefault(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitConstantDefault" ):
                listener.exitConstantDefault(self)


    class LambdaContext(PrimaryExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.PrimaryExpressionContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def identifier(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.IdentifierContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.IdentifierContext,i)

        def expression(self):
            return self.getTypedRuleContext(SqlBaseParser.ExpressionContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterLambda" ):
                listener.enterLambda(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitLambda" ):
                listener.exitLambda(self)


    class ParenthesizedExpressionContext(PrimaryExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.PrimaryExpressionContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def expression(self):
            return self.getTypedRuleContext(SqlBaseParser.ExpressionContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterParenthesizedExpression" ):
                listener.enterParenthesizedExpression(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitParenthesizedExpression" ):
                listener.exitParenthesizedExpression(self)


    class ExtractContext(PrimaryExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.PrimaryExpressionContext
            super().__init__(parser)
            self.field = None # IdentifierContext
            self.source = None # ValueExpressionContext
            self.copyFrom(ctx)

        def EXTRACT(self):
            return self.getToken(SqlBaseParser.EXTRACT, 0)
        def FROM(self):
            return self.getToken(SqlBaseParser.FROM, 0)
        def identifier(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierContext,0)

        def valueExpression(self):
            return self.getTypedRuleContext(SqlBaseParser.ValueExpressionContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterExtract" ):
                listener.enterExtract(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitExtract" ):
                listener.exitExtract(self)


    class TrimContext(PrimaryExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.PrimaryExpressionContext
            super().__init__(parser)
            self.trimOption = None # Token
            self.trimStr = None # ValueExpressionContext
            self.srcStr = None # ValueExpressionContext
            self.copyFrom(ctx)

        def TRIM(self):
            return self.getToken(SqlBaseParser.TRIM, 0)
        def FROM(self):
            return self.getToken(SqlBaseParser.FROM, 0)
        def valueExpression(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ValueExpressionContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ValueExpressionContext,i)

        def BOTH(self):
            return self.getToken(SqlBaseParser.BOTH, 0)
        def LEADING(self):
            return self.getToken(SqlBaseParser.LEADING, 0)
        def TRAILING(self):
            return self.getToken(SqlBaseParser.TRAILING, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterTrim" ):
                listener.enterTrim(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitTrim" ):
                listener.exitTrim(self)


    class FunctionCallContext(PrimaryExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.PrimaryExpressionContext
            super().__init__(parser)
            self._expression = None # ExpressionContext
            self.argument = list() # of ExpressionContexts
            self.where = None # BooleanExpressionContext
            self.copyFrom(ctx)

        def functionName(self):
            return self.getTypedRuleContext(SqlBaseParser.FunctionNameContext,0)

        def FILTER(self):
            return self.getToken(SqlBaseParser.FILTER, 0)
        def WHERE(self):
            return self.getToken(SqlBaseParser.WHERE, 0)
        def OVER(self):
            return self.getToken(SqlBaseParser.OVER, 0)
        def windowSpec(self):
            return self.getTypedRuleContext(SqlBaseParser.WindowSpecContext,0)

        def expression(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ExpressionContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ExpressionContext,i)

        def booleanExpression(self):
            return self.getTypedRuleContext(SqlBaseParser.BooleanExpressionContext,0)

        def setQuantifier(self):
            return self.getTypedRuleContext(SqlBaseParser.SetQuantifierContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterFunctionCall" ):
                listener.enterFunctionCall(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitFunctionCall" ):
                listener.exitFunctionCall(self)


    class SearchedCaseContext(PrimaryExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.PrimaryExpressionContext
            super().__init__(parser)
            self.elseExpression = None # ExpressionContext
            self.copyFrom(ctx)

        def CASE(self):
            return self.getToken(SqlBaseParser.CASE, 0)
        def END(self):
            return self.getToken(SqlBaseParser.END, 0)
        def whenClause(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.WhenClauseContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.WhenClauseContext,i)

        def ELSE(self):
            return self.getToken(SqlBaseParser.ELSE, 0)
        def expression(self):
            return self.getTypedRuleContext(SqlBaseParser.ExpressionContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSearchedCase" ):
                listener.enterSearchedCase(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSearchedCase" ):
                listener.exitSearchedCase(self)


    class PositionContext(PrimaryExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.PrimaryExpressionContext
            super().__init__(parser)
            self.substr = None # ValueExpressionContext
            self.str = None # ValueExpressionContext
            self.copyFrom(ctx)

        def POSITION(self):
            return self.getToken(SqlBaseParser.POSITION, 0)
        def IN(self):
            return self.getToken(SqlBaseParser.IN, 0)
        def valueExpression(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ValueExpressionContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ValueExpressionContext,i)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterPosition" ):
                listener.enterPosition(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitPosition" ):
                listener.exitPosition(self)


    class FirstContext(PrimaryExpressionContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.PrimaryExpressionContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def FIRST(self):
            return self.getToken(SqlBaseParser.FIRST, 0)
        def expression(self):
            return self.getTypedRuleContext(SqlBaseParser.ExpressionContext,0)

        def IGNORE(self):
            return self.getToken(SqlBaseParser.IGNORE, 0)
        def NULLS(self):
            return self.getToken(SqlBaseParser.NULLS, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterFirst" ):
                listener.enterFirst(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitFirst" ):
                listener.exitFirst(self)



    def primaryExpression(self, _p:int=0):
        _parentctx = self._ctx
        _parentState = self.state
        localctx = SqlBaseParser.PrimaryExpressionContext(self, self._ctx, _parentState)
        _prevctx = localctx
        _startState = 194
        self.enterRecursionRule(localctx, 194, self.RULE_primaryExpression, _p)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2644
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,337,self._ctx)
            if la_ == 1:
                localctx = SqlBaseParser.CurrentDatetimeContext(self, localctx)
                self._ctx = localctx
                _prevctx = localctx

                self.state = 2461
                localctx.name = self._input.LT(1)
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.CURRENT_DATE or _la==SqlBaseParser.CURRENT_TIMESTAMP):
                    localctx.name = self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                pass

            elif la_ == 2:
                localctx = SqlBaseParser.SearchedCaseContext(self, localctx)
                self._ctx = localctx
                _prevctx = localctx
                self.state = 2462
                self.match(SqlBaseParser.CASE)
                self.state = 2464
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while True:
                    self.state = 2463
                    self.whenClause()
                    self.state = 2466
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    if not (_la==SqlBaseParser.WHEN):
                        break

                self.state = 2470
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.ELSE:
                    self.state = 2468
                    self.match(SqlBaseParser.ELSE)
                    self.state = 2469
                    localctx.elseExpression = self.expression()


                self.state = 2472
                self.match(SqlBaseParser.END)
                pass

            elif la_ == 3:
                localctx = SqlBaseParser.SimpleCaseContext(self, localctx)
                self._ctx = localctx
                _prevctx = localctx
                self.state = 2474
                self.match(SqlBaseParser.CASE)
                self.state = 2475
                localctx.value = self.expression()
                self.state = 2477
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while True:
                    self.state = 2476
                    self.whenClause()
                    self.state = 2479
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    if not (_la==SqlBaseParser.WHEN):
                        break

                self.state = 2483
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.ELSE:
                    self.state = 2481
                    self.match(SqlBaseParser.ELSE)
                    self.state = 2482
                    localctx.elseExpression = self.expression()


                self.state = 2485
                self.match(SqlBaseParser.END)
                pass

            elif la_ == 4:
                localctx = SqlBaseParser.CastContext(self, localctx)
                self._ctx = localctx
                _prevctx = localctx
                self.state = 2487
                self.match(SqlBaseParser.CAST)
                self.state = 2488
                self.match(SqlBaseParser.T__1)
                self.state = 2489
                self.expression()
                self.state = 2490
                self.match(SqlBaseParser.AS)
                self.state = 2491
                self.dataType()
                self.state = 2492
                self.match(SqlBaseParser.T__2)
                pass

            elif la_ == 5:
                localctx = SqlBaseParser.StructContext(self, localctx)
                self._ctx = localctx
                _prevctx = localctx
                self.state = 2494
                self.match(SqlBaseParser.STRUCT)
                self.state = 2495
                self.match(SqlBaseParser.T__1)
                self.state = 2504
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,323,self._ctx)
                if la_ == 1:
                    self.state = 2496
                    localctx._namedExpression = self.namedExpression()
                    localctx.argument.append(localctx._namedExpression)
                    self.state = 2501
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    while _la==SqlBaseParser.T__3:
                        self.state = 2497
                        self.match(SqlBaseParser.T__3)
                        self.state = 2498
                        localctx._namedExpression = self.namedExpression()
                        localctx.argument.append(localctx._namedExpression)
                        self.state = 2503
                        self._errHandler.sync(self)
                        _la = self._input.LA(1)



                self.state = 2506
                self.match(SqlBaseParser.T__2)
                pass

            elif la_ == 6:
                localctx = SqlBaseParser.FirstContext(self, localctx)
                self._ctx = localctx
                _prevctx = localctx
                self.state = 2507
                self.match(SqlBaseParser.FIRST)
                self.state = 2508
                self.match(SqlBaseParser.T__1)
                self.state = 2509
                self.expression()
                self.state = 2512
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.IGNORE:
                    self.state = 2510
                    self.match(SqlBaseParser.IGNORE)
                    self.state = 2511
                    self.match(SqlBaseParser.NULLS)


                self.state = 2514
                self.match(SqlBaseParser.T__2)
                pass

            elif la_ == 7:
                localctx = SqlBaseParser.LastContext(self, localctx)
                self._ctx = localctx
                _prevctx = localctx
                self.state = 2516
                self.match(SqlBaseParser.LAST)
                self.state = 2517
                self.match(SqlBaseParser.T__1)
                self.state = 2518
                self.expression()
                self.state = 2521
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.IGNORE:
                    self.state = 2519
                    self.match(SqlBaseParser.IGNORE)
                    self.state = 2520
                    self.match(SqlBaseParser.NULLS)


                self.state = 2523
                self.match(SqlBaseParser.T__2)
                pass

            elif la_ == 8:
                localctx = SqlBaseParser.PositionContext(self, localctx)
                self._ctx = localctx
                _prevctx = localctx
                self.state = 2525
                self.match(SqlBaseParser.POSITION)
                self.state = 2526
                self.match(SqlBaseParser.T__1)
                self.state = 2527
                localctx.substr = self.valueExpression(0)
                self.state = 2528
                self.match(SqlBaseParser.IN)
                self.state = 2529
                localctx.str = self.valueExpression(0)
                self.state = 2530
                self.match(SqlBaseParser.T__2)
                pass

            elif la_ == 9:
                localctx = SqlBaseParser.ConstantDefaultContext(self, localctx)
                self._ctx = localctx
                _prevctx = localctx
                self.state = 2532
                self.constant()
                pass

            elif la_ == 10:
                localctx = SqlBaseParser.StarContext(self, localctx)
                self._ctx = localctx
                _prevctx = localctx
                self.state = 2533
                self.match(SqlBaseParser.ASTERISK)
                pass

            elif la_ == 11:
                localctx = SqlBaseParser.StarContext(self, localctx)
                self._ctx = localctx
                _prevctx = localctx
                self.state = 2534
                self.qualifiedName()
                self.state = 2535
                self.match(SqlBaseParser.T__4)
                self.state = 2536
                self.match(SqlBaseParser.ASTERISK)
                pass

            elif la_ == 12:
                localctx = SqlBaseParser.RowConstructorContext(self, localctx)
                self._ctx = localctx
                _prevctx = localctx
                self.state = 2538
                self.match(SqlBaseParser.T__1)
                self.state = 2539
                self.namedExpression()
                self.state = 2542
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while True:
                    self.state = 2540
                    self.match(SqlBaseParser.T__3)
                    self.state = 2541
                    self.namedExpression()
                    self.state = 2544
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    if not (_la==SqlBaseParser.T__3):
                        break

                self.state = 2546
                self.match(SqlBaseParser.T__2)
                pass

            elif la_ == 13:
                localctx = SqlBaseParser.SubqueryExpressionContext(self, localctx)
                self._ctx = localctx
                _prevctx = localctx
                self.state = 2548
                self.match(SqlBaseParser.T__1)
                self.state = 2549
                self.query()
                self.state = 2550
                self.match(SqlBaseParser.T__2)
                pass

            elif la_ == 14:
                localctx = SqlBaseParser.FunctionCallContext(self, localctx)
                self._ctx = localctx
                _prevctx = localctx
                self.state = 2552
                self.functionName()
                self.state = 2553
                self.match(SqlBaseParser.T__1)
                self.state = 2565
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,329,self._ctx)
                if la_ == 1:
                    self.state = 2555
                    self._errHandler.sync(self)
                    la_ = self._interp.adaptivePredict(self._input,327,self._ctx)
                    if la_ == 1:
                        self.state = 2554
                        self.setQuantifier()


                    self.state = 2557
                    localctx._expression = self.expression()
                    localctx.argument.append(localctx._expression)
                    self.state = 2562
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    while _la==SqlBaseParser.T__3:
                        self.state = 2558
                        self.match(SqlBaseParser.T__3)
                        self.state = 2559
                        localctx._expression = self.expression()
                        localctx.argument.append(localctx._expression)
                        self.state = 2564
                        self._errHandler.sync(self)
                        _la = self._input.LA(1)



                self.state = 2567
                self.match(SqlBaseParser.T__2)
                self.state = 2574
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,330,self._ctx)
                if la_ == 1:
                    self.state = 2568
                    self.match(SqlBaseParser.FILTER)
                    self.state = 2569
                    self.match(SqlBaseParser.T__1)
                    self.state = 2570
                    self.match(SqlBaseParser.WHERE)
                    self.state = 2571
                    localctx.where = self.booleanExpression(0)
                    self.state = 2572
                    self.match(SqlBaseParser.T__2)


                self.state = 2578
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,331,self._ctx)
                if la_ == 1:
                    self.state = 2576
                    self.match(SqlBaseParser.OVER)
                    self.state = 2577
                    self.windowSpec()


                pass

            elif la_ == 15:
                localctx = SqlBaseParser.LambdaContext(self, localctx)
                self._ctx = localctx
                _prevctx = localctx
                self.state = 2580
                self.identifier()
                self.state = 2581
                self.match(SqlBaseParser.T__7)
                self.state = 2582
                self.expression()
                pass

            elif la_ == 16:
                localctx = SqlBaseParser.LambdaContext(self, localctx)
                self._ctx = localctx
                _prevctx = localctx
                self.state = 2584
                self.match(SqlBaseParser.T__1)
                self.state = 2585
                self.identifier()
                self.state = 2588
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while True:
                    self.state = 2586
                    self.match(SqlBaseParser.T__3)
                    self.state = 2587
                    self.identifier()
                    self.state = 2590
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    if not (_la==SqlBaseParser.T__3):
                        break

                self.state = 2592
                self.match(SqlBaseParser.T__2)
                self.state = 2593
                self.match(SqlBaseParser.T__7)
                self.state = 2594
                self.expression()
                pass

            elif la_ == 17:
                localctx = SqlBaseParser.ColumnReferenceContext(self, localctx)
                self._ctx = localctx
                _prevctx = localctx
                self.state = 2596
                self.identifier()
                pass

            elif la_ == 18:
                localctx = SqlBaseParser.ParenthesizedExpressionContext(self, localctx)
                self._ctx = localctx
                _prevctx = localctx
                self.state = 2597
                self.match(SqlBaseParser.T__1)
                self.state = 2598
                self.expression()
                self.state = 2599
                self.match(SqlBaseParser.T__2)
                pass

            elif la_ == 19:
                localctx = SqlBaseParser.ExtractContext(self, localctx)
                self._ctx = localctx
                _prevctx = localctx
                self.state = 2601
                self.match(SqlBaseParser.EXTRACT)
                self.state = 2602
                self.match(SqlBaseParser.T__1)
                self.state = 2603
                localctx.field = self.identifier()
                self.state = 2604
                self.match(SqlBaseParser.FROM)
                self.state = 2605
                localctx.source = self.valueExpression(0)
                self.state = 2606
                self.match(SqlBaseParser.T__2)
                pass

            elif la_ == 20:
                localctx = SqlBaseParser.SubstringContext(self, localctx)
                self._ctx = localctx
                _prevctx = localctx
                self.state = 2608
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.SUBSTR or _la==SqlBaseParser.SUBSTRING):
                    self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                self.state = 2609
                self.match(SqlBaseParser.T__1)
                self.state = 2610
                localctx.str = self.valueExpression(0)
                self.state = 2611
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.T__3 or _la==SqlBaseParser.FROM):
                    self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                self.state = 2612
                localctx.pos = self.valueExpression(0)
                self.state = 2615
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.T__3 or _la==SqlBaseParser.FOR:
                    self.state = 2613
                    _la = self._input.LA(1)
                    if not(_la==SqlBaseParser.T__3 or _la==SqlBaseParser.FOR):
                        self._errHandler.recoverInline(self)
                    else:
                        self._errHandler.reportMatch(self)
                        self.consume()
                    self.state = 2614
                    localctx.len = self.valueExpression(0)


                self.state = 2617
                self.match(SqlBaseParser.T__2)
                pass

            elif la_ == 21:
                localctx = SqlBaseParser.TrimContext(self, localctx)
                self._ctx = localctx
                _prevctx = localctx
                self.state = 2619
                self.match(SqlBaseParser.TRIM)
                self.state = 2620
                self.match(SqlBaseParser.T__1)
                self.state = 2622
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,334,self._ctx)
                if la_ == 1:
                    self.state = 2621
                    localctx.trimOption = self._input.LT(1)
                    _la = self._input.LA(1)
                    if not(_la==SqlBaseParser.BOTH or _la==SqlBaseParser.LEADING or _la==SqlBaseParser.TRAILING):
                        localctx.trimOption = self._errHandler.recoverInline(self)
                    else:
                        self._errHandler.reportMatch(self)
                        self.consume()


                self.state = 2625
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,335,self._ctx)
                if la_ == 1:
                    self.state = 2624
                    localctx.trimStr = self.valueExpression(0)


                self.state = 2627
                self.match(SqlBaseParser.FROM)
                self.state = 2628
                localctx.srcStr = self.valueExpression(0)
                self.state = 2629
                self.match(SqlBaseParser.T__2)
                pass

            elif la_ == 22:
                localctx = SqlBaseParser.OverlayContext(self, localctx)
                self._ctx = localctx
                _prevctx = localctx
                self.state = 2631
                self.match(SqlBaseParser.OVERLAY)
                self.state = 2632
                self.match(SqlBaseParser.T__1)
                self.state = 2633
                localctx.input = self.valueExpression(0)
                self.state = 2634
                self.match(SqlBaseParser.PLACING)
                self.state = 2635
                localctx.replace = self.valueExpression(0)
                self.state = 2636
                self.match(SqlBaseParser.FROM)
                self.state = 2637
                localctx.position = self.valueExpression(0)
                self.state = 2640
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.FOR:
                    self.state = 2638
                    self.match(SqlBaseParser.FOR)
                    self.state = 2639
                    localctx.length = self.valueExpression(0)


                self.state = 2642
                self.match(SqlBaseParser.T__2)
                pass


            self._ctx.stop = self._input.LT(-1)
            self.state = 2656
            self._errHandler.sync(self)
            _alt = self._interp.adaptivePredict(self._input,339,self._ctx)
            while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                if _alt==1:
                    if self._parseListeners is not None:
                        self.triggerExitRuleEvent()
                    _prevctx = localctx
                    self.state = 2654
                    self._errHandler.sync(self)
                    la_ = self._interp.adaptivePredict(self._input,338,self._ctx)
                    if la_ == 1:
                        localctx = SqlBaseParser.SubscriptContext(self, SqlBaseParser.PrimaryExpressionContext(self, _parentctx, _parentState))
                        localctx.value = _prevctx
                        self.pushNewRecursionContext(localctx, _startState, self.RULE_primaryExpression)
                        self.state = 2646
                        if not self.precpred(self._ctx, 8):
                            from antlr4.error.Errors import FailedPredicateException
                            raise FailedPredicateException(self, "self.precpred(self._ctx, 8)")
                        self.state = 2647
                        self.match(SqlBaseParser.T__8)
                        self.state = 2648
                        localctx.index = self.valueExpression(0)
                        self.state = 2649
                        self.match(SqlBaseParser.T__9)
                        pass

                    elif la_ == 2:
                        localctx = SqlBaseParser.DereferenceContext(self, SqlBaseParser.PrimaryExpressionContext(self, _parentctx, _parentState))
                        localctx.base = _prevctx
                        self.pushNewRecursionContext(localctx, _startState, self.RULE_primaryExpression)
                        self.state = 2651
                        if not self.precpred(self._ctx, 6):
                            from antlr4.error.Errors import FailedPredicateException
                            raise FailedPredicateException(self, "self.precpred(self._ctx, 6)")
                        self.state = 2652
                        self.match(SqlBaseParser.T__4)
                        self.state = 2653
                        localctx.fieldName = self.identifier()
                        pass


                self.state = 2658
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,339,self._ctx)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.unrollRecursionContexts(_parentctx)
        return localctx

    class ConstantContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser


        def getRuleIndex(self):
            return SqlBaseParser.RULE_constant


        def copyFrom(self, ctx:ParserRuleContext):
            super().copyFrom(ctx)



    class NullLiteralContext(ConstantContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.ConstantContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def NULL(self):
            return self.getToken(SqlBaseParser.NULL, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterNullLiteral" ):
                listener.enterNullLiteral(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitNullLiteral" ):
                listener.exitNullLiteral(self)


    class StringLiteralContext(ConstantContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.ConstantContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def STRING(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.STRING)
            else:
                return self.getToken(SqlBaseParser.STRING, i)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterStringLiteral" ):
                listener.enterStringLiteral(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitStringLiteral" ):
                listener.exitStringLiteral(self)


    class TypeConstructorContext(ConstantContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.ConstantContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def identifier(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierContext,0)

        def STRING(self):
            return self.getToken(SqlBaseParser.STRING, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterTypeConstructor" ):
                listener.enterTypeConstructor(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitTypeConstructor" ):
                listener.exitTypeConstructor(self)


    class IntervalLiteralContext(ConstantContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.ConstantContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def interval(self):
            return self.getTypedRuleContext(SqlBaseParser.IntervalContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterIntervalLiteral" ):
                listener.enterIntervalLiteral(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitIntervalLiteral" ):
                listener.exitIntervalLiteral(self)


    class NumericLiteralContext(ConstantContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.ConstantContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def number(self):
            return self.getTypedRuleContext(SqlBaseParser.NumberContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterNumericLiteral" ):
                listener.enterNumericLiteral(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitNumericLiteral" ):
                listener.exitNumericLiteral(self)


    class BooleanLiteralContext(ConstantContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.ConstantContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def booleanValue(self):
            return self.getTypedRuleContext(SqlBaseParser.BooleanValueContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterBooleanLiteral" ):
                listener.enterBooleanLiteral(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitBooleanLiteral" ):
                listener.exitBooleanLiteral(self)



    def constant(self):

        localctx = SqlBaseParser.ConstantContext(self, self._ctx, self.state)
        self.enterRule(localctx, 196, self.RULE_constant)
        try:
            self.state = 2671
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,341,self._ctx)
            if la_ == 1:
                localctx = SqlBaseParser.NullLiteralContext(self, localctx)
                self.enterOuterAlt(localctx, 1)
                self.state = 2659
                self.match(SqlBaseParser.NULL)
                pass

            elif la_ == 2:
                localctx = SqlBaseParser.IntervalLiteralContext(self, localctx)
                self.enterOuterAlt(localctx, 2)
                self.state = 2660
                self.interval()
                pass

            elif la_ == 3:
                localctx = SqlBaseParser.TypeConstructorContext(self, localctx)
                self.enterOuterAlt(localctx, 3)
                self.state = 2661
                self.identifier()
                self.state = 2662
                self.match(SqlBaseParser.STRING)
                pass

            elif la_ == 4:
                localctx = SqlBaseParser.NumericLiteralContext(self, localctx)
                self.enterOuterAlt(localctx, 4)
                self.state = 2664
                self.number()
                pass

            elif la_ == 5:
                localctx = SqlBaseParser.BooleanLiteralContext(self, localctx)
                self.enterOuterAlt(localctx, 5)
                self.state = 2665
                self.booleanValue()
                pass

            elif la_ == 6:
                localctx = SqlBaseParser.StringLiteralContext(self, localctx)
                self.enterOuterAlt(localctx, 6)
                self.state = 2667
                self._errHandler.sync(self)
                _alt = 1
                while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                    if _alt == 1:
                        self.state = 2666
                        self.match(SqlBaseParser.STRING)

                    else:
                        raise NoViableAltException(self)
                    self.state = 2669
                    self._errHandler.sync(self)
                    _alt = self._interp.adaptivePredict(self._input,340,self._ctx)

                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class ComparisonOperatorContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def EQ(self):
            return self.getToken(SqlBaseParser.EQ, 0)

        def NEQ(self):
            return self.getToken(SqlBaseParser.NEQ, 0)

        def NEQJ(self):
            return self.getToken(SqlBaseParser.NEQJ, 0)

        def LT(self):
            return self.getToken(SqlBaseParser.LT, 0)

        def LTE(self):
            return self.getToken(SqlBaseParser.LTE, 0)

        def GT(self):
            return self.getToken(SqlBaseParser.GT, 0)

        def GTE(self):
            return self.getToken(SqlBaseParser.GTE, 0)

        def NSEQ(self):
            return self.getToken(SqlBaseParser.NSEQ, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_comparisonOperator

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterComparisonOperator" ):
                listener.enterComparisonOperator(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitComparisonOperator" ):
                listener.exitComparisonOperator(self)




    def comparisonOperator(self):

        localctx = SqlBaseParser.ComparisonOperatorContext(self, self._ctx, self.state)
        self.enterRule(localctx, 198, self.RULE_comparisonOperator)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2673
            _la = self._input.LA(1)
            if not(((((_la - 264)) & ~0x3f) == 0 and ((1 << (_la - 264)) & ((1 << (SqlBaseParser.EQ - 264)) | (1 << (SqlBaseParser.NSEQ - 264)) | (1 << (SqlBaseParser.NEQ - 264)) | (1 << (SqlBaseParser.NEQJ - 264)) | (1 << (SqlBaseParser.LT - 264)) | (1 << (SqlBaseParser.LTE - 264)) | (1 << (SqlBaseParser.GT - 264)) | (1 << (SqlBaseParser.GTE - 264)))) != 0)):
                self._errHandler.recoverInline(self)
            else:
                self._errHandler.reportMatch(self)
                self.consume()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class ArithmeticOperatorContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def PLUS(self):
            return self.getToken(SqlBaseParser.PLUS, 0)

        def MINUS(self):
            return self.getToken(SqlBaseParser.MINUS, 0)

        def ASTERISK(self):
            return self.getToken(SqlBaseParser.ASTERISK, 0)

        def SLASH(self):
            return self.getToken(SqlBaseParser.SLASH, 0)

        def PERCENT(self):
            return self.getToken(SqlBaseParser.PERCENT, 0)

        def DIV(self):
            return self.getToken(SqlBaseParser.DIV, 0)

        def TILDE(self):
            return self.getToken(SqlBaseParser.TILDE, 0)

        def AMPERSAND(self):
            return self.getToken(SqlBaseParser.AMPERSAND, 0)

        def PIPE(self):
            return self.getToken(SqlBaseParser.PIPE, 0)

        def CONCAT_PIPE(self):
            return self.getToken(SqlBaseParser.CONCAT_PIPE, 0)

        def HAT(self):
            return self.getToken(SqlBaseParser.HAT, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_arithmeticOperator

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterArithmeticOperator" ):
                listener.enterArithmeticOperator(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitArithmeticOperator" ):
                listener.exitArithmeticOperator(self)




    def arithmeticOperator(self):

        localctx = SqlBaseParser.ArithmeticOperatorContext(self, self._ctx, self.state)
        self.enterRule(localctx, 200, self.RULE_arithmeticOperator)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2675
            _la = self._input.LA(1)
            if not(((((_la - 272)) & ~0x3f) == 0 and ((1 << (_la - 272)) & ((1 << (SqlBaseParser.PLUS - 272)) | (1 << (SqlBaseParser.MINUS - 272)) | (1 << (SqlBaseParser.ASTERISK - 272)) | (1 << (SqlBaseParser.SLASH - 272)) | (1 << (SqlBaseParser.PERCENT - 272)) | (1 << (SqlBaseParser.DIV - 272)) | (1 << (SqlBaseParser.TILDE - 272)) | (1 << (SqlBaseParser.AMPERSAND - 272)) | (1 << (SqlBaseParser.PIPE - 272)) | (1 << (SqlBaseParser.CONCAT_PIPE - 272)) | (1 << (SqlBaseParser.HAT - 272)))) != 0)):
                self._errHandler.recoverInline(self)
            else:
                self._errHandler.reportMatch(self)
                self.consume()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class PredicateOperatorContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def OR(self):
            return self.getToken(SqlBaseParser.OR, 0)

        def AND(self):
            return self.getToken(SqlBaseParser.AND, 0)

        def IN(self):
            return self.getToken(SqlBaseParser.IN, 0)

        def NOT(self):
            return self.getToken(SqlBaseParser.NOT, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_predicateOperator

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterPredicateOperator" ):
                listener.enterPredicateOperator(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitPredicateOperator" ):
                listener.exitPredicateOperator(self)




    def predicateOperator(self):

        localctx = SqlBaseParser.PredicateOperatorContext(self, self._ctx, self.state)
        self.enterRule(localctx, 202, self.RULE_predicateOperator)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2677
            _la = self._input.LA(1)
            if not(_la==SqlBaseParser.AND or ((((_la - 113)) & ~0x3f) == 0 and ((1 << (_la - 113)) & ((1 << (SqlBaseParser.IN - 113)) | (1 << (SqlBaseParser.NOT - 113)) | (1 << (SqlBaseParser.OR - 113)))) != 0)):
                self._errHandler.recoverInline(self)
            else:
                self._errHandler.reportMatch(self)
                self.consume()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class BooleanValueContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def TRUE(self):
            return self.getToken(SqlBaseParser.TRUE, 0)

        def FALSE(self):
            return self.getToken(SqlBaseParser.FALSE, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_booleanValue

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterBooleanValue" ):
                listener.enterBooleanValue(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitBooleanValue" ):
                listener.exitBooleanValue(self)




    def booleanValue(self):

        localctx = SqlBaseParser.BooleanValueContext(self, self._ctx, self.state)
        self.enterRule(localctx, 204, self.RULE_booleanValue)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2679
            _la = self._input.LA(1)
            if not(_la==SqlBaseParser.FALSE or _la==SqlBaseParser.TRUE):
                self._errHandler.recoverInline(self)
            else:
                self._errHandler.reportMatch(self)
                self.consume()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class IntervalContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def INTERVAL(self):
            return self.getToken(SqlBaseParser.INTERVAL, 0)

        def errorCapturingMultiUnitsInterval(self):
            return self.getTypedRuleContext(SqlBaseParser.ErrorCapturingMultiUnitsIntervalContext,0)


        def errorCapturingUnitToUnitInterval(self):
            return self.getTypedRuleContext(SqlBaseParser.ErrorCapturingUnitToUnitIntervalContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_interval

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterInterval" ):
                listener.enterInterval(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitInterval" ):
                listener.exitInterval(self)




    def interval(self):

        localctx = SqlBaseParser.IntervalContext(self, self._ctx, self.state)
        self.enterRule(localctx, 206, self.RULE_interval)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2681
            self.match(SqlBaseParser.INTERVAL)
            self.state = 2684
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,342,self._ctx)
            if la_ == 1:
                self.state = 2682
                self.errorCapturingMultiUnitsInterval()

            elif la_ == 2:
                self.state = 2683
                self.errorCapturingUnitToUnitInterval()


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class ErrorCapturingMultiUnitsIntervalContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def multiUnitsInterval(self):
            return self.getTypedRuleContext(SqlBaseParser.MultiUnitsIntervalContext,0)


        def unitToUnitInterval(self):
            return self.getTypedRuleContext(SqlBaseParser.UnitToUnitIntervalContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_errorCapturingMultiUnitsInterval

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterErrorCapturingMultiUnitsInterval" ):
                listener.enterErrorCapturingMultiUnitsInterval(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitErrorCapturingMultiUnitsInterval" ):
                listener.exitErrorCapturingMultiUnitsInterval(self)




    def errorCapturingMultiUnitsInterval(self):

        localctx = SqlBaseParser.ErrorCapturingMultiUnitsIntervalContext(self, self._ctx, self.state)
        self.enterRule(localctx, 208, self.RULE_errorCapturingMultiUnitsInterval)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2686
            self.multiUnitsInterval()
            self.state = 2688
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,343,self._ctx)
            if la_ == 1:
                self.state = 2687
                self.unitToUnitInterval()


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class MultiUnitsIntervalContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def intervalValue(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.IntervalValueContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.IntervalValueContext,i)


        def intervalUnit(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.IntervalUnitContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.IntervalUnitContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_multiUnitsInterval

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterMultiUnitsInterval" ):
                listener.enterMultiUnitsInterval(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitMultiUnitsInterval" ):
                listener.exitMultiUnitsInterval(self)




    def multiUnitsInterval(self):

        localctx = SqlBaseParser.MultiUnitsIntervalContext(self, self._ctx, self.state)
        self.enterRule(localctx, 210, self.RULE_multiUnitsInterval)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2693
            self._errHandler.sync(self)
            _alt = 1
            while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                if _alt == 1:
                    self.state = 2690
                    self.intervalValue()
                    self.state = 2691
                    self.intervalUnit()

                else:
                    raise NoViableAltException(self)
                self.state = 2695
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,344,self._ctx)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class ErrorCapturingUnitToUnitIntervalContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.body = None # UnitToUnitIntervalContext
            self.error1 = None # MultiUnitsIntervalContext
            self.error2 = None # UnitToUnitIntervalContext

        def unitToUnitInterval(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.UnitToUnitIntervalContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.UnitToUnitIntervalContext,i)


        def multiUnitsInterval(self):
            return self.getTypedRuleContext(SqlBaseParser.MultiUnitsIntervalContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_errorCapturingUnitToUnitInterval

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterErrorCapturingUnitToUnitInterval" ):
                listener.enterErrorCapturingUnitToUnitInterval(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitErrorCapturingUnitToUnitInterval" ):
                listener.exitErrorCapturingUnitToUnitInterval(self)




    def errorCapturingUnitToUnitInterval(self):

        localctx = SqlBaseParser.ErrorCapturingUnitToUnitIntervalContext(self, self._ctx, self.state)
        self.enterRule(localctx, 212, self.RULE_errorCapturingUnitToUnitInterval)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2697
            localctx.body = self.unitToUnitInterval()
            self.state = 2700
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,345,self._ctx)
            if la_ == 1:
                self.state = 2698
                localctx.error1 = self.multiUnitsInterval()

            elif la_ == 2:
                self.state = 2699
                localctx.error2 = self.unitToUnitInterval()


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class UnitToUnitIntervalContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.value = None # IntervalValueContext
            self.from_ = None # IntervalUnitContext
            self.to = None # IntervalUnitContext

        def TO(self):
            return self.getToken(SqlBaseParser.TO, 0)

        def intervalValue(self):
            return self.getTypedRuleContext(SqlBaseParser.IntervalValueContext,0)


        def intervalUnit(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.IntervalUnitContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.IntervalUnitContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_unitToUnitInterval

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterUnitToUnitInterval" ):
                listener.enterUnitToUnitInterval(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitUnitToUnitInterval" ):
                listener.exitUnitToUnitInterval(self)




    def unitToUnitInterval(self):

        localctx = SqlBaseParser.UnitToUnitIntervalContext(self, self._ctx, self.state)
        self.enterRule(localctx, 214, self.RULE_unitToUnitInterval)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2702
            localctx.value = self.intervalValue()
            self.state = 2703
            localctx.from_ = self.intervalUnit()
            self.state = 2704
            self.match(SqlBaseParser.TO)
            self.state = 2705
            localctx.to = self.intervalUnit()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class IntervalValueContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def INTEGER_VALUE(self):
            return self.getToken(SqlBaseParser.INTEGER_VALUE, 0)

        def DECIMAL_VALUE(self):
            return self.getToken(SqlBaseParser.DECIMAL_VALUE, 0)

        def PLUS(self):
            return self.getToken(SqlBaseParser.PLUS, 0)

        def MINUS(self):
            return self.getToken(SqlBaseParser.MINUS, 0)

        def STRING(self):
            return self.getToken(SqlBaseParser.STRING, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_intervalValue

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterIntervalValue" ):
                listener.enterIntervalValue(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitIntervalValue" ):
                listener.exitIntervalValue(self)




    def intervalValue(self):

        localctx = SqlBaseParser.IntervalValueContext(self, self._ctx, self.state)
        self.enterRule(localctx, 216, self.RULE_intervalValue)
        self._la = 0 # Token type
        try:
            self.state = 2712
            self._errHandler.sync(self)
            token = self._input.LA(1)
            if token in [SqlBaseParser.PLUS, SqlBaseParser.MINUS, SqlBaseParser.INTEGER_VALUE, SqlBaseParser.DECIMAL_VALUE]:
                self.enterOuterAlt(localctx, 1)
                self.state = 2708
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.PLUS or _la==SqlBaseParser.MINUS:
                    self.state = 2707
                    _la = self._input.LA(1)
                    if not(_la==SqlBaseParser.PLUS or _la==SqlBaseParser.MINUS):
                        self._errHandler.recoverInline(self)
                    else:
                        self._errHandler.reportMatch(self)
                        self.consume()


                self.state = 2710
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.INTEGER_VALUE or _la==SqlBaseParser.DECIMAL_VALUE):
                    self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                pass
            elif token in [SqlBaseParser.STRING]:
                self.enterOuterAlt(localctx, 2)
                self.state = 2711
                self.match(SqlBaseParser.STRING)
                pass
            else:
                raise NoViableAltException(self)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class IntervalUnitContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def DAY(self):
            return self.getToken(SqlBaseParser.DAY, 0)

        def HOUR(self):
            return self.getToken(SqlBaseParser.HOUR, 0)

        def MINUTE(self):
            return self.getToken(SqlBaseParser.MINUTE, 0)

        def MONTH(self):
            return self.getToken(SqlBaseParser.MONTH, 0)

        def SECOND(self):
            return self.getToken(SqlBaseParser.SECOND, 0)

        def YEAR(self):
            return self.getToken(SqlBaseParser.YEAR, 0)

        def identifier(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_intervalUnit

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterIntervalUnit" ):
                listener.enterIntervalUnit(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitIntervalUnit" ):
                listener.exitIntervalUnit(self)




    def intervalUnit(self):

        localctx = SqlBaseParser.IntervalUnitContext(self, self._ctx, self.state)
        self.enterRule(localctx, 218, self.RULE_intervalUnit)
        try:
            self.state = 2721
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,348,self._ctx)
            if la_ == 1:
                self.enterOuterAlt(localctx, 1)
                self.state = 2714
                self.match(SqlBaseParser.DAY)
                pass

            elif la_ == 2:
                self.enterOuterAlt(localctx, 2)
                self.state = 2715
                self.match(SqlBaseParser.HOUR)
                pass

            elif la_ == 3:
                self.enterOuterAlt(localctx, 3)
                self.state = 2716
                self.match(SqlBaseParser.MINUTE)
                pass

            elif la_ == 4:
                self.enterOuterAlt(localctx, 4)
                self.state = 2717
                self.match(SqlBaseParser.MONTH)
                pass

            elif la_ == 5:
                self.enterOuterAlt(localctx, 5)
                self.state = 2718
                self.match(SqlBaseParser.SECOND)
                pass

            elif la_ == 6:
                self.enterOuterAlt(localctx, 6)
                self.state = 2719
                self.match(SqlBaseParser.YEAR)
                pass

            elif la_ == 7:
                self.enterOuterAlt(localctx, 7)
                self.state = 2720
                self.identifier()
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class ColPositionContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.position = None # Token
            self.afterCol = None # ErrorCapturingIdentifierContext

        def FIRST(self):
            return self.getToken(SqlBaseParser.FIRST, 0)

        def AFTER(self):
            return self.getToken(SqlBaseParser.AFTER, 0)

        def errorCapturingIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.ErrorCapturingIdentifierContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_colPosition

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterColPosition" ):
                listener.enterColPosition(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitColPosition" ):
                listener.exitColPosition(self)




    def colPosition(self):

        localctx = SqlBaseParser.ColPositionContext(self, self._ctx, self.state)
        self.enterRule(localctx, 220, self.RULE_colPosition)
        try:
            self.state = 2726
            self._errHandler.sync(self)
            token = self._input.LA(1)
            if token in [SqlBaseParser.FIRST]:
                self.enterOuterAlt(localctx, 1)
                self.state = 2723
                localctx.position = self.match(SqlBaseParser.FIRST)
                pass
            elif token in [SqlBaseParser.AFTER]:
                self.enterOuterAlt(localctx, 2)
                self.state = 2724
                localctx.position = self.match(SqlBaseParser.AFTER)
                self.state = 2725
                localctx.afterCol = self.errorCapturingIdentifier()
                pass
            else:
                raise NoViableAltException(self)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class DataTypeContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser


        def getRuleIndex(self):
            return SqlBaseParser.RULE_dataType


        def copyFrom(self, ctx:ParserRuleContext):
            super().copyFrom(ctx)



    class ComplexDataTypeContext(DataTypeContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.DataTypeContext
            super().__init__(parser)
            self.complex = None # Token
            self.copyFrom(ctx)

        def dataType(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.DataTypeContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.DataTypeContext,i)

        def ARRAY(self):
            return self.getToken(SqlBaseParser.ARRAY, 0)
        def MAP(self):
            return self.getToken(SqlBaseParser.MAP, 0)
        def STRUCT(self):
            return self.getToken(SqlBaseParser.STRUCT, 0)
        def NEQ(self):
            return self.getToken(SqlBaseParser.NEQ, 0)
        def complexColTypeList(self):
            return self.getTypedRuleContext(SqlBaseParser.ComplexColTypeListContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterComplexDataType" ):
                listener.enterComplexDataType(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitComplexDataType" ):
                listener.exitComplexDataType(self)


    class PrimitiveDataTypeContext(DataTypeContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.DataTypeContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def identifier(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierContext,0)

        def INTEGER_VALUE(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.INTEGER_VALUE)
            else:
                return self.getToken(SqlBaseParser.INTEGER_VALUE, i)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterPrimitiveDataType" ):
                listener.enterPrimitiveDataType(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitPrimitiveDataType" ):
                listener.exitPrimitiveDataType(self)



    def dataType(self):

        localctx = SqlBaseParser.DataTypeContext(self, self._ctx, self.state)
        self.enterRule(localctx, 222, self.RULE_dataType)
        self._la = 0 # Token type
        try:
            self.state = 2762
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,354,self._ctx)
            if la_ == 1:
                localctx = SqlBaseParser.ComplexDataTypeContext(self, localctx)
                self.enterOuterAlt(localctx, 1)
                self.state = 2728
                localctx.complex = self.match(SqlBaseParser.ARRAY)
                self.state = 2729
                self.match(SqlBaseParser.LT)
                self.state = 2730
                self.dataType()
                self.state = 2731
                self.match(SqlBaseParser.GT)
                pass

            elif la_ == 2:
                localctx = SqlBaseParser.ComplexDataTypeContext(self, localctx)
                self.enterOuterAlt(localctx, 2)
                self.state = 2733
                localctx.complex = self.match(SqlBaseParser.MAP)
                self.state = 2734
                self.match(SqlBaseParser.LT)
                self.state = 2735
                self.dataType()
                self.state = 2736
                self.match(SqlBaseParser.T__3)
                self.state = 2737
                self.dataType()
                self.state = 2738
                self.match(SqlBaseParser.GT)
                pass

            elif la_ == 3:
                localctx = SqlBaseParser.ComplexDataTypeContext(self, localctx)
                self.enterOuterAlt(localctx, 3)
                self.state = 2740
                localctx.complex = self.match(SqlBaseParser.STRUCT)
                self.state = 2747
                self._errHandler.sync(self)
                token = self._input.LA(1)
                if token in [SqlBaseParser.LT]:
                    self.state = 2741
                    self.match(SqlBaseParser.LT)
                    self.state = 2743
                    self._errHandler.sync(self)
                    la_ = self._interp.adaptivePredict(self._input,350,self._ctx)
                    if la_ == 1:
                        self.state = 2742
                        self.complexColTypeList()


                    self.state = 2745
                    self.match(SqlBaseParser.GT)
                    pass
                elif token in [SqlBaseParser.NEQ]:
                    self.state = 2746
                    self.match(SqlBaseParser.NEQ)
                    pass
                else:
                    raise NoViableAltException(self)

                pass

            elif la_ == 4:
                localctx = SqlBaseParser.PrimitiveDataTypeContext(self, localctx)
                self.enterOuterAlt(localctx, 4)
                self.state = 2749
                self.identifier()
                self.state = 2760
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,353,self._ctx)
                if la_ == 1:
                    self.state = 2750
                    self.match(SqlBaseParser.T__1)
                    self.state = 2751
                    self.match(SqlBaseParser.INTEGER_VALUE)
                    self.state = 2756
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    while _la==SqlBaseParser.T__3:
                        self.state = 2752
                        self.match(SqlBaseParser.T__3)
                        self.state = 2753
                        self.match(SqlBaseParser.INTEGER_VALUE)
                        self.state = 2758
                        self._errHandler.sync(self)
                        _la = self._input.LA(1)

                    self.state = 2759
                    self.match(SqlBaseParser.T__2)


                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class QualifiedColTypeWithPositionListContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def qualifiedColTypeWithPosition(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.QualifiedColTypeWithPositionContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.QualifiedColTypeWithPositionContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_qualifiedColTypeWithPositionList

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterQualifiedColTypeWithPositionList" ):
                listener.enterQualifiedColTypeWithPositionList(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitQualifiedColTypeWithPositionList" ):
                listener.exitQualifiedColTypeWithPositionList(self)




    def qualifiedColTypeWithPositionList(self):

        localctx = SqlBaseParser.QualifiedColTypeWithPositionListContext(self, self._ctx, self.state)
        self.enterRule(localctx, 224, self.RULE_qualifiedColTypeWithPositionList)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2764
            self.qualifiedColTypeWithPosition()
            self.state = 2769
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while _la==SqlBaseParser.T__3:
                self.state = 2765
                self.match(SqlBaseParser.T__3)
                self.state = 2766
                self.qualifiedColTypeWithPosition()
                self.state = 2771
                self._errHandler.sync(self)
                _la = self._input.LA(1)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class QualifiedColTypeWithPositionContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.name = None # MultipartIdentifierContext

        def dataType(self):
            return self.getTypedRuleContext(SqlBaseParser.DataTypeContext,0)


        def multipartIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.MultipartIdentifierContext,0)


        def NOT(self):
            return self.getToken(SqlBaseParser.NOT, 0)

        def NULL(self):
            return self.getToken(SqlBaseParser.NULL, 0)

        def commentSpec(self):
            return self.getTypedRuleContext(SqlBaseParser.CommentSpecContext,0)


        def colPosition(self):
            return self.getTypedRuleContext(SqlBaseParser.ColPositionContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_qualifiedColTypeWithPosition

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterQualifiedColTypeWithPosition" ):
                listener.enterQualifiedColTypeWithPosition(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitQualifiedColTypeWithPosition" ):
                listener.exitQualifiedColTypeWithPosition(self)




    def qualifiedColTypeWithPosition(self):

        localctx = SqlBaseParser.QualifiedColTypeWithPositionContext(self, self._ctx, self.state)
        self.enterRule(localctx, 226, self.RULE_qualifiedColTypeWithPosition)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2772
            localctx.name = self.multipartIdentifier()
            self.state = 2773
            self.dataType()
            self.state = 2776
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==SqlBaseParser.NOT:
                self.state = 2774
                self.match(SqlBaseParser.NOT)
                self.state = 2775
                self.match(SqlBaseParser.NULL)


            self.state = 2779
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==SqlBaseParser.COMMENT:
                self.state = 2778
                self.commentSpec()


            self.state = 2782
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==SqlBaseParser.AFTER or _la==SqlBaseParser.FIRST:
                self.state = 2781
                self.colPosition()


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class ColTypeListContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def colType(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ColTypeContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ColTypeContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_colTypeList

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterColTypeList" ):
                listener.enterColTypeList(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitColTypeList" ):
                listener.exitColTypeList(self)




    def colTypeList(self):

        localctx = SqlBaseParser.ColTypeListContext(self, self._ctx, self.state)
        self.enterRule(localctx, 228, self.RULE_colTypeList)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2784
            self.colType()
            self.state = 2789
            self._errHandler.sync(self)
            _alt = self._interp.adaptivePredict(self._input,359,self._ctx)
            while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                if _alt==1:
                    self.state = 2785
                    self.match(SqlBaseParser.T__3)
                    self.state = 2786
                    self.colType()
                self.state = 2791
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,359,self._ctx)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class ColTypeContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.colName = None # ErrorCapturingIdentifierContext

        def dataType(self):
            return self.getTypedRuleContext(SqlBaseParser.DataTypeContext,0)


        def errorCapturingIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.ErrorCapturingIdentifierContext,0)


        def NOT(self):
            return self.getToken(SqlBaseParser.NOT, 0)

        def NULL(self):
            return self.getToken(SqlBaseParser.NULL, 0)

        def commentSpec(self):
            return self.getTypedRuleContext(SqlBaseParser.CommentSpecContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_colType

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterColType" ):
                listener.enterColType(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitColType" ):
                listener.exitColType(self)




    def colType(self):

        localctx = SqlBaseParser.ColTypeContext(self, self._ctx, self.state)
        self.enterRule(localctx, 230, self.RULE_colType)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2792
            localctx.colName = self.errorCapturingIdentifier()
            self.state = 2793
            self.dataType()
            self.state = 2796
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,360,self._ctx)
            if la_ == 1:
                self.state = 2794
                self.match(SqlBaseParser.NOT)
                self.state = 2795
                self.match(SqlBaseParser.NULL)


            self.state = 2799
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,361,self._ctx)
            if la_ == 1:
                self.state = 2798
                self.commentSpec()


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class ComplexColTypeListContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def complexColType(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ComplexColTypeContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ComplexColTypeContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_complexColTypeList

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterComplexColTypeList" ):
                listener.enterComplexColTypeList(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitComplexColTypeList" ):
                listener.exitComplexColTypeList(self)




    def complexColTypeList(self):

        localctx = SqlBaseParser.ComplexColTypeListContext(self, self._ctx, self.state)
        self.enterRule(localctx, 232, self.RULE_complexColTypeList)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2801
            self.complexColType()
            self.state = 2806
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while _la==SqlBaseParser.T__3:
                self.state = 2802
                self.match(SqlBaseParser.T__3)
                self.state = 2803
                self.complexColType()
                self.state = 2808
                self._errHandler.sync(self)
                _la = self._input.LA(1)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class ComplexColTypeContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def identifier(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierContext,0)


        def dataType(self):
            return self.getTypedRuleContext(SqlBaseParser.DataTypeContext,0)


        def NOT(self):
            return self.getToken(SqlBaseParser.NOT, 0)

        def NULL(self):
            return self.getToken(SqlBaseParser.NULL, 0)

        def commentSpec(self):
            return self.getTypedRuleContext(SqlBaseParser.CommentSpecContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_complexColType

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterComplexColType" ):
                listener.enterComplexColType(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitComplexColType" ):
                listener.exitComplexColType(self)




    def complexColType(self):

        localctx = SqlBaseParser.ComplexColTypeContext(self, self._ctx, self.state)
        self.enterRule(localctx, 234, self.RULE_complexColType)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2809
            self.identifier()
            self.state = 2810
            self.match(SqlBaseParser.T__10)
            self.state = 2811
            self.dataType()
            self.state = 2814
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==SqlBaseParser.NOT:
                self.state = 2812
                self.match(SqlBaseParser.NOT)
                self.state = 2813
                self.match(SqlBaseParser.NULL)


            self.state = 2817
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==SqlBaseParser.COMMENT:
                self.state = 2816
                self.commentSpec()


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class WhenClauseContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.condition = None # ExpressionContext
            self.result = None # ExpressionContext

        def WHEN(self):
            return self.getToken(SqlBaseParser.WHEN, 0)

        def THEN(self):
            return self.getToken(SqlBaseParser.THEN, 0)

        def expression(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ExpressionContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ExpressionContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_whenClause

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterWhenClause" ):
                listener.enterWhenClause(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitWhenClause" ):
                listener.exitWhenClause(self)




    def whenClause(self):

        localctx = SqlBaseParser.WhenClauseContext(self, self._ctx, self.state)
        self.enterRule(localctx, 236, self.RULE_whenClause)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2819
            self.match(SqlBaseParser.WHEN)
            self.state = 2820
            localctx.condition = self.expression()
            self.state = 2821
            self.match(SqlBaseParser.THEN)
            self.state = 2822
            localctx.result = self.expression()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class WindowClauseContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def WINDOW(self):
            return self.getToken(SqlBaseParser.WINDOW, 0)

        def namedWindow(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.NamedWindowContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.NamedWindowContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_windowClause

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterWindowClause" ):
                listener.enterWindowClause(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitWindowClause" ):
                listener.exitWindowClause(self)




    def windowClause(self):

        localctx = SqlBaseParser.WindowClauseContext(self, self._ctx, self.state)
        self.enterRule(localctx, 238, self.RULE_windowClause)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2824
            self.match(SqlBaseParser.WINDOW)
            self.state = 2825
            self.namedWindow()
            self.state = 2830
            self._errHandler.sync(self)
            _alt = self._interp.adaptivePredict(self._input,365,self._ctx)
            while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                if _alt==1:
                    self.state = 2826
                    self.match(SqlBaseParser.T__3)
                    self.state = 2827
                    self.namedWindow()
                self.state = 2832
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,365,self._ctx)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class NamedWindowContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.name = None # ErrorCapturingIdentifierContext

        def AS(self):
            return self.getToken(SqlBaseParser.AS, 0)

        def windowSpec(self):
            return self.getTypedRuleContext(SqlBaseParser.WindowSpecContext,0)


        def errorCapturingIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.ErrorCapturingIdentifierContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_namedWindow

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterNamedWindow" ):
                listener.enterNamedWindow(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitNamedWindow" ):
                listener.exitNamedWindow(self)




    def namedWindow(self):

        localctx = SqlBaseParser.NamedWindowContext(self, self._ctx, self.state)
        self.enterRule(localctx, 240, self.RULE_namedWindow)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2833
            localctx.name = self.errorCapturingIdentifier()
            self.state = 2834
            self.match(SqlBaseParser.AS)
            self.state = 2835
            self.windowSpec()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class WindowSpecContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser


        def getRuleIndex(self):
            return SqlBaseParser.RULE_windowSpec


        def copyFrom(self, ctx:ParserRuleContext):
            super().copyFrom(ctx)



    class WindowRefContext(WindowSpecContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.WindowSpecContext
            super().__init__(parser)
            self.name = None # ErrorCapturingIdentifierContext
            self.copyFrom(ctx)

        def errorCapturingIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.ErrorCapturingIdentifierContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterWindowRef" ):
                listener.enterWindowRef(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitWindowRef" ):
                listener.exitWindowRef(self)


    class WindowDefContext(WindowSpecContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.WindowSpecContext
            super().__init__(parser)
            self._expression = None # ExpressionContext
            self.partition = list() # of ExpressionContexts
            self.copyFrom(ctx)

        def CLUSTER(self):
            return self.getToken(SqlBaseParser.CLUSTER, 0)
        def BY(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.BY)
            else:
                return self.getToken(SqlBaseParser.BY, i)
        def expression(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.ExpressionContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.ExpressionContext,i)

        def windowFrame(self):
            return self.getTypedRuleContext(SqlBaseParser.WindowFrameContext,0)

        def sortItem(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.SortItemContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.SortItemContext,i)

        def PARTITION(self):
            return self.getToken(SqlBaseParser.PARTITION, 0)
        def DISTRIBUTE(self):
            return self.getToken(SqlBaseParser.DISTRIBUTE, 0)
        def ORDER(self):
            return self.getToken(SqlBaseParser.ORDER, 0)
        def SORT(self):
            return self.getToken(SqlBaseParser.SORT, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterWindowDef" ):
                listener.enterWindowDef(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitWindowDef" ):
                listener.exitWindowDef(self)



    def windowSpec(self):

        localctx = SqlBaseParser.WindowSpecContext(self, self._ctx, self.state)
        self.enterRule(localctx, 242, self.RULE_windowSpec)
        self._la = 0 # Token type
        try:
            self.state = 2883
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,373,self._ctx)
            if la_ == 1:
                localctx = SqlBaseParser.WindowRefContext(self, localctx)
                self.enterOuterAlt(localctx, 1)
                self.state = 2837
                localctx.name = self.errorCapturingIdentifier()
                pass

            elif la_ == 2:
                localctx = SqlBaseParser.WindowRefContext(self, localctx)
                self.enterOuterAlt(localctx, 2)
                self.state = 2838
                self.match(SqlBaseParser.T__1)
                self.state = 2839
                localctx.name = self.errorCapturingIdentifier()
                self.state = 2840
                self.match(SqlBaseParser.T__2)
                pass

            elif la_ == 3:
                localctx = SqlBaseParser.WindowDefContext(self, localctx)
                self.enterOuterAlt(localctx, 3)
                self.state = 2842
                self.match(SqlBaseParser.T__1)
                self.state = 2877
                self._errHandler.sync(self)
                token = self._input.LA(1)
                if token in [SqlBaseParser.CLUSTER]:
                    self.state = 2843
                    self.match(SqlBaseParser.CLUSTER)
                    self.state = 2844
                    self.match(SqlBaseParser.BY)
                    self.state = 2845
                    localctx._expression = self.expression()
                    localctx.partition.append(localctx._expression)
                    self.state = 2850
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    while _la==SqlBaseParser.T__3:
                        self.state = 2846
                        self.match(SqlBaseParser.T__3)
                        self.state = 2847
                        localctx._expression = self.expression()
                        localctx.partition.append(localctx._expression)
                        self.state = 2852
                        self._errHandler.sync(self)
                        _la = self._input.LA(1)

                    pass
                elif token in [SqlBaseParser.T__2, SqlBaseParser.DISTRIBUTE, SqlBaseParser.ORDER, SqlBaseParser.PARTITION, SqlBaseParser.RANGE, SqlBaseParser.ROWS, SqlBaseParser.SORT]:
                    self.state = 2863
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    if _la==SqlBaseParser.DISTRIBUTE or _la==SqlBaseParser.PARTITION:
                        self.state = 2853
                        _la = self._input.LA(1)
                        if not(_la==SqlBaseParser.DISTRIBUTE or _la==SqlBaseParser.PARTITION):
                            self._errHandler.recoverInline(self)
                        else:
                            self._errHandler.reportMatch(self)
                            self.consume()
                        self.state = 2854
                        self.match(SqlBaseParser.BY)
                        self.state = 2855
                        localctx._expression = self.expression()
                        localctx.partition.append(localctx._expression)
                        self.state = 2860
                        self._errHandler.sync(self)
                        _la = self._input.LA(1)
                        while _la==SqlBaseParser.T__3:
                            self.state = 2856
                            self.match(SqlBaseParser.T__3)
                            self.state = 2857
                            localctx._expression = self.expression()
                            localctx.partition.append(localctx._expression)
                            self.state = 2862
                            self._errHandler.sync(self)
                            _la = self._input.LA(1)



                    self.state = 2875
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    if _la==SqlBaseParser.ORDER or _la==SqlBaseParser.SORT:
                        self.state = 2865
                        _la = self._input.LA(1)
                        if not(_la==SqlBaseParser.ORDER or _la==SqlBaseParser.SORT):
                            self._errHandler.recoverInline(self)
                        else:
                            self._errHandler.reportMatch(self)
                            self.consume()
                        self.state = 2866
                        self.match(SqlBaseParser.BY)
                        self.state = 2867
                        self.sortItem()
                        self.state = 2872
                        self._errHandler.sync(self)
                        _la = self._input.LA(1)
                        while _la==SqlBaseParser.T__3:
                            self.state = 2868
                            self.match(SqlBaseParser.T__3)
                            self.state = 2869
                            self.sortItem()
                            self.state = 2874
                            self._errHandler.sync(self)
                            _la = self._input.LA(1)



                    pass
                else:
                    raise NoViableAltException(self)

                self.state = 2880
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.RANGE or _la==SqlBaseParser.ROWS:
                    self.state = 2879
                    self.windowFrame()


                self.state = 2882
                self.match(SqlBaseParser.T__2)
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class WindowFrameContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.frameType = None # Token
            self.start = None # FrameBoundContext
            self.end = None # FrameBoundContext

        def RANGE(self):
            return self.getToken(SqlBaseParser.RANGE, 0)

        def frameBound(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.FrameBoundContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.FrameBoundContext,i)


        def ROWS(self):
            return self.getToken(SqlBaseParser.ROWS, 0)

        def BETWEEN(self):
            return self.getToken(SqlBaseParser.BETWEEN, 0)

        def AND(self):
            return self.getToken(SqlBaseParser.AND, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_windowFrame

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterWindowFrame" ):
                listener.enterWindowFrame(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitWindowFrame" ):
                listener.exitWindowFrame(self)




    def windowFrame(self):

        localctx = SqlBaseParser.WindowFrameContext(self, self._ctx, self.state)
        self.enterRule(localctx, 244, self.RULE_windowFrame)
        try:
            self.state = 2901
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,374,self._ctx)
            if la_ == 1:
                self.enterOuterAlt(localctx, 1)
                self.state = 2885
                localctx.frameType = self.match(SqlBaseParser.RANGE)
                self.state = 2886
                localctx.start = self.frameBound()
                pass

            elif la_ == 2:
                self.enterOuterAlt(localctx, 2)
                self.state = 2887
                localctx.frameType = self.match(SqlBaseParser.ROWS)
                self.state = 2888
                localctx.start = self.frameBound()
                pass

            elif la_ == 3:
                self.enterOuterAlt(localctx, 3)
                self.state = 2889
                localctx.frameType = self.match(SqlBaseParser.RANGE)
                self.state = 2890
                self.match(SqlBaseParser.BETWEEN)
                self.state = 2891
                localctx.start = self.frameBound()
                self.state = 2892
                self.match(SqlBaseParser.AND)
                self.state = 2893
                localctx.end = self.frameBound()
                pass

            elif la_ == 4:
                self.enterOuterAlt(localctx, 4)
                self.state = 2895
                localctx.frameType = self.match(SqlBaseParser.ROWS)
                self.state = 2896
                self.match(SqlBaseParser.BETWEEN)
                self.state = 2897
                localctx.start = self.frameBound()
                self.state = 2898
                self.match(SqlBaseParser.AND)
                self.state = 2899
                localctx.end = self.frameBound()
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class FrameBoundContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.boundType = None # Token

        def UNBOUNDED(self):
            return self.getToken(SqlBaseParser.UNBOUNDED, 0)

        def PRECEDING(self):
            return self.getToken(SqlBaseParser.PRECEDING, 0)

        def FOLLOWING(self):
            return self.getToken(SqlBaseParser.FOLLOWING, 0)

        def ROW(self):
            return self.getToken(SqlBaseParser.ROW, 0)

        def CURRENT(self):
            return self.getToken(SqlBaseParser.CURRENT, 0)

        def expression(self):
            return self.getTypedRuleContext(SqlBaseParser.ExpressionContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_frameBound

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterFrameBound" ):
                listener.enterFrameBound(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitFrameBound" ):
                listener.exitFrameBound(self)




    def frameBound(self):

        localctx = SqlBaseParser.FrameBoundContext(self, self._ctx, self.state)
        self.enterRule(localctx, 246, self.RULE_frameBound)
        self._la = 0 # Token type
        try:
            self.state = 2910
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,375,self._ctx)
            if la_ == 1:
                self.enterOuterAlt(localctx, 1)
                self.state = 2903
                self.match(SqlBaseParser.UNBOUNDED)
                self.state = 2904
                localctx.boundType = self._input.LT(1)
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.FOLLOWING or _la==SqlBaseParser.PRECEDING):
                    localctx.boundType = self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                pass

            elif la_ == 2:
                self.enterOuterAlt(localctx, 2)
                self.state = 2905
                localctx.boundType = self.match(SqlBaseParser.CURRENT)
                self.state = 2906
                self.match(SqlBaseParser.ROW)
                pass

            elif la_ == 3:
                self.enterOuterAlt(localctx, 3)
                self.state = 2907
                self.expression()
                self.state = 2908
                localctx.boundType = self._input.LT(1)
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.FOLLOWING or _la==SqlBaseParser.PRECEDING):
                    localctx.boundType = self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class QualifiedNameListContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def qualifiedName(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.QualifiedNameContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.QualifiedNameContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_qualifiedNameList

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterQualifiedNameList" ):
                listener.enterQualifiedNameList(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitQualifiedNameList" ):
                listener.exitQualifiedNameList(self)




    def qualifiedNameList(self):

        localctx = SqlBaseParser.QualifiedNameListContext(self, self._ctx, self.state)
        self.enterRule(localctx, 248, self.RULE_qualifiedNameList)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2912
            self.qualifiedName()
            self.state = 2917
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while _la==SqlBaseParser.T__3:
                self.state = 2913
                self.match(SqlBaseParser.T__3)
                self.state = 2914
                self.qualifiedName()
                self.state = 2919
                self._errHandler.sync(self)
                _la = self._input.LA(1)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class FunctionNameContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def qualifiedName(self):
            return self.getTypedRuleContext(SqlBaseParser.QualifiedNameContext,0)


        def FILTER(self):
            return self.getToken(SqlBaseParser.FILTER, 0)

        def LEFT(self):
            return self.getToken(SqlBaseParser.LEFT, 0)

        def RIGHT(self):
            return self.getToken(SqlBaseParser.RIGHT, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_functionName

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterFunctionName" ):
                listener.enterFunctionName(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitFunctionName" ):
                listener.exitFunctionName(self)




    def functionName(self):

        localctx = SqlBaseParser.FunctionNameContext(self, self._ctx, self.state)
        self.enterRule(localctx, 250, self.RULE_functionName)
        try:
            self.state = 2924
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,377,self._ctx)
            if la_ == 1:
                self.enterOuterAlt(localctx, 1)
                self.state = 2920
                self.qualifiedName()
                pass

            elif la_ == 2:
                self.enterOuterAlt(localctx, 2)
                self.state = 2921
                self.match(SqlBaseParser.FILTER)
                pass

            elif la_ == 3:
                self.enterOuterAlt(localctx, 3)
                self.state = 2922
                self.match(SqlBaseParser.LEFT)
                pass

            elif la_ == 4:
                self.enterOuterAlt(localctx, 4)
                self.state = 2923
                self.match(SqlBaseParser.RIGHT)
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class QualifiedNameContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def identifier(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.IdentifierContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.IdentifierContext,i)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_qualifiedName

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterQualifiedName" ):
                listener.enterQualifiedName(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitQualifiedName" ):
                listener.exitQualifiedName(self)




    def qualifiedName(self):

        localctx = SqlBaseParser.QualifiedNameContext(self, self._ctx, self.state)
        self.enterRule(localctx, 252, self.RULE_qualifiedName)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2926
            self.identifier()
            self.state = 2931
            self._errHandler.sync(self)
            _alt = self._interp.adaptivePredict(self._input,378,self._ctx)
            while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                if _alt==1:
                    self.state = 2927
                    self.match(SqlBaseParser.T__4)
                    self.state = 2928
                    self.identifier()
                self.state = 2933
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,378,self._ctx)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class ErrorCapturingIdentifierContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def identifier(self):
            return self.getTypedRuleContext(SqlBaseParser.IdentifierContext,0)


        def errorCapturingIdentifierExtra(self):
            return self.getTypedRuleContext(SqlBaseParser.ErrorCapturingIdentifierExtraContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_errorCapturingIdentifier

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterErrorCapturingIdentifier" ):
                listener.enterErrorCapturingIdentifier(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitErrorCapturingIdentifier" ):
                listener.exitErrorCapturingIdentifier(self)




    def errorCapturingIdentifier(self):

        localctx = SqlBaseParser.ErrorCapturingIdentifierContext(self, self._ctx, self.state)
        self.enterRule(localctx, 254, self.RULE_errorCapturingIdentifier)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2934
            self.identifier()
            self.state = 2935
            self.errorCapturingIdentifierExtra()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class ErrorCapturingIdentifierExtraContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser


        def getRuleIndex(self):
            return SqlBaseParser.RULE_errorCapturingIdentifierExtra


        def copyFrom(self, ctx:ParserRuleContext):
            super().copyFrom(ctx)



    class ErrorIdentContext(ErrorCapturingIdentifierExtraContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.ErrorCapturingIdentifierExtraContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def MINUS(self, i:int=None):
            if i is None:
                return self.getTokens(SqlBaseParser.MINUS)
            else:
                return self.getToken(SqlBaseParser.MINUS, i)
        def identifier(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(SqlBaseParser.IdentifierContext)
            else:
                return self.getTypedRuleContext(SqlBaseParser.IdentifierContext,i)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterErrorIdent" ):
                listener.enterErrorIdent(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitErrorIdent" ):
                listener.exitErrorIdent(self)


    class RealIdentContext(ErrorCapturingIdentifierExtraContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.ErrorCapturingIdentifierExtraContext
            super().__init__(parser)
            self.copyFrom(ctx)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterRealIdent" ):
                listener.enterRealIdent(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitRealIdent" ):
                listener.exitRealIdent(self)



    def errorCapturingIdentifierExtra(self):

        localctx = SqlBaseParser.ErrorCapturingIdentifierExtraContext(self, self._ctx, self.state)
        self.enterRule(localctx, 256, self.RULE_errorCapturingIdentifierExtra)
        try:
            self.state = 2944
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,380,self._ctx)
            if la_ == 1:
                localctx = SqlBaseParser.ErrorIdentContext(self, localctx)
                self.enterOuterAlt(localctx, 1)
                self.state = 2939
                self._errHandler.sync(self)
                _alt = 1
                while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                    if _alt == 1:
                        self.state = 2937
                        self.match(SqlBaseParser.MINUS)
                        self.state = 2938
                        self.identifier()

                    else:
                        raise NoViableAltException(self)
                    self.state = 2941
                    self._errHandler.sync(self)
                    _alt = self._interp.adaptivePredict(self._input,379,self._ctx)

                pass

            elif la_ == 2:
                localctx = SqlBaseParser.RealIdentContext(self, localctx)
                self.enterOuterAlt(localctx, 2)

                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class IdentifierContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def strictIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.StrictIdentifierContext,0)


        def strictNonReserved(self):
            return self.getTypedRuleContext(SqlBaseParser.StrictNonReservedContext,0)


        def getRuleIndex(self):
            return SqlBaseParser.RULE_identifier

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterIdentifier" ):
                listener.enterIdentifier(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitIdentifier" ):
                listener.exitIdentifier(self)




    def identifier(self):

        localctx = SqlBaseParser.IdentifierContext(self, self._ctx, self.state)
        self.enterRule(localctx, 258, self.RULE_identifier)
        try:
            self.state = 2949
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,381,self._ctx)
            if la_ == 1:
                self.enterOuterAlt(localctx, 1)
                self.state = 2946
                self.strictIdentifier()
                pass

            elif la_ == 2:
                self.enterOuterAlt(localctx, 2)
                self.state = 2947
                if not not self.SQL_standard_keyword_behavior:
                    from antlr4.error.Errors import FailedPredicateException
                    raise FailedPredicateException(self, "not self.SQL_standard_keyword_behavior")
                self.state = 2948
                self.strictNonReserved()
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class StrictIdentifierContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser


        def getRuleIndex(self):
            return SqlBaseParser.RULE_strictIdentifier


        def copyFrom(self, ctx:ParserRuleContext):
            super().copyFrom(ctx)



    class QuotedIdentifierAlternativeContext(StrictIdentifierContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StrictIdentifierContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def quotedIdentifier(self):
            return self.getTypedRuleContext(SqlBaseParser.QuotedIdentifierContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterQuotedIdentifierAlternative" ):
                listener.enterQuotedIdentifierAlternative(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitQuotedIdentifierAlternative" ):
                listener.exitQuotedIdentifierAlternative(self)


    class UnquotedIdentifierContext(StrictIdentifierContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.StrictIdentifierContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def IDENTIFIER(self):
            return self.getToken(SqlBaseParser.IDENTIFIER, 0)
        def ansiNonReserved(self):
            return self.getTypedRuleContext(SqlBaseParser.AnsiNonReservedContext,0)

        def nonReserved(self):
            return self.getTypedRuleContext(SqlBaseParser.NonReservedContext,0)


        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterUnquotedIdentifier" ):
                listener.enterUnquotedIdentifier(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitUnquotedIdentifier" ):
                listener.exitUnquotedIdentifier(self)



    def strictIdentifier(self):

        localctx = SqlBaseParser.StrictIdentifierContext(self, self._ctx, self.state)
        self.enterRule(localctx, 260, self.RULE_strictIdentifier)
        try:
            self.state = 2957
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,382,self._ctx)
            if la_ == 1:
                localctx = SqlBaseParser.UnquotedIdentifierContext(self, localctx)
                self.enterOuterAlt(localctx, 1)
                self.state = 2951
                self.match(SqlBaseParser.IDENTIFIER)
                pass

            elif la_ == 2:
                localctx = SqlBaseParser.QuotedIdentifierAlternativeContext(self, localctx)
                self.enterOuterAlt(localctx, 2)
                self.state = 2952
                self.quotedIdentifier()
                pass

            elif la_ == 3:
                localctx = SqlBaseParser.UnquotedIdentifierContext(self, localctx)
                self.enterOuterAlt(localctx, 3)
                self.state = 2953
                if not self.SQL_standard_keyword_behavior:
                    from antlr4.error.Errors import FailedPredicateException
                    raise FailedPredicateException(self, "self.SQL_standard_keyword_behavior")
                self.state = 2954
                self.ansiNonReserved()
                pass

            elif la_ == 4:
                localctx = SqlBaseParser.UnquotedIdentifierContext(self, localctx)
                self.enterOuterAlt(localctx, 4)
                self.state = 2955
                if not not self.SQL_standard_keyword_behavior:
                    from antlr4.error.Errors import FailedPredicateException
                    raise FailedPredicateException(self, "not self.SQL_standard_keyword_behavior")
                self.state = 2956
                self.nonReserved()
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class QuotedIdentifierContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def BACKQUOTED_IDENTIFIER(self):
            return self.getToken(SqlBaseParser.BACKQUOTED_IDENTIFIER, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_quotedIdentifier

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterQuotedIdentifier" ):
                listener.enterQuotedIdentifier(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitQuotedIdentifier" ):
                listener.exitQuotedIdentifier(self)




    def quotedIdentifier(self):

        localctx = SqlBaseParser.QuotedIdentifierContext(self, self._ctx, self.state)
        self.enterRule(localctx, 262, self.RULE_quotedIdentifier)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 2959
            self.match(SqlBaseParser.BACKQUOTED_IDENTIFIER)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class NumberContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser


        def getRuleIndex(self):
            return SqlBaseParser.RULE_number


        def copyFrom(self, ctx:ParserRuleContext):
            super().copyFrom(ctx)



    class DecimalLiteralContext(NumberContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.NumberContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def DECIMAL_VALUE(self):
            return self.getToken(SqlBaseParser.DECIMAL_VALUE, 0)
        def MINUS(self):
            return self.getToken(SqlBaseParser.MINUS, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterDecimalLiteral" ):
                listener.enterDecimalLiteral(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitDecimalLiteral" ):
                listener.exitDecimalLiteral(self)


    class BigIntLiteralContext(NumberContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.NumberContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def BIGINT_LITERAL(self):
            return self.getToken(SqlBaseParser.BIGINT_LITERAL, 0)
        def MINUS(self):
            return self.getToken(SqlBaseParser.MINUS, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterBigIntLiteral" ):
                listener.enterBigIntLiteral(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitBigIntLiteral" ):
                listener.exitBigIntLiteral(self)


    class TinyIntLiteralContext(NumberContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.NumberContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def TINYINT_LITERAL(self):
            return self.getToken(SqlBaseParser.TINYINT_LITERAL, 0)
        def MINUS(self):
            return self.getToken(SqlBaseParser.MINUS, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterTinyIntLiteral" ):
                listener.enterTinyIntLiteral(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitTinyIntLiteral" ):
                listener.exitTinyIntLiteral(self)


    class LegacyDecimalLiteralContext(NumberContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.NumberContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def EXPONENT_VALUE(self):
            return self.getToken(SqlBaseParser.EXPONENT_VALUE, 0)
        def DECIMAL_VALUE(self):
            return self.getToken(SqlBaseParser.DECIMAL_VALUE, 0)
        def MINUS(self):
            return self.getToken(SqlBaseParser.MINUS, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterLegacyDecimalLiteral" ):
                listener.enterLegacyDecimalLiteral(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitLegacyDecimalLiteral" ):
                listener.exitLegacyDecimalLiteral(self)


    class BigDecimalLiteralContext(NumberContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.NumberContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def BIGDECIMAL_LITERAL(self):
            return self.getToken(SqlBaseParser.BIGDECIMAL_LITERAL, 0)
        def MINUS(self):
            return self.getToken(SqlBaseParser.MINUS, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterBigDecimalLiteral" ):
                listener.enterBigDecimalLiteral(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitBigDecimalLiteral" ):
                listener.exitBigDecimalLiteral(self)


    class ExponentLiteralContext(NumberContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.NumberContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def EXPONENT_VALUE(self):
            return self.getToken(SqlBaseParser.EXPONENT_VALUE, 0)
        def MINUS(self):
            return self.getToken(SqlBaseParser.MINUS, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterExponentLiteral" ):
                listener.enterExponentLiteral(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitExponentLiteral" ):
                listener.exitExponentLiteral(self)


    class DoubleLiteralContext(NumberContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.NumberContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def DOUBLE_LITERAL(self):
            return self.getToken(SqlBaseParser.DOUBLE_LITERAL, 0)
        def MINUS(self):
            return self.getToken(SqlBaseParser.MINUS, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterDoubleLiteral" ):
                listener.enterDoubleLiteral(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitDoubleLiteral" ):
                listener.exitDoubleLiteral(self)


    class IntegerLiteralContext(NumberContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.NumberContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def INTEGER_VALUE(self):
            return self.getToken(SqlBaseParser.INTEGER_VALUE, 0)
        def MINUS(self):
            return self.getToken(SqlBaseParser.MINUS, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterIntegerLiteral" ):
                listener.enterIntegerLiteral(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitIntegerLiteral" ):
                listener.exitIntegerLiteral(self)


    class SmallIntLiteralContext(NumberContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a SqlBaseParser.NumberContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def SMALLINT_LITERAL(self):
            return self.getToken(SqlBaseParser.SMALLINT_LITERAL, 0)
        def MINUS(self):
            return self.getToken(SqlBaseParser.MINUS, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSmallIntLiteral" ):
                listener.enterSmallIntLiteral(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSmallIntLiteral" ):
                listener.exitSmallIntLiteral(self)



    def number(self):

        localctx = SqlBaseParser.NumberContext(self, self._ctx, self.state)
        self.enterRule(localctx, 264, self.RULE_number)
        self._la = 0 # Token type
        try:
            self.state = 3000
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,392,self._ctx)
            if la_ == 1:
                localctx = SqlBaseParser.ExponentLiteralContext(self, localctx)
                self.enterOuterAlt(localctx, 1)
                self.state = 2961
                if not not self.legacy_exponent_literal_as_decimal_enabled:
                    from antlr4.error.Errors import FailedPredicateException
                    raise FailedPredicateException(self, "not self.legacy_exponent_literal_as_decimal_enabled")
                self.state = 2963
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.MINUS:
                    self.state = 2962
                    self.match(SqlBaseParser.MINUS)


                self.state = 2965
                self.match(SqlBaseParser.EXPONENT_VALUE)
                pass

            elif la_ == 2:
                localctx = SqlBaseParser.DecimalLiteralContext(self, localctx)
                self.enterOuterAlt(localctx, 2)
                self.state = 2966
                if not not self.legacy_exponent_literal_as_decimal_enabled:
                    from antlr4.error.Errors import FailedPredicateException
                    raise FailedPredicateException(self, "not self.legacy_exponent_literal_as_decimal_enabled")
                self.state = 2968
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.MINUS:
                    self.state = 2967
                    self.match(SqlBaseParser.MINUS)


                self.state = 2970
                self.match(SqlBaseParser.DECIMAL_VALUE)
                pass

            elif la_ == 3:
                localctx = SqlBaseParser.LegacyDecimalLiteralContext(self, localctx)
                self.enterOuterAlt(localctx, 3)
                self.state = 2971
                if not self.legacy_exponent_literal_as_decimal_enabled:
                    from antlr4.error.Errors import FailedPredicateException
                    raise FailedPredicateException(self, "self.legacy_exponent_literal_as_decimal_enabled")
                self.state = 2973
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.MINUS:
                    self.state = 2972
                    self.match(SqlBaseParser.MINUS)


                self.state = 2975
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.EXPONENT_VALUE or _la==SqlBaseParser.DECIMAL_VALUE):
                    self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                pass

            elif la_ == 4:
                localctx = SqlBaseParser.IntegerLiteralContext(self, localctx)
                self.enterOuterAlt(localctx, 4)
                self.state = 2977
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.MINUS:
                    self.state = 2976
                    self.match(SqlBaseParser.MINUS)


                self.state = 2979
                self.match(SqlBaseParser.INTEGER_VALUE)
                pass

            elif la_ == 5:
                localctx = SqlBaseParser.BigIntLiteralContext(self, localctx)
                self.enterOuterAlt(localctx, 5)
                self.state = 2981
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.MINUS:
                    self.state = 2980
                    self.match(SqlBaseParser.MINUS)


                self.state = 2983
                self.match(SqlBaseParser.BIGINT_LITERAL)
                pass

            elif la_ == 6:
                localctx = SqlBaseParser.SmallIntLiteralContext(self, localctx)
                self.enterOuterAlt(localctx, 6)
                self.state = 2985
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.MINUS:
                    self.state = 2984
                    self.match(SqlBaseParser.MINUS)


                self.state = 2987
                self.match(SqlBaseParser.SMALLINT_LITERAL)
                pass

            elif la_ == 7:
                localctx = SqlBaseParser.TinyIntLiteralContext(self, localctx)
                self.enterOuterAlt(localctx, 7)
                self.state = 2989
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.MINUS:
                    self.state = 2988
                    self.match(SqlBaseParser.MINUS)


                self.state = 2991
                self.match(SqlBaseParser.TINYINT_LITERAL)
                pass

            elif la_ == 8:
                localctx = SqlBaseParser.DoubleLiteralContext(self, localctx)
                self.enterOuterAlt(localctx, 8)
                self.state = 2993
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.MINUS:
                    self.state = 2992
                    self.match(SqlBaseParser.MINUS)


                self.state = 2995
                self.match(SqlBaseParser.DOUBLE_LITERAL)
                pass

            elif la_ == 9:
                localctx = SqlBaseParser.BigDecimalLiteralContext(self, localctx)
                self.enterOuterAlt(localctx, 9)
                self.state = 2997
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if _la==SqlBaseParser.MINUS:
                    self.state = 2996
                    self.match(SqlBaseParser.MINUS)


                self.state = 2999
                self.match(SqlBaseParser.BIGDECIMAL_LITERAL)
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class AlterColumnActionContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser
            self.setOrDrop = None # Token

        def TYPE(self):
            return self.getToken(SqlBaseParser.TYPE, 0)

        def dataType(self):
            return self.getTypedRuleContext(SqlBaseParser.DataTypeContext,0)


        def commentSpec(self):
            return self.getTypedRuleContext(SqlBaseParser.CommentSpecContext,0)


        def colPosition(self):
            return self.getTypedRuleContext(SqlBaseParser.ColPositionContext,0)


        def NOT(self):
            return self.getToken(SqlBaseParser.NOT, 0)

        def NULL(self):
            return self.getToken(SqlBaseParser.NULL, 0)

        def SET(self):
            return self.getToken(SqlBaseParser.SET, 0)

        def DROP(self):
            return self.getToken(SqlBaseParser.DROP, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_alterColumnAction

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterAlterColumnAction" ):
                listener.enterAlterColumnAction(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitAlterColumnAction" ):
                listener.exitAlterColumnAction(self)




    def alterColumnAction(self):

        localctx = SqlBaseParser.AlterColumnActionContext(self, self._ctx, self.state)
        self.enterRule(localctx, 266, self.RULE_alterColumnAction)
        self._la = 0 # Token type
        try:
            self.state = 3009
            self._errHandler.sync(self)
            token = self._input.LA(1)
            if token in [SqlBaseParser.TYPE]:
                self.enterOuterAlt(localctx, 1)
                self.state = 3002
                self.match(SqlBaseParser.TYPE)
                self.state = 3003
                self.dataType()
                pass
            elif token in [SqlBaseParser.COMMENT]:
                self.enterOuterAlt(localctx, 2)
                self.state = 3004
                self.commentSpec()
                pass
            elif token in [SqlBaseParser.AFTER, SqlBaseParser.FIRST]:
                self.enterOuterAlt(localctx, 3)
                self.state = 3005
                self.colPosition()
                pass
            elif token in [SqlBaseParser.DROP, SqlBaseParser.SET]:
                self.enterOuterAlt(localctx, 4)
                self.state = 3006
                localctx.setOrDrop = self._input.LT(1)
                _la = self._input.LA(1)
                if not(_la==SqlBaseParser.DROP or _la==SqlBaseParser.SET):
                    localctx.setOrDrop = self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                self.state = 3007
                self.match(SqlBaseParser.NOT)
                self.state = 3008
                self.match(SqlBaseParser.NULL)
                pass
            else:
                raise NoViableAltException(self)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class AnsiNonReservedContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def ADD(self):
            return self.getToken(SqlBaseParser.ADD, 0)

        def AFTER(self):
            return self.getToken(SqlBaseParser.AFTER, 0)

        def ALTER(self):
            return self.getToken(SqlBaseParser.ALTER, 0)

        def ANALYZE(self):
            return self.getToken(SqlBaseParser.ANALYZE, 0)

        def ARCHIVE(self):
            return self.getToken(SqlBaseParser.ARCHIVE, 0)

        def ARRAY(self):
            return self.getToken(SqlBaseParser.ARRAY, 0)

        def ASC(self):
            return self.getToken(SqlBaseParser.ASC, 0)

        def AT(self):
            return self.getToken(SqlBaseParser.AT, 0)

        def BETWEEN(self):
            return self.getToken(SqlBaseParser.BETWEEN, 0)

        def BUCKET(self):
            return self.getToken(SqlBaseParser.BUCKET, 0)

        def BUCKETS(self):
            return self.getToken(SqlBaseParser.BUCKETS, 0)

        def BY(self):
            return self.getToken(SqlBaseParser.BY, 0)

        def CACHE(self):
            return self.getToken(SqlBaseParser.CACHE, 0)

        def CASCADE(self):
            return self.getToken(SqlBaseParser.CASCADE, 0)

        def CHANGE(self):
            return self.getToken(SqlBaseParser.CHANGE, 0)

        def CLEAR(self):
            return self.getToken(SqlBaseParser.CLEAR, 0)

        def CLUSTER(self):
            return self.getToken(SqlBaseParser.CLUSTER, 0)

        def CLUSTERED(self):
            return self.getToken(SqlBaseParser.CLUSTERED, 0)

        def CODEGEN(self):
            return self.getToken(SqlBaseParser.CODEGEN, 0)

        def COLLECTION(self):
            return self.getToken(SqlBaseParser.COLLECTION, 0)

        def COLUMNS(self):
            return self.getToken(SqlBaseParser.COLUMNS, 0)

        def COMMENT(self):
            return self.getToken(SqlBaseParser.COMMENT, 0)

        def COMMIT(self):
            return self.getToken(SqlBaseParser.COMMIT, 0)

        def COMPACT(self):
            return self.getToken(SqlBaseParser.COMPACT, 0)

        def COMPACTIONS(self):
            return self.getToken(SqlBaseParser.COMPACTIONS, 0)

        def COMPUTE(self):
            return self.getToken(SqlBaseParser.COMPUTE, 0)

        def CONCATENATE(self):
            return self.getToken(SqlBaseParser.CONCATENATE, 0)

        def COST(self):
            return self.getToken(SqlBaseParser.COST, 0)

        def CUBE(self):
            return self.getToken(SqlBaseParser.CUBE, 0)

        def CURRENT(self):
            return self.getToken(SqlBaseParser.CURRENT, 0)

        def DATA(self):
            return self.getToken(SqlBaseParser.DATA, 0)

        def DATABASE(self):
            return self.getToken(SqlBaseParser.DATABASE, 0)

        def DATABASES(self):
            return self.getToken(SqlBaseParser.DATABASES, 0)

        def DBPROPERTIES(self):
            return self.getToken(SqlBaseParser.DBPROPERTIES, 0)

        def DEFINED(self):
            return self.getToken(SqlBaseParser.DEFINED, 0)

        def DELETE(self):
            return self.getToken(SqlBaseParser.DELETE, 0)

        def DELIMITED(self):
            return self.getToken(SqlBaseParser.DELIMITED, 0)

        def DESC(self):
            return self.getToken(SqlBaseParser.DESC, 0)

        def DESCRIBE(self):
            return self.getToken(SqlBaseParser.DESCRIBE, 0)

        def DFS(self):
            return self.getToken(SqlBaseParser.DFS, 0)

        def DIRECTORIES(self):
            return self.getToken(SqlBaseParser.DIRECTORIES, 0)

        def DIRECTORY(self):
            return self.getToken(SqlBaseParser.DIRECTORY, 0)

        def DISTRIBUTE(self):
            return self.getToken(SqlBaseParser.DISTRIBUTE, 0)

        def DIV(self):
            return self.getToken(SqlBaseParser.DIV, 0)

        def DROP(self):
            return self.getToken(SqlBaseParser.DROP, 0)

        def ESCAPED(self):
            return self.getToken(SqlBaseParser.ESCAPED, 0)

        def EXCHANGE(self):
            return self.getToken(SqlBaseParser.EXCHANGE, 0)

        def EXISTS(self):
            return self.getToken(SqlBaseParser.EXISTS, 0)

        def EXPLAIN(self):
            return self.getToken(SqlBaseParser.EXPLAIN, 0)

        def EXPORT(self):
            return self.getToken(SqlBaseParser.EXPORT, 0)

        def EXTENDED(self):
            return self.getToken(SqlBaseParser.EXTENDED, 0)

        def EXTERNAL(self):
            return self.getToken(SqlBaseParser.EXTERNAL, 0)

        def EXTRACT(self):
            return self.getToken(SqlBaseParser.EXTRACT, 0)

        def FIELDS(self):
            return self.getToken(SqlBaseParser.FIELDS, 0)

        def FILEFORMAT(self):
            return self.getToken(SqlBaseParser.FILEFORMAT, 0)

        def FIRST(self):
            return self.getToken(SqlBaseParser.FIRST, 0)

        def FOLLOWING(self):
            return self.getToken(SqlBaseParser.FOLLOWING, 0)

        def FORMAT(self):
            return self.getToken(SqlBaseParser.FORMAT, 0)

        def FORMATTED(self):
            return self.getToken(SqlBaseParser.FORMATTED, 0)

        def FUNCTION(self):
            return self.getToken(SqlBaseParser.FUNCTION, 0)

        def FUNCTIONS(self):
            return self.getToken(SqlBaseParser.FUNCTIONS, 0)

        def GLOBAL(self):
            return self.getToken(SqlBaseParser.GLOBAL, 0)

        def GROUPING(self):
            return self.getToken(SqlBaseParser.GROUPING, 0)

        def IF(self):
            return self.getToken(SqlBaseParser.IF, 0)

        def IGNORE(self):
            return self.getToken(SqlBaseParser.IGNORE, 0)

        def IMPORT(self):
            return self.getToken(SqlBaseParser.IMPORT, 0)

        def INDEX(self):
            return self.getToken(SqlBaseParser.INDEX, 0)

        def INDEXES(self):
            return self.getToken(SqlBaseParser.INDEXES, 0)

        def INPATH(self):
            return self.getToken(SqlBaseParser.INPATH, 0)

        def INPUTFORMAT(self):
            return self.getToken(SqlBaseParser.INPUTFORMAT, 0)

        def INSERT(self):
            return self.getToken(SqlBaseParser.INSERT, 0)

        def INTERVAL(self):
            return self.getToken(SqlBaseParser.INTERVAL, 0)

        def ITEMS(self):
            return self.getToken(SqlBaseParser.ITEMS, 0)

        def KEYS(self):
            return self.getToken(SqlBaseParser.KEYS, 0)

        def LAST(self):
            return self.getToken(SqlBaseParser.LAST, 0)

        def LATERAL(self):
            return self.getToken(SqlBaseParser.LATERAL, 0)

        def LAZY(self):
            return self.getToken(SqlBaseParser.LAZY, 0)

        def LIKE(self):
            return self.getToken(SqlBaseParser.LIKE, 0)

        def LIMIT(self):
            return self.getToken(SqlBaseParser.LIMIT, 0)

        def LINES(self):
            return self.getToken(SqlBaseParser.LINES, 0)

        def LIST(self):
            return self.getToken(SqlBaseParser.LIST, 0)

        def LOAD(self):
            return self.getToken(SqlBaseParser.LOAD, 0)

        def LOCAL(self):
            return self.getToken(SqlBaseParser.LOCAL, 0)

        def LOCATION(self):
            return self.getToken(SqlBaseParser.LOCATION, 0)

        def LOCK(self):
            return self.getToken(SqlBaseParser.LOCK, 0)

        def LOCKS(self):
            return self.getToken(SqlBaseParser.LOCKS, 0)

        def LOGICAL(self):
            return self.getToken(SqlBaseParser.LOGICAL, 0)

        def MACRO(self):
            return self.getToken(SqlBaseParser.MACRO, 0)

        def MAP(self):
            return self.getToken(SqlBaseParser.MAP, 0)

        def MATCHED(self):
            return self.getToken(SqlBaseParser.MATCHED, 0)

        def MERGE(self):
            return self.getToken(SqlBaseParser.MERGE, 0)

        def MSCK(self):
            return self.getToken(SqlBaseParser.MSCK, 0)

        def NAMESPACE(self):
            return self.getToken(SqlBaseParser.NAMESPACE, 0)

        def NAMESPACES(self):
            return self.getToken(SqlBaseParser.NAMESPACES, 0)

        def NO(self):
            return self.getToken(SqlBaseParser.NO, 0)

        def NULLS(self):
            return self.getToken(SqlBaseParser.NULLS, 0)

        def OF(self):
            return self.getToken(SqlBaseParser.OF, 0)

        def OPTION(self):
            return self.getToken(SqlBaseParser.OPTION, 0)

        def OPTIONS(self):
            return self.getToken(SqlBaseParser.OPTIONS, 0)

        def OUT(self):
            return self.getToken(SqlBaseParser.OUT, 0)

        def OUTPUTFORMAT(self):
            return self.getToken(SqlBaseParser.OUTPUTFORMAT, 0)

        def OVER(self):
            return self.getToken(SqlBaseParser.OVER, 0)

        def OVERLAY(self):
            return self.getToken(SqlBaseParser.OVERLAY, 0)

        def OVERWRITE(self):
            return self.getToken(SqlBaseParser.OVERWRITE, 0)

        def PARTITION(self):
            return self.getToken(SqlBaseParser.PARTITION, 0)

        def PARTITIONED(self):
            return self.getToken(SqlBaseParser.PARTITIONED, 0)

        def PARTITIONS(self):
            return self.getToken(SqlBaseParser.PARTITIONS, 0)

        def PERCENTLIT(self):
            return self.getToken(SqlBaseParser.PERCENTLIT, 0)

        def PIVOT(self):
            return self.getToken(SqlBaseParser.PIVOT, 0)

        def PLACING(self):
            return self.getToken(SqlBaseParser.PLACING, 0)

        def POSITION(self):
            return self.getToken(SqlBaseParser.POSITION, 0)

        def PRECEDING(self):
            return self.getToken(SqlBaseParser.PRECEDING, 0)

        def PRINCIPALS(self):
            return self.getToken(SqlBaseParser.PRINCIPALS, 0)

        def PROPERTIES(self):
            return self.getToken(SqlBaseParser.PROPERTIES, 0)

        def PURGE(self):
            return self.getToken(SqlBaseParser.PURGE, 0)

        def QUERY(self):
            return self.getToken(SqlBaseParser.QUERY, 0)

        def RANGE(self):
            return self.getToken(SqlBaseParser.RANGE, 0)

        def RECORDREADER(self):
            return self.getToken(SqlBaseParser.RECORDREADER, 0)

        def RECORDWRITER(self):
            return self.getToken(SqlBaseParser.RECORDWRITER, 0)

        def RECOVER(self):
            return self.getToken(SqlBaseParser.RECOVER, 0)

        def REDUCE(self):
            return self.getToken(SqlBaseParser.REDUCE, 0)

        def REFRESH(self):
            return self.getToken(SqlBaseParser.REFRESH, 0)

        def RENAME(self):
            return self.getToken(SqlBaseParser.RENAME, 0)

        def REPAIR(self):
            return self.getToken(SqlBaseParser.REPAIR, 0)

        def REPLACE(self):
            return self.getToken(SqlBaseParser.REPLACE, 0)

        def RESET(self):
            return self.getToken(SqlBaseParser.RESET, 0)

        def RESTRICT(self):
            return self.getToken(SqlBaseParser.RESTRICT, 0)

        def REVOKE(self):
            return self.getToken(SqlBaseParser.REVOKE, 0)

        def RLIKE(self):
            return self.getToken(SqlBaseParser.RLIKE, 0)

        def ROLE(self):
            return self.getToken(SqlBaseParser.ROLE, 0)

        def ROLES(self):
            return self.getToken(SqlBaseParser.ROLES, 0)

        def ROLLBACK(self):
            return self.getToken(SqlBaseParser.ROLLBACK, 0)

        def ROLLUP(self):
            return self.getToken(SqlBaseParser.ROLLUP, 0)

        def ROW(self):
            return self.getToken(SqlBaseParser.ROW, 0)

        def ROWS(self):
            return self.getToken(SqlBaseParser.ROWS, 0)

        def SCHEMA(self):
            return self.getToken(SqlBaseParser.SCHEMA, 0)

        def SEPARATED(self):
            return self.getToken(SqlBaseParser.SEPARATED, 0)

        def SERDE(self):
            return self.getToken(SqlBaseParser.SERDE, 0)

        def SERDEPROPERTIES(self):
            return self.getToken(SqlBaseParser.SERDEPROPERTIES, 0)

        def SET(self):
            return self.getToken(SqlBaseParser.SET, 0)

        def SETS(self):
            return self.getToken(SqlBaseParser.SETS, 0)

        def SHOW(self):
            return self.getToken(SqlBaseParser.SHOW, 0)

        def SKEWED(self):
            return self.getToken(SqlBaseParser.SKEWED, 0)

        def SORT(self):
            return self.getToken(SqlBaseParser.SORT, 0)

        def SORTED(self):
            return self.getToken(SqlBaseParser.SORTED, 0)

        def START(self):
            return self.getToken(SqlBaseParser.START, 0)

        def STATISTICS(self):
            return self.getToken(SqlBaseParser.STATISTICS, 0)

        def STORED(self):
            return self.getToken(SqlBaseParser.STORED, 0)

        def STRATIFY(self):
            return self.getToken(SqlBaseParser.STRATIFY, 0)

        def STRUCT(self):
            return self.getToken(SqlBaseParser.STRUCT, 0)

        def SUBSTR(self):
            return self.getToken(SqlBaseParser.SUBSTR, 0)

        def SUBSTRING(self):
            return self.getToken(SqlBaseParser.SUBSTRING, 0)

        def TABLES(self):
            return self.getToken(SqlBaseParser.TABLES, 0)

        def TABLESAMPLE(self):
            return self.getToken(SqlBaseParser.TABLESAMPLE, 0)

        def TBLPROPERTIES(self):
            return self.getToken(SqlBaseParser.TBLPROPERTIES, 0)

        def TEMPORARY(self):
            return self.getToken(SqlBaseParser.TEMPORARY, 0)

        def TERMINATED(self):
            return self.getToken(SqlBaseParser.TERMINATED, 0)

        def TOUCH(self):
            return self.getToken(SqlBaseParser.TOUCH, 0)

        def TRANSACTION(self):
            return self.getToken(SqlBaseParser.TRANSACTION, 0)

        def TRANSACTIONS(self):
            return self.getToken(SqlBaseParser.TRANSACTIONS, 0)

        def TRANSFORM(self):
            return self.getToken(SqlBaseParser.TRANSFORM, 0)

        def TRIM(self):
            return self.getToken(SqlBaseParser.TRIM, 0)

        def TRUE(self):
            return self.getToken(SqlBaseParser.TRUE, 0)

        def TRUNCATE(self):
            return self.getToken(SqlBaseParser.TRUNCATE, 0)

        def UNARCHIVE(self):
            return self.getToken(SqlBaseParser.UNARCHIVE, 0)

        def UNBOUNDED(self):
            return self.getToken(SqlBaseParser.UNBOUNDED, 0)

        def UNCACHE(self):
            return self.getToken(SqlBaseParser.UNCACHE, 0)

        def UNLOCK(self):
            return self.getToken(SqlBaseParser.UNLOCK, 0)

        def UNSET(self):
            return self.getToken(SqlBaseParser.UNSET, 0)

        def UPDATE(self):
            return self.getToken(SqlBaseParser.UPDATE, 0)

        def USE(self):
            return self.getToken(SqlBaseParser.USE, 0)

        def VALUES(self):
            return self.getToken(SqlBaseParser.VALUES, 0)

        def VIEW(self):
            return self.getToken(SqlBaseParser.VIEW, 0)

        def VIEWS(self):
            return self.getToken(SqlBaseParser.VIEWS, 0)

        def WINDOW(self):
            return self.getToken(SqlBaseParser.WINDOW, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_ansiNonReserved

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterAnsiNonReserved" ):
                listener.enterAnsiNonReserved(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitAnsiNonReserved" ):
                listener.exitAnsiNonReserved(self)




    def ansiNonReserved(self):

        localctx = SqlBaseParser.AnsiNonReservedContext(self, self._ctx, self.state)
        self.enterRule(localctx, 268, self.RULE_ansiNonReserved)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 3011
            _la = self._input.LA(1)
            if not((((_la) & ~0x3f) == 0 and ((1 << _la) & ((1 << SqlBaseParser.ADD) | (1 << SqlBaseParser.AFTER) | (1 << SqlBaseParser.ALTER) | (1 << SqlBaseParser.ANALYZE) | (1 << SqlBaseParser.ARCHIVE) | (1 << SqlBaseParser.ARRAY) | (1 << SqlBaseParser.ASC) | (1 << SqlBaseParser.AT) | (1 << SqlBaseParser.BETWEEN) | (1 << SqlBaseParser.BUCKET) | (1 << SqlBaseParser.BUCKETS) | (1 << SqlBaseParser.BY) | (1 << SqlBaseParser.CACHE) | (1 << SqlBaseParser.CASCADE) | (1 << SqlBaseParser.CHANGE) | (1 << SqlBaseParser.CLEAR) | (1 << SqlBaseParser.CLUSTER) | (1 << SqlBaseParser.CLUSTERED) | (1 << SqlBaseParser.CODEGEN) | (1 << SqlBaseParser.COLLECTION) | (1 << SqlBaseParser.COLUMNS) | (1 << SqlBaseParser.COMMENT) | (1 << SqlBaseParser.COMMIT) | (1 << SqlBaseParser.COMPACT) | (1 << SqlBaseParser.COMPACTIONS) | (1 << SqlBaseParser.COMPUTE) | (1 << SqlBaseParser.CONCATENATE) | (1 << SqlBaseParser.COST) | (1 << SqlBaseParser.CUBE) | (1 << SqlBaseParser.CURRENT) | (1 << SqlBaseParser.DATA) | (1 << SqlBaseParser.DATABASE) | (1 << SqlBaseParser.DATABASES))) != 0) or ((((_la - 65)) & ~0x3f) == 0 and ((1 << (_la - 65)) & ((1 << (SqlBaseParser.DBPROPERTIES - 65)) | (1 << (SqlBaseParser.DEFINED - 65)) | (1 << (SqlBaseParser.DELETE - 65)) | (1 << (SqlBaseParser.DELIMITED - 65)) | (1 << (SqlBaseParser.DESC - 65)) | (1 << (SqlBaseParser.DESCRIBE - 65)) | (1 << (SqlBaseParser.DFS - 65)) | (1 << (SqlBaseParser.DIRECTORIES - 65)) | (1 << (SqlBaseParser.DIRECTORY - 65)) | (1 << (SqlBaseParser.DISTRIBUTE - 65)) | (1 << (SqlBaseParser.DROP - 65)) | (1 << (SqlBaseParser.ESCAPED - 65)) | (1 << (SqlBaseParser.EXCHANGE - 65)) | (1 << (SqlBaseParser.EXISTS - 65)) | (1 << (SqlBaseParser.EXPLAIN - 65)) | (1 << (SqlBaseParser.EXPORT - 65)) | (1 << (SqlBaseParser.EXTENDED - 65)) | (1 << (SqlBaseParser.EXTERNAL - 65)) | (1 << (SqlBaseParser.EXTRACT - 65)) | (1 << (SqlBaseParser.FIELDS - 65)) | (1 << (SqlBaseParser.FILEFORMAT - 65)) | (1 << (SqlBaseParser.FIRST - 65)) | (1 << (SqlBaseParser.FOLLOWING - 65)) | (1 << (SqlBaseParser.FORMAT - 65)) | (1 << (SqlBaseParser.FORMATTED - 65)) | (1 << (SqlBaseParser.FUNCTION - 65)) | (1 << (SqlBaseParser.FUNCTIONS - 65)) | (1 << (SqlBaseParser.GLOBAL - 65)) | (1 << (SqlBaseParser.GROUPING - 65)) | (1 << (SqlBaseParser.IF - 65)) | (1 << (SqlBaseParser.IGNORE - 65)) | (1 << (SqlBaseParser.IMPORT - 65)) | (1 << (SqlBaseParser.INDEX - 65)) | (1 << (SqlBaseParser.INDEXES - 65)) | (1 << (SqlBaseParser.INPATH - 65)) | (1 << (SqlBaseParser.INPUTFORMAT - 65)) | (1 << (SqlBaseParser.INSERT - 65)) | (1 << (SqlBaseParser.INTERVAL - 65)) | (1 << (SqlBaseParser.ITEMS - 65)) | (1 << (SqlBaseParser.KEYS - 65)) | (1 << (SqlBaseParser.LAST - 65)) | (1 << (SqlBaseParser.LATERAL - 65)))) != 0) or ((((_la - 129)) & ~0x3f) == 0 and ((1 << (_la - 129)) & ((1 << (SqlBaseParser.LAZY - 129)) | (1 << (SqlBaseParser.LIKE - 129)) | (1 << (SqlBaseParser.LIMIT - 129)) | (1 << (SqlBaseParser.LINES - 129)) | (1 << (SqlBaseParser.LIST - 129)) | (1 << (SqlBaseParser.LOAD - 129)) | (1 << (SqlBaseParser.LOCAL - 129)) | (1 << (SqlBaseParser.LOCATION - 129)) | (1 << (SqlBaseParser.LOCK - 129)) | (1 << (SqlBaseParser.LOCKS - 129)) | (1 << (SqlBaseParser.LOGICAL - 129)) | (1 << (SqlBaseParser.MACRO - 129)) | (1 << (SqlBaseParser.MAP - 129)) | (1 << (SqlBaseParser.MATCHED - 129)) | (1 << (SqlBaseParser.MERGE - 129)) | (1 << (SqlBaseParser.MSCK - 129)) | (1 << (SqlBaseParser.NAMESPACE - 129)) | (1 << (SqlBaseParser.NAMESPACES - 129)) | (1 << (SqlBaseParser.NO - 129)) | (1 << (SqlBaseParser.NULLS - 129)) | (1 << (SqlBaseParser.OF - 129)) | (1 << (SqlBaseParser.OPTION - 129)) | (1 << (SqlBaseParser.OPTIONS - 129)) | (1 << (SqlBaseParser.OUT - 129)) | (1 << (SqlBaseParser.OUTPUTFORMAT - 129)) | (1 << (SqlBaseParser.OVER - 129)) | (1 << (SqlBaseParser.OVERLAY - 129)) | (1 << (SqlBaseParser.OVERWRITE - 129)) | (1 << (SqlBaseParser.PARTITION - 129)) | (1 << (SqlBaseParser.PARTITIONED - 129)) | (1 << (SqlBaseParser.PARTITIONS - 129)) | (1 << (SqlBaseParser.PERCENTLIT - 129)) | (1 << (SqlBaseParser.PIVOT - 129)) | (1 << (SqlBaseParser.PLACING - 129)) | (1 << (SqlBaseParser.POSITION - 129)) | (1 << (SqlBaseParser.PRECEDING - 129)) | (1 << (SqlBaseParser.PRINCIPALS - 129)) | (1 << (SqlBaseParser.PROPERTIES - 129)) | (1 << (SqlBaseParser.PURGE - 129)) | (1 << (SqlBaseParser.QUERY - 129)) | (1 << (SqlBaseParser.RANGE - 129)) | (1 << (SqlBaseParser.RECORDREADER - 129)) | (1 << (SqlBaseParser.RECORDWRITER - 129)) | (1 << (SqlBaseParser.RECOVER - 129)) | (1 << (SqlBaseParser.REDUCE - 129)) | (1 << (SqlBaseParser.REFRESH - 129)) | (1 << (SqlBaseParser.RENAME - 129)) | (1 << (SqlBaseParser.REPAIR - 129)) | (1 << (SqlBaseParser.REPLACE - 129)))) != 0) or ((((_la - 193)) & ~0x3f) == 0 and ((1 << (_la - 193)) & ((1 << (SqlBaseParser.RESET - 193)) | (1 << (SqlBaseParser.RESTRICT - 193)) | (1 << (SqlBaseParser.REVOKE - 193)) | (1 << (SqlBaseParser.RLIKE - 193)) | (1 << (SqlBaseParser.ROLE - 193)) | (1 << (SqlBaseParser.ROLES - 193)) | (1 << (SqlBaseParser.ROLLBACK - 193)) | (1 << (SqlBaseParser.ROLLUP - 193)) | (1 << (SqlBaseParser.ROW - 193)) | (1 << (SqlBaseParser.ROWS - 193)) | (1 << (SqlBaseParser.SCHEMA - 193)) | (1 << (SqlBaseParser.SEPARATED - 193)) | (1 << (SqlBaseParser.SERDE - 193)) | (1 << (SqlBaseParser.SERDEPROPERTIES - 193)) | (1 << (SqlBaseParser.SET - 193)) | (1 << (SqlBaseParser.SETS - 193)) | (1 << (SqlBaseParser.SHOW - 193)) | (1 << (SqlBaseParser.SKEWED - 193)) | (1 << (SqlBaseParser.SORT - 193)) | (1 << (SqlBaseParser.SORTED - 193)) | (1 << (SqlBaseParser.START - 193)) | (1 << (SqlBaseParser.STATISTICS - 193)) | (1 << (SqlBaseParser.STORED - 193)) | (1 << (SqlBaseParser.STRATIFY - 193)) | (1 << (SqlBaseParser.STRUCT - 193)) | (1 << (SqlBaseParser.SUBSTR - 193)) | (1 << (SqlBaseParser.SUBSTRING - 193)) | (1 << (SqlBaseParser.TABLES - 193)) | (1 << (SqlBaseParser.TABLESAMPLE - 193)) | (1 << (SqlBaseParser.TBLPROPERTIES - 193)) | (1 << (SqlBaseParser.TEMPORARY - 193)) | (1 << (SqlBaseParser.TERMINATED - 193)) | (1 << (SqlBaseParser.TOUCH - 193)) | (1 << (SqlBaseParser.TRANSACTION - 193)) | (1 << (SqlBaseParser.TRANSACTIONS - 193)) | (1 << (SqlBaseParser.TRANSFORM - 193)) | (1 << (SqlBaseParser.TRIM - 193)) | (1 << (SqlBaseParser.TRUE - 193)) | (1 << (SqlBaseParser.TRUNCATE - 193)) | (1 << (SqlBaseParser.UNARCHIVE - 193)) | (1 << (SqlBaseParser.UNBOUNDED - 193)) | (1 << (SqlBaseParser.UNCACHE - 193)) | (1 << (SqlBaseParser.UNLOCK - 193)) | (1 << (SqlBaseParser.UNSET - 193)) | (1 << (SqlBaseParser.UPDATE - 193)) | (1 << (SqlBaseParser.USE - 193)) | (1 << (SqlBaseParser.VALUES - 193)))) != 0) or ((((_la - 257)) & ~0x3f) == 0 and ((1 << (_la - 257)) & ((1 << (SqlBaseParser.VIEW - 257)) | (1 << (SqlBaseParser.VIEWS - 257)) | (1 << (SqlBaseParser.WINDOW - 257)) | (1 << (SqlBaseParser.DIV - 257)))) != 0)):
                self._errHandler.recoverInline(self)
            else:
                self._errHandler.reportMatch(self)
                self.consume()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class StrictNonReservedContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def ANTI(self):
            return self.getToken(SqlBaseParser.ANTI, 0)

        def CROSS(self):
            return self.getToken(SqlBaseParser.CROSS, 0)

        def EXCEPT(self):
            return self.getToken(SqlBaseParser.EXCEPT, 0)

        def FULL(self):
            return self.getToken(SqlBaseParser.FULL, 0)

        def INNER(self):
            return self.getToken(SqlBaseParser.INNER, 0)

        def INTERSECT(self):
            return self.getToken(SqlBaseParser.INTERSECT, 0)

        def JOIN(self):
            return self.getToken(SqlBaseParser.JOIN, 0)

        def LEFT(self):
            return self.getToken(SqlBaseParser.LEFT, 0)

        def NATURAL(self):
            return self.getToken(SqlBaseParser.NATURAL, 0)

        def ON(self):
            return self.getToken(SqlBaseParser.ON, 0)

        def RIGHT(self):
            return self.getToken(SqlBaseParser.RIGHT, 0)

        def SEMI(self):
            return self.getToken(SqlBaseParser.SEMI, 0)

        def SETMINUS(self):
            return self.getToken(SqlBaseParser.SETMINUS, 0)

        def UNION(self):
            return self.getToken(SqlBaseParser.UNION, 0)

        def USING(self):
            return self.getToken(SqlBaseParser.USING, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_strictNonReserved

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterStrictNonReserved" ):
                listener.enterStrictNonReserved(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitStrictNonReserved" ):
                listener.exitStrictNonReserved(self)




    def strictNonReserved(self):

        localctx = SqlBaseParser.StrictNonReservedContext(self, self._ctx, self.state)
        self.enterRule(localctx, 270, self.RULE_strictNonReserved)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 3013
            _la = self._input.LA(1)
            if not(((((_la - 18)) & ~0x3f) == 0 and ((1 << (_la - 18)) & ((1 << (SqlBaseParser.ANTI - 18)) | (1 << (SqlBaseParser.CROSS - 18)) | (1 << (SqlBaseParser.EXCEPT - 18)))) != 0) or ((((_la - 101)) & ~0x3f) == 0 and ((1 << (_la - 101)) & ((1 << (SqlBaseParser.FULL - 101)) | (1 << (SqlBaseParser.INNER - 101)) | (1 << (SqlBaseParser.INTERSECT - 101)) | (1 << (SqlBaseParser.JOIN - 101)) | (1 << (SqlBaseParser.LEFT - 101)) | (1 << (SqlBaseParser.NATURAL - 101)) | (1 << (SqlBaseParser.ON - 101)))) != 0) or ((((_la - 196)) & ~0x3f) == 0 and ((1 << (_la - 196)) & ((1 << (SqlBaseParser.RIGHT - 196)) | (1 << (SqlBaseParser.SEMI - 196)) | (1 << (SqlBaseParser.SETMINUS - 196)) | (1 << (SqlBaseParser.UNION - 196)) | (1 << (SqlBaseParser.USING - 196)))) != 0)):
                self._errHandler.recoverInline(self)
            else:
                self._errHandler.reportMatch(self)
                self.consume()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class NonReservedContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def ADD(self):
            return self.getToken(SqlBaseParser.ADD, 0)

        def AFTER(self):
            return self.getToken(SqlBaseParser.AFTER, 0)

        def ALL(self):
            return self.getToken(SqlBaseParser.ALL, 0)

        def ALTER(self):
            return self.getToken(SqlBaseParser.ALTER, 0)

        def ANALYZE(self):
            return self.getToken(SqlBaseParser.ANALYZE, 0)

        def AND(self):
            return self.getToken(SqlBaseParser.AND, 0)

        def ANY(self):
            return self.getToken(SqlBaseParser.ANY, 0)

        def ARCHIVE(self):
            return self.getToken(SqlBaseParser.ARCHIVE, 0)

        def ARRAY(self):
            return self.getToken(SqlBaseParser.ARRAY, 0)

        def AS(self):
            return self.getToken(SqlBaseParser.AS, 0)

        def ASC(self):
            return self.getToken(SqlBaseParser.ASC, 0)

        def AT(self):
            return self.getToken(SqlBaseParser.AT, 0)

        def AUTHORIZATION(self):
            return self.getToken(SqlBaseParser.AUTHORIZATION, 0)

        def BETWEEN(self):
            return self.getToken(SqlBaseParser.BETWEEN, 0)

        def BOTH(self):
            return self.getToken(SqlBaseParser.BOTH, 0)

        def BUCKET(self):
            return self.getToken(SqlBaseParser.BUCKET, 0)

        def BUCKETS(self):
            return self.getToken(SqlBaseParser.BUCKETS, 0)

        def BY(self):
            return self.getToken(SqlBaseParser.BY, 0)

        def CACHE(self):
            return self.getToken(SqlBaseParser.CACHE, 0)

        def CASCADE(self):
            return self.getToken(SqlBaseParser.CASCADE, 0)

        def CASE(self):
            return self.getToken(SqlBaseParser.CASE, 0)

        def CAST(self):
            return self.getToken(SqlBaseParser.CAST, 0)

        def CHANGE(self):
            return self.getToken(SqlBaseParser.CHANGE, 0)

        def CHECK(self):
            return self.getToken(SqlBaseParser.CHECK, 0)

        def CLEAR(self):
            return self.getToken(SqlBaseParser.CLEAR, 0)

        def CLUSTER(self):
            return self.getToken(SqlBaseParser.CLUSTER, 0)

        def CLUSTERED(self):
            return self.getToken(SqlBaseParser.CLUSTERED, 0)

        def CODEGEN(self):
            return self.getToken(SqlBaseParser.CODEGEN, 0)

        def COLLATE(self):
            return self.getToken(SqlBaseParser.COLLATE, 0)

        def COLLECTION(self):
            return self.getToken(SqlBaseParser.COLLECTION, 0)

        def COLUMN(self):
            return self.getToken(SqlBaseParser.COLUMN, 0)

        def COLUMNS(self):
            return self.getToken(SqlBaseParser.COLUMNS, 0)

        def COMMENT(self):
            return self.getToken(SqlBaseParser.COMMENT, 0)

        def COMMIT(self):
            return self.getToken(SqlBaseParser.COMMIT, 0)

        def COMPACT(self):
            return self.getToken(SqlBaseParser.COMPACT, 0)

        def COMPACTIONS(self):
            return self.getToken(SqlBaseParser.COMPACTIONS, 0)

        def COMPUTE(self):
            return self.getToken(SqlBaseParser.COMPUTE, 0)

        def CONCATENATE(self):
            return self.getToken(SqlBaseParser.CONCATENATE, 0)

        def CONSTRAINT(self):
            return self.getToken(SqlBaseParser.CONSTRAINT, 0)

        def COST(self):
            return self.getToken(SqlBaseParser.COST, 0)

        def CREATE(self):
            return self.getToken(SqlBaseParser.CREATE, 0)

        def CUBE(self):
            return self.getToken(SqlBaseParser.CUBE, 0)

        def CURRENT(self):
            return self.getToken(SqlBaseParser.CURRENT, 0)

        def CURRENT_DATE(self):
            return self.getToken(SqlBaseParser.CURRENT_DATE, 0)

        def CURRENT_TIME(self):
            return self.getToken(SqlBaseParser.CURRENT_TIME, 0)

        def CURRENT_TIMESTAMP(self):
            return self.getToken(SqlBaseParser.CURRENT_TIMESTAMP, 0)

        def CURRENT_USER(self):
            return self.getToken(SqlBaseParser.CURRENT_USER, 0)

        def DATA(self):
            return self.getToken(SqlBaseParser.DATA, 0)

        def DATABASE(self):
            return self.getToken(SqlBaseParser.DATABASE, 0)

        def DATABASES(self):
            return self.getToken(SqlBaseParser.DATABASES, 0)

        def DAY(self):
            return self.getToken(SqlBaseParser.DAY, 0)

        def DBPROPERTIES(self):
            return self.getToken(SqlBaseParser.DBPROPERTIES, 0)

        def DEFINED(self):
            return self.getToken(SqlBaseParser.DEFINED, 0)

        def DELETE(self):
            return self.getToken(SqlBaseParser.DELETE, 0)

        def DELIMITED(self):
            return self.getToken(SqlBaseParser.DELIMITED, 0)

        def DESC(self):
            return self.getToken(SqlBaseParser.DESC, 0)

        def DESCRIBE(self):
            return self.getToken(SqlBaseParser.DESCRIBE, 0)

        def DFS(self):
            return self.getToken(SqlBaseParser.DFS, 0)

        def DIRECTORIES(self):
            return self.getToken(SqlBaseParser.DIRECTORIES, 0)

        def DIRECTORY(self):
            return self.getToken(SqlBaseParser.DIRECTORY, 0)

        def DISTINCT(self):
            return self.getToken(SqlBaseParser.DISTINCT, 0)

        def DISTRIBUTE(self):
            return self.getToken(SqlBaseParser.DISTRIBUTE, 0)

        def DIV(self):
            return self.getToken(SqlBaseParser.DIV, 0)

        def DROP(self):
            return self.getToken(SqlBaseParser.DROP, 0)

        def ELSE(self):
            return self.getToken(SqlBaseParser.ELSE, 0)

        def END(self):
            return self.getToken(SqlBaseParser.END, 0)

        def ESCAPE(self):
            return self.getToken(SqlBaseParser.ESCAPE, 0)

        def ESCAPED(self):
            return self.getToken(SqlBaseParser.ESCAPED, 0)

        def EXCHANGE(self):
            return self.getToken(SqlBaseParser.EXCHANGE, 0)

        def EXISTS(self):
            return self.getToken(SqlBaseParser.EXISTS, 0)

        def EXPLAIN(self):
            return self.getToken(SqlBaseParser.EXPLAIN, 0)

        def EXPORT(self):
            return self.getToken(SqlBaseParser.EXPORT, 0)

        def EXTENDED(self):
            return self.getToken(SqlBaseParser.EXTENDED, 0)

        def EXTERNAL(self):
            return self.getToken(SqlBaseParser.EXTERNAL, 0)

        def EXTRACT(self):
            return self.getToken(SqlBaseParser.EXTRACT, 0)

        def FALSE(self):
            return self.getToken(SqlBaseParser.FALSE, 0)

        def FETCH(self):
            return self.getToken(SqlBaseParser.FETCH, 0)

        def FILTER(self):
            return self.getToken(SqlBaseParser.FILTER, 0)

        def FIELDS(self):
            return self.getToken(SqlBaseParser.FIELDS, 0)

        def FILEFORMAT(self):
            return self.getToken(SqlBaseParser.FILEFORMAT, 0)

        def FIRST(self):
            return self.getToken(SqlBaseParser.FIRST, 0)

        def FOLLOWING(self):
            return self.getToken(SqlBaseParser.FOLLOWING, 0)

        def FOR(self):
            return self.getToken(SqlBaseParser.FOR, 0)

        def FOREIGN(self):
            return self.getToken(SqlBaseParser.FOREIGN, 0)

        def FORMAT(self):
            return self.getToken(SqlBaseParser.FORMAT, 0)

        def FORMATTED(self):
            return self.getToken(SqlBaseParser.FORMATTED, 0)

        def FROM(self):
            return self.getToken(SqlBaseParser.FROM, 0)

        def FUNCTION(self):
            return self.getToken(SqlBaseParser.FUNCTION, 0)

        def FUNCTIONS(self):
            return self.getToken(SqlBaseParser.FUNCTIONS, 0)

        def GLOBAL(self):
            return self.getToken(SqlBaseParser.GLOBAL, 0)

        def GRANT(self):
            return self.getToken(SqlBaseParser.GRANT, 0)

        def GROUP(self):
            return self.getToken(SqlBaseParser.GROUP, 0)

        def GROUPING(self):
            return self.getToken(SqlBaseParser.GROUPING, 0)

        def HAVING(self):
            return self.getToken(SqlBaseParser.HAVING, 0)

        def HOUR(self):
            return self.getToken(SqlBaseParser.HOUR, 0)

        def IF(self):
            return self.getToken(SqlBaseParser.IF, 0)

        def IGNORE(self):
            return self.getToken(SqlBaseParser.IGNORE, 0)

        def IMPORT(self):
            return self.getToken(SqlBaseParser.IMPORT, 0)

        def IN(self):
            return self.getToken(SqlBaseParser.IN, 0)

        def INDEX(self):
            return self.getToken(SqlBaseParser.INDEX, 0)

        def INDEXES(self):
            return self.getToken(SqlBaseParser.INDEXES, 0)

        def INPATH(self):
            return self.getToken(SqlBaseParser.INPATH, 0)

        def INPUTFORMAT(self):
            return self.getToken(SqlBaseParser.INPUTFORMAT, 0)

        def INSERT(self):
            return self.getToken(SqlBaseParser.INSERT, 0)

        def INTERVAL(self):
            return self.getToken(SqlBaseParser.INTERVAL, 0)

        def INTO(self):
            return self.getToken(SqlBaseParser.INTO, 0)

        def IS(self):
            return self.getToken(SqlBaseParser.IS, 0)

        def ITEMS(self):
            return self.getToken(SqlBaseParser.ITEMS, 0)

        def KEYS(self):
            return self.getToken(SqlBaseParser.KEYS, 0)

        def LAST(self):
            return self.getToken(SqlBaseParser.LAST, 0)

        def LATERAL(self):
            return self.getToken(SqlBaseParser.LATERAL, 0)

        def LAZY(self):
            return self.getToken(SqlBaseParser.LAZY, 0)

        def LEADING(self):
            return self.getToken(SqlBaseParser.LEADING, 0)

        def LIKE(self):
            return self.getToken(SqlBaseParser.LIKE, 0)

        def LIMIT(self):
            return self.getToken(SqlBaseParser.LIMIT, 0)

        def LINES(self):
            return self.getToken(SqlBaseParser.LINES, 0)

        def LIST(self):
            return self.getToken(SqlBaseParser.LIST, 0)

        def LOAD(self):
            return self.getToken(SqlBaseParser.LOAD, 0)

        def LOCAL(self):
            return self.getToken(SqlBaseParser.LOCAL, 0)

        def LOCATION(self):
            return self.getToken(SqlBaseParser.LOCATION, 0)

        def LOCK(self):
            return self.getToken(SqlBaseParser.LOCK, 0)

        def LOCKS(self):
            return self.getToken(SqlBaseParser.LOCKS, 0)

        def LOGICAL(self):
            return self.getToken(SqlBaseParser.LOGICAL, 0)

        def MACRO(self):
            return self.getToken(SqlBaseParser.MACRO, 0)

        def MAP(self):
            return self.getToken(SqlBaseParser.MAP, 0)

        def MATCHED(self):
            return self.getToken(SqlBaseParser.MATCHED, 0)

        def MERGE(self):
            return self.getToken(SqlBaseParser.MERGE, 0)

        def MINUTE(self):
            return self.getToken(SqlBaseParser.MINUTE, 0)

        def MONTH(self):
            return self.getToken(SqlBaseParser.MONTH, 0)

        def MSCK(self):
            return self.getToken(SqlBaseParser.MSCK, 0)

        def NAMESPACE(self):
            return self.getToken(SqlBaseParser.NAMESPACE, 0)

        def NAMESPACES(self):
            return self.getToken(SqlBaseParser.NAMESPACES, 0)

        def NO(self):
            return self.getToken(SqlBaseParser.NO, 0)

        def NOT(self):
            return self.getToken(SqlBaseParser.NOT, 0)

        def NULL(self):
            return self.getToken(SqlBaseParser.NULL, 0)

        def NULLS(self):
            return self.getToken(SqlBaseParser.NULLS, 0)

        def OF(self):
            return self.getToken(SqlBaseParser.OF, 0)

        def ONLY(self):
            return self.getToken(SqlBaseParser.ONLY, 0)

        def OPTION(self):
            return self.getToken(SqlBaseParser.OPTION, 0)

        def OPTIONS(self):
            return self.getToken(SqlBaseParser.OPTIONS, 0)

        def OR(self):
            return self.getToken(SqlBaseParser.OR, 0)

        def ORDER(self):
            return self.getToken(SqlBaseParser.ORDER, 0)

        def OUT(self):
            return self.getToken(SqlBaseParser.OUT, 0)

        def OUTER(self):
            return self.getToken(SqlBaseParser.OUTER, 0)

        def OUTPUTFORMAT(self):
            return self.getToken(SqlBaseParser.OUTPUTFORMAT, 0)

        def OVER(self):
            return self.getToken(SqlBaseParser.OVER, 0)

        def OVERLAPS(self):
            return self.getToken(SqlBaseParser.OVERLAPS, 0)

        def OVERLAY(self):
            return self.getToken(SqlBaseParser.OVERLAY, 0)

        def OVERWRITE(self):
            return self.getToken(SqlBaseParser.OVERWRITE, 0)

        def PARTITION(self):
            return self.getToken(SqlBaseParser.PARTITION, 0)

        def PARTITIONED(self):
            return self.getToken(SqlBaseParser.PARTITIONED, 0)

        def PARTITIONS(self):
            return self.getToken(SqlBaseParser.PARTITIONS, 0)

        def PERCENTLIT(self):
            return self.getToken(SqlBaseParser.PERCENTLIT, 0)

        def PIVOT(self):
            return self.getToken(SqlBaseParser.PIVOT, 0)

        def PLACING(self):
            return self.getToken(SqlBaseParser.PLACING, 0)

        def POSITION(self):
            return self.getToken(SqlBaseParser.POSITION, 0)

        def PRECEDING(self):
            return self.getToken(SqlBaseParser.PRECEDING, 0)

        def PRIMARY(self):
            return self.getToken(SqlBaseParser.PRIMARY, 0)

        def PRINCIPALS(self):
            return self.getToken(SqlBaseParser.PRINCIPALS, 0)

        def PROPERTIES(self):
            return self.getToken(SqlBaseParser.PROPERTIES, 0)

        def PURGE(self):
            return self.getToken(SqlBaseParser.PURGE, 0)

        def QUERY(self):
            return self.getToken(SqlBaseParser.QUERY, 0)

        def RANGE(self):
            return self.getToken(SqlBaseParser.RANGE, 0)

        def RECORDREADER(self):
            return self.getToken(SqlBaseParser.RECORDREADER, 0)

        def RECORDWRITER(self):
            return self.getToken(SqlBaseParser.RECORDWRITER, 0)

        def RECOVER(self):
            return self.getToken(SqlBaseParser.RECOVER, 0)

        def REDUCE(self):
            return self.getToken(SqlBaseParser.REDUCE, 0)

        def REFERENCES(self):
            return self.getToken(SqlBaseParser.REFERENCES, 0)

        def REFRESH(self):
            return self.getToken(SqlBaseParser.REFRESH, 0)

        def RENAME(self):
            return self.getToken(SqlBaseParser.RENAME, 0)

        def REPAIR(self):
            return self.getToken(SqlBaseParser.REPAIR, 0)

        def REPLACE(self):
            return self.getToken(SqlBaseParser.REPLACE, 0)

        def RESET(self):
            return self.getToken(SqlBaseParser.RESET, 0)

        def RESTRICT(self):
            return self.getToken(SqlBaseParser.RESTRICT, 0)

        def REVOKE(self):
            return self.getToken(SqlBaseParser.REVOKE, 0)

        def RLIKE(self):
            return self.getToken(SqlBaseParser.RLIKE, 0)

        def ROLE(self):
            return self.getToken(SqlBaseParser.ROLE, 0)

        def ROLES(self):
            return self.getToken(SqlBaseParser.ROLES, 0)

        def ROLLBACK(self):
            return self.getToken(SqlBaseParser.ROLLBACK, 0)

        def ROLLUP(self):
            return self.getToken(SqlBaseParser.ROLLUP, 0)

        def ROW(self):
            return self.getToken(SqlBaseParser.ROW, 0)

        def ROWS(self):
            return self.getToken(SqlBaseParser.ROWS, 0)

        def SCHEMA(self):
            return self.getToken(SqlBaseParser.SCHEMA, 0)

        def SECOND(self):
            return self.getToken(SqlBaseParser.SECOND, 0)

        def SELECT(self):
            return self.getToken(SqlBaseParser.SELECT, 0)

        def SEPARATED(self):
            return self.getToken(SqlBaseParser.SEPARATED, 0)

        def SERDE(self):
            return self.getToken(SqlBaseParser.SERDE, 0)

        def SERDEPROPERTIES(self):
            return self.getToken(SqlBaseParser.SERDEPROPERTIES, 0)

        def SESSION_USER(self):
            return self.getToken(SqlBaseParser.SESSION_USER, 0)

        def SET(self):
            return self.getToken(SqlBaseParser.SET, 0)

        def SETS(self):
            return self.getToken(SqlBaseParser.SETS, 0)

        def SHOW(self):
            return self.getToken(SqlBaseParser.SHOW, 0)

        def SKEWED(self):
            return self.getToken(SqlBaseParser.SKEWED, 0)

        def SOME(self):
            return self.getToken(SqlBaseParser.SOME, 0)

        def SORT(self):
            return self.getToken(SqlBaseParser.SORT, 0)

        def SORTED(self):
            return self.getToken(SqlBaseParser.SORTED, 0)

        def START(self):
            return self.getToken(SqlBaseParser.START, 0)

        def STATISTICS(self):
            return self.getToken(SqlBaseParser.STATISTICS, 0)

        def STORED(self):
            return self.getToken(SqlBaseParser.STORED, 0)

        def STRATIFY(self):
            return self.getToken(SqlBaseParser.STRATIFY, 0)

        def STRUCT(self):
            return self.getToken(SqlBaseParser.STRUCT, 0)

        def SUBSTR(self):
            return self.getToken(SqlBaseParser.SUBSTR, 0)

        def SUBSTRING(self):
            return self.getToken(SqlBaseParser.SUBSTRING, 0)

        def TABLE(self):
            return self.getToken(SqlBaseParser.TABLE, 0)

        def TABLES(self):
            return self.getToken(SqlBaseParser.TABLES, 0)

        def TABLESAMPLE(self):
            return self.getToken(SqlBaseParser.TABLESAMPLE, 0)

        def TBLPROPERTIES(self):
            return self.getToken(SqlBaseParser.TBLPROPERTIES, 0)

        def TEMPORARY(self):
            return self.getToken(SqlBaseParser.TEMPORARY, 0)

        def TERMINATED(self):
            return self.getToken(SqlBaseParser.TERMINATED, 0)

        def THEN(self):
            return self.getToken(SqlBaseParser.THEN, 0)

        def TO(self):
            return self.getToken(SqlBaseParser.TO, 0)

        def TOUCH(self):
            return self.getToken(SqlBaseParser.TOUCH, 0)

        def TRAILING(self):
            return self.getToken(SqlBaseParser.TRAILING, 0)

        def TRANSACTION(self):
            return self.getToken(SqlBaseParser.TRANSACTION, 0)

        def TRANSACTIONS(self):
            return self.getToken(SqlBaseParser.TRANSACTIONS, 0)

        def TRANSFORM(self):
            return self.getToken(SqlBaseParser.TRANSFORM, 0)

        def TRIM(self):
            return self.getToken(SqlBaseParser.TRIM, 0)

        def TRUE(self):
            return self.getToken(SqlBaseParser.TRUE, 0)

        def TRUNCATE(self):
            return self.getToken(SqlBaseParser.TRUNCATE, 0)

        def TYPE(self):
            return self.getToken(SqlBaseParser.TYPE, 0)

        def UNARCHIVE(self):
            return self.getToken(SqlBaseParser.UNARCHIVE, 0)

        def UNBOUNDED(self):
            return self.getToken(SqlBaseParser.UNBOUNDED, 0)

        def UNCACHE(self):
            return self.getToken(SqlBaseParser.UNCACHE, 0)

        def UNIQUE(self):
            return self.getToken(SqlBaseParser.UNIQUE, 0)

        def UNKNOWN(self):
            return self.getToken(SqlBaseParser.UNKNOWN, 0)

        def UNLOCK(self):
            return self.getToken(SqlBaseParser.UNLOCK, 0)

        def UNSET(self):
            return self.getToken(SqlBaseParser.UNSET, 0)

        def UPDATE(self):
            return self.getToken(SqlBaseParser.UPDATE, 0)

        def USE(self):
            return self.getToken(SqlBaseParser.USE, 0)

        def USER(self):
            return self.getToken(SqlBaseParser.USER, 0)

        def VALUES(self):
            return self.getToken(SqlBaseParser.VALUES, 0)

        def VIEW(self):
            return self.getToken(SqlBaseParser.VIEW, 0)

        def VIEWS(self):
            return self.getToken(SqlBaseParser.VIEWS, 0)

        def WHEN(self):
            return self.getToken(SqlBaseParser.WHEN, 0)

        def WHERE(self):
            return self.getToken(SqlBaseParser.WHERE, 0)

        def WINDOW(self):
            return self.getToken(SqlBaseParser.WINDOW, 0)

        def WITH(self):
            return self.getToken(SqlBaseParser.WITH, 0)

        def YEAR(self):
            return self.getToken(SqlBaseParser.YEAR, 0)

        def getRuleIndex(self):
            return SqlBaseParser.RULE_nonReserved

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterNonReserved" ):
                listener.enterNonReserved(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitNonReserved" ):
                listener.exitNonReserved(self)




    def nonReserved(self):

        localctx = SqlBaseParser.NonReservedContext(self, self._ctx, self.state)
        self.enterRule(localctx, 272, self.RULE_nonReserved)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 3015
            _la = self._input.LA(1)
            if not((((_la) & ~0x3f) == 0 and ((1 << _la) & ((1 << SqlBaseParser.ADD) | (1 << SqlBaseParser.AFTER) | (1 << SqlBaseParser.ALL) | (1 << SqlBaseParser.ALTER) | (1 << SqlBaseParser.ANALYZE) | (1 << SqlBaseParser.AND) | (1 << SqlBaseParser.ANY) | (1 << SqlBaseParser.ARCHIVE) | (1 << SqlBaseParser.ARRAY) | (1 << SqlBaseParser.AS) | (1 << SqlBaseParser.ASC) | (1 << SqlBaseParser.AT) | (1 << SqlBaseParser.AUTHORIZATION) | (1 << SqlBaseParser.BETWEEN) | (1 << SqlBaseParser.BOTH) | (1 << SqlBaseParser.BUCKET) | (1 << SqlBaseParser.BUCKETS) | (1 << SqlBaseParser.BY) | (1 << SqlBaseParser.CACHE) | (1 << SqlBaseParser.CASCADE) | (1 << SqlBaseParser.CASE) | (1 << SqlBaseParser.CAST) | (1 << SqlBaseParser.CHANGE) | (1 << SqlBaseParser.CHECK) | (1 << SqlBaseParser.CLEAR) | (1 << SqlBaseParser.CLUSTER) | (1 << SqlBaseParser.CLUSTERED) | (1 << SqlBaseParser.CODEGEN) | (1 << SqlBaseParser.COLLATE) | (1 << SqlBaseParser.COLLECTION) | (1 << SqlBaseParser.COLUMN) | (1 << SqlBaseParser.COLUMNS) | (1 << SqlBaseParser.COMMENT) | (1 << SqlBaseParser.COMMIT) | (1 << SqlBaseParser.COMPACT) | (1 << SqlBaseParser.COMPACTIONS) | (1 << SqlBaseParser.COMPUTE) | (1 << SqlBaseParser.CONCATENATE) | (1 << SqlBaseParser.CONSTRAINT) | (1 << SqlBaseParser.COST) | (1 << SqlBaseParser.CREATE) | (1 << SqlBaseParser.CUBE) | (1 << SqlBaseParser.CURRENT) | (1 << SqlBaseParser.CURRENT_DATE) | (1 << SqlBaseParser.CURRENT_TIME) | (1 << SqlBaseParser.CURRENT_TIMESTAMP) | (1 << SqlBaseParser.CURRENT_USER) | (1 << SqlBaseParser.DATA) | (1 << SqlBaseParser.DATABASE) | (1 << SqlBaseParser.DATABASES))) != 0) or ((((_la - 64)) & ~0x3f) == 0 and ((1 << (_la - 64)) & ((1 << (SqlBaseParser.DAY - 64)) | (1 << (SqlBaseParser.DBPROPERTIES - 64)) | (1 << (SqlBaseParser.DEFINED - 64)) | (1 << (SqlBaseParser.DELETE - 64)) | (1 << (SqlBaseParser.DELIMITED - 64)) | (1 << (SqlBaseParser.DESC - 64)) | (1 << (SqlBaseParser.DESCRIBE - 64)) | (1 << (SqlBaseParser.DFS - 64)) | (1 << (SqlBaseParser.DIRECTORIES - 64)) | (1 << (SqlBaseParser.DIRECTORY - 64)) | (1 << (SqlBaseParser.DISTINCT - 64)) | (1 << (SqlBaseParser.DISTRIBUTE - 64)) | (1 << (SqlBaseParser.DROP - 64)) | (1 << (SqlBaseParser.ELSE - 64)) | (1 << (SqlBaseParser.END - 64)) | (1 << (SqlBaseParser.ESCAPE - 64)) | (1 << (SqlBaseParser.ESCAPED - 64)) | (1 << (SqlBaseParser.EXCHANGE - 64)) | (1 << (SqlBaseParser.EXISTS - 64)) | (1 << (SqlBaseParser.EXPLAIN - 64)) | (1 << (SqlBaseParser.EXPORT - 64)) | (1 << (SqlBaseParser.EXTENDED - 64)) | (1 << (SqlBaseParser.EXTERNAL - 64)) | (1 << (SqlBaseParser.EXTRACT - 64)) | (1 << (SqlBaseParser.FALSE - 64)) | (1 << (SqlBaseParser.FETCH - 64)) | (1 << (SqlBaseParser.FIELDS - 64)) | (1 << (SqlBaseParser.FILTER - 64)) | (1 << (SqlBaseParser.FILEFORMAT - 64)) | (1 << (SqlBaseParser.FIRST - 64)) | (1 << (SqlBaseParser.FOLLOWING - 64)) | (1 << (SqlBaseParser.FOR - 64)) | (1 << (SqlBaseParser.FOREIGN - 64)) | (1 << (SqlBaseParser.FORMAT - 64)) | (1 << (SqlBaseParser.FORMATTED - 64)) | (1 << (SqlBaseParser.FROM - 64)) | (1 << (SqlBaseParser.FUNCTION - 64)) | (1 << (SqlBaseParser.FUNCTIONS - 64)) | (1 << (SqlBaseParser.GLOBAL - 64)) | (1 << (SqlBaseParser.GRANT - 64)) | (1 << (SqlBaseParser.GROUP - 64)) | (1 << (SqlBaseParser.GROUPING - 64)) | (1 << (SqlBaseParser.HAVING - 64)) | (1 << (SqlBaseParser.HOUR - 64)) | (1 << (SqlBaseParser.IF - 64)) | (1 << (SqlBaseParser.IGNORE - 64)) | (1 << (SqlBaseParser.IMPORT - 64)) | (1 << (SqlBaseParser.IN - 64)) | (1 << (SqlBaseParser.INDEX - 64)) | (1 << (SqlBaseParser.INDEXES - 64)) | (1 << (SqlBaseParser.INPATH - 64)) | (1 << (SqlBaseParser.INPUTFORMAT - 64)) | (1 << (SqlBaseParser.INSERT - 64)) | (1 << (SqlBaseParser.INTERVAL - 64)) | (1 << (SqlBaseParser.INTO - 64)) | (1 << (SqlBaseParser.IS - 64)) | (1 << (SqlBaseParser.ITEMS - 64)) | (1 << (SqlBaseParser.KEYS - 64)) | (1 << (SqlBaseParser.LAST - 64)))) != 0) or ((((_la - 128)) & ~0x3f) == 0 and ((1 << (_la - 128)) & ((1 << (SqlBaseParser.LATERAL - 128)) | (1 << (SqlBaseParser.LAZY - 128)) | (1 << (SqlBaseParser.LEADING - 128)) | (1 << (SqlBaseParser.LIKE - 128)) | (1 << (SqlBaseParser.LIMIT - 128)) | (1 << (SqlBaseParser.LINES - 128)) | (1 << (SqlBaseParser.LIST - 128)) | (1 << (SqlBaseParser.LOAD - 128)) | (1 << (SqlBaseParser.LOCAL - 128)) | (1 << (SqlBaseParser.LOCATION - 128)) | (1 << (SqlBaseParser.LOCK - 128)) | (1 << (SqlBaseParser.LOCKS - 128)) | (1 << (SqlBaseParser.LOGICAL - 128)) | (1 << (SqlBaseParser.MACRO - 128)) | (1 << (SqlBaseParser.MAP - 128)) | (1 << (SqlBaseParser.MATCHED - 128)) | (1 << (SqlBaseParser.MERGE - 128)) | (1 << (SqlBaseParser.MINUTE - 128)) | (1 << (SqlBaseParser.MONTH - 128)) | (1 << (SqlBaseParser.MSCK - 128)) | (1 << (SqlBaseParser.NAMESPACE - 128)) | (1 << (SqlBaseParser.NAMESPACES - 128)) | (1 << (SqlBaseParser.NO - 128)) | (1 << (SqlBaseParser.NOT - 128)) | (1 << (SqlBaseParser.NULL - 128)) | (1 << (SqlBaseParser.NULLS - 128)) | (1 << (SqlBaseParser.OF - 128)) | (1 << (SqlBaseParser.ONLY - 128)) | (1 << (SqlBaseParser.OPTION - 128)) | (1 << (SqlBaseParser.OPTIONS - 128)) | (1 << (SqlBaseParser.OR - 128)) | (1 << (SqlBaseParser.ORDER - 128)) | (1 << (SqlBaseParser.OUT - 128)) | (1 << (SqlBaseParser.OUTER - 128)) | (1 << (SqlBaseParser.OUTPUTFORMAT - 128)) | (1 << (SqlBaseParser.OVER - 128)) | (1 << (SqlBaseParser.OVERLAPS - 128)) | (1 << (SqlBaseParser.OVERLAY - 128)) | (1 << (SqlBaseParser.OVERWRITE - 128)) | (1 << (SqlBaseParser.PARTITION - 128)) | (1 << (SqlBaseParser.PARTITIONED - 128)) | (1 << (SqlBaseParser.PARTITIONS - 128)) | (1 << (SqlBaseParser.PERCENTLIT - 128)) | (1 << (SqlBaseParser.PIVOT - 128)) | (1 << (SqlBaseParser.PLACING - 128)) | (1 << (SqlBaseParser.POSITION - 128)) | (1 << (SqlBaseParser.PRECEDING - 128)) | (1 << (SqlBaseParser.PRIMARY - 128)) | (1 << (SqlBaseParser.PRINCIPALS - 128)) | (1 << (SqlBaseParser.PROPERTIES - 128)) | (1 << (SqlBaseParser.PURGE - 128)) | (1 << (SqlBaseParser.QUERY - 128)) | (1 << (SqlBaseParser.RANGE - 128)) | (1 << (SqlBaseParser.RECORDREADER - 128)) | (1 << (SqlBaseParser.RECORDWRITER - 128)) | (1 << (SqlBaseParser.RECOVER - 128)) | (1 << (SqlBaseParser.REDUCE - 128)) | (1 << (SqlBaseParser.REFERENCES - 128)) | (1 << (SqlBaseParser.REFRESH - 128)) | (1 << (SqlBaseParser.RENAME - 128)) | (1 << (SqlBaseParser.REPAIR - 128)))) != 0) or ((((_la - 192)) & ~0x3f) == 0 and ((1 << (_la - 192)) & ((1 << (SqlBaseParser.REPLACE - 192)) | (1 << (SqlBaseParser.RESET - 192)) | (1 << (SqlBaseParser.RESTRICT - 192)) | (1 << (SqlBaseParser.REVOKE - 192)) | (1 << (SqlBaseParser.RLIKE - 192)) | (1 << (SqlBaseParser.ROLE - 192)) | (1 << (SqlBaseParser.ROLES - 192)) | (1 << (SqlBaseParser.ROLLBACK - 192)) | (1 << (SqlBaseParser.ROLLUP - 192)) | (1 << (SqlBaseParser.ROW - 192)) | (1 << (SqlBaseParser.ROWS - 192)) | (1 << (SqlBaseParser.SCHEMA - 192)) | (1 << (SqlBaseParser.SECOND - 192)) | (1 << (SqlBaseParser.SELECT - 192)) | (1 << (SqlBaseParser.SEPARATED - 192)) | (1 << (SqlBaseParser.SERDE - 192)) | (1 << (SqlBaseParser.SERDEPROPERTIES - 192)) | (1 << (SqlBaseParser.SESSION_USER - 192)) | (1 << (SqlBaseParser.SET - 192)) | (1 << (SqlBaseParser.SETS - 192)) | (1 << (SqlBaseParser.SHOW - 192)) | (1 << (SqlBaseParser.SKEWED - 192)) | (1 << (SqlBaseParser.SOME - 192)) | (1 << (SqlBaseParser.SORT - 192)) | (1 << (SqlBaseParser.SORTED - 192)) | (1 << (SqlBaseParser.START - 192)) | (1 << (SqlBaseParser.STATISTICS - 192)) | (1 << (SqlBaseParser.STORED - 192)) | (1 << (SqlBaseParser.STRATIFY - 192)) | (1 << (SqlBaseParser.STRUCT - 192)) | (1 << (SqlBaseParser.SUBSTR - 192)) | (1 << (SqlBaseParser.SUBSTRING - 192)) | (1 << (SqlBaseParser.TABLE - 192)) | (1 << (SqlBaseParser.TABLES - 192)) | (1 << (SqlBaseParser.TABLESAMPLE - 192)) | (1 << (SqlBaseParser.TBLPROPERTIES - 192)) | (1 << (SqlBaseParser.TEMPORARY - 192)) | (1 << (SqlBaseParser.TERMINATED - 192)) | (1 << (SqlBaseParser.THEN - 192)) | (1 << (SqlBaseParser.TO - 192)) | (1 << (SqlBaseParser.TOUCH - 192)) | (1 << (SqlBaseParser.TRAILING - 192)) | (1 << (SqlBaseParser.TRANSACTION - 192)) | (1 << (SqlBaseParser.TRANSACTIONS - 192)) | (1 << (SqlBaseParser.TRANSFORM - 192)) | (1 << (SqlBaseParser.TRIM - 192)) | (1 << (SqlBaseParser.TRUE - 192)) | (1 << (SqlBaseParser.TRUNCATE - 192)) | (1 << (SqlBaseParser.TYPE - 192)) | (1 << (SqlBaseParser.UNARCHIVE - 192)) | (1 << (SqlBaseParser.UNBOUNDED - 192)) | (1 << (SqlBaseParser.UNCACHE - 192)) | (1 << (SqlBaseParser.UNIQUE - 192)) | (1 << (SqlBaseParser.UNKNOWN - 192)) | (1 << (SqlBaseParser.UNLOCK - 192)) | (1 << (SqlBaseParser.UNSET - 192)) | (1 << (SqlBaseParser.UPDATE - 192)) | (1 << (SqlBaseParser.USE - 192)) | (1 << (SqlBaseParser.USER - 192)))) != 0) or ((((_la - 256)) & ~0x3f) == 0 and ((1 << (_la - 256)) & ((1 << (SqlBaseParser.VALUES - 256)) | (1 << (SqlBaseParser.VIEW - 256)) | (1 << (SqlBaseParser.VIEWS - 256)) | (1 << (SqlBaseParser.WHEN - 256)) | (1 << (SqlBaseParser.WHERE - 256)) | (1 << (SqlBaseParser.WINDOW - 256)) | (1 << (SqlBaseParser.WITH - 256)) | (1 << (SqlBaseParser.YEAR - 256)) | (1 << (SqlBaseParser.DIV - 256)))) != 0)):
                self._errHandler.recoverInline(self)
            else:
                self._errHandler.reportMatch(self)
                self.consume()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx



    def sempred(self, localctx:RuleContext, ruleIndex:int, predIndex:int):
        if self._predicates == None:
            self._predicates = dict()
        self._predicates[7] = self.statement_sempred
        self._predicates[40] = self.queryTerm_sempred
        self._predicates[94] = self.booleanExpression_sempred
        self._predicates[96] = self.valueExpression_sempred
        self._predicates[97] = self.primaryExpression_sempred
        self._predicates[129] = self.identifier_sempred
        self._predicates[130] = self.strictIdentifier_sempred
        self._predicates[132] = self.number_sempred
        pred = self._predicates.get(ruleIndex, None)
        if pred is None:
            raise Exception("No predicate with index:" + str(ruleIndex))
        else:
            return pred(localctx, predIndex)

    def statement_sempred(self, localctx:StatementContext, predIndex:int):
            if predIndex == 0:
                return not self.legacy_create_hive_table_by_default_enabled


            if predIndex == 1:
                return self.legacy_create_hive_table_by_default_enabled


    def queryTerm_sempred(self, localctx:QueryTermContext, predIndex:int):
            if predIndex == 2:
                return self.precpred(self._ctx, 3)


            if predIndex == 3:
                return self.legacy_setops_precedence_enbled


            if predIndex == 4:
                return self.precpred(self._ctx, 2)


            if predIndex == 5:
                return not self.legacy_setops_precedence_enbled


            if predIndex == 6:
                return self.precpred(self._ctx, 1)


            if predIndex == 7:
                return not self.legacy_setops_precedence_enbled


    def booleanExpression_sempred(self, localctx:BooleanExpressionContext, predIndex:int):
            if predIndex == 8:
                return self.precpred(self._ctx, 2)


            if predIndex == 9:
                return self.precpred(self._ctx, 1)


    def valueExpression_sempred(self, localctx:ValueExpressionContext, predIndex:int):
            if predIndex == 10:
                return self.precpred(self._ctx, 6)


            if predIndex == 11:
                return self.precpred(self._ctx, 5)


            if predIndex == 12:
                return self.precpred(self._ctx, 4)


            if predIndex == 13:
                return self.precpred(self._ctx, 3)


            if predIndex == 14:
                return self.precpred(self._ctx, 2)


            if predIndex == 15:
                return self.precpred(self._ctx, 1)


    def primaryExpression_sempred(self, localctx:PrimaryExpressionContext, predIndex:int):
            if predIndex == 16:
                return self.precpred(self._ctx, 8)


            if predIndex == 17:
                return self.precpred(self._ctx, 6)


    def identifier_sempred(self, localctx:IdentifierContext, predIndex:int):
            if predIndex == 18:
                return not self.SQL_standard_keyword_behavior


    def strictIdentifier_sempred(self, localctx:StrictIdentifierContext, predIndex:int):
            if predIndex == 19:
                return self.SQL_standard_keyword_behavior


            if predIndex == 20:
                return not self.SQL_standard_keyword_behavior


    def number_sempred(self, localctx:NumberContext, predIndex:int):
            if predIndex == 21:
                return not self.legacy_exponent_literal_as_decimal_enabled


            if predIndex == 22:
                return not self.legacy_exponent_literal_as_decimal_enabled


            if predIndex == 23:
                return self.legacy_exponent_literal_as_decimal_enabled





