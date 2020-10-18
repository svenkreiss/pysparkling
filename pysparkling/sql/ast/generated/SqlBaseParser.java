// Generated from /home/eguyomarch/prog/pysparkling/pysparkling/sql/ast/grammar/SqlBase.g4 by ANTLR 4.7.1
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class SqlBaseParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.7.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		T__9=10, T__10=11, ADD=12, AFTER=13, ALL=14, ALTER=15, ANALYZE=16, AND=17, 
		ANTI=18, ANY=19, ARCHIVE=20, ARRAY=21, AS=22, ASC=23, AT=24, AUTHORIZATION=25, 
		BETWEEN=26, BOTH=27, BUCKET=28, BUCKETS=29, BY=30, CACHE=31, CASCADE=32, 
		CASE=33, CAST=34, CHANGE=35, CHECK=36, CLEAR=37, CLUSTER=38, CLUSTERED=39, 
		CODEGEN=40, COLLATE=41, COLLECTION=42, COLUMN=43, COLUMNS=44, COMMENT=45, 
		COMMIT=46, COMPACT=47, COMPACTIONS=48, COMPUTE=49, CONCATENATE=50, CONSTRAINT=51, 
		COST=52, CREATE=53, CROSS=54, CUBE=55, CURRENT=56, CURRENT_DATE=57, CURRENT_TIME=58, 
		CURRENT_TIMESTAMP=59, CURRENT_USER=60, DATA=61, DATABASE=62, DATABASES=63, 
		DAY=64, DBPROPERTIES=65, DEFINED=66, DELETE=67, DELIMITED=68, DESC=69, 
		DESCRIBE=70, DFS=71, DIRECTORIES=72, DIRECTORY=73, DISTINCT=74, DISTRIBUTE=75, 
		DROP=76, ELSE=77, END=78, ESCAPE=79, ESCAPED=80, EXCEPT=81, EXCHANGE=82, 
		EXISTS=83, EXPLAIN=84, EXPORT=85, EXTENDED=86, EXTERNAL=87, EXTRACT=88, 
		FALSE=89, FETCH=90, FIELDS=91, FILTER=92, FILEFORMAT=93, FIRST=94, FOLLOWING=95, 
		FOR=96, FOREIGN=97, FORMAT=98, FORMATTED=99, FROM=100, FULL=101, FUNCTION=102, 
		FUNCTIONS=103, GLOBAL=104, GRANT=105, GROUP=106, GROUPING=107, HAVING=108, 
		HOUR=109, IF=110, IGNORE=111, IMPORT=112, IN=113, INDEX=114, INDEXES=115, 
		INNER=116, INPATH=117, INPUTFORMAT=118, INSERT=119, INTERSECT=120, INTERVAL=121, 
		INTO=122, IS=123, ITEMS=124, JOIN=125, KEYS=126, LAST=127, LATERAL=128, 
		LAZY=129, LEADING=130, LEFT=131, LIKE=132, LIMIT=133, LINES=134, LIST=135, 
		LOAD=136, LOCAL=137, LOCATION=138, LOCK=139, LOCKS=140, LOGICAL=141, MACRO=142, 
		MAP=143, MATCHED=144, MERGE=145, MINUTE=146, MONTH=147, MSCK=148, NAMESPACE=149, 
		NAMESPACES=150, NATURAL=151, NO=152, NOT=153, NULL=154, NULLS=155, OF=156, 
		ON=157, ONLY=158, OPTION=159, OPTIONS=160, OR=161, ORDER=162, OUT=163, 
		OUTER=164, OUTPUTFORMAT=165, OVER=166, OVERLAPS=167, OVERLAY=168, OVERWRITE=169, 
		PARTITION=170, PARTITIONED=171, PARTITIONS=172, PERCENTLIT=173, PIVOT=174, 
		PLACING=175, POSITION=176, PRECEDING=177, PRIMARY=178, PRINCIPALS=179, 
		PROPERTIES=180, PURGE=181, QUERY=182, RANGE=183, RECORDREADER=184, RECORDWRITER=185, 
		RECOVER=186, REDUCE=187, REFERENCES=188, REFRESH=189, RENAME=190, REPAIR=191, 
		REPLACE=192, RESET=193, RESTRICT=194, REVOKE=195, RIGHT=196, RLIKE=197, 
		ROLE=198, ROLES=199, ROLLBACK=200, ROLLUP=201, ROW=202, ROWS=203, SCHEMA=204, 
		SECOND=205, SELECT=206, SEMI=207, SEPARATED=208, SERDE=209, SERDEPROPERTIES=210, 
		SESSION_USER=211, SET=212, SETMINUS=213, SETS=214, SHOW=215, SKEWED=216, 
		SOME=217, SORT=218, SORTED=219, START=220, STATISTICS=221, STORED=222, 
		STRATIFY=223, STRUCT=224, SUBSTR=225, SUBSTRING=226, TABLE=227, TABLES=228, 
		TABLESAMPLE=229, TBLPROPERTIES=230, TEMPORARY=231, TERMINATED=232, THEN=233, 
		TO=234, TOUCH=235, TRAILING=236, TRANSACTION=237, TRANSACTIONS=238, TRANSFORM=239, 
		TRIM=240, TRUE=241, TRUNCATE=242, TYPE=243, UNARCHIVE=244, UNBOUNDED=245, 
		UNCACHE=246, UNION=247, UNIQUE=248, UNKNOWN=249, UNLOCK=250, UNSET=251, 
		UPDATE=252, USE=253, USER=254, USING=255, VALUES=256, VIEW=257, VIEWS=258, 
		WHEN=259, WHERE=260, WINDOW=261, WITH=262, YEAR=263, EQ=264, NSEQ=265, 
		NEQ=266, NEQJ=267, LT=268, LTE=269, GT=270, GTE=271, PLUS=272, MINUS=273, 
		ASTERISK=274, SLASH=275, PERCENT=276, DIV=277, TILDE=278, AMPERSAND=279, 
		PIPE=280, CONCAT_PIPE=281, HAT=282, STRING=283, BIGINT_LITERAL=284, SMALLINT_LITERAL=285, 
		TINYINT_LITERAL=286, INTEGER_VALUE=287, EXPONENT_VALUE=288, DECIMAL_VALUE=289, 
		DOUBLE_LITERAL=290, BIGDECIMAL_LITERAL=291, IDENTIFIER=292, BACKQUOTED_IDENTIFIER=293, 
		SIMPLE_COMMENT=294, BRACKETED_COMMENT=295, WS=296, UNRECOGNIZED=297;
	public static final int
		RULE_singleStatement = 0, RULE_singleExpression = 1, RULE_singleTableIdentifier = 2, 
		RULE_singleMultipartIdentifier = 3, RULE_singleFunctionIdentifier = 4, 
		RULE_singleDataType = 5, RULE_singleTableSchema = 6, RULE_statement = 7, 
		RULE_unsupportedHiveNativeCommands = 8, RULE_createTableHeader = 9, RULE_replaceTableHeader = 10, 
		RULE_bucketSpec = 11, RULE_skewSpec = 12, RULE_locationSpec = 13, RULE_commentSpec = 14, 
		RULE_query = 15, RULE_insertInto = 16, RULE_partitionSpecLocation = 17, 
		RULE_partitionSpec = 18, RULE_partitionVal = 19, RULE_namespace = 20, 
		RULE_describeFuncName = 21, RULE_describeColName = 22, RULE_ctes = 23, 
		RULE_namedQuery = 24, RULE_tableProvider = 25, RULE_createTableClauses = 26, 
		RULE_tablePropertyList = 27, RULE_tableProperty = 28, RULE_tablePropertyKey = 29, 
		RULE_tablePropertyValue = 30, RULE_constantList = 31, RULE_nestedConstantList = 32, 
		RULE_createFileFormat = 33, RULE_fileFormat = 34, RULE_storageHandler = 35, 
		RULE_resource = 36, RULE_dmlStatementNoWith = 37, RULE_queryOrganization = 38, 
		RULE_multiInsertQueryBody = 39, RULE_queryTerm = 40, RULE_queryPrimary = 41, 
		RULE_sortItem = 42, RULE_fromStatement = 43, RULE_fromStatementBody = 44, 
		RULE_querySpecification = 45, RULE_transformClause = 46, RULE_selectClause = 47, 
		RULE_setClause = 48, RULE_matchedClause = 49, RULE_notMatchedClause = 50, 
		RULE_matchedAction = 51, RULE_notMatchedAction = 52, RULE_assignmentList = 53, 
		RULE_assignment = 54, RULE_whereClause = 55, RULE_havingClause = 56, RULE_hint = 57, 
		RULE_hintStatement = 58, RULE_fromClause = 59, RULE_aggregationClause = 60, 
		RULE_groupingSet = 61, RULE_pivotClause = 62, RULE_pivotColumn = 63, RULE_pivotValue = 64, 
		RULE_lateralView = 65, RULE_setQuantifier = 66, RULE_relation = 67, RULE_joinRelation = 68, 
		RULE_joinType = 69, RULE_joinCriteria = 70, RULE_sample = 71, RULE_sampleMethod = 72, 
		RULE_identifierList = 73, RULE_identifierSeq = 74, RULE_orderedIdentifierList = 75, 
		RULE_orderedIdentifier = 76, RULE_identifierCommentList = 77, RULE_identifierComment = 78, 
		RULE_relationPrimary = 79, RULE_inlineTable = 80, RULE_functionTable = 81, 
		RULE_tableAlias = 82, RULE_rowFormat = 83, RULE_multipartIdentifierList = 84, 
		RULE_multipartIdentifier = 85, RULE_tableIdentifier = 86, RULE_functionIdentifier = 87, 
		RULE_namedExpression = 88, RULE_namedExpressionSeq = 89, RULE_transformList = 90, 
		RULE_transform = 91, RULE_transformArgument = 92, RULE_expression = 93, 
		RULE_booleanExpression = 94, RULE_predicate = 95, RULE_valueExpression = 96, 
		RULE_primaryExpression = 97, RULE_constant = 98, RULE_comparisonOperator = 99, 
		RULE_arithmeticOperator = 100, RULE_predicateOperator = 101, RULE_booleanValue = 102, 
		RULE_interval = 103, RULE_errorCapturingMultiUnitsInterval = 104, RULE_multiUnitsInterval = 105, 
		RULE_errorCapturingUnitToUnitInterval = 106, RULE_unitToUnitInterval = 107, 
		RULE_intervalValue = 108, RULE_intervalUnit = 109, RULE_colPosition = 110, 
		RULE_dataType = 111, RULE_qualifiedColTypeWithPositionList = 112, RULE_qualifiedColTypeWithPosition = 113, 
		RULE_colTypeList = 114, RULE_colType = 115, RULE_complexColTypeList = 116, 
		RULE_complexColType = 117, RULE_whenClause = 118, RULE_windowClause = 119, 
		RULE_namedWindow = 120, RULE_windowSpec = 121, RULE_windowFrame = 122, 
		RULE_frameBound = 123, RULE_qualifiedNameList = 124, RULE_functionName = 125, 
		RULE_qualifiedName = 126, RULE_errorCapturingIdentifier = 127, RULE_errorCapturingIdentifierExtra = 128, 
		RULE_identifier = 129, RULE_strictIdentifier = 130, RULE_quotedIdentifier = 131, 
		RULE_number = 132, RULE_alterColumnAction = 133, RULE_ansiNonReserved = 134, 
		RULE_strictNonReserved = 135, RULE_nonReserved = 136;
	public static final String[] ruleNames = {
		"singleStatement", "singleExpression", "singleTableIdentifier", "singleMultipartIdentifier", 
		"singleFunctionIdentifier", "singleDataType", "singleTableSchema", "statement", 
		"unsupportedHiveNativeCommands", "createTableHeader", "replaceTableHeader", 
		"bucketSpec", "skewSpec", "locationSpec", "commentSpec", "query", "insertInto", 
		"partitionSpecLocation", "partitionSpec", "partitionVal", "namespace", 
		"describeFuncName", "describeColName", "ctes", "namedQuery", "tableProvider", 
		"createTableClauses", "tablePropertyList", "tableProperty", "tablePropertyKey", 
		"tablePropertyValue", "constantList", "nestedConstantList", "createFileFormat", 
		"fileFormat", "storageHandler", "resource", "dmlStatementNoWith", "queryOrganization", 
		"multiInsertQueryBody", "queryTerm", "queryPrimary", "sortItem", "fromStatement", 
		"fromStatementBody", "querySpecification", "transformClause", "selectClause", 
		"setClause", "matchedClause", "notMatchedClause", "matchedAction", "notMatchedAction", 
		"assignmentList", "assignment", "whereClause", "havingClause", "hint", 
		"hintStatement", "fromClause", "aggregationClause", "groupingSet", "pivotClause", 
		"pivotColumn", "pivotValue", "lateralView", "setQuantifier", "relation", 
		"joinRelation", "joinType", "joinCriteria", "sample", "sampleMethod", 
		"identifierList", "identifierSeq", "orderedIdentifierList", "orderedIdentifier", 
		"identifierCommentList", "identifierComment", "relationPrimary", "inlineTable", 
		"functionTable", "tableAlias", "rowFormat", "multipartIdentifierList", 
		"multipartIdentifier", "tableIdentifier", "functionIdentifier", "namedExpression", 
		"namedExpressionSeq", "transformList", "transform", "transformArgument", 
		"expression", "booleanExpression", "predicate", "valueExpression", "primaryExpression", 
		"constant", "comparisonOperator", "arithmeticOperator", "predicateOperator", 
		"booleanValue", "interval", "errorCapturingMultiUnitsInterval", "multiUnitsInterval", 
		"errorCapturingUnitToUnitInterval", "unitToUnitInterval", "intervalValue", 
		"intervalUnit", "colPosition", "dataType", "qualifiedColTypeWithPositionList", 
		"qualifiedColTypeWithPosition", "colTypeList", "colType", "complexColTypeList", 
		"complexColType", "whenClause", "windowClause", "namedWindow", "windowSpec", 
		"windowFrame", "frameBound", "qualifiedNameList", "functionName", "qualifiedName", 
		"errorCapturingIdentifier", "errorCapturingIdentifierExtra", "identifier", 
		"strictIdentifier", "quotedIdentifier", "number", "alterColumnAction", 
		"ansiNonReserved", "strictNonReserved", "nonReserved"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "';'", "'('", "')'", "','", "'.'", "'/*+'", "'*/'", "'->'", "'['", 
		"']'", "':'", "'ADD'", "'AFTER'", "'ALL'", "'ALTER'", "'ANALYZE'", "'AND'", 
		"'ANTI'", "'ANY'", "'ARCHIVE'", "'ARRAY'", "'AS'", "'ASC'", "'AT'", "'AUTHORIZATION'", 
		"'BETWEEN'", "'BOTH'", "'BUCKET'", "'BUCKETS'", "'BY'", "'CACHE'", "'CASCADE'", 
		"'CASE'", "'CAST'", "'CHANGE'", "'CHECK'", "'CLEAR'", "'CLUSTER'", "'CLUSTERED'", 
		"'CODEGEN'", "'COLLATE'", "'COLLECTION'", "'COLUMN'", "'COLUMNS'", "'COMMENT'", 
		"'COMMIT'", "'COMPACT'", "'COMPACTIONS'", "'COMPUTE'", "'CONCATENATE'", 
		"'CONSTRAINT'", "'COST'", "'CREATE'", "'CROSS'", "'CUBE'", "'CURRENT'", 
		"'CURRENT_DATE'", "'CURRENT_TIME'", "'CURRENT_TIMESTAMP'", "'CURRENT_USER'", 
		"'DATA'", "'DATABASE'", null, "'DAY'", "'DBPROPERTIES'", "'DEFINED'", 
		"'DELETE'", "'DELIMITED'", "'DESC'", "'DESCRIBE'", "'DFS'", "'DIRECTORIES'", 
		"'DIRECTORY'", "'DISTINCT'", "'DISTRIBUTE'", "'DROP'", "'ELSE'", "'END'", 
		"'ESCAPE'", "'ESCAPED'", "'EXCEPT'", "'EXCHANGE'", "'EXISTS'", "'EXPLAIN'", 
		"'EXPORT'", "'EXTENDED'", "'EXTERNAL'", "'EXTRACT'", "'FALSE'", "'FETCH'", 
		"'FIELDS'", "'FILTER'", "'FILEFORMAT'", "'FIRST'", "'FOLLOWING'", "'FOR'", 
		"'FOREIGN'", "'FORMAT'", "'FORMATTED'", "'FROM'", "'FULL'", "'FUNCTION'", 
		"'FUNCTIONS'", "'GLOBAL'", "'GRANT'", "'GROUP'", "'GROUPING'", "'HAVING'", 
		"'HOUR'", "'IF'", "'IGNORE'", "'IMPORT'", "'IN'", "'INDEX'", "'INDEXES'", 
		"'INNER'", "'INPATH'", "'INPUTFORMAT'", "'INSERT'", "'INTERSECT'", "'INTERVAL'", 
		"'INTO'", "'IS'", "'ITEMS'", "'JOIN'", "'KEYS'", "'LAST'", "'LATERAL'", 
		"'LAZY'", "'LEADING'", "'LEFT'", "'LIKE'", "'LIMIT'", "'LINES'", "'LIST'", 
		"'LOAD'", "'LOCAL'", "'LOCATION'", "'LOCK'", "'LOCKS'", "'LOGICAL'", "'MACRO'", 
		"'MAP'", "'MATCHED'", "'MERGE'", "'MINUTE'", "'MONTH'", "'MSCK'", "'NAMESPACE'", 
		"'NAMESPACES'", "'NATURAL'", "'NO'", null, "'NULL'", "'NULLS'", "'OF'", 
		"'ON'", "'ONLY'", "'OPTION'", "'OPTIONS'", "'OR'", "'ORDER'", "'OUT'", 
		"'OUTER'", "'OUTPUTFORMAT'", "'OVER'", "'OVERLAPS'", "'OVERLAY'", "'OVERWRITE'", 
		"'PARTITION'", "'PARTITIONED'", "'PARTITIONS'", "'PERCENT'", "'PIVOT'", 
		"'PLACING'", "'POSITION'", "'PRECEDING'", "'PRIMARY'", "'PRINCIPALS'", 
		"'PROPERTIES'", "'PURGE'", "'QUERY'", "'RANGE'", "'RECORDREADER'", "'RECORDWRITER'", 
		"'RECOVER'", "'REDUCE'", "'REFERENCES'", "'REFRESH'", "'RENAME'", "'REPAIR'", 
		"'REPLACE'", "'RESET'", "'RESTRICT'", "'REVOKE'", "'RIGHT'", null, "'ROLE'", 
		"'ROLES'", "'ROLLBACK'", "'ROLLUP'", "'ROW'", "'ROWS'", "'SCHEMA'", "'SECOND'", 
		"'SELECT'", "'SEMI'", "'SEPARATED'", "'SERDE'", "'SERDEPROPERTIES'", "'SESSION_USER'", 
		"'SET'", "'MINUS'", "'SETS'", "'SHOW'", "'SKEWED'", "'SOME'", "'SORT'", 
		"'SORTED'", "'START'", "'STATISTICS'", "'STORED'", "'STRATIFY'", "'STRUCT'", 
		"'SUBSTR'", "'SUBSTRING'", "'TABLE'", "'TABLES'", "'TABLESAMPLE'", "'TBLPROPERTIES'", 
		null, "'TERMINATED'", "'THEN'", "'TO'", "'TOUCH'", "'TRAILING'", "'TRANSACTION'", 
		"'TRANSACTIONS'", "'TRANSFORM'", "'TRIM'", "'TRUE'", "'TRUNCATE'", "'TYPE'", 
		"'UNARCHIVE'", "'UNBOUNDED'", "'UNCACHE'", "'UNION'", "'UNIQUE'", "'UNKNOWN'", 
		"'UNLOCK'", "'UNSET'", "'UPDATE'", "'USE'", "'USER'", "'USING'", "'VALUES'", 
		"'VIEW'", "'VIEWS'", "'WHEN'", "'WHERE'", "'WINDOW'", "'WITH'", "'YEAR'", 
		null, "'<=>'", "'<>'", "'!='", "'<'", null, "'>'", null, "'+'", "'-'", 
		"'*'", "'/'", "'%'", "'DIV'", "'~'", "'&'", "'|'", "'||'", "'^'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		"ADD", "AFTER", "ALL", "ALTER", "ANALYZE", "AND", "ANTI", "ANY", "ARCHIVE", 
		"ARRAY", "AS", "ASC", "AT", "AUTHORIZATION", "BETWEEN", "BOTH", "BUCKET", 
		"BUCKETS", "BY", "CACHE", "CASCADE", "CASE", "CAST", "CHANGE", "CHECK", 
		"CLEAR", "CLUSTER", "CLUSTERED", "CODEGEN", "COLLATE", "COLLECTION", "COLUMN", 
		"COLUMNS", "COMMENT", "COMMIT", "COMPACT", "COMPACTIONS", "COMPUTE", "CONCATENATE", 
		"CONSTRAINT", "COST", "CREATE", "CROSS", "CUBE", "CURRENT", "CURRENT_DATE", 
		"CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "DATA", "DATABASE", 
		"DATABASES", "DAY", "DBPROPERTIES", "DEFINED", "DELETE", "DELIMITED", 
		"DESC", "DESCRIBE", "DFS", "DIRECTORIES", "DIRECTORY", "DISTINCT", "DISTRIBUTE", 
		"DROP", "ELSE", "END", "ESCAPE", "ESCAPED", "EXCEPT", "EXCHANGE", "EXISTS", 
		"EXPLAIN", "EXPORT", "EXTENDED", "EXTERNAL", "EXTRACT", "FALSE", "FETCH", 
		"FIELDS", "FILTER", "FILEFORMAT", "FIRST", "FOLLOWING", "FOR", "FOREIGN", 
		"FORMAT", "FORMATTED", "FROM", "FULL", "FUNCTION", "FUNCTIONS", "GLOBAL", 
		"GRANT", "GROUP", "GROUPING", "HAVING", "HOUR", "IF", "IGNORE", "IMPORT", 
		"IN", "INDEX", "INDEXES", "INNER", "INPATH", "INPUTFORMAT", "INSERT", 
		"INTERSECT", "INTERVAL", "INTO", "IS", "ITEMS", "JOIN", "KEYS", "LAST", 
		"LATERAL", "LAZY", "LEADING", "LEFT", "LIKE", "LIMIT", "LINES", "LIST", 
		"LOAD", "LOCAL", "LOCATION", "LOCK", "LOCKS", "LOGICAL", "MACRO", "MAP", 
		"MATCHED", "MERGE", "MINUTE", "MONTH", "MSCK", "NAMESPACE", "NAMESPACES", 
		"NATURAL", "NO", "NOT", "NULL", "NULLS", "OF", "ON", "ONLY", "OPTION", 
		"OPTIONS", "OR", "ORDER", "OUT", "OUTER", "OUTPUTFORMAT", "OVER", "OVERLAPS", 
		"OVERLAY", "OVERWRITE", "PARTITION", "PARTITIONED", "PARTITIONS", "PERCENTLIT", 
		"PIVOT", "PLACING", "POSITION", "PRECEDING", "PRIMARY", "PRINCIPALS", 
		"PROPERTIES", "PURGE", "QUERY", "RANGE", "RECORDREADER", "RECORDWRITER", 
		"RECOVER", "REDUCE", "REFERENCES", "REFRESH", "RENAME", "REPAIR", "REPLACE", 
		"RESET", "RESTRICT", "REVOKE", "RIGHT", "RLIKE", "ROLE", "ROLES", "ROLLBACK", 
		"ROLLUP", "ROW", "ROWS", "SCHEMA", "SECOND", "SELECT", "SEMI", "SEPARATED", 
		"SERDE", "SERDEPROPERTIES", "SESSION_USER", "SET", "SETMINUS", "SETS", 
		"SHOW", "SKEWED", "SOME", "SORT", "SORTED", "START", "STATISTICS", "STORED", 
		"STRATIFY", "STRUCT", "SUBSTR", "SUBSTRING", "TABLE", "TABLES", "TABLESAMPLE", 
		"TBLPROPERTIES", "TEMPORARY", "TERMINATED", "THEN", "TO", "TOUCH", "TRAILING", 
		"TRANSACTION", "TRANSACTIONS", "TRANSFORM", "TRIM", "TRUE", "TRUNCATE", 
		"TYPE", "UNARCHIVE", "UNBOUNDED", "UNCACHE", "UNION", "UNIQUE", "UNKNOWN", 
		"UNLOCK", "UNSET", "UPDATE", "USE", "USER", "USING", "VALUES", "VIEW", 
		"VIEWS", "WHEN", "WHERE", "WINDOW", "WITH", "YEAR", "EQ", "NSEQ", "NEQ", 
		"NEQJ", "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", 
		"PERCENT", "DIV", "TILDE", "AMPERSAND", "PIPE", "CONCAT_PIPE", "HAT", 
		"STRING", "BIGINT_LITERAL", "SMALLINT_LITERAL", "TINYINT_LITERAL", "INTEGER_VALUE", 
		"EXPONENT_VALUE", "DECIMAL_VALUE", "DOUBLE_LITERAL", "BIGDECIMAL_LITERAL", 
		"IDENTIFIER", "BACKQUOTED_IDENTIFIER", "SIMPLE_COMMENT", "BRACKETED_COMMENT", 
		"WS", "UNRECOGNIZED"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "SqlBase.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }


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

	public SqlBaseParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class SingleStatementContext extends ParserRuleContext {
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleStatement(this);
		}
	}

	public final SingleStatementContext singleStatement() throws RecognitionException {
		SingleStatementContext _localctx = new SingleStatementContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_singleStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(274);
			statement();
			setState(278);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__0) {
				{
				{
				setState(275);
				match(T__0);
				}
				}
				setState(280);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(281);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SingleExpressionContext extends ParserRuleContext {
		public NamedExpressionContext namedExpression() {
			return getRuleContext(NamedExpressionContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleExpression(this);
		}
	}

	public final SingleExpressionContext singleExpression() throws RecognitionException {
		SingleExpressionContext _localctx = new SingleExpressionContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_singleExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(283);
			namedExpression();
			setState(284);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SingleTableIdentifierContext extends ParserRuleContext {
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleTableIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleTableIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleTableIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleTableIdentifier(this);
		}
	}

	public final SingleTableIdentifierContext singleTableIdentifier() throws RecognitionException {
		SingleTableIdentifierContext _localctx = new SingleTableIdentifierContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_singleTableIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(286);
			tableIdentifier();
			setState(287);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SingleMultipartIdentifierContext extends ParserRuleContext {
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleMultipartIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleMultipartIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleMultipartIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleMultipartIdentifier(this);
		}
	}

	public final SingleMultipartIdentifierContext singleMultipartIdentifier() throws RecognitionException {
		SingleMultipartIdentifierContext _localctx = new SingleMultipartIdentifierContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_singleMultipartIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(289);
			multipartIdentifier();
			setState(290);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SingleFunctionIdentifierContext extends ParserRuleContext {
		public FunctionIdentifierContext functionIdentifier() {
			return getRuleContext(FunctionIdentifierContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleFunctionIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleFunctionIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleFunctionIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleFunctionIdentifier(this);
		}
	}

	public final SingleFunctionIdentifierContext singleFunctionIdentifier() throws RecognitionException {
		SingleFunctionIdentifierContext _localctx = new SingleFunctionIdentifierContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_singleFunctionIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(292);
			functionIdentifier();
			setState(293);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SingleDataTypeContext extends ParserRuleContext {
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleDataTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleDataType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleDataType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleDataType(this);
		}
	}

	public final SingleDataTypeContext singleDataType() throws RecognitionException {
		SingleDataTypeContext _localctx = new SingleDataTypeContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_singleDataType);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(295);
			dataType();
			setState(296);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SingleTableSchemaContext extends ParserRuleContext {
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleTableSchemaContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleTableSchema; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleTableSchema(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleTableSchema(this);
		}
	}

	public final SingleTableSchemaContext singleTableSchema() throws RecognitionException {
		SingleTableSchemaContext _localctx = new SingleTableSchemaContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_singleTableSchema);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(298);
			colTypeList();
			setState(299);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StatementContext extends ParserRuleContext {
		public StatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_statement; }
	 
		public StatementContext() { }
		public void copyFrom(StatementContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ExplainContext extends StatementContext {
		public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode LOGICAL() { return getToken(SqlBaseParser.LOGICAL, 0); }
		public TerminalNode FORMATTED() { return getToken(SqlBaseParser.FORMATTED, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public TerminalNode CODEGEN() { return getToken(SqlBaseParser.CODEGEN, 0); }
		public TerminalNode COST() { return getToken(SqlBaseParser.COST, 0); }
		public ExplainContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExplain(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExplain(this);
		}
	}
	public static class ResetConfigurationContext extends StatementContext {
		public TerminalNode RESET() { return getToken(SqlBaseParser.RESET, 0); }
		public ResetConfigurationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterResetConfiguration(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitResetConfiguration(this);
		}
	}
	public static class AlterViewQueryContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public AlterViewQueryContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAlterViewQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAlterViewQuery(this);
		}
	}
	public static class UseContext extends StatementContext {
		public TerminalNode USE() { return getToken(SqlBaseParser.USE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode NAMESPACE() { return getToken(SqlBaseParser.NAMESPACE, 0); }
		public UseContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUse(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUse(this);
		}
	}
	public static class DropNamespaceContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public NamespaceContext namespace() {
			return getRuleContext(NamespaceContext.class,0);
		}
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode RESTRICT() { return getToken(SqlBaseParser.RESTRICT, 0); }
		public TerminalNode CASCADE() { return getToken(SqlBaseParser.CASCADE, 0); }
		public DropNamespaceContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropNamespace(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropNamespace(this);
		}
	}
	public static class CreateTempViewUsingContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TableProviderContext tableProvider() {
			return getRuleContext(TableProviderContext.class,0);
		}
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode GLOBAL() { return getToken(SqlBaseParser.GLOBAL, 0); }
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public TerminalNode OPTIONS() { return getToken(SqlBaseParser.OPTIONS, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public CreateTempViewUsingContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateTempViewUsing(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateTempViewUsing(this);
		}
	}
	public static class RenameTableContext extends StatementContext {
		public MultipartIdentifierContext from_;
		public MultipartIdentifierContext to;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public List<MultipartIdentifierContext> multipartIdentifier() {
			return getRuleContexts(MultipartIdentifierContext.class);
		}
		public MultipartIdentifierContext multipartIdentifier(int i) {
			return getRuleContext(MultipartIdentifierContext.class,i);
		}
		public RenameTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRenameTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRenameTable(this);
		}
	}
	public static class FailNativeCommandContext extends StatementContext {
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public UnsupportedHiveNativeCommandsContext unsupportedHiveNativeCommands() {
			return getRuleContext(UnsupportedHiveNativeCommandsContext.class,0);
		}
		public FailNativeCommandContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFailNativeCommand(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFailNativeCommand(this);
		}
	}
	public static class ClearCacheContext extends StatementContext {
		public TerminalNode CLEAR() { return getToken(SqlBaseParser.CLEAR, 0); }
		public TerminalNode CACHE() { return getToken(SqlBaseParser.CACHE, 0); }
		public ClearCacheContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterClearCache(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitClearCache(this);
		}
	}
	public static class DropViewContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public DropViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropView(this);
		}
	}
	public static class ShowTablesContext extends StatementContext {
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public ShowTablesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowTables(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowTables(this);
		}
	}
	public static class RecoverPartitionsContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode RECOVER() { return getToken(SqlBaseParser.RECOVER, 0); }
		public TerminalNode PARTITIONS() { return getToken(SqlBaseParser.PARTITIONS, 0); }
		public RecoverPartitionsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRecoverPartitions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRecoverPartitions(this);
		}
	}
	public static class ShowCurrentNamespaceContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public TerminalNode NAMESPACE() { return getToken(SqlBaseParser.NAMESPACE, 0); }
		public ShowCurrentNamespaceContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowCurrentNamespace(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowCurrentNamespace(this);
		}
	}
	public static class RenameTablePartitionContext extends StatementContext {
		public PartitionSpecContext from_;
		public PartitionSpecContext to;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public List<PartitionSpecContext> partitionSpec() {
			return getRuleContexts(PartitionSpecContext.class);
		}
		public PartitionSpecContext partitionSpec(int i) {
			return getRuleContext(PartitionSpecContext.class,i);
		}
		public RenameTablePartitionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRenameTablePartition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRenameTablePartition(this);
		}
	}
	public static class RepairTableContext extends StatementContext {
		public TerminalNode MSCK() { return getToken(SqlBaseParser.MSCK, 0); }
		public TerminalNode REPAIR() { return getToken(SqlBaseParser.REPAIR, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public RepairTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRepairTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRepairTable(this);
		}
	}
	public static class RefreshResourceContext extends StatementContext {
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public RefreshResourceContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRefreshResource(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRefreshResource(this);
		}
	}
	public static class ShowCreateTableContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode SERDE() { return getToken(SqlBaseParser.SERDE, 0); }
		public ShowCreateTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowCreateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowCreateTable(this);
		}
	}
	public static class ShowNamespacesContext extends StatementContext {
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode DATABASES() { return getToken(SqlBaseParser.DATABASES, 0); }
		public TerminalNode NAMESPACES() { return getToken(SqlBaseParser.NAMESPACES, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public ShowNamespacesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowNamespaces(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowNamespaces(this);
		}
	}
	public static class ShowColumnsContext extends StatementContext {
		public MultipartIdentifierContext table;
		public MultipartIdentifierContext ns;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public List<TerminalNode> FROM() { return getTokens(SqlBaseParser.FROM); }
		public TerminalNode FROM(int i) {
			return getToken(SqlBaseParser.FROM, i);
		}
		public List<TerminalNode> IN() { return getTokens(SqlBaseParser.IN); }
		public TerminalNode IN(int i) {
			return getToken(SqlBaseParser.IN, i);
		}
		public List<MultipartIdentifierContext> multipartIdentifier() {
			return getRuleContexts(MultipartIdentifierContext.class);
		}
		public MultipartIdentifierContext multipartIdentifier(int i) {
			return getRuleContext(MultipartIdentifierContext.class,i);
		}
		public ShowColumnsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowColumns(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowColumns(this);
		}
	}
	public static class ReplaceTableContext extends StatementContext {
		public ReplaceTableHeaderContext replaceTableHeader() {
			return getRuleContext(ReplaceTableHeaderContext.class,0);
		}
		public TableProviderContext tableProvider() {
			return getRuleContext(TableProviderContext.class,0);
		}
		public CreateTableClausesContext createTableClauses() {
			return getRuleContext(CreateTableClausesContext.class,0);
		}
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public ReplaceTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterReplaceTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitReplaceTable(this);
		}
	}
	public static class AddTablePartitionContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public List<PartitionSpecLocationContext> partitionSpecLocation() {
			return getRuleContexts(PartitionSpecLocationContext.class);
		}
		public PartitionSpecLocationContext partitionSpecLocation(int i) {
			return getRuleContext(PartitionSpecLocationContext.class,i);
		}
		public AddTablePartitionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAddTablePartition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAddTablePartition(this);
		}
	}
	public static class SetNamespaceLocationContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public NamespaceContext namespace() {
			return getRuleContext(NamespaceContext.class,0);
		}
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public SetNamespaceLocationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetNamespaceLocation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetNamespaceLocation(this);
		}
	}
	public static class RefreshTableContext extends StatementContext {
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public RefreshTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRefreshTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRefreshTable(this);
		}
	}
	public static class SetNamespacePropertiesContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public NamespaceContext namespace() {
			return getRuleContext(NamespaceContext.class,0);
		}
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public TerminalNode DBPROPERTIES() { return getToken(SqlBaseParser.DBPROPERTIES, 0); }
		public TerminalNode PROPERTIES() { return getToken(SqlBaseParser.PROPERTIES, 0); }
		public SetNamespacePropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetNamespaceProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetNamespaceProperties(this);
		}
	}
	public static class ManageResourceContext extends StatementContext {
		public Token op;
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode LIST() { return getToken(SqlBaseParser.LIST, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ManageResourceContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterManageResource(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitManageResource(this);
		}
	}
	public static class AnalyzeContext extends StatementContext {
		public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode COMPUTE() { return getToken(SqlBaseParser.COMPUTE, 0); }
		public TerminalNode STATISTICS() { return getToken(SqlBaseParser.STATISTICS, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public IdentifierSeqContext identifierSeq() {
			return getRuleContext(IdentifierSeqContext.class,0);
		}
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public AnalyzeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAnalyze(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAnalyze(this);
		}
	}
	public static class CreateHiveTableContext extends StatementContext {
		public ColTypeListContext columns;
		public ColTypeListContext partitionColumns;
		public IdentifierListContext partitionColumnNames;
		public TablePropertyListContext tableProps;
		public CreateTableHeaderContext createTableHeader() {
			return getRuleContext(CreateTableHeaderContext.class,0);
		}
		public List<CommentSpecContext> commentSpec() {
			return getRuleContexts(CommentSpecContext.class);
		}
		public CommentSpecContext commentSpec(int i) {
			return getRuleContext(CommentSpecContext.class,i);
		}
		public List<BucketSpecContext> bucketSpec() {
			return getRuleContexts(BucketSpecContext.class);
		}
		public BucketSpecContext bucketSpec(int i) {
			return getRuleContext(BucketSpecContext.class,i);
		}
		public List<SkewSpecContext> skewSpec() {
			return getRuleContexts(SkewSpecContext.class);
		}
		public SkewSpecContext skewSpec(int i) {
			return getRuleContext(SkewSpecContext.class,i);
		}
		public List<RowFormatContext> rowFormat() {
			return getRuleContexts(RowFormatContext.class);
		}
		public RowFormatContext rowFormat(int i) {
			return getRuleContext(RowFormatContext.class,i);
		}
		public List<CreateFileFormatContext> createFileFormat() {
			return getRuleContexts(CreateFileFormatContext.class);
		}
		public CreateFileFormatContext createFileFormat(int i) {
			return getRuleContext(CreateFileFormatContext.class,i);
		}
		public List<LocationSpecContext> locationSpec() {
			return getRuleContexts(LocationSpecContext.class);
		}
		public LocationSpecContext locationSpec(int i) {
			return getRuleContext(LocationSpecContext.class,i);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public List<ColTypeListContext> colTypeList() {
			return getRuleContexts(ColTypeListContext.class);
		}
		public ColTypeListContext colTypeList(int i) {
			return getRuleContext(ColTypeListContext.class,i);
		}
		public List<TerminalNode> PARTITIONED() { return getTokens(SqlBaseParser.PARTITIONED); }
		public TerminalNode PARTITIONED(int i) {
			return getToken(SqlBaseParser.PARTITIONED, i);
		}
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public List<TerminalNode> TBLPROPERTIES() { return getTokens(SqlBaseParser.TBLPROPERTIES); }
		public TerminalNode TBLPROPERTIES(int i) {
			return getToken(SqlBaseParser.TBLPROPERTIES, i);
		}
		public List<IdentifierListContext> identifierList() {
			return getRuleContexts(IdentifierListContext.class);
		}
		public IdentifierListContext identifierList(int i) {
			return getRuleContext(IdentifierListContext.class,i);
		}
		public List<TablePropertyListContext> tablePropertyList() {
			return getRuleContexts(TablePropertyListContext.class);
		}
		public TablePropertyListContext tablePropertyList(int i) {
			return getRuleContext(TablePropertyListContext.class,i);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public CreateHiveTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateHiveTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateHiveTable(this);
		}
	}
	public static class CreateFunctionContext extends StatementContext {
		public Token className;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public List<ResourceContext> resource() {
			return getRuleContexts(ResourceContext.class);
		}
		public ResourceContext resource(int i) {
			return getRuleContext(ResourceContext.class,i);
		}
		public CreateFunctionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateFunction(this);
		}
	}
	public static class ShowTableContext extends StatementContext {
		public MultipartIdentifierContext ns;
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public ShowTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowTable(this);
		}
	}
	public static class HiveReplaceColumnsContext extends StatementContext {
		public MultipartIdentifierContext table;
		public QualifiedColTypeWithPositionListContext columns;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public QualifiedColTypeWithPositionListContext qualifiedColTypeWithPositionList() {
			return getRuleContext(QualifiedColTypeWithPositionListContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public HiveReplaceColumnsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterHiveReplaceColumns(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitHiveReplaceColumns(this);
		}
	}
	public static class CommentNamespaceContext extends StatementContext {
		public Token comment;
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public NamespaceContext namespace() {
			return getRuleContext(NamespaceContext.class,0);
		}
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public CommentNamespaceContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCommentNamespace(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCommentNamespace(this);
		}
	}
	public static class CreateTableContext extends StatementContext {
		public CreateTableHeaderContext createTableHeader() {
			return getRuleContext(CreateTableHeaderContext.class,0);
		}
		public CreateTableClausesContext createTableClauses() {
			return getRuleContext(CreateTableClausesContext.class,0);
		}
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public TableProviderContext tableProvider() {
			return getRuleContext(TableProviderContext.class,0);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public CreateTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateTable(this);
		}
	}
	public static class DmlStatementContext extends StatementContext {
		public DmlStatementNoWithContext dmlStatementNoWith() {
			return getRuleContext(DmlStatementNoWithContext.class,0);
		}
		public CtesContext ctes() {
			return getRuleContext(CtesContext.class,0);
		}
		public DmlStatementContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDmlStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDmlStatement(this);
		}
	}
	public static class CreateTableLikeContext extends StatementContext {
		public TableIdentifierContext target;
		public TableIdentifierContext source;
		public TablePropertyListContext tableProps;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public List<TableIdentifierContext> tableIdentifier() {
			return getRuleContexts(TableIdentifierContext.class);
		}
		public TableIdentifierContext tableIdentifier(int i) {
			return getRuleContext(TableIdentifierContext.class,i);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public List<TableProviderContext> tableProvider() {
			return getRuleContexts(TableProviderContext.class);
		}
		public TableProviderContext tableProvider(int i) {
			return getRuleContext(TableProviderContext.class,i);
		}
		public List<RowFormatContext> rowFormat() {
			return getRuleContexts(RowFormatContext.class);
		}
		public RowFormatContext rowFormat(int i) {
			return getRuleContext(RowFormatContext.class,i);
		}
		public List<CreateFileFormatContext> createFileFormat() {
			return getRuleContexts(CreateFileFormatContext.class);
		}
		public CreateFileFormatContext createFileFormat(int i) {
			return getRuleContext(CreateFileFormatContext.class,i);
		}
		public List<LocationSpecContext> locationSpec() {
			return getRuleContexts(LocationSpecContext.class);
		}
		public LocationSpecContext locationSpec(int i) {
			return getRuleContext(LocationSpecContext.class,i);
		}
		public List<TerminalNode> TBLPROPERTIES() { return getTokens(SqlBaseParser.TBLPROPERTIES); }
		public TerminalNode TBLPROPERTIES(int i) {
			return getToken(SqlBaseParser.TBLPROPERTIES, i);
		}
		public List<TablePropertyListContext> tablePropertyList() {
			return getRuleContexts(TablePropertyListContext.class);
		}
		public TablePropertyListContext tablePropertyList(int i) {
			return getRuleContext(TablePropertyListContext.class,i);
		}
		public CreateTableLikeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateTableLike(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateTableLike(this);
		}
	}
	public static class UncacheTableContext extends StatementContext {
		public TerminalNode UNCACHE() { return getToken(SqlBaseParser.UNCACHE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public UncacheTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUncacheTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUncacheTable(this);
		}
	}
	public static class DropFunctionContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public DropFunctionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropFunction(this);
		}
	}
	public static class DescribeRelationContext extends StatementContext {
		public Token option;
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public DescribeColNameContext describeColName() {
			return getRuleContext(DescribeColNameContext.class,0);
		}
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public TerminalNode FORMATTED() { return getToken(SqlBaseParser.FORMATTED, 0); }
		public DescribeRelationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescribeRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescribeRelation(this);
		}
	}
	public static class LoadDataContext extends StatementContext {
		public Token path;
		public TerminalNode LOAD() { return getToken(SqlBaseParser.LOAD, 0); }
		public TerminalNode DATA() { return getToken(SqlBaseParser.DATA, 0); }
		public TerminalNode INPATH() { return getToken(SqlBaseParser.INPATH, 0); }
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode LOCAL() { return getToken(SqlBaseParser.LOCAL, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlBaseParser.OVERWRITE, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public LoadDataContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLoadData(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLoadData(this);
		}
	}
	public static class ShowPartitionsContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode PARTITIONS() { return getToken(SqlBaseParser.PARTITIONS, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public ShowPartitionsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowPartitions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowPartitions(this);
		}
	}
	public static class DescribeFunctionContext extends StatementContext {
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public DescribeFuncNameContext describeFuncName() {
			return getRuleContext(DescribeFuncNameContext.class,0);
		}
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public DescribeFunctionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescribeFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescribeFunction(this);
		}
	}
	public static class RenameTableColumnContext extends StatementContext {
		public MultipartIdentifierContext table;
		public MultipartIdentifierContext from_;
		public ErrorCapturingIdentifierContext to;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public List<MultipartIdentifierContext> multipartIdentifier() {
			return getRuleContexts(MultipartIdentifierContext.class);
		}
		public MultipartIdentifierContext multipartIdentifier(int i) {
			return getRuleContext(MultipartIdentifierContext.class,i);
		}
		public ErrorCapturingIdentifierContext errorCapturingIdentifier() {
			return getRuleContext(ErrorCapturingIdentifierContext.class,0);
		}
		public RenameTableColumnContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRenameTableColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRenameTableColumn(this);
		}
	}
	public static class StatementDefaultContext extends StatementContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public StatementDefaultContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStatementDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStatementDefault(this);
		}
	}
	public static class HiveChangeColumnContext extends StatementContext {
		public MultipartIdentifierContext table;
		public MultipartIdentifierContext colName;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode CHANGE() { return getToken(SqlBaseParser.CHANGE, 0); }
		public ColTypeContext colType() {
			return getRuleContext(ColTypeContext.class,0);
		}
		public List<MultipartIdentifierContext> multipartIdentifier() {
			return getRuleContexts(MultipartIdentifierContext.class);
		}
		public MultipartIdentifierContext multipartIdentifier(int i) {
			return getRuleContext(MultipartIdentifierContext.class,i);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public ColPositionContext colPosition() {
			return getRuleContext(ColPositionContext.class,0);
		}
		public HiveChangeColumnContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterHiveChangeColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitHiveChangeColumn(this);
		}
	}
	public static class DescribeQueryContext extends StatementContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode QUERY() { return getToken(SqlBaseParser.QUERY, 0); }
		public DescribeQueryContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescribeQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescribeQuery(this);
		}
	}
	public static class TruncateTableContext extends StatementContext {
		public TerminalNode TRUNCATE() { return getToken(SqlBaseParser.TRUNCATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TruncateTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTruncateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTruncateTable(this);
		}
	}
	public static class SetTableSerDeContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode SERDE() { return getToken(SqlBaseParser.SERDE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlBaseParser.SERDEPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public SetTableSerDeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetTableSerDe(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetTableSerDe(this);
		}
	}
	public static class CreateViewContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public IdentifierCommentListContext identifierCommentList() {
			return getRuleContext(IdentifierCommentListContext.class,0);
		}
		public List<CommentSpecContext> commentSpec() {
			return getRuleContexts(CommentSpecContext.class);
		}
		public CommentSpecContext commentSpec(int i) {
			return getRuleContext(CommentSpecContext.class,i);
		}
		public List<TerminalNode> PARTITIONED() { return getTokens(SqlBaseParser.PARTITIONED); }
		public TerminalNode PARTITIONED(int i) {
			return getToken(SqlBaseParser.PARTITIONED, i);
		}
		public List<TerminalNode> ON() { return getTokens(SqlBaseParser.ON); }
		public TerminalNode ON(int i) {
			return getToken(SqlBaseParser.ON, i);
		}
		public List<IdentifierListContext> identifierList() {
			return getRuleContexts(IdentifierListContext.class);
		}
		public IdentifierListContext identifierList(int i) {
			return getRuleContext(IdentifierListContext.class,i);
		}
		public List<TerminalNode> TBLPROPERTIES() { return getTokens(SqlBaseParser.TBLPROPERTIES); }
		public TerminalNode TBLPROPERTIES(int i) {
			return getToken(SqlBaseParser.TBLPROPERTIES, i);
		}
		public List<TablePropertyListContext> tablePropertyList() {
			return getRuleContexts(TablePropertyListContext.class);
		}
		public TablePropertyListContext tablePropertyList(int i) {
			return getRuleContext(TablePropertyListContext.class,i);
		}
		public TerminalNode GLOBAL() { return getToken(SqlBaseParser.GLOBAL, 0); }
		public CreateViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateView(this);
		}
	}
	public static class DropTablePartitionsContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public List<PartitionSpecContext> partitionSpec() {
			return getRuleContexts(PartitionSpecContext.class);
		}
		public PartitionSpecContext partitionSpec(int i) {
			return getRuleContext(PartitionSpecContext.class,i);
		}
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode PURGE() { return getToken(SqlBaseParser.PURGE, 0); }
		public DropTablePartitionsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropTablePartitions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropTablePartitions(this);
		}
	}
	public static class SetConfigurationContext extends StatementContext {
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public SetConfigurationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetConfiguration(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetConfiguration(this);
		}
	}
	public static class DropTableContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode PURGE() { return getToken(SqlBaseParser.PURGE, 0); }
		public DropTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropTable(this);
		}
	}
	public static class DescribeNamespaceContext extends StatementContext {
		public NamespaceContext namespace() {
			return getRuleContext(NamespaceContext.class,0);
		}
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public DescribeNamespaceContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescribeNamespace(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescribeNamespace(this);
		}
	}
	public static class AlterTableAlterColumnContext extends StatementContext {
		public MultipartIdentifierContext table;
		public MultipartIdentifierContext column;
		public List<TerminalNode> ALTER() { return getTokens(SqlBaseParser.ALTER); }
		public TerminalNode ALTER(int i) {
			return getToken(SqlBaseParser.ALTER, i);
		}
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public List<MultipartIdentifierContext> multipartIdentifier() {
			return getRuleContexts(MultipartIdentifierContext.class);
		}
		public MultipartIdentifierContext multipartIdentifier(int i) {
			return getRuleContext(MultipartIdentifierContext.class,i);
		}
		public TerminalNode CHANGE() { return getToken(SqlBaseParser.CHANGE, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public AlterColumnActionContext alterColumnAction() {
			return getRuleContext(AlterColumnActionContext.class,0);
		}
		public AlterTableAlterColumnContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAlterTableAlterColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAlterTableAlterColumn(this);
		}
	}
	public static class CommentTableContext extends StatementContext {
		public Token comment;
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public CommentTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCommentTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCommentTable(this);
		}
	}
	public static class CreateNamespaceContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public NamespaceContext namespace() {
			return getRuleContext(NamespaceContext.class,0);
		}
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public List<CommentSpecContext> commentSpec() {
			return getRuleContexts(CommentSpecContext.class);
		}
		public CommentSpecContext commentSpec(int i) {
			return getRuleContext(CommentSpecContext.class,i);
		}
		public List<LocationSpecContext> locationSpec() {
			return getRuleContexts(LocationSpecContext.class);
		}
		public LocationSpecContext locationSpec(int i) {
			return getRuleContext(LocationSpecContext.class,i);
		}
		public List<TerminalNode> WITH() { return getTokens(SqlBaseParser.WITH); }
		public TerminalNode WITH(int i) {
			return getToken(SqlBaseParser.WITH, i);
		}
		public List<TablePropertyListContext> tablePropertyList() {
			return getRuleContexts(TablePropertyListContext.class);
		}
		public TablePropertyListContext tablePropertyList(int i) {
			return getRuleContext(TablePropertyListContext.class,i);
		}
		public List<TerminalNode> DBPROPERTIES() { return getTokens(SqlBaseParser.DBPROPERTIES); }
		public TerminalNode DBPROPERTIES(int i) {
			return getToken(SqlBaseParser.DBPROPERTIES, i);
		}
		public List<TerminalNode> PROPERTIES() { return getTokens(SqlBaseParser.PROPERTIES); }
		public TerminalNode PROPERTIES(int i) {
			return getToken(SqlBaseParser.PROPERTIES, i);
		}
		public CreateNamespaceContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateNamespace(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateNamespace(this);
		}
	}
	public static class ShowTblPropertiesContext extends StatementContext {
		public MultipartIdentifierContext table;
		public TablePropertyKeyContext key;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TablePropertyKeyContext tablePropertyKey() {
			return getRuleContext(TablePropertyKeyContext.class,0);
		}
		public ShowTblPropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowTblProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowTblProperties(this);
		}
	}
	public static class UnsetTablePropertiesContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode UNSET() { return getToken(SqlBaseParser.UNSET, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public UnsetTablePropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnsetTableProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnsetTableProperties(this);
		}
	}
	public static class SetTableLocationContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public SetTableLocationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetTableLocation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetTableLocation(this);
		}
	}
	public static class DropTableColumnsContext extends StatementContext {
		public MultipartIdentifierListContext columns;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public MultipartIdentifierListContext multipartIdentifierList() {
			return getRuleContext(MultipartIdentifierListContext.class,0);
		}
		public DropTableColumnsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropTableColumns(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropTableColumns(this);
		}
	}
	public static class ShowViewsContext extends StatementContext {
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode VIEWS() { return getToken(SqlBaseParser.VIEWS, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public ShowViewsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowViews(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowViews(this);
		}
	}
	public static class ShowFunctionsContext extends StatementContext {
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ShowFunctionsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowFunctions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowFunctions(this);
		}
	}
	public static class CacheTableContext extends StatementContext {
		public TablePropertyListContext options;
		public TerminalNode CACHE() { return getToken(SqlBaseParser.CACHE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode LAZY() { return getToken(SqlBaseParser.LAZY, 0); }
		public TerminalNode OPTIONS() { return getToken(SqlBaseParser.OPTIONS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public CacheTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCacheTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCacheTable(this);
		}
	}
	public static class AddTableColumnsContext extends StatementContext {
		public QualifiedColTypeWithPositionListContext columns;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public QualifiedColTypeWithPositionListContext qualifiedColTypeWithPositionList() {
			return getRuleContext(QualifiedColTypeWithPositionListContext.class,0);
		}
		public AddTableColumnsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAddTableColumns(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAddTableColumns(this);
		}
	}
	public static class SetTablePropertiesContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public SetTablePropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetTableProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetTableProperties(this);
		}
	}

	public final StatementContext statement() throws RecognitionException {
		StatementContext _localctx = new StatementContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_statement);
		int _la;
		try {
			int _alt;
			setState(1025);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,110,_ctx) ) {
			case 1:
				_localctx = new StatementDefaultContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(301);
				query();
				}
				break;
			case 2:
				_localctx = new DmlStatementContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(303);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(302);
					ctes();
					}
				}

				setState(305);
				dmlStatementNoWith();
				}
				break;
			case 3:
				_localctx = new UseContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(306);
				match(USE);
				setState(308);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
				case 1:
					{
					setState(307);
					match(NAMESPACE);
					}
					break;
				}
				setState(310);
				multipartIdentifier();
				}
				break;
			case 4:
				_localctx = new CreateNamespaceContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(311);
				match(CREATE);
				setState(312);
				namespace();
				setState(316);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,3,_ctx) ) {
				case 1:
					{
					setState(313);
					match(IF);
					setState(314);
					match(NOT);
					setState(315);
					match(EXISTS);
					}
					break;
				}
				setState(318);
				multipartIdentifier();
				setState(326);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMENT || _la==LOCATION || _la==WITH) {
					{
					setState(324);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case COMMENT:
						{
						setState(319);
						commentSpec();
						}
						break;
					case LOCATION:
						{
						setState(320);
						locationSpec();
						}
						break;
					case WITH:
						{
						{
						setState(321);
						match(WITH);
						setState(322);
						_la = _input.LA(1);
						if ( !(_la==DBPROPERTIES || _la==PROPERTIES) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(323);
						tablePropertyList();
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					setState(328);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case 5:
				_localctx = new SetNamespacePropertiesContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(329);
				match(ALTER);
				setState(330);
				namespace();
				setState(331);
				multipartIdentifier();
				setState(332);
				match(SET);
				setState(333);
				_la = _input.LA(1);
				if ( !(_la==DBPROPERTIES || _la==PROPERTIES) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(334);
				tablePropertyList();
				}
				break;
			case 6:
				_localctx = new SetNamespaceLocationContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(336);
				match(ALTER);
				setState(337);
				namespace();
				setState(338);
				multipartIdentifier();
				setState(339);
				match(SET);
				setState(340);
				locationSpec();
				}
				break;
			case 7:
				_localctx = new DropNamespaceContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(342);
				match(DROP);
				setState(343);
				namespace();
				setState(346);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
				case 1:
					{
					setState(344);
					match(IF);
					setState(345);
					match(EXISTS);
					}
					break;
				}
				setState(348);
				multipartIdentifier();
				setState(350);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==CASCADE || _la==RESTRICT) {
					{
					setState(349);
					_la = _input.LA(1);
					if ( !(_la==CASCADE || _la==RESTRICT) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				}
				break;
			case 8:
				_localctx = new ShowNamespacesContext(_localctx);
				enterOuterAlt(_localctx, 8);
				{
				setState(352);
				match(SHOW);
				setState(353);
				_la = _input.LA(1);
				if ( !(_la==DATABASES || _la==NAMESPACES) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(356);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(354);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(355);
					multipartIdentifier();
					}
				}

				setState(362);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE || _la==STRING) {
					{
					setState(359);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==LIKE) {
						{
						setState(358);
						match(LIKE);
						}
					}

					setState(361);
					((ShowNamespacesContext)_localctx).pattern = match(STRING);
					}
				}

				}
				break;
			case 9:
				_localctx = new CreateTableContext(_localctx);
				enterOuterAlt(_localctx, 9);
				{
				setState(364);
				if (!(not self.legacy_create_hive_table_by_default_enabled)) throw new FailedPredicateException(this, "not self.legacy_create_hive_table_by_default_enabled");
				setState(365);
				createTableHeader();
				setState(370);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
				case 1:
					{
					setState(366);
					match(T__1);
					setState(367);
					colTypeList();
					setState(368);
					match(T__2);
					}
					break;
				}
				setState(373);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==USING) {
					{
					setState(372);
					tableProvider();
					}
				}

				setState(375);
				createTableClauses();
				setState(380);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1 || _la==AS || _la==FROM || _la==MAP || ((((_la - 187)) & ~0x3f) == 0 && ((1L << (_la - 187)) & ((1L << (REDUCE - 187)) | (1L << (SELECT - 187)) | (1L << (TABLE - 187)))) != 0) || _la==VALUES || _la==WITH) {
					{
					setState(377);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(376);
						match(AS);
						}
					}

					setState(379);
					query();
					}
				}

				}
				break;
			case 10:
				_localctx = new CreateTableContext(_localctx);
				enterOuterAlt(_localctx, 10);
				{
				setState(382);
				if (!(self.legacy_create_hive_table_by_default_enabled)) throw new FailedPredicateException(this, "self.legacy_create_hive_table_by_default_enabled");
				setState(383);
				createTableHeader();
				setState(388);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1) {
					{
					setState(384);
					match(T__1);
					setState(385);
					colTypeList();
					setState(386);
					match(T__2);
					}
				}

				setState(390);
				tableProvider();
				setState(391);
				createTableClauses();
				setState(396);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1 || _la==AS || _la==FROM || _la==MAP || ((((_la - 187)) & ~0x3f) == 0 && ((1L << (_la - 187)) & ((1L << (REDUCE - 187)) | (1L << (SELECT - 187)) | (1L << (TABLE - 187)))) != 0) || _la==VALUES || _la==WITH) {
					{
					setState(393);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(392);
						match(AS);
						}
					}

					setState(395);
					query();
					}
				}

				}
				break;
			case 11:
				_localctx = new CreateHiveTableContext(_localctx);
				enterOuterAlt(_localctx, 11);
				{
				setState(398);
				createTableHeader();
				setState(403);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,18,_ctx) ) {
				case 1:
					{
					setState(399);
					match(T__1);
					setState(400);
					((CreateHiveTableContext)_localctx).columns = colTypeList();
					setState(401);
					match(T__2);
					}
					break;
				}
				setState(426);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==CLUSTERED || _la==COMMENT || _la==LOCATION || _la==PARTITIONED || ((((_la - 202)) & ~0x3f) == 0 && ((1L << (_la - 202)) & ((1L << (ROW - 202)) | (1L << (SKEWED - 202)) | (1L << (STORED - 202)) | (1L << (TBLPROPERTIES - 202)))) != 0)) {
					{
					setState(424);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case COMMENT:
						{
						setState(405);
						commentSpec();
						}
						break;
					case PARTITIONED:
						{
						setState(415);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,19,_ctx) ) {
						case 1:
							{
							setState(406);
							match(PARTITIONED);
							setState(407);
							match(BY);
							setState(408);
							match(T__1);
							setState(409);
							((CreateHiveTableContext)_localctx).partitionColumns = colTypeList();
							setState(410);
							match(T__2);
							}
							break;
						case 2:
							{
							setState(412);
							match(PARTITIONED);
							setState(413);
							match(BY);
							setState(414);
							((CreateHiveTableContext)_localctx).partitionColumnNames = identifierList();
							}
							break;
						}
						}
						break;
					case CLUSTERED:
						{
						setState(417);
						bucketSpec();
						}
						break;
					case SKEWED:
						{
						setState(418);
						skewSpec();
						}
						break;
					case ROW:
						{
						setState(419);
						rowFormat();
						}
						break;
					case STORED:
						{
						setState(420);
						createFileFormat();
						}
						break;
					case LOCATION:
						{
						setState(421);
						locationSpec();
						}
						break;
					case TBLPROPERTIES:
						{
						{
						setState(422);
						match(TBLPROPERTIES);
						setState(423);
						((CreateHiveTableContext)_localctx).tableProps = tablePropertyList();
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					setState(428);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(433);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1 || _la==AS || _la==FROM || _la==MAP || ((((_la - 187)) & ~0x3f) == 0 && ((1L << (_la - 187)) & ((1L << (REDUCE - 187)) | (1L << (SELECT - 187)) | (1L << (TABLE - 187)))) != 0) || _la==VALUES || _la==WITH) {
					{
					setState(430);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(429);
						match(AS);
						}
					}

					setState(432);
					query();
					}
				}

				}
				break;
			case 12:
				_localctx = new CreateTableLikeContext(_localctx);
				enterOuterAlt(_localctx, 12);
				{
				setState(435);
				match(CREATE);
				setState(436);
				match(TABLE);
				setState(440);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,24,_ctx) ) {
				case 1:
					{
					setState(437);
					match(IF);
					setState(438);
					match(NOT);
					setState(439);
					match(EXISTS);
					}
					break;
				}
				setState(442);
				((CreateTableLikeContext)_localctx).target = tableIdentifier();
				setState(443);
				match(LIKE);
				setState(444);
				((CreateTableLikeContext)_localctx).source = tableIdentifier();
				setState(453);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==LOCATION || ((((_la - 202)) & ~0x3f) == 0 && ((1L << (_la - 202)) & ((1L << (ROW - 202)) | (1L << (STORED - 202)) | (1L << (TBLPROPERTIES - 202)) | (1L << (USING - 202)))) != 0)) {
					{
					setState(451);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case USING:
						{
						setState(445);
						tableProvider();
						}
						break;
					case ROW:
						{
						setState(446);
						rowFormat();
						}
						break;
					case STORED:
						{
						setState(447);
						createFileFormat();
						}
						break;
					case LOCATION:
						{
						setState(448);
						locationSpec();
						}
						break;
					case TBLPROPERTIES:
						{
						{
						setState(449);
						match(TBLPROPERTIES);
						setState(450);
						((CreateTableLikeContext)_localctx).tableProps = tablePropertyList();
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					setState(455);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case 13:
				_localctx = new ReplaceTableContext(_localctx);
				enterOuterAlt(_localctx, 13);
				{
				setState(456);
				replaceTableHeader();
				setState(461);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1) {
					{
					setState(457);
					match(T__1);
					setState(458);
					colTypeList();
					setState(459);
					match(T__2);
					}
				}

				setState(463);
				tableProvider();
				setState(464);
				createTableClauses();
				setState(469);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1 || _la==AS || _la==FROM || _la==MAP || ((((_la - 187)) & ~0x3f) == 0 && ((1L << (_la - 187)) & ((1L << (REDUCE - 187)) | (1L << (SELECT - 187)) | (1L << (TABLE - 187)))) != 0) || _la==VALUES || _la==WITH) {
					{
					setState(466);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(465);
						match(AS);
						}
					}

					setState(468);
					query();
					}
				}

				}
				break;
			case 14:
				_localctx = new AnalyzeContext(_localctx);
				enterOuterAlt(_localctx, 14);
				{
				setState(471);
				match(ANALYZE);
				setState(472);
				match(TABLE);
				setState(473);
				multipartIdentifier();
				setState(475);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(474);
					partitionSpec();
					}
				}

				setState(477);
				match(COMPUTE);
				setState(478);
				match(STATISTICS);
				setState(486);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
				case 1:
					{
					setState(479);
					identifier();
					}
					break;
				case 2:
					{
					setState(480);
					match(FOR);
					setState(481);
					match(COLUMNS);
					setState(482);
					identifierSeq();
					}
					break;
				case 3:
					{
					setState(483);
					match(FOR);
					setState(484);
					match(ALL);
					setState(485);
					match(COLUMNS);
					}
					break;
				}
				}
				break;
			case 15:
				_localctx = new AddTableColumnsContext(_localctx);
				enterOuterAlt(_localctx, 15);
				{
				setState(488);
				match(ALTER);
				setState(489);
				match(TABLE);
				setState(490);
				multipartIdentifier();
				setState(491);
				match(ADD);
				setState(492);
				_la = _input.LA(1);
				if ( !(_la==COLUMN || _la==COLUMNS) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(493);
				((AddTableColumnsContext)_localctx).columns = qualifiedColTypeWithPositionList();
				}
				break;
			case 16:
				_localctx = new AddTableColumnsContext(_localctx);
				enterOuterAlt(_localctx, 16);
				{
				setState(495);
				match(ALTER);
				setState(496);
				match(TABLE);
				setState(497);
				multipartIdentifier();
				setState(498);
				match(ADD);
				setState(499);
				_la = _input.LA(1);
				if ( !(_la==COLUMN || _la==COLUMNS) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(500);
				match(T__1);
				setState(501);
				((AddTableColumnsContext)_localctx).columns = qualifiedColTypeWithPositionList();
				setState(502);
				match(T__2);
				}
				break;
			case 17:
				_localctx = new RenameTableColumnContext(_localctx);
				enterOuterAlt(_localctx, 17);
				{
				setState(504);
				match(ALTER);
				setState(505);
				match(TABLE);
				setState(506);
				((RenameTableColumnContext)_localctx).table = multipartIdentifier();
				setState(507);
				match(RENAME);
				setState(508);
				match(COLUMN);
				setState(509);
				((RenameTableColumnContext)_localctx).from_ = multipartIdentifier();
				setState(510);
				match(TO);
				setState(511);
				((RenameTableColumnContext)_localctx).to = errorCapturingIdentifier();
				}
				break;
			case 18:
				_localctx = new DropTableColumnsContext(_localctx);
				enterOuterAlt(_localctx, 18);
				{
				setState(513);
				match(ALTER);
				setState(514);
				match(TABLE);
				setState(515);
				multipartIdentifier();
				setState(516);
				match(DROP);
				setState(517);
				_la = _input.LA(1);
				if ( !(_la==COLUMN || _la==COLUMNS) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(518);
				match(T__1);
				setState(519);
				((DropTableColumnsContext)_localctx).columns = multipartIdentifierList();
				setState(520);
				match(T__2);
				}
				break;
			case 19:
				_localctx = new DropTableColumnsContext(_localctx);
				enterOuterAlt(_localctx, 19);
				{
				setState(522);
				match(ALTER);
				setState(523);
				match(TABLE);
				setState(524);
				multipartIdentifier();
				setState(525);
				match(DROP);
				setState(526);
				_la = _input.LA(1);
				if ( !(_la==COLUMN || _la==COLUMNS) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(527);
				((DropTableColumnsContext)_localctx).columns = multipartIdentifierList();
				}
				break;
			case 20:
				_localctx = new RenameTableContext(_localctx);
				enterOuterAlt(_localctx, 20);
				{
				setState(529);
				match(ALTER);
				setState(530);
				_la = _input.LA(1);
				if ( !(_la==TABLE || _la==VIEW) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(531);
				((RenameTableContext)_localctx).from_ = multipartIdentifier();
				setState(532);
				match(RENAME);
				setState(533);
				match(TO);
				setState(534);
				((RenameTableContext)_localctx).to = multipartIdentifier();
				}
				break;
			case 21:
				_localctx = new SetTablePropertiesContext(_localctx);
				enterOuterAlt(_localctx, 21);
				{
				setState(536);
				match(ALTER);
				setState(537);
				_la = _input.LA(1);
				if ( !(_la==TABLE || _la==VIEW) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(538);
				multipartIdentifier();
				setState(539);
				match(SET);
				setState(540);
				match(TBLPROPERTIES);
				setState(541);
				tablePropertyList();
				}
				break;
			case 22:
				_localctx = new UnsetTablePropertiesContext(_localctx);
				enterOuterAlt(_localctx, 22);
				{
				setState(543);
				match(ALTER);
				setState(544);
				_la = _input.LA(1);
				if ( !(_la==TABLE || _la==VIEW) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(545);
				multipartIdentifier();
				setState(546);
				match(UNSET);
				setState(547);
				match(TBLPROPERTIES);
				setState(550);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(548);
					match(IF);
					setState(549);
					match(EXISTS);
					}
				}

				setState(552);
				tablePropertyList();
				}
				break;
			case 23:
				_localctx = new AlterTableAlterColumnContext(_localctx);
				enterOuterAlt(_localctx, 23);
				{
				setState(554);
				match(ALTER);
				setState(555);
				match(TABLE);
				setState(556);
				((AlterTableAlterColumnContext)_localctx).table = multipartIdentifier();
				setState(557);
				_la = _input.LA(1);
				if ( !(_la==ALTER || _la==CHANGE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(559);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,33,_ctx) ) {
				case 1:
					{
					setState(558);
					match(COLUMN);
					}
					break;
				}
				setState(561);
				((AlterTableAlterColumnContext)_localctx).column = multipartIdentifier();
				setState(563);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AFTER || _la==COMMENT || _la==DROP || _la==FIRST || _la==SET || _la==TYPE) {
					{
					setState(562);
					alterColumnAction();
					}
				}

				}
				break;
			case 24:
				_localctx = new HiveChangeColumnContext(_localctx);
				enterOuterAlt(_localctx, 24);
				{
				setState(565);
				match(ALTER);
				setState(566);
				match(TABLE);
				setState(567);
				((HiveChangeColumnContext)_localctx).table = multipartIdentifier();
				setState(569);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(568);
					partitionSpec();
					}
				}

				setState(571);
				match(CHANGE);
				setState(573);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,36,_ctx) ) {
				case 1:
					{
					setState(572);
					match(COLUMN);
					}
					break;
				}
				setState(575);
				((HiveChangeColumnContext)_localctx).colName = multipartIdentifier();
				setState(576);
				colType();
				setState(578);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AFTER || _la==FIRST) {
					{
					setState(577);
					colPosition();
					}
				}

				}
				break;
			case 25:
				_localctx = new HiveReplaceColumnsContext(_localctx);
				enterOuterAlt(_localctx, 25);
				{
				setState(580);
				match(ALTER);
				setState(581);
				match(TABLE);
				setState(582);
				((HiveReplaceColumnsContext)_localctx).table = multipartIdentifier();
				setState(584);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(583);
					partitionSpec();
					}
				}

				setState(586);
				match(REPLACE);
				setState(587);
				match(COLUMNS);
				setState(588);
				match(T__1);
				setState(589);
				((HiveReplaceColumnsContext)_localctx).columns = qualifiedColTypeWithPositionList();
				setState(590);
				match(T__2);
				}
				break;
			case 26:
				_localctx = new SetTableSerDeContext(_localctx);
				enterOuterAlt(_localctx, 26);
				{
				setState(592);
				match(ALTER);
				setState(593);
				match(TABLE);
				setState(594);
				multipartIdentifier();
				setState(596);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(595);
					partitionSpec();
					}
				}

				setState(598);
				match(SET);
				setState(599);
				match(SERDE);
				setState(600);
				match(STRING);
				setState(604);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(601);
					match(WITH);
					setState(602);
					match(SERDEPROPERTIES);
					setState(603);
					tablePropertyList();
					}
				}

				}
				break;
			case 27:
				_localctx = new SetTableSerDeContext(_localctx);
				enterOuterAlt(_localctx, 27);
				{
				setState(606);
				match(ALTER);
				setState(607);
				match(TABLE);
				setState(608);
				multipartIdentifier();
				setState(610);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(609);
					partitionSpec();
					}
				}

				setState(612);
				match(SET);
				setState(613);
				match(SERDEPROPERTIES);
				setState(614);
				tablePropertyList();
				}
				break;
			case 28:
				_localctx = new AddTablePartitionContext(_localctx);
				enterOuterAlt(_localctx, 28);
				{
				setState(616);
				match(ALTER);
				setState(617);
				_la = _input.LA(1);
				if ( !(_la==TABLE || _la==VIEW) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(618);
				multipartIdentifier();
				setState(619);
				match(ADD);
				setState(623);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(620);
					match(IF);
					setState(621);
					match(NOT);
					setState(622);
					match(EXISTS);
					}
				}

				setState(626); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(625);
					partitionSpecLocation();
					}
					}
					setState(628); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==PARTITION );
				}
				break;
			case 29:
				_localctx = new RenameTablePartitionContext(_localctx);
				enterOuterAlt(_localctx, 29);
				{
				setState(630);
				match(ALTER);
				setState(631);
				match(TABLE);
				setState(632);
				multipartIdentifier();
				setState(633);
				((RenameTablePartitionContext)_localctx).from_ = partitionSpec();
				setState(634);
				match(RENAME);
				setState(635);
				match(TO);
				setState(636);
				((RenameTablePartitionContext)_localctx).to = partitionSpec();
				}
				break;
			case 30:
				_localctx = new DropTablePartitionsContext(_localctx);
				enterOuterAlt(_localctx, 30);
				{
				setState(638);
				match(ALTER);
				setState(639);
				_la = _input.LA(1);
				if ( !(_la==TABLE || _la==VIEW) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(640);
				multipartIdentifier();
				setState(641);
				match(DROP);
				setState(644);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(642);
					match(IF);
					setState(643);
					match(EXISTS);
					}
				}

				setState(646);
				partitionSpec();
				setState(651);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(647);
					match(T__3);
					setState(648);
					partitionSpec();
					}
					}
					setState(653);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(655);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PURGE) {
					{
					setState(654);
					match(PURGE);
					}
				}

				}
				break;
			case 31:
				_localctx = new SetTableLocationContext(_localctx);
				enterOuterAlt(_localctx, 31);
				{
				setState(657);
				match(ALTER);
				setState(658);
				match(TABLE);
				setState(659);
				multipartIdentifier();
				setState(661);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(660);
					partitionSpec();
					}
				}

				setState(663);
				match(SET);
				setState(664);
				locationSpec();
				}
				break;
			case 32:
				_localctx = new RecoverPartitionsContext(_localctx);
				enterOuterAlt(_localctx, 32);
				{
				setState(666);
				match(ALTER);
				setState(667);
				match(TABLE);
				setState(668);
				multipartIdentifier();
				setState(669);
				match(RECOVER);
				setState(670);
				match(PARTITIONS);
				}
				break;
			case 33:
				_localctx = new DropTableContext(_localctx);
				enterOuterAlt(_localctx, 33);
				{
				setState(672);
				match(DROP);
				setState(673);
				match(TABLE);
				setState(676);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,48,_ctx) ) {
				case 1:
					{
					setState(674);
					match(IF);
					setState(675);
					match(EXISTS);
					}
					break;
				}
				setState(678);
				multipartIdentifier();
				setState(680);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PURGE) {
					{
					setState(679);
					match(PURGE);
					}
				}

				}
				break;
			case 34:
				_localctx = new DropViewContext(_localctx);
				enterOuterAlt(_localctx, 34);
				{
				setState(682);
				match(DROP);
				setState(683);
				match(VIEW);
				setState(686);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,50,_ctx) ) {
				case 1:
					{
					setState(684);
					match(IF);
					setState(685);
					match(EXISTS);
					}
					break;
				}
				setState(688);
				multipartIdentifier();
				}
				break;
			case 35:
				_localctx = new CreateViewContext(_localctx);
				enterOuterAlt(_localctx, 35);
				{
				setState(689);
				match(CREATE);
				setState(692);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OR) {
					{
					setState(690);
					match(OR);
					setState(691);
					match(REPLACE);
					}
				}

				setState(698);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==GLOBAL || _la==TEMPORARY) {
					{
					setState(695);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==GLOBAL) {
						{
						setState(694);
						match(GLOBAL);
						}
					}

					setState(697);
					match(TEMPORARY);
					}
				}

				setState(700);
				match(VIEW);
				setState(704);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,54,_ctx) ) {
				case 1:
					{
					setState(701);
					match(IF);
					setState(702);
					match(NOT);
					setState(703);
					match(EXISTS);
					}
					break;
				}
				setState(706);
				multipartIdentifier();
				setState(708);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1) {
					{
					setState(707);
					identifierCommentList();
					}
				}

				setState(718);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMENT || _la==PARTITIONED || _la==TBLPROPERTIES) {
					{
					setState(716);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case COMMENT:
						{
						setState(710);
						commentSpec();
						}
						break;
					case PARTITIONED:
						{
						{
						setState(711);
						match(PARTITIONED);
						setState(712);
						match(ON);
						setState(713);
						identifierList();
						}
						}
						break;
					case TBLPROPERTIES:
						{
						{
						setState(714);
						match(TBLPROPERTIES);
						setState(715);
						tablePropertyList();
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					setState(720);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(721);
				match(AS);
				setState(722);
				query();
				}
				break;
			case 36:
				_localctx = new CreateTempViewUsingContext(_localctx);
				enterOuterAlt(_localctx, 36);
				{
				setState(724);
				match(CREATE);
				setState(727);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OR) {
					{
					setState(725);
					match(OR);
					setState(726);
					match(REPLACE);
					}
				}

				setState(730);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==GLOBAL) {
					{
					setState(729);
					match(GLOBAL);
					}
				}

				setState(732);
				match(TEMPORARY);
				setState(733);
				match(VIEW);
				setState(734);
				tableIdentifier();
				setState(739);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1) {
					{
					setState(735);
					match(T__1);
					setState(736);
					colTypeList();
					setState(737);
					match(T__2);
					}
				}

				setState(741);
				tableProvider();
				setState(744);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPTIONS) {
					{
					setState(742);
					match(OPTIONS);
					setState(743);
					tablePropertyList();
					}
				}

				}
				break;
			case 37:
				_localctx = new AlterViewQueryContext(_localctx);
				enterOuterAlt(_localctx, 37);
				{
				setState(746);
				match(ALTER);
				setState(747);
				match(VIEW);
				setState(748);
				multipartIdentifier();
				setState(750);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(749);
					match(AS);
					}
				}

				setState(752);
				query();
				}
				break;
			case 38:
				_localctx = new CreateFunctionContext(_localctx);
				enterOuterAlt(_localctx, 38);
				{
				setState(754);
				match(CREATE);
				setState(757);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OR) {
					{
					setState(755);
					match(OR);
					setState(756);
					match(REPLACE);
					}
				}

				setState(760);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TEMPORARY) {
					{
					setState(759);
					match(TEMPORARY);
					}
				}

				setState(762);
				match(FUNCTION);
				setState(766);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,65,_ctx) ) {
				case 1:
					{
					setState(763);
					match(IF);
					setState(764);
					match(NOT);
					setState(765);
					match(EXISTS);
					}
					break;
				}
				setState(768);
				multipartIdentifier();
				setState(769);
				match(AS);
				setState(770);
				((CreateFunctionContext)_localctx).className = match(STRING);
				setState(780);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==USING) {
					{
					setState(771);
					match(USING);
					setState(772);
					resource();
					setState(777);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(773);
						match(T__3);
						setState(774);
						resource();
						}
						}
						setState(779);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				}
				break;
			case 39:
				_localctx = new DropFunctionContext(_localctx);
				enterOuterAlt(_localctx, 39);
				{
				setState(782);
				match(DROP);
				setState(784);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TEMPORARY) {
					{
					setState(783);
					match(TEMPORARY);
					}
				}

				setState(786);
				match(FUNCTION);
				setState(789);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,69,_ctx) ) {
				case 1:
					{
					setState(787);
					match(IF);
					setState(788);
					match(EXISTS);
					}
					break;
				}
				setState(791);
				multipartIdentifier();
				}
				break;
			case 40:
				_localctx = new ExplainContext(_localctx);
				enterOuterAlt(_localctx, 40);
				{
				setState(792);
				match(EXPLAIN);
				setState(794);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,70,_ctx) ) {
				case 1:
					{
					setState(793);
					_la = _input.LA(1);
					if ( !(_la==CODEGEN || _la==COST || ((((_la - 86)) & ~0x3f) == 0 && ((1L << (_la - 86)) & ((1L << (EXTENDED - 86)) | (1L << (FORMATTED - 86)) | (1L << (LOGICAL - 86)))) != 0)) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					break;
				}
				setState(796);
				statement();
				}
				break;
			case 41:
				_localctx = new ShowTablesContext(_localctx);
				enterOuterAlt(_localctx, 41);
				{
				setState(797);
				match(SHOW);
				setState(798);
				match(TABLES);
				setState(801);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(799);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(800);
					multipartIdentifier();
					}
				}

				setState(807);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE || _la==STRING) {
					{
					setState(804);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==LIKE) {
						{
						setState(803);
						match(LIKE);
						}
					}

					setState(806);
					((ShowTablesContext)_localctx).pattern = match(STRING);
					}
				}

				}
				break;
			case 42:
				_localctx = new ShowTableContext(_localctx);
				enterOuterAlt(_localctx, 42);
				{
				setState(809);
				match(SHOW);
				setState(810);
				match(TABLE);
				setState(811);
				match(EXTENDED);
				setState(814);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(812);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(813);
					((ShowTableContext)_localctx).ns = multipartIdentifier();
					}
				}

				setState(816);
				match(LIKE);
				setState(817);
				((ShowTableContext)_localctx).pattern = match(STRING);
				setState(819);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(818);
					partitionSpec();
					}
				}

				}
				break;
			case 43:
				_localctx = new ShowTblPropertiesContext(_localctx);
				enterOuterAlt(_localctx, 43);
				{
				setState(821);
				match(SHOW);
				setState(822);
				match(TBLPROPERTIES);
				setState(823);
				((ShowTblPropertiesContext)_localctx).table = multipartIdentifier();
				setState(828);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1) {
					{
					setState(824);
					match(T__1);
					setState(825);
					((ShowTblPropertiesContext)_localctx).key = tablePropertyKey();
					setState(826);
					match(T__2);
					}
				}

				}
				break;
			case 44:
				_localctx = new ShowColumnsContext(_localctx);
				enterOuterAlt(_localctx, 44);
				{
				setState(830);
				match(SHOW);
				setState(831);
				match(COLUMNS);
				setState(832);
				_la = _input.LA(1);
				if ( !(_la==FROM || _la==IN) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(833);
				((ShowColumnsContext)_localctx).table = multipartIdentifier();
				setState(836);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(834);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(835);
					((ShowColumnsContext)_localctx).ns = multipartIdentifier();
					}
				}

				}
				break;
			case 45:
				_localctx = new ShowViewsContext(_localctx);
				enterOuterAlt(_localctx, 45);
				{
				setState(838);
				match(SHOW);
				setState(839);
				match(VIEWS);
				setState(842);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(840);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(841);
					multipartIdentifier();
					}
				}

				setState(848);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE || _la==STRING) {
					{
					setState(845);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==LIKE) {
						{
						setState(844);
						match(LIKE);
						}
					}

					setState(847);
					((ShowViewsContext)_localctx).pattern = match(STRING);
					}
				}

				}
				break;
			case 46:
				_localctx = new ShowPartitionsContext(_localctx);
				enterOuterAlt(_localctx, 46);
				{
				setState(850);
				match(SHOW);
				setState(851);
				match(PARTITIONS);
				setState(852);
				multipartIdentifier();
				setState(854);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(853);
					partitionSpec();
					}
				}

				}
				break;
			case 47:
				_localctx = new ShowFunctionsContext(_localctx);
				enterOuterAlt(_localctx, 47);
				{
				setState(856);
				match(SHOW);
				setState(858);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,82,_ctx) ) {
				case 1:
					{
					setState(857);
					identifier();
					}
					break;
				}
				setState(860);
				match(FUNCTIONS);
				setState(868);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,85,_ctx) ) {
				case 1:
					{
					setState(862);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,83,_ctx) ) {
					case 1:
						{
						setState(861);
						match(LIKE);
						}
						break;
					}
					setState(866);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,84,_ctx) ) {
					case 1:
						{
						setState(864);
						multipartIdentifier();
						}
						break;
					case 2:
						{
						setState(865);
						((ShowFunctionsContext)_localctx).pattern = match(STRING);
						}
						break;
					}
					}
					break;
				}
				}
				break;
			case 48:
				_localctx = new ShowCreateTableContext(_localctx);
				enterOuterAlt(_localctx, 48);
				{
				setState(870);
				match(SHOW);
				setState(871);
				match(CREATE);
				setState(872);
				match(TABLE);
				setState(873);
				multipartIdentifier();
				setState(876);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(874);
					match(AS);
					setState(875);
					match(SERDE);
					}
				}

				}
				break;
			case 49:
				_localctx = new ShowCurrentNamespaceContext(_localctx);
				enterOuterAlt(_localctx, 49);
				{
				setState(878);
				match(SHOW);
				setState(879);
				match(CURRENT);
				setState(880);
				match(NAMESPACE);
				}
				break;
			case 50:
				_localctx = new DescribeFunctionContext(_localctx);
				enterOuterAlt(_localctx, 50);
				{
				setState(881);
				_la = _input.LA(1);
				if ( !(_la==DESC || _la==DESCRIBE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(882);
				match(FUNCTION);
				setState(884);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,87,_ctx) ) {
				case 1:
					{
					setState(883);
					match(EXTENDED);
					}
					break;
				}
				setState(886);
				describeFuncName();
				}
				break;
			case 51:
				_localctx = new DescribeNamespaceContext(_localctx);
				enterOuterAlt(_localctx, 51);
				{
				setState(887);
				_la = _input.LA(1);
				if ( !(_la==DESC || _la==DESCRIBE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(888);
				namespace();
				setState(890);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,88,_ctx) ) {
				case 1:
					{
					setState(889);
					match(EXTENDED);
					}
					break;
				}
				setState(892);
				multipartIdentifier();
				}
				break;
			case 52:
				_localctx = new DescribeRelationContext(_localctx);
				enterOuterAlt(_localctx, 52);
				{
				setState(894);
				_la = _input.LA(1);
				if ( !(_la==DESC || _la==DESCRIBE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(896);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,89,_ctx) ) {
				case 1:
					{
					setState(895);
					match(TABLE);
					}
					break;
				}
				setState(899);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,90,_ctx) ) {
				case 1:
					{
					setState(898);
					((DescribeRelationContext)_localctx).option = _input.LT(1);
					_la = _input.LA(1);
					if ( !(_la==EXTENDED || _la==FORMATTED) ) {
						((DescribeRelationContext)_localctx).option = (Token)_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					break;
				}
				setState(901);
				multipartIdentifier();
				setState(903);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,91,_ctx) ) {
				case 1:
					{
					setState(902);
					partitionSpec();
					}
					break;
				}
				setState(906);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,92,_ctx) ) {
				case 1:
					{
					setState(905);
					describeColName();
					}
					break;
				}
				}
				break;
			case 53:
				_localctx = new DescribeQueryContext(_localctx);
				enterOuterAlt(_localctx, 53);
				{
				setState(908);
				_la = _input.LA(1);
				if ( !(_la==DESC || _la==DESCRIBE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(910);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==QUERY) {
					{
					setState(909);
					match(QUERY);
					}
				}

				setState(912);
				query();
				}
				break;
			case 54:
				_localctx = new CommentNamespaceContext(_localctx);
				enterOuterAlt(_localctx, 54);
				{
				setState(913);
				match(COMMENT);
				setState(914);
				match(ON);
				setState(915);
				namespace();
				setState(916);
				multipartIdentifier();
				setState(917);
				match(IS);
				setState(918);
				((CommentNamespaceContext)_localctx).comment = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==NULL || _la==STRING) ) {
					((CommentNamespaceContext)_localctx).comment = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case 55:
				_localctx = new CommentTableContext(_localctx);
				enterOuterAlt(_localctx, 55);
				{
				setState(920);
				match(COMMENT);
				setState(921);
				match(ON);
				setState(922);
				match(TABLE);
				setState(923);
				multipartIdentifier();
				setState(924);
				match(IS);
				setState(925);
				((CommentTableContext)_localctx).comment = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==NULL || _la==STRING) ) {
					((CommentTableContext)_localctx).comment = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case 56:
				_localctx = new RefreshTableContext(_localctx);
				enterOuterAlt(_localctx, 56);
				{
				setState(927);
				match(REFRESH);
				setState(928);
				match(TABLE);
				setState(929);
				multipartIdentifier();
				}
				break;
			case 57:
				_localctx = new RefreshResourceContext(_localctx);
				enterOuterAlt(_localctx, 57);
				{
				setState(930);
				match(REFRESH);
				setState(938);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,95,_ctx) ) {
				case 1:
					{
					setState(931);
					match(STRING);
					}
					break;
				case 2:
					{
					setState(935);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,94,_ctx);
					while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
						if ( _alt==1+1 ) {
							{
							{
							setState(932);
							matchWildcard();
							}
							} 
						}
						setState(937);
						_errHandler.sync(this);
						_alt = getInterpreter().adaptivePredict(_input,94,_ctx);
					}
					}
					break;
				}
				}
				break;
			case 58:
				_localctx = new CacheTableContext(_localctx);
				enterOuterAlt(_localctx, 58);
				{
				setState(940);
				match(CACHE);
				setState(942);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LAZY) {
					{
					setState(941);
					match(LAZY);
					}
				}

				setState(944);
				match(TABLE);
				setState(945);
				multipartIdentifier();
				setState(948);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPTIONS) {
					{
					setState(946);
					match(OPTIONS);
					setState(947);
					((CacheTableContext)_localctx).options = tablePropertyList();
					}
				}

				setState(954);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1 || _la==AS || _la==FROM || _la==MAP || ((((_la - 187)) & ~0x3f) == 0 && ((1L << (_la - 187)) & ((1L << (REDUCE - 187)) | (1L << (SELECT - 187)) | (1L << (TABLE - 187)))) != 0) || _la==VALUES || _la==WITH) {
					{
					setState(951);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(950);
						match(AS);
						}
					}

					setState(953);
					query();
					}
				}

				}
				break;
			case 59:
				_localctx = new UncacheTableContext(_localctx);
				enterOuterAlt(_localctx, 59);
				{
				setState(956);
				match(UNCACHE);
				setState(957);
				match(TABLE);
				setState(960);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,100,_ctx) ) {
				case 1:
					{
					setState(958);
					match(IF);
					setState(959);
					match(EXISTS);
					}
					break;
				}
				setState(962);
				multipartIdentifier();
				}
				break;
			case 60:
				_localctx = new ClearCacheContext(_localctx);
				enterOuterAlt(_localctx, 60);
				{
				setState(963);
				match(CLEAR);
				setState(964);
				match(CACHE);
				}
				break;
			case 61:
				_localctx = new LoadDataContext(_localctx);
				enterOuterAlt(_localctx, 61);
				{
				setState(965);
				match(LOAD);
				setState(966);
				match(DATA);
				setState(968);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LOCAL) {
					{
					setState(967);
					match(LOCAL);
					}
				}

				setState(970);
				match(INPATH);
				setState(971);
				((LoadDataContext)_localctx).path = match(STRING);
				setState(973);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OVERWRITE) {
					{
					setState(972);
					match(OVERWRITE);
					}
				}

				setState(975);
				match(INTO);
				setState(976);
				match(TABLE);
				setState(977);
				multipartIdentifier();
				setState(979);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(978);
					partitionSpec();
					}
				}

				}
				break;
			case 62:
				_localctx = new TruncateTableContext(_localctx);
				enterOuterAlt(_localctx, 62);
				{
				setState(981);
				match(TRUNCATE);
				setState(982);
				match(TABLE);
				setState(983);
				multipartIdentifier();
				setState(985);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(984);
					partitionSpec();
					}
				}

				}
				break;
			case 63:
				_localctx = new RepairTableContext(_localctx);
				enterOuterAlt(_localctx, 63);
				{
				setState(987);
				match(MSCK);
				setState(988);
				match(REPAIR);
				setState(989);
				match(TABLE);
				setState(990);
				multipartIdentifier();
				}
				break;
			case 64:
				_localctx = new ManageResourceContext(_localctx);
				enterOuterAlt(_localctx, 64);
				{
				setState(991);
				((ManageResourceContext)_localctx).op = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ADD || _la==LIST) ) {
					((ManageResourceContext)_localctx).op = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(992);
				identifier();
				setState(1000);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,106,_ctx) ) {
				case 1:
					{
					setState(993);
					match(STRING);
					}
					break;
				case 2:
					{
					setState(997);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,105,_ctx);
					while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
						if ( _alt==1+1 ) {
							{
							{
							setState(994);
							matchWildcard();
							}
							} 
						}
						setState(999);
						_errHandler.sync(this);
						_alt = getInterpreter().adaptivePredict(_input,105,_ctx);
					}
					}
					break;
				}
				}
				break;
			case 65:
				_localctx = new FailNativeCommandContext(_localctx);
				enterOuterAlt(_localctx, 65);
				{
				setState(1002);
				match(SET);
				setState(1003);
				match(ROLE);
				setState(1007);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,107,_ctx);
				while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(1004);
						matchWildcard();
						}
						} 
					}
					setState(1009);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,107,_ctx);
				}
				}
				break;
			case 66:
				_localctx = new SetConfigurationContext(_localctx);
				enterOuterAlt(_localctx, 66);
				{
				setState(1010);
				match(SET);
				setState(1014);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,108,_ctx);
				while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(1011);
						matchWildcard();
						}
						} 
					}
					setState(1016);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,108,_ctx);
				}
				}
				break;
			case 67:
				_localctx = new ResetConfigurationContext(_localctx);
				enterOuterAlt(_localctx, 67);
				{
				setState(1017);
				match(RESET);
				}
				break;
			case 68:
				_localctx = new FailNativeCommandContext(_localctx);
				enterOuterAlt(_localctx, 68);
				{
				setState(1018);
				unsupportedHiveNativeCommands();
				setState(1022);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,109,_ctx);
				while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(1019);
						matchWildcard();
						}
						} 
					}
					setState(1024);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,109,_ctx);
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class UnsupportedHiveNativeCommandsContext extends ParserRuleContext {
		public Token kw1;
		public Token kw2;
		public Token kw3;
		public Token kw4;
		public Token kw5;
		public Token kw6;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode GRANT() { return getToken(SqlBaseParser.GRANT, 0); }
		public TerminalNode REVOKE() { return getToken(SqlBaseParser.REVOKE, 0); }
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode PRINCIPALS() { return getToken(SqlBaseParser.PRINCIPALS, 0); }
		public TerminalNode ROLES() { return getToken(SqlBaseParser.ROLES, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public TerminalNode EXPORT() { return getToken(SqlBaseParser.EXPORT, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode IMPORT() { return getToken(SqlBaseParser.IMPORT, 0); }
		public TerminalNode COMPACTIONS() { return getToken(SqlBaseParser.COMPACTIONS, 0); }
		public TerminalNode TRANSACTIONS() { return getToken(SqlBaseParser.TRANSACTIONS, 0); }
		public TerminalNode INDEXES() { return getToken(SqlBaseParser.INDEXES, 0); }
		public TerminalNode LOCKS() { return getToken(SqlBaseParser.LOCKS, 0); }
		public TerminalNode INDEX() { return getToken(SqlBaseParser.INDEX, 0); }
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode LOCK() { return getToken(SqlBaseParser.LOCK, 0); }
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public TerminalNode UNLOCK() { return getToken(SqlBaseParser.UNLOCK, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode MACRO() { return getToken(SqlBaseParser.MACRO, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode CLUSTERED() { return getToken(SqlBaseParser.CLUSTERED, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public TerminalNode SORTED() { return getToken(SqlBaseParser.SORTED, 0); }
		public TerminalNode SKEWED() { return getToken(SqlBaseParser.SKEWED, 0); }
		public TerminalNode STORED() { return getToken(SqlBaseParser.STORED, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode DIRECTORIES() { return getToken(SqlBaseParser.DIRECTORIES, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode LOCATION() { return getToken(SqlBaseParser.LOCATION, 0); }
		public TerminalNode EXCHANGE() { return getToken(SqlBaseParser.EXCHANGE, 0); }
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public TerminalNode ARCHIVE() { return getToken(SqlBaseParser.ARCHIVE, 0); }
		public TerminalNode UNARCHIVE() { return getToken(SqlBaseParser.UNARCHIVE, 0); }
		public TerminalNode TOUCH() { return getToken(SqlBaseParser.TOUCH, 0); }
		public TerminalNode COMPACT() { return getToken(SqlBaseParser.COMPACT, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode CONCATENATE() { return getToken(SqlBaseParser.CONCATENATE, 0); }
		public TerminalNode FILEFORMAT() { return getToken(SqlBaseParser.FILEFORMAT, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public TerminalNode START() { return getToken(SqlBaseParser.START, 0); }
		public TerminalNode TRANSACTION() { return getToken(SqlBaseParser.TRANSACTION, 0); }
		public TerminalNode COMMIT() { return getToken(SqlBaseParser.COMMIT, 0); }
		public TerminalNode ROLLBACK() { return getToken(SqlBaseParser.ROLLBACK, 0); }
		public TerminalNode DFS() { return getToken(SqlBaseParser.DFS, 0); }
		public UnsupportedHiveNativeCommandsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unsupportedHiveNativeCommands; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnsupportedHiveNativeCommands(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnsupportedHiveNativeCommands(this);
		}
	}

	public final UnsupportedHiveNativeCommandsContext unsupportedHiveNativeCommands() throws RecognitionException {
		UnsupportedHiveNativeCommandsContext _localctx = new UnsupportedHiveNativeCommandsContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_unsupportedHiveNativeCommands);
		int _la;
		try {
			setState(1195);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,118,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1027);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(CREATE);
				setState(1028);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1029);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DROP);
				setState(1030);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1031);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(GRANT);
				setState(1033);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,111,_ctx) ) {
				case 1:
					{
					setState(1032);
					((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
					}
					break;
				}
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1035);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(REVOKE);
				setState(1037);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,112,_ctx) ) {
				case 1:
					{
					setState(1036);
					((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
					}
					break;
				}
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(1039);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(1040);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(GRANT);
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(1041);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(1042);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
				setState(1044);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,113,_ctx) ) {
				case 1:
					{
					setState(1043);
					((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(GRANT);
					}
					break;
				}
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(1046);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(1047);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(PRINCIPALS);
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(1048);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(1049);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLES);
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(1050);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(1051);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(CURRENT);
				setState(1052);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(ROLES);
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(1053);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(EXPORT);
				setState(1054);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				}
				break;
			case 11:
				enterOuterAlt(_localctx, 11);
				{
				setState(1055);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(IMPORT);
				setState(1056);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				}
				break;
			case 12:
				enterOuterAlt(_localctx, 12);
				{
				setState(1057);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(1058);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(COMPACTIONS);
				}
				break;
			case 13:
				enterOuterAlt(_localctx, 13);
				{
				setState(1059);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(1060);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(CREATE);
				setState(1061);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(TABLE);
				}
				break;
			case 14:
				enterOuterAlt(_localctx, 14);
				{
				setState(1062);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(1063);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TRANSACTIONS);
				}
				break;
			case 15:
				enterOuterAlt(_localctx, 15);
				{
				setState(1064);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(1065);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(INDEXES);
				}
				break;
			case 16:
				enterOuterAlt(_localctx, 16);
				{
				setState(1066);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(1067);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(LOCKS);
				}
				break;
			case 17:
				enterOuterAlt(_localctx, 17);
				{
				setState(1068);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(CREATE);
				setState(1069);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(INDEX);
				}
				break;
			case 18:
				enterOuterAlt(_localctx, 18);
				{
				setState(1070);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DROP);
				setState(1071);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(INDEX);
				}
				break;
			case 19:
				enterOuterAlt(_localctx, 19);
				{
				setState(1072);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1073);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(INDEX);
				}
				break;
			case 20:
				enterOuterAlt(_localctx, 20);
				{
				setState(1074);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(LOCK);
				setState(1075);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				}
				break;
			case 21:
				enterOuterAlt(_localctx, 21);
				{
				setState(1076);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(LOCK);
				setState(1077);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(DATABASE);
				}
				break;
			case 22:
				enterOuterAlt(_localctx, 22);
				{
				setState(1078);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(UNLOCK);
				setState(1079);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				}
				break;
			case 23:
				enterOuterAlt(_localctx, 23);
				{
				setState(1080);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(UNLOCK);
				setState(1081);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(DATABASE);
				}
				break;
			case 24:
				enterOuterAlt(_localctx, 24);
				{
				setState(1082);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(CREATE);
				setState(1083);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TEMPORARY);
				setState(1084);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(MACRO);
				}
				break;
			case 25:
				enterOuterAlt(_localctx, 25);
				{
				setState(1085);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DROP);
				setState(1086);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TEMPORARY);
				setState(1087);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(MACRO);
				}
				break;
			case 26:
				enterOuterAlt(_localctx, 26);
				{
				setState(1088);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1089);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1090);
				tableIdentifier();
				setState(1091);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(NOT);
				setState(1092);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(CLUSTERED);
				}
				break;
			case 27:
				enterOuterAlt(_localctx, 27);
				{
				setState(1094);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1095);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1096);
				tableIdentifier();
				setState(1097);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(CLUSTERED);
				setState(1098);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(BY);
				}
				break;
			case 28:
				enterOuterAlt(_localctx, 28);
				{
				setState(1100);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1101);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1102);
				tableIdentifier();
				setState(1103);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(NOT);
				setState(1104);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(SORTED);
				}
				break;
			case 29:
				enterOuterAlt(_localctx, 29);
				{
				setState(1106);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1107);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1108);
				tableIdentifier();
				setState(1109);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(SKEWED);
				setState(1110);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(BY);
				}
				break;
			case 30:
				enterOuterAlt(_localctx, 30);
				{
				setState(1112);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1113);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1114);
				tableIdentifier();
				setState(1115);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(NOT);
				setState(1116);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(SKEWED);
				}
				break;
			case 31:
				enterOuterAlt(_localctx, 31);
				{
				setState(1118);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1119);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1120);
				tableIdentifier();
				setState(1121);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(NOT);
				setState(1122);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(STORED);
				setState(1123);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw5 = match(AS);
				setState(1124);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw6 = match(DIRECTORIES);
				}
				break;
			case 32:
				enterOuterAlt(_localctx, 32);
				{
				setState(1126);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1127);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1128);
				tableIdentifier();
				setState(1129);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(SET);
				setState(1130);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(SKEWED);
				setState(1131);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw5 = match(LOCATION);
				}
				break;
			case 33:
				enterOuterAlt(_localctx, 33);
				{
				setState(1133);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1134);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1135);
				tableIdentifier();
				setState(1136);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(EXCHANGE);
				setState(1137);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(PARTITION);
				}
				break;
			case 34:
				enterOuterAlt(_localctx, 34);
				{
				setState(1139);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1140);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1141);
				tableIdentifier();
				setState(1142);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(ARCHIVE);
				setState(1143);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(PARTITION);
				}
				break;
			case 35:
				enterOuterAlt(_localctx, 35);
				{
				setState(1145);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1146);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1147);
				tableIdentifier();
				setState(1148);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(UNARCHIVE);
				setState(1149);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(PARTITION);
				}
				break;
			case 36:
				enterOuterAlt(_localctx, 36);
				{
				setState(1151);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1152);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1153);
				tableIdentifier();
				setState(1154);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(TOUCH);
				}
				break;
			case 37:
				enterOuterAlt(_localctx, 37);
				{
				setState(1156);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1157);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1158);
				tableIdentifier();
				setState(1160);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1159);
					partitionSpec();
					}
				}

				setState(1162);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(COMPACT);
				}
				break;
			case 38:
				enterOuterAlt(_localctx, 38);
				{
				setState(1164);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1165);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1166);
				tableIdentifier();
				setState(1168);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1167);
					partitionSpec();
					}
				}

				setState(1170);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(CONCATENATE);
				}
				break;
			case 39:
				enterOuterAlt(_localctx, 39);
				{
				setState(1172);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1173);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1174);
				tableIdentifier();
				setState(1176);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1175);
					partitionSpec();
					}
				}

				setState(1178);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(SET);
				setState(1179);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(FILEFORMAT);
				}
				break;
			case 40:
				enterOuterAlt(_localctx, 40);
				{
				setState(1181);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1182);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1183);
				tableIdentifier();
				setState(1185);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1184);
					partitionSpec();
					}
				}

				setState(1187);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(REPLACE);
				setState(1188);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(COLUMNS);
				}
				break;
			case 41:
				enterOuterAlt(_localctx, 41);
				{
				setState(1190);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(START);
				setState(1191);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TRANSACTION);
				}
				break;
			case 42:
				enterOuterAlt(_localctx, 42);
				{
				setState(1192);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(COMMIT);
				}
				break;
			case 43:
				enterOuterAlt(_localctx, 43);
				{
				setState(1193);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ROLLBACK);
				}
				break;
			case 44:
				enterOuterAlt(_localctx, 44);
				{
				setState(1194);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DFS);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CreateTableHeaderContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode EXTERNAL() { return getToken(SqlBaseParser.EXTERNAL, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public CreateTableHeaderContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createTableHeader; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateTableHeader(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateTableHeader(this);
		}
	}

	public final CreateTableHeaderContext createTableHeader() throws RecognitionException {
		CreateTableHeaderContext _localctx = new CreateTableHeaderContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_createTableHeader);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1197);
			match(CREATE);
			setState(1199);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==TEMPORARY) {
				{
				setState(1198);
				match(TEMPORARY);
				}
			}

			setState(1202);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==EXTERNAL) {
				{
				setState(1201);
				match(EXTERNAL);
				}
			}

			setState(1204);
			match(TABLE);
			setState(1208);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,121,_ctx) ) {
			case 1:
				{
				setState(1205);
				match(IF);
				setState(1206);
				match(NOT);
				setState(1207);
				match(EXISTS);
				}
				break;
			}
			setState(1210);
			multipartIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ReplaceTableHeaderContext extends ParserRuleContext {
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public ReplaceTableHeaderContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_replaceTableHeader; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterReplaceTableHeader(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitReplaceTableHeader(this);
		}
	}

	public final ReplaceTableHeaderContext replaceTableHeader() throws RecognitionException {
		ReplaceTableHeaderContext _localctx = new ReplaceTableHeaderContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_replaceTableHeader);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1214);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==CREATE) {
				{
				setState(1212);
				match(CREATE);
				setState(1213);
				match(OR);
				}
			}

			setState(1216);
			match(REPLACE);
			setState(1217);
			match(TABLE);
			setState(1218);
			multipartIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BucketSpecContext extends ParserRuleContext {
		public TerminalNode CLUSTERED() { return getToken(SqlBaseParser.CLUSTERED, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode BUCKETS() { return getToken(SqlBaseParser.BUCKETS, 0); }
		public TerminalNode SORTED() { return getToken(SqlBaseParser.SORTED, 0); }
		public OrderedIdentifierListContext orderedIdentifierList() {
			return getRuleContext(OrderedIdentifierListContext.class,0);
		}
		public BucketSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_bucketSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBucketSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBucketSpec(this);
		}
	}

	public final BucketSpecContext bucketSpec() throws RecognitionException {
		BucketSpecContext _localctx = new BucketSpecContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_bucketSpec);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1220);
			match(CLUSTERED);
			setState(1221);
			match(BY);
			setState(1222);
			identifierList();
			setState(1226);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SORTED) {
				{
				setState(1223);
				match(SORTED);
				setState(1224);
				match(BY);
				setState(1225);
				orderedIdentifierList();
				}
			}

			setState(1228);
			match(INTO);
			setState(1229);
			match(INTEGER_VALUE);
			setState(1230);
			match(BUCKETS);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SkewSpecContext extends ParserRuleContext {
		public TerminalNode SKEWED() { return getToken(SqlBaseParser.SKEWED, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public ConstantListContext constantList() {
			return getRuleContext(ConstantListContext.class,0);
		}
		public NestedConstantListContext nestedConstantList() {
			return getRuleContext(NestedConstantListContext.class,0);
		}
		public TerminalNode STORED() { return getToken(SqlBaseParser.STORED, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode DIRECTORIES() { return getToken(SqlBaseParser.DIRECTORIES, 0); }
		public SkewSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_skewSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSkewSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSkewSpec(this);
		}
	}

	public final SkewSpecContext skewSpec() throws RecognitionException {
		SkewSpecContext _localctx = new SkewSpecContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_skewSpec);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1232);
			match(SKEWED);
			setState(1233);
			match(BY);
			setState(1234);
			identifierList();
			setState(1235);
			match(ON);
			setState(1238);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,124,_ctx) ) {
			case 1:
				{
				setState(1236);
				constantList();
				}
				break;
			case 2:
				{
				setState(1237);
				nestedConstantList();
				}
				break;
			}
			setState(1243);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,125,_ctx) ) {
			case 1:
				{
				setState(1240);
				match(STORED);
				setState(1241);
				match(AS);
				setState(1242);
				match(DIRECTORIES);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LocationSpecContext extends ParserRuleContext {
		public TerminalNode LOCATION() { return getToken(SqlBaseParser.LOCATION, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public LocationSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_locationSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLocationSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLocationSpec(this);
		}
	}

	public final LocationSpecContext locationSpec() throws RecognitionException {
		LocationSpecContext _localctx = new LocationSpecContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_locationSpec);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1245);
			match(LOCATION);
			setState(1246);
			match(STRING);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CommentSpecContext extends ParserRuleContext {
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public CommentSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_commentSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCommentSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCommentSpec(this);
		}
	}

	public final CommentSpecContext commentSpec() throws RecognitionException {
		CommentSpecContext _localctx = new CommentSpecContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_commentSpec);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1248);
			match(COMMENT);
			setState(1249);
			match(STRING);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryContext extends ParserRuleContext {
		public QueryTermContext queryTerm() {
			return getRuleContext(QueryTermContext.class,0);
		}
		public QueryOrganizationContext queryOrganization() {
			return getRuleContext(QueryOrganizationContext.class,0);
		}
		public CtesContext ctes() {
			return getRuleContext(CtesContext.class,0);
		}
		public QueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_query; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuery(this);
		}
	}

	public final QueryContext query() throws RecognitionException {
		QueryContext _localctx = new QueryContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_query);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1252);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITH) {
				{
				setState(1251);
				ctes();
				}
			}

			setState(1254);
			queryTerm(0);
			setState(1255);
			queryOrganization();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class InsertIntoContext extends ParserRuleContext {
		public InsertIntoContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insertInto; }
	 
		public InsertIntoContext() { }
		public void copyFrom(InsertIntoContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class InsertOverwriteHiveDirContext extends InsertIntoContext {
		public Token path;
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlBaseParser.OVERWRITE, 0); }
		public TerminalNode DIRECTORY() { return getToken(SqlBaseParser.DIRECTORY, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode LOCAL() { return getToken(SqlBaseParser.LOCAL, 0); }
		public RowFormatContext rowFormat() {
			return getRuleContext(RowFormatContext.class,0);
		}
		public CreateFileFormatContext createFileFormat() {
			return getRuleContext(CreateFileFormatContext.class,0);
		}
		public InsertOverwriteHiveDirContext(InsertIntoContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInsertOverwriteHiveDir(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInsertOverwriteHiveDir(this);
		}
	}
	public static class InsertOverwriteDirContext extends InsertIntoContext {
		public Token path;
		public TablePropertyListContext options;
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlBaseParser.OVERWRITE, 0); }
		public TerminalNode DIRECTORY() { return getToken(SqlBaseParser.DIRECTORY, 0); }
		public TableProviderContext tableProvider() {
			return getRuleContext(TableProviderContext.class,0);
		}
		public TerminalNode LOCAL() { return getToken(SqlBaseParser.LOCAL, 0); }
		public TerminalNode OPTIONS() { return getToken(SqlBaseParser.OPTIONS, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public InsertOverwriteDirContext(InsertIntoContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInsertOverwriteDir(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInsertOverwriteDir(this);
		}
	}
	public static class InsertOverwriteTableContext extends InsertIntoContext {
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlBaseParser.OVERWRITE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public InsertOverwriteTableContext(InsertIntoContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInsertOverwriteTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInsertOverwriteTable(this);
		}
	}
	public static class InsertIntoTableContext extends InsertIntoContext {
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public InsertIntoTableContext(InsertIntoContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInsertIntoTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInsertIntoTable(this);
		}
	}

	public final InsertIntoContext insertInto() throws RecognitionException {
		InsertIntoContext _localctx = new InsertIntoContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_insertInto);
		int _la;
		try {
			setState(1312);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,139,_ctx) ) {
			case 1:
				_localctx = new InsertOverwriteTableContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1257);
				match(INSERT);
				setState(1258);
				match(OVERWRITE);
				setState(1260);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,127,_ctx) ) {
				case 1:
					{
					setState(1259);
					match(TABLE);
					}
					break;
				}
				setState(1262);
				multipartIdentifier();
				setState(1269);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1263);
					partitionSpec();
					setState(1267);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==IF) {
						{
						setState(1264);
						match(IF);
						setState(1265);
						match(NOT);
						setState(1266);
						match(EXISTS);
						}
					}

					}
				}

				}
				break;
			case 2:
				_localctx = new InsertIntoTableContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1271);
				match(INSERT);
				setState(1272);
				match(INTO);
				setState(1274);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,130,_ctx) ) {
				case 1:
					{
					setState(1273);
					match(TABLE);
					}
					break;
				}
				setState(1276);
				multipartIdentifier();
				setState(1278);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1277);
					partitionSpec();
					}
				}

				setState(1283);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(1280);
					match(IF);
					setState(1281);
					match(NOT);
					setState(1282);
					match(EXISTS);
					}
				}

				}
				break;
			case 3:
				_localctx = new InsertOverwriteHiveDirContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1285);
				match(INSERT);
				setState(1286);
				match(OVERWRITE);
				setState(1288);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LOCAL) {
					{
					setState(1287);
					match(LOCAL);
					}
				}

				setState(1290);
				match(DIRECTORY);
				setState(1291);
				((InsertOverwriteHiveDirContext)_localctx).path = match(STRING);
				setState(1293);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ROW) {
					{
					setState(1292);
					rowFormat();
					}
				}

				setState(1296);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==STORED) {
					{
					setState(1295);
					createFileFormat();
					}
				}

				}
				break;
			case 4:
				_localctx = new InsertOverwriteDirContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1298);
				match(INSERT);
				setState(1299);
				match(OVERWRITE);
				setState(1301);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LOCAL) {
					{
					setState(1300);
					match(LOCAL);
					}
				}

				setState(1303);
				match(DIRECTORY);
				setState(1305);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==STRING) {
					{
					setState(1304);
					((InsertOverwriteDirContext)_localctx).path = match(STRING);
					}
				}

				setState(1307);
				tableProvider();
				setState(1310);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPTIONS) {
					{
					setState(1308);
					match(OPTIONS);
					setState(1309);
					((InsertOverwriteDirContext)_localctx).options = tablePropertyList();
					}
				}

				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PartitionSpecLocationContext extends ParserRuleContext {
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public PartitionSpecLocationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partitionSpecLocation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPartitionSpecLocation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPartitionSpecLocation(this);
		}
	}

	public final PartitionSpecLocationContext partitionSpecLocation() throws RecognitionException {
		PartitionSpecLocationContext _localctx = new PartitionSpecLocationContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_partitionSpecLocation);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1314);
			partitionSpec();
			setState(1316);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LOCATION) {
				{
				setState(1315);
				locationSpec();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PartitionSpecContext extends ParserRuleContext {
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public List<PartitionValContext> partitionVal() {
			return getRuleContexts(PartitionValContext.class);
		}
		public PartitionValContext partitionVal(int i) {
			return getRuleContext(PartitionValContext.class,i);
		}
		public PartitionSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partitionSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPartitionSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPartitionSpec(this);
		}
	}

	public final PartitionSpecContext partitionSpec() throws RecognitionException {
		PartitionSpecContext _localctx = new PartitionSpecContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_partitionSpec);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1318);
			match(PARTITION);
			setState(1319);
			match(T__1);
			setState(1320);
			partitionVal();
			setState(1325);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(1321);
				match(T__3);
				setState(1322);
				partitionVal();
				}
				}
				setState(1327);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1328);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PartitionValContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public PartitionValContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partitionVal; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPartitionVal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPartitionVal(this);
		}
	}

	public final PartitionValContext partitionVal() throws RecognitionException {
		PartitionValContext _localctx = new PartitionValContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_partitionVal);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1330);
			identifier();
			setState(1333);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==EQ) {
				{
				setState(1331);
				match(EQ);
				setState(1332);
				constant();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NamespaceContext extends ParserRuleContext {
		public TerminalNode NAMESPACE() { return getToken(SqlBaseParser.NAMESPACE, 0); }
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public NamespaceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namespace; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNamespace(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNamespace(this);
		}
	}

	public final NamespaceContext namespace() throws RecognitionException {
		NamespaceContext _localctx = new NamespaceContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_namespace);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1335);
			_la = _input.LA(1);
			if ( !(_la==DATABASE || _la==NAMESPACE || _la==SCHEMA) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DescribeFuncNameContext extends ParserRuleContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ComparisonOperatorContext comparisonOperator() {
			return getRuleContext(ComparisonOperatorContext.class,0);
		}
		public ArithmeticOperatorContext arithmeticOperator() {
			return getRuleContext(ArithmeticOperatorContext.class,0);
		}
		public PredicateOperatorContext predicateOperator() {
			return getRuleContext(PredicateOperatorContext.class,0);
		}
		public DescribeFuncNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_describeFuncName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescribeFuncName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescribeFuncName(this);
		}
	}

	public final DescribeFuncNameContext describeFuncName() throws RecognitionException {
		DescribeFuncNameContext _localctx = new DescribeFuncNameContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_describeFuncName);
		try {
			setState(1342);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,143,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1337);
				qualifiedName();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1338);
				match(STRING);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1339);
				comparisonOperator();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1340);
				arithmeticOperator();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(1341);
				predicateOperator();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DescribeColNameContext extends ParserRuleContext {
		public IdentifierContext identifier;
		public List<IdentifierContext> nameParts = new ArrayList<IdentifierContext>();
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public DescribeColNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_describeColName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescribeColName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescribeColName(this);
		}
	}

	public final DescribeColNameContext describeColName() throws RecognitionException {
		DescribeColNameContext _localctx = new DescribeColNameContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_describeColName);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1344);
			((DescribeColNameContext)_localctx).identifier = identifier();
			((DescribeColNameContext)_localctx).nameParts.add(((DescribeColNameContext)_localctx).identifier);
			setState(1349);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__4) {
				{
				{
				setState(1345);
				match(T__4);
				setState(1346);
				((DescribeColNameContext)_localctx).identifier = identifier();
				((DescribeColNameContext)_localctx).nameParts.add(((DescribeColNameContext)_localctx).identifier);
				}
				}
				setState(1351);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CtesContext extends ParserRuleContext {
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public List<NamedQueryContext> namedQuery() {
			return getRuleContexts(NamedQueryContext.class);
		}
		public NamedQueryContext namedQuery(int i) {
			return getRuleContext(NamedQueryContext.class,i);
		}
		public CtesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ctes; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCtes(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCtes(this);
		}
	}

	public final CtesContext ctes() throws RecognitionException {
		CtesContext _localctx = new CtesContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_ctes);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1352);
			match(WITH);
			setState(1353);
			namedQuery();
			setState(1358);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(1354);
				match(T__3);
				setState(1355);
				namedQuery();
				}
				}
				setState(1360);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NamedQueryContext extends ParserRuleContext {
		public ErrorCapturingIdentifierContext name;
		public IdentifierListContext columnAliases;
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public ErrorCapturingIdentifierContext errorCapturingIdentifier() {
			return getRuleContext(ErrorCapturingIdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public NamedQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedQuery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNamedQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNamedQuery(this);
		}
	}

	public final NamedQueryContext namedQuery() throws RecognitionException {
		NamedQueryContext _localctx = new NamedQueryContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_namedQuery);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1361);
			((NamedQueryContext)_localctx).name = errorCapturingIdentifier();
			setState(1363);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,146,_ctx) ) {
			case 1:
				{
				setState(1362);
				((NamedQueryContext)_localctx).columnAliases = identifierList();
				}
				break;
			}
			setState(1366);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AS) {
				{
				setState(1365);
				match(AS);
				}
			}

			setState(1368);
			match(T__1);
			setState(1369);
			query();
			setState(1370);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TableProviderContext extends ParserRuleContext {
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TableProviderContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableProvider; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableProvider(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableProvider(this);
		}
	}

	public final TableProviderContext tableProvider() throws RecognitionException {
		TableProviderContext _localctx = new TableProviderContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_tableProvider);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1372);
			match(USING);
			setState(1373);
			multipartIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CreateTableClausesContext extends ParserRuleContext {
		public TablePropertyListContext options;
		public TransformListContext partitioning;
		public TablePropertyListContext tableProps;
		public List<BucketSpecContext> bucketSpec() {
			return getRuleContexts(BucketSpecContext.class);
		}
		public BucketSpecContext bucketSpec(int i) {
			return getRuleContext(BucketSpecContext.class,i);
		}
		public List<LocationSpecContext> locationSpec() {
			return getRuleContexts(LocationSpecContext.class);
		}
		public LocationSpecContext locationSpec(int i) {
			return getRuleContext(LocationSpecContext.class,i);
		}
		public List<CommentSpecContext> commentSpec() {
			return getRuleContexts(CommentSpecContext.class);
		}
		public CommentSpecContext commentSpec(int i) {
			return getRuleContext(CommentSpecContext.class,i);
		}
		public List<TerminalNode> OPTIONS() { return getTokens(SqlBaseParser.OPTIONS); }
		public TerminalNode OPTIONS(int i) {
			return getToken(SqlBaseParser.OPTIONS, i);
		}
		public List<TerminalNode> PARTITIONED() { return getTokens(SqlBaseParser.PARTITIONED); }
		public TerminalNode PARTITIONED(int i) {
			return getToken(SqlBaseParser.PARTITIONED, i);
		}
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public List<TerminalNode> TBLPROPERTIES() { return getTokens(SqlBaseParser.TBLPROPERTIES); }
		public TerminalNode TBLPROPERTIES(int i) {
			return getToken(SqlBaseParser.TBLPROPERTIES, i);
		}
		public List<TablePropertyListContext> tablePropertyList() {
			return getRuleContexts(TablePropertyListContext.class);
		}
		public TablePropertyListContext tablePropertyList(int i) {
			return getRuleContext(TablePropertyListContext.class,i);
		}
		public List<TransformListContext> transformList() {
			return getRuleContexts(TransformListContext.class);
		}
		public TransformListContext transformList(int i) {
			return getRuleContext(TransformListContext.class,i);
		}
		public CreateTableClausesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createTableClauses; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateTableClauses(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateTableClauses(this);
		}
	}

	public final CreateTableClausesContext createTableClauses() throws RecognitionException {
		CreateTableClausesContext _localctx = new CreateTableClausesContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_createTableClauses);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1387);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==CLUSTERED || _la==COMMENT || ((((_la - 138)) & ~0x3f) == 0 && ((1L << (_la - 138)) & ((1L << (LOCATION - 138)) | (1L << (OPTIONS - 138)) | (1L << (PARTITIONED - 138)))) != 0) || _la==TBLPROPERTIES) {
				{
				setState(1385);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case OPTIONS:
					{
					{
					setState(1375);
					match(OPTIONS);
					setState(1376);
					((CreateTableClausesContext)_localctx).options = tablePropertyList();
					}
					}
					break;
				case PARTITIONED:
					{
					{
					setState(1377);
					match(PARTITIONED);
					setState(1378);
					match(BY);
					setState(1379);
					((CreateTableClausesContext)_localctx).partitioning = transformList();
					}
					}
					break;
				case CLUSTERED:
					{
					setState(1380);
					bucketSpec();
					}
					break;
				case LOCATION:
					{
					setState(1381);
					locationSpec();
					}
					break;
				case COMMENT:
					{
					setState(1382);
					commentSpec();
					}
					break;
				case TBLPROPERTIES:
					{
					{
					setState(1383);
					match(TBLPROPERTIES);
					setState(1384);
					((CreateTableClausesContext)_localctx).tableProps = tablePropertyList();
					}
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				setState(1389);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TablePropertyListContext extends ParserRuleContext {
		public List<TablePropertyContext> tableProperty() {
			return getRuleContexts(TablePropertyContext.class);
		}
		public TablePropertyContext tableProperty(int i) {
			return getRuleContext(TablePropertyContext.class,i);
		}
		public TablePropertyListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tablePropertyList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTablePropertyList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTablePropertyList(this);
		}
	}

	public final TablePropertyListContext tablePropertyList() throws RecognitionException {
		TablePropertyListContext _localctx = new TablePropertyListContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_tablePropertyList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1390);
			match(T__1);
			setState(1391);
			tableProperty();
			setState(1396);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(1392);
				match(T__3);
				setState(1393);
				tableProperty();
				}
				}
				setState(1398);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1399);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TablePropertyContext extends ParserRuleContext {
		public TablePropertyKeyContext key;
		public TablePropertyValueContext value;
		public TablePropertyKeyContext tablePropertyKey() {
			return getRuleContext(TablePropertyKeyContext.class,0);
		}
		public TablePropertyValueContext tablePropertyValue() {
			return getRuleContext(TablePropertyValueContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public TablePropertyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableProperty; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableProperty(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableProperty(this);
		}
	}

	public final TablePropertyContext tableProperty() throws RecognitionException {
		TablePropertyContext _localctx = new TablePropertyContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_tableProperty);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1401);
			((TablePropertyContext)_localctx).key = tablePropertyKey();
			setState(1406);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==FALSE || ((((_la - 241)) & ~0x3f) == 0 && ((1L << (_la - 241)) & ((1L << (TRUE - 241)) | (1L << (EQ - 241)) | (1L << (STRING - 241)) | (1L << (INTEGER_VALUE - 241)) | (1L << (DECIMAL_VALUE - 241)))) != 0)) {
				{
				setState(1403);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==EQ) {
					{
					setState(1402);
					match(EQ);
					}
				}

				setState(1405);
				((TablePropertyContext)_localctx).value = tablePropertyValue();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TablePropertyKeyContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TablePropertyKeyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tablePropertyKey; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTablePropertyKey(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTablePropertyKey(this);
		}
	}

	public final TablePropertyKeyContext tablePropertyKey() throws RecognitionException {
		TablePropertyKeyContext _localctx = new TablePropertyKeyContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_tablePropertyKey);
		int _la;
		try {
			setState(1417);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,154,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1408);
				identifier();
				setState(1413);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__4) {
					{
					{
					setState(1409);
					match(T__4);
					setState(1410);
					identifier();
					}
					}
					setState(1415);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1416);
				match(STRING);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TablePropertyValueContext extends ParserRuleContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public BooleanValueContext booleanValue() {
			return getRuleContext(BooleanValueContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TablePropertyValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tablePropertyValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTablePropertyValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTablePropertyValue(this);
		}
	}

	public final TablePropertyValueContext tablePropertyValue() throws RecognitionException {
		TablePropertyValueContext _localctx = new TablePropertyValueContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_tablePropertyValue);
		try {
			setState(1423);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INTEGER_VALUE:
				enterOuterAlt(_localctx, 1);
				{
				setState(1419);
				match(INTEGER_VALUE);
				}
				break;
			case DECIMAL_VALUE:
				enterOuterAlt(_localctx, 2);
				{
				setState(1420);
				match(DECIMAL_VALUE);
				}
				break;
			case FALSE:
			case TRUE:
				enterOuterAlt(_localctx, 3);
				{
				setState(1421);
				booleanValue();
				}
				break;
			case STRING:
				enterOuterAlt(_localctx, 4);
				{
				setState(1422);
				match(STRING);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ConstantListContext extends ParserRuleContext {
		public List<ConstantContext> constant() {
			return getRuleContexts(ConstantContext.class);
		}
		public ConstantContext constant(int i) {
			return getRuleContext(ConstantContext.class,i);
		}
		public ConstantListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constantList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterConstantList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitConstantList(this);
		}
	}

	public final ConstantListContext constantList() throws RecognitionException {
		ConstantListContext _localctx = new ConstantListContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_constantList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1425);
			match(T__1);
			setState(1426);
			constant();
			setState(1431);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(1427);
				match(T__3);
				setState(1428);
				constant();
				}
				}
				setState(1433);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1434);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NestedConstantListContext extends ParserRuleContext {
		public List<ConstantListContext> constantList() {
			return getRuleContexts(ConstantListContext.class);
		}
		public ConstantListContext constantList(int i) {
			return getRuleContext(ConstantListContext.class,i);
		}
		public NestedConstantListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nestedConstantList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNestedConstantList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNestedConstantList(this);
		}
	}

	public final NestedConstantListContext nestedConstantList() throws RecognitionException {
		NestedConstantListContext _localctx = new NestedConstantListContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_nestedConstantList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1436);
			match(T__1);
			setState(1437);
			constantList();
			setState(1442);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(1438);
				match(T__3);
				setState(1439);
				constantList();
				}
				}
				setState(1444);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1445);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CreateFileFormatContext extends ParserRuleContext {
		public TerminalNode STORED() { return getToken(SqlBaseParser.STORED, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public FileFormatContext fileFormat() {
			return getRuleContext(FileFormatContext.class,0);
		}
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public StorageHandlerContext storageHandler() {
			return getRuleContext(StorageHandlerContext.class,0);
		}
		public CreateFileFormatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createFileFormat; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateFileFormat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateFileFormat(this);
		}
	}

	public final CreateFileFormatContext createFileFormat() throws RecognitionException {
		CreateFileFormatContext _localctx = new CreateFileFormatContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_createFileFormat);
		try {
			setState(1453);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,158,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1447);
				match(STORED);
				setState(1448);
				match(AS);
				setState(1449);
				fileFormat();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1450);
				match(STORED);
				setState(1451);
				match(BY);
				setState(1452);
				storageHandler();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FileFormatContext extends ParserRuleContext {
		public FileFormatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fileFormat; }
	 
		public FileFormatContext() { }
		public void copyFrom(FileFormatContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class TableFileFormatContext extends FileFormatContext {
		public Token inFmt;
		public Token outFmt;
		public TerminalNode INPUTFORMAT() { return getToken(SqlBaseParser.INPUTFORMAT, 0); }
		public TerminalNode OUTPUTFORMAT() { return getToken(SqlBaseParser.OUTPUTFORMAT, 0); }
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public TableFileFormatContext(FileFormatContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableFileFormat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableFileFormat(this);
		}
	}
	public static class GenericFileFormatContext extends FileFormatContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public GenericFileFormatContext(FileFormatContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGenericFileFormat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGenericFileFormat(this);
		}
	}

	public final FileFormatContext fileFormat() throws RecognitionException {
		FileFormatContext _localctx = new FileFormatContext(_ctx, getState());
		enterRule(_localctx, 68, RULE_fileFormat);
		try {
			setState(1460);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,159,_ctx) ) {
			case 1:
				_localctx = new TableFileFormatContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1455);
				match(INPUTFORMAT);
				setState(1456);
				((TableFileFormatContext)_localctx).inFmt = match(STRING);
				setState(1457);
				match(OUTPUTFORMAT);
				setState(1458);
				((TableFileFormatContext)_localctx).outFmt = match(STRING);
				}
				break;
			case 2:
				_localctx = new GenericFileFormatContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1459);
				identifier();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StorageHandlerContext extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlBaseParser.SERDEPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public StorageHandlerContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_storageHandler; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStorageHandler(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStorageHandler(this);
		}
	}

	public final StorageHandlerContext storageHandler() throws RecognitionException {
		StorageHandlerContext _localctx = new StorageHandlerContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_storageHandler);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1462);
			match(STRING);
			setState(1466);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,160,_ctx) ) {
			case 1:
				{
				setState(1463);
				match(WITH);
				setState(1464);
				match(SERDEPROPERTIES);
				setState(1465);
				tablePropertyList();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ResourceContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ResourceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_resource; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterResource(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitResource(this);
		}
	}

	public final ResourceContext resource() throws RecognitionException {
		ResourceContext _localctx = new ResourceContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_resource);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1468);
			identifier();
			setState(1469);
			match(STRING);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DmlStatementNoWithContext extends ParserRuleContext {
		public DmlStatementNoWithContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dmlStatementNoWith; }
	 
		public DmlStatementNoWithContext() { }
		public void copyFrom(DmlStatementNoWithContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class DeleteFromTableContext extends DmlStatementNoWithContext {
		public TerminalNode DELETE() { return getToken(SqlBaseParser.DELETE, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TableAliasContext tableAlias() {
			return getRuleContext(TableAliasContext.class,0);
		}
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public DeleteFromTableContext(DmlStatementNoWithContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDeleteFromTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDeleteFromTable(this);
		}
	}
	public static class SingleInsertQueryContext extends DmlStatementNoWithContext {
		public InsertIntoContext insertInto() {
			return getRuleContext(InsertIntoContext.class,0);
		}
		public QueryTermContext queryTerm() {
			return getRuleContext(QueryTermContext.class,0);
		}
		public QueryOrganizationContext queryOrganization() {
			return getRuleContext(QueryOrganizationContext.class,0);
		}
		public SingleInsertQueryContext(DmlStatementNoWithContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleInsertQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleInsertQuery(this);
		}
	}
	public static class MultiInsertQueryContext extends DmlStatementNoWithContext {
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
		}
		public List<MultiInsertQueryBodyContext> multiInsertQueryBody() {
			return getRuleContexts(MultiInsertQueryBodyContext.class);
		}
		public MultiInsertQueryBodyContext multiInsertQueryBody(int i) {
			return getRuleContext(MultiInsertQueryBodyContext.class,i);
		}
		public MultiInsertQueryContext(DmlStatementNoWithContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMultiInsertQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMultiInsertQuery(this);
		}
	}
	public static class UpdateTableContext extends DmlStatementNoWithContext {
		public TerminalNode UPDATE() { return getToken(SqlBaseParser.UPDATE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TableAliasContext tableAlias() {
			return getRuleContext(TableAliasContext.class,0);
		}
		public SetClauseContext setClause() {
			return getRuleContext(SetClauseContext.class,0);
		}
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public UpdateTableContext(DmlStatementNoWithContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUpdateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUpdateTable(this);
		}
	}
	public static class MergeIntoTableContext extends DmlStatementNoWithContext {
		public MultipartIdentifierContext target;
		public TableAliasContext targetAlias;
		public MultipartIdentifierContext source;
		public QueryContext sourceQuery;
		public TableAliasContext sourceAlias;
		public BooleanExpressionContext mergeCondition;
		public TerminalNode MERGE() { return getToken(SqlBaseParser.MERGE, 0); }
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public List<MultipartIdentifierContext> multipartIdentifier() {
			return getRuleContexts(MultipartIdentifierContext.class);
		}
		public MultipartIdentifierContext multipartIdentifier(int i) {
			return getRuleContext(MultipartIdentifierContext.class,i);
		}
		public List<TableAliasContext> tableAlias() {
			return getRuleContexts(TableAliasContext.class);
		}
		public TableAliasContext tableAlias(int i) {
			return getRuleContext(TableAliasContext.class,i);
		}
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public List<MatchedClauseContext> matchedClause() {
			return getRuleContexts(MatchedClauseContext.class);
		}
		public MatchedClauseContext matchedClause(int i) {
			return getRuleContext(MatchedClauseContext.class,i);
		}
		public List<NotMatchedClauseContext> notMatchedClause() {
			return getRuleContexts(NotMatchedClauseContext.class);
		}
		public NotMatchedClauseContext notMatchedClause(int i) {
			return getRuleContext(NotMatchedClauseContext.class,i);
		}
		public MergeIntoTableContext(DmlStatementNoWithContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMergeIntoTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMergeIntoTable(this);
		}
	}

	public final DmlStatementNoWithContext dmlStatementNoWith() throws RecognitionException {
		DmlStatementNoWithContext _localctx = new DmlStatementNoWithContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_dmlStatementNoWith);
		int _la;
		try {
			int _alt;
			setState(1522);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INSERT:
				_localctx = new SingleInsertQueryContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1471);
				insertInto();
				setState(1472);
				queryTerm(0);
				setState(1473);
				queryOrganization();
				}
				break;
			case FROM:
				_localctx = new MultiInsertQueryContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1475);
				fromClause();
				setState(1477); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1476);
					multiInsertQueryBody();
					}
					}
					setState(1479); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==INSERT );
				}
				break;
			case DELETE:
				_localctx = new DeleteFromTableContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1481);
				match(DELETE);
				setState(1482);
				match(FROM);
				setState(1483);
				multipartIdentifier();
				setState(1484);
				tableAlias();
				setState(1486);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WHERE) {
					{
					setState(1485);
					whereClause();
					}
				}

				}
				break;
			case UPDATE:
				_localctx = new UpdateTableContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1488);
				match(UPDATE);
				setState(1489);
				multipartIdentifier();
				setState(1490);
				tableAlias();
				setState(1491);
				setClause();
				setState(1493);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WHERE) {
					{
					setState(1492);
					whereClause();
					}
				}

				}
				break;
			case MERGE:
				_localctx = new MergeIntoTableContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(1495);
				match(MERGE);
				setState(1496);
				match(INTO);
				setState(1497);
				((MergeIntoTableContext)_localctx).target = multipartIdentifier();
				setState(1498);
				((MergeIntoTableContext)_localctx).targetAlias = tableAlias();
				setState(1499);
				match(USING);
				setState(1505);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,164,_ctx) ) {
				case 1:
					{
					setState(1500);
					((MergeIntoTableContext)_localctx).source = multipartIdentifier();
					}
					break;
				case 2:
					{
					setState(1501);
					match(T__1);
					setState(1502);
					((MergeIntoTableContext)_localctx).sourceQuery = query();
					setState(1503);
					match(T__2);
					}
					break;
				}
				setState(1507);
				((MergeIntoTableContext)_localctx).sourceAlias = tableAlias();
				setState(1508);
				match(ON);
				setState(1509);
				((MergeIntoTableContext)_localctx).mergeCondition = booleanExpression(0);
				setState(1513);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,165,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1510);
						matchedClause();
						}
						} 
					}
					setState(1515);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,165,_ctx);
				}
				setState(1519);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==WHEN) {
					{
					{
					setState(1516);
					notMatchedClause();
					}
					}
					setState(1521);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryOrganizationContext extends ParserRuleContext {
		public SortItemContext sortItem;
		public List<SortItemContext> order = new ArrayList<SortItemContext>();
		public ExpressionContext expression;
		public List<ExpressionContext> clusterBy = new ArrayList<ExpressionContext>();
		public List<ExpressionContext> distributeBy = new ArrayList<ExpressionContext>();
		public List<SortItemContext> sort = new ArrayList<SortItemContext>();
		public ExpressionContext limit;
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public TerminalNode CLUSTER() { return getToken(SqlBaseParser.CLUSTER, 0); }
		public TerminalNode DISTRIBUTE() { return getToken(SqlBaseParser.DISTRIBUTE, 0); }
		public TerminalNode SORT() { return getToken(SqlBaseParser.SORT, 0); }
		public WindowClauseContext windowClause() {
			return getRuleContext(WindowClauseContext.class,0);
		}
		public TerminalNode LIMIT() { return getToken(SqlBaseParser.LIMIT, 0); }
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public QueryOrganizationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryOrganization; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryOrganization(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryOrganization(this);
		}
	}

	public final QueryOrganizationContext queryOrganization() throws RecognitionException {
		QueryOrganizationContext _localctx = new QueryOrganizationContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_queryOrganization);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1534);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,169,_ctx) ) {
			case 1:
				{
				setState(1524);
				match(ORDER);
				setState(1525);
				match(BY);
				setState(1526);
				((QueryOrganizationContext)_localctx).sortItem = sortItem();
				((QueryOrganizationContext)_localctx).order.add(((QueryOrganizationContext)_localctx).sortItem);
				setState(1531);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,168,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1527);
						match(T__3);
						setState(1528);
						((QueryOrganizationContext)_localctx).sortItem = sortItem();
						((QueryOrganizationContext)_localctx).order.add(((QueryOrganizationContext)_localctx).sortItem);
						}
						} 
					}
					setState(1533);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,168,_ctx);
				}
				}
				break;
			}
			setState(1546);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,171,_ctx) ) {
			case 1:
				{
				setState(1536);
				match(CLUSTER);
				setState(1537);
				match(BY);
				setState(1538);
				((QueryOrganizationContext)_localctx).expression = expression();
				((QueryOrganizationContext)_localctx).clusterBy.add(((QueryOrganizationContext)_localctx).expression);
				setState(1543);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,170,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1539);
						match(T__3);
						setState(1540);
						((QueryOrganizationContext)_localctx).expression = expression();
						((QueryOrganizationContext)_localctx).clusterBy.add(((QueryOrganizationContext)_localctx).expression);
						}
						} 
					}
					setState(1545);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,170,_ctx);
				}
				}
				break;
			}
			setState(1558);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,173,_ctx) ) {
			case 1:
				{
				setState(1548);
				match(DISTRIBUTE);
				setState(1549);
				match(BY);
				setState(1550);
				((QueryOrganizationContext)_localctx).expression = expression();
				((QueryOrganizationContext)_localctx).distributeBy.add(((QueryOrganizationContext)_localctx).expression);
				setState(1555);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,172,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1551);
						match(T__3);
						setState(1552);
						((QueryOrganizationContext)_localctx).expression = expression();
						((QueryOrganizationContext)_localctx).distributeBy.add(((QueryOrganizationContext)_localctx).expression);
						}
						} 
					}
					setState(1557);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,172,_ctx);
				}
				}
				break;
			}
			setState(1570);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,175,_ctx) ) {
			case 1:
				{
				setState(1560);
				match(SORT);
				setState(1561);
				match(BY);
				setState(1562);
				((QueryOrganizationContext)_localctx).sortItem = sortItem();
				((QueryOrganizationContext)_localctx).sort.add(((QueryOrganizationContext)_localctx).sortItem);
				setState(1567);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,174,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1563);
						match(T__3);
						setState(1564);
						((QueryOrganizationContext)_localctx).sortItem = sortItem();
						((QueryOrganizationContext)_localctx).sort.add(((QueryOrganizationContext)_localctx).sortItem);
						}
						} 
					}
					setState(1569);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,174,_ctx);
				}
				}
				break;
			}
			setState(1573);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,176,_ctx) ) {
			case 1:
				{
				setState(1572);
				windowClause();
				}
				break;
			}
			setState(1580);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,178,_ctx) ) {
			case 1:
				{
				setState(1575);
				match(LIMIT);
				setState(1578);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,177,_ctx) ) {
				case 1:
					{
					setState(1576);
					match(ALL);
					}
					break;
				case 2:
					{
					setState(1577);
					((QueryOrganizationContext)_localctx).limit = expression();
					}
					break;
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MultiInsertQueryBodyContext extends ParserRuleContext {
		public InsertIntoContext insertInto() {
			return getRuleContext(InsertIntoContext.class,0);
		}
		public FromStatementBodyContext fromStatementBody() {
			return getRuleContext(FromStatementBodyContext.class,0);
		}
		public MultiInsertQueryBodyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multiInsertQueryBody; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMultiInsertQueryBody(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMultiInsertQueryBody(this);
		}
	}

	public final MultiInsertQueryBodyContext multiInsertQueryBody() throws RecognitionException {
		MultiInsertQueryBodyContext _localctx = new MultiInsertQueryBodyContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_multiInsertQueryBody);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1582);
			insertInto();
			setState(1583);
			fromStatementBody();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryTermContext extends ParserRuleContext {
		public QueryTermContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryTerm; }
	 
		public QueryTermContext() { }
		public void copyFrom(QueryTermContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class QueryTermDefaultContext extends QueryTermContext {
		public QueryPrimaryContext queryPrimary() {
			return getRuleContext(QueryPrimaryContext.class,0);
		}
		public QueryTermDefaultContext(QueryTermContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryTermDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryTermDefault(this);
		}
	}
	public static class SetOperationContext extends QueryTermContext {
		public QueryTermContext left;
		public Token operator;
		public QueryTermContext right;
		public List<QueryTermContext> queryTerm() {
			return getRuleContexts(QueryTermContext.class);
		}
		public QueryTermContext queryTerm(int i) {
			return getRuleContext(QueryTermContext.class,i);
		}
		public TerminalNode INTERSECT() { return getToken(SqlBaseParser.INTERSECT, 0); }
		public TerminalNode UNION() { return getToken(SqlBaseParser.UNION, 0); }
		public TerminalNode EXCEPT() { return getToken(SqlBaseParser.EXCEPT, 0); }
		public TerminalNode SETMINUS() { return getToken(SqlBaseParser.SETMINUS, 0); }
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public SetOperationContext(QueryTermContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetOperation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetOperation(this);
		}
	}

	public final QueryTermContext queryTerm() throws RecognitionException {
		return queryTerm(0);
	}

	private QueryTermContext queryTerm(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		QueryTermContext _localctx = new QueryTermContext(_ctx, _parentState);
		QueryTermContext _prevctx = _localctx;
		int _startState = 80;
		enterRecursionRule(_localctx, 80, RULE_queryTerm, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			_localctx = new QueryTermDefaultContext(_localctx);
			_ctx = _localctx;
			_prevctx = _localctx;

			setState(1586);
			queryPrimary();
			}
			_ctx.stop = _input.LT(-1);
			setState(1611);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,183,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(1609);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,182,_ctx) ) {
					case 1:
						{
						_localctx = new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
						((SetOperationContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
						setState(1588);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(1589);
						if (!(self.legacy_setops_precedence_enbled)) throw new FailedPredicateException(this, "self.legacy_setops_precedence_enbled");
						setState(1590);
						((SetOperationContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==EXCEPT || _la==INTERSECT || _la==SETMINUS || _la==UNION) ) {
							((SetOperationContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(1592);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ALL || _la==DISTINCT) {
							{
							setState(1591);
							setQuantifier();
							}
						}

						setState(1594);
						((SetOperationContext)_localctx).right = queryTerm(4);
						}
						break;
					case 2:
						{
						_localctx = new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
						((SetOperationContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
						setState(1595);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(1596);
						if (!(not self.legacy_setops_precedence_enbled)) throw new FailedPredicateException(this, "not self.legacy_setops_precedence_enbled");
						setState(1597);
						((SetOperationContext)_localctx).operator = match(INTERSECT);
						setState(1599);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ALL || _la==DISTINCT) {
							{
							setState(1598);
							setQuantifier();
							}
						}

						setState(1601);
						((SetOperationContext)_localctx).right = queryTerm(3);
						}
						break;
					case 3:
						{
						_localctx = new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
						((SetOperationContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
						setState(1602);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(1603);
						if (!(not self.legacy_setops_precedence_enbled)) throw new FailedPredicateException(this, "not self.legacy_setops_precedence_enbled");
						setState(1604);
						((SetOperationContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==EXCEPT || _la==SETMINUS || _la==UNION) ) {
							((SetOperationContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(1606);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ALL || _la==DISTINCT) {
							{
							setState(1605);
							setQuantifier();
							}
						}

						setState(1608);
						((SetOperationContext)_localctx).right = queryTerm(2);
						}
						break;
					}
					} 
				}
				setState(1613);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,183,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class QueryPrimaryContext extends ParserRuleContext {
		public QueryPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryPrimary; }
	 
		public QueryPrimaryContext() { }
		public void copyFrom(QueryPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class SubqueryContext extends QueryPrimaryContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public SubqueryContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubquery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubquery(this);
		}
	}
	public static class QueryPrimaryDefaultContext extends QueryPrimaryContext {
		public QuerySpecificationContext querySpecification() {
			return getRuleContext(QuerySpecificationContext.class,0);
		}
		public QueryPrimaryDefaultContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryPrimaryDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryPrimaryDefault(this);
		}
	}
	public static class InlineTableDefault1Context extends QueryPrimaryContext {
		public InlineTableContext inlineTable() {
			return getRuleContext(InlineTableContext.class,0);
		}
		public InlineTableDefault1Context(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInlineTableDefault1(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInlineTableDefault1(this);
		}
	}
	public static class FromStmtContext extends QueryPrimaryContext {
		public FromStatementContext fromStatement() {
			return getRuleContext(FromStatementContext.class,0);
		}
		public FromStmtContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFromStmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFromStmt(this);
		}
	}
	public static class TableContext extends QueryPrimaryContext {
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TableContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTable(this);
		}
	}

	public final QueryPrimaryContext queryPrimary() throws RecognitionException {
		QueryPrimaryContext _localctx = new QueryPrimaryContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_queryPrimary);
		try {
			setState(1623);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case MAP:
			case REDUCE:
			case SELECT:
				_localctx = new QueryPrimaryDefaultContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1614);
				querySpecification();
				}
				break;
			case FROM:
				_localctx = new FromStmtContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1615);
				fromStatement();
				}
				break;
			case TABLE:
				_localctx = new TableContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1616);
				match(TABLE);
				setState(1617);
				multipartIdentifier();
				}
				break;
			case VALUES:
				_localctx = new InlineTableDefault1Context(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1618);
				inlineTable();
				}
				break;
			case T__1:
				_localctx = new SubqueryContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(1619);
				match(T__1);
				setState(1620);
				query();
				setState(1621);
				match(T__2);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SortItemContext extends ParserRuleContext {
		public Token ordering;
		public Token nullOrder;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public SortItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sortItem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSortItem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSortItem(this);
		}
	}

	public final SortItemContext sortItem() throws RecognitionException {
		SortItemContext _localctx = new SortItemContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_sortItem);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1625);
			expression();
			setState(1627);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,185,_ctx) ) {
			case 1:
				{
				setState(1626);
				((SortItemContext)_localctx).ordering = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
					((SortItemContext)_localctx).ordering = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			}
			setState(1631);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,186,_ctx) ) {
			case 1:
				{
				setState(1629);
				match(NULLS);
				setState(1630);
				((SortItemContext)_localctx).nullOrder = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==FIRST || _la==LAST) ) {
					((SortItemContext)_localctx).nullOrder = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FromStatementContext extends ParserRuleContext {
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
		}
		public List<FromStatementBodyContext> fromStatementBody() {
			return getRuleContexts(FromStatementBodyContext.class);
		}
		public FromStatementBodyContext fromStatementBody(int i) {
			return getRuleContext(FromStatementBodyContext.class,i);
		}
		public FromStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fromStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFromStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFromStatement(this);
		}
	}

	public final FromStatementContext fromStatement() throws RecognitionException {
		FromStatementContext _localctx = new FromStatementContext(_ctx, getState());
		enterRule(_localctx, 86, RULE_fromStatement);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1633);
			fromClause();
			setState(1635); 
			_errHandler.sync(this);
			_alt = 1;
			do {
				switch (_alt) {
				case 1:
					{
					{
					setState(1634);
					fromStatementBody();
					}
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(1637); 
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,187,_ctx);
			} while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FromStatementBodyContext extends ParserRuleContext {
		public TransformClauseContext transformClause() {
			return getRuleContext(TransformClauseContext.class,0);
		}
		public QueryOrganizationContext queryOrganization() {
			return getRuleContext(QueryOrganizationContext.class,0);
		}
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public SelectClauseContext selectClause() {
			return getRuleContext(SelectClauseContext.class,0);
		}
		public List<LateralViewContext> lateralView() {
			return getRuleContexts(LateralViewContext.class);
		}
		public LateralViewContext lateralView(int i) {
			return getRuleContext(LateralViewContext.class,i);
		}
		public AggregationClauseContext aggregationClause() {
			return getRuleContext(AggregationClauseContext.class,0);
		}
		public HavingClauseContext havingClause() {
			return getRuleContext(HavingClauseContext.class,0);
		}
		public WindowClauseContext windowClause() {
			return getRuleContext(WindowClauseContext.class,0);
		}
		public FromStatementBodyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fromStatementBody; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFromStatementBody(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFromStatementBody(this);
		}
	}

	public final FromStatementBodyContext fromStatementBody() throws RecognitionException {
		FromStatementBodyContext _localctx = new FromStatementBodyContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_fromStatementBody);
		try {
			int _alt;
			setState(1666);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,194,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1639);
				transformClause();
				setState(1641);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,188,_ctx) ) {
				case 1:
					{
					setState(1640);
					whereClause();
					}
					break;
				}
				setState(1643);
				queryOrganization();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1645);
				selectClause();
				setState(1649);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,189,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1646);
						lateralView();
						}
						} 
					}
					setState(1651);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,189,_ctx);
				}
				setState(1653);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,190,_ctx) ) {
				case 1:
					{
					setState(1652);
					whereClause();
					}
					break;
				}
				setState(1656);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,191,_ctx) ) {
				case 1:
					{
					setState(1655);
					aggregationClause();
					}
					break;
				}
				setState(1659);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,192,_ctx) ) {
				case 1:
					{
					setState(1658);
					havingClause();
					}
					break;
				}
				setState(1662);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,193,_ctx) ) {
				case 1:
					{
					setState(1661);
					windowClause();
					}
					break;
				}
				setState(1664);
				queryOrganization();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QuerySpecificationContext extends ParserRuleContext {
		public QuerySpecificationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_querySpecification; }
	 
		public QuerySpecificationContext() { }
		public void copyFrom(QuerySpecificationContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class RegularQuerySpecificationContext extends QuerySpecificationContext {
		public SelectClauseContext selectClause() {
			return getRuleContext(SelectClauseContext.class,0);
		}
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
		}
		public List<LateralViewContext> lateralView() {
			return getRuleContexts(LateralViewContext.class);
		}
		public LateralViewContext lateralView(int i) {
			return getRuleContext(LateralViewContext.class,i);
		}
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public AggregationClauseContext aggregationClause() {
			return getRuleContext(AggregationClauseContext.class,0);
		}
		public HavingClauseContext havingClause() {
			return getRuleContext(HavingClauseContext.class,0);
		}
		public WindowClauseContext windowClause() {
			return getRuleContext(WindowClauseContext.class,0);
		}
		public RegularQuerySpecificationContext(QuerySpecificationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRegularQuerySpecification(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRegularQuerySpecification(this);
		}
	}
	public static class TransformQuerySpecificationContext extends QuerySpecificationContext {
		public TransformClauseContext transformClause() {
			return getRuleContext(TransformClauseContext.class,0);
		}
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
		}
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public TransformQuerySpecificationContext(QuerySpecificationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTransformQuerySpecification(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTransformQuerySpecification(this);
		}
	}

	public final QuerySpecificationContext querySpecification() throws RecognitionException {
		QuerySpecificationContext _localctx = new QuerySpecificationContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_querySpecification);
		try {
			int _alt;
			setState(1697);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,203,_ctx) ) {
			case 1:
				_localctx = new TransformQuerySpecificationContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1668);
				transformClause();
				setState(1670);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,195,_ctx) ) {
				case 1:
					{
					setState(1669);
					fromClause();
					}
					break;
				}
				setState(1673);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,196,_ctx) ) {
				case 1:
					{
					setState(1672);
					whereClause();
					}
					break;
				}
				}
				break;
			case 2:
				_localctx = new RegularQuerySpecificationContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1675);
				selectClause();
				setState(1677);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,197,_ctx) ) {
				case 1:
					{
					setState(1676);
					fromClause();
					}
					break;
				}
				setState(1682);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,198,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1679);
						lateralView();
						}
						} 
					}
					setState(1684);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,198,_ctx);
				}
				setState(1686);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,199,_ctx) ) {
				case 1:
					{
					setState(1685);
					whereClause();
					}
					break;
				}
				setState(1689);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,200,_ctx) ) {
				case 1:
					{
					setState(1688);
					aggregationClause();
					}
					break;
				}
				setState(1692);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,201,_ctx) ) {
				case 1:
					{
					setState(1691);
					havingClause();
					}
					break;
				}
				setState(1695);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,202,_ctx) ) {
				case 1:
					{
					setState(1694);
					windowClause();
					}
					break;
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TransformClauseContext extends ParserRuleContext {
		public Token kind;
		public RowFormatContext inRowFormat;
		public Token recordWriter;
		public Token script;
		public RowFormatContext outRowFormat;
		public Token recordReader;
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public TerminalNode SELECT() { return getToken(SqlBaseParser.SELECT, 0); }
		public NamedExpressionSeqContext namedExpressionSeq() {
			return getRuleContext(NamedExpressionSeqContext.class,0);
		}
		public TerminalNode TRANSFORM() { return getToken(SqlBaseParser.TRANSFORM, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode REDUCE() { return getToken(SqlBaseParser.REDUCE, 0); }
		public TerminalNode RECORDWRITER() { return getToken(SqlBaseParser.RECORDWRITER, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode RECORDREADER() { return getToken(SqlBaseParser.RECORDREADER, 0); }
		public List<RowFormatContext> rowFormat() {
			return getRuleContexts(RowFormatContext.class);
		}
		public RowFormatContext rowFormat(int i) {
			return getRuleContext(RowFormatContext.class,i);
		}
		public IdentifierSeqContext identifierSeq() {
			return getRuleContext(IdentifierSeqContext.class,0);
		}
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public TransformClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_transformClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTransformClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTransformClause(this);
		}
	}

	public final TransformClauseContext transformClause() throws RecognitionException {
		TransformClauseContext _localctx = new TransformClauseContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_transformClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1709);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SELECT:
				{
				setState(1699);
				match(SELECT);
				setState(1700);
				((TransformClauseContext)_localctx).kind = match(TRANSFORM);
				setState(1701);
				match(T__1);
				setState(1702);
				namedExpressionSeq();
				setState(1703);
				match(T__2);
				}
				break;
			case MAP:
				{
				setState(1705);
				((TransformClauseContext)_localctx).kind = match(MAP);
				setState(1706);
				namedExpressionSeq();
				}
				break;
			case REDUCE:
				{
				setState(1707);
				((TransformClauseContext)_localctx).kind = match(REDUCE);
				setState(1708);
				namedExpressionSeq();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(1712);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ROW) {
				{
				setState(1711);
				((TransformClauseContext)_localctx).inRowFormat = rowFormat();
				}
			}

			setState(1716);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==RECORDWRITER) {
				{
				setState(1714);
				match(RECORDWRITER);
				setState(1715);
				((TransformClauseContext)_localctx).recordWriter = match(STRING);
				}
			}

			setState(1718);
			match(USING);
			setState(1719);
			((TransformClauseContext)_localctx).script = match(STRING);
			setState(1732);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,209,_ctx) ) {
			case 1:
				{
				setState(1720);
				match(AS);
				setState(1730);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,208,_ctx) ) {
				case 1:
					{
					setState(1721);
					identifierSeq();
					}
					break;
				case 2:
					{
					setState(1722);
					colTypeList();
					}
					break;
				case 3:
					{
					{
					setState(1723);
					match(T__1);
					setState(1726);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,207,_ctx) ) {
					case 1:
						{
						setState(1724);
						identifierSeq();
						}
						break;
					case 2:
						{
						setState(1725);
						colTypeList();
						}
						break;
					}
					setState(1728);
					match(T__2);
					}
					}
					break;
				}
				}
				break;
			}
			setState(1735);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,210,_ctx) ) {
			case 1:
				{
				setState(1734);
				((TransformClauseContext)_localctx).outRowFormat = rowFormat();
				}
				break;
			}
			setState(1739);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,211,_ctx) ) {
			case 1:
				{
				setState(1737);
				match(RECORDREADER);
				setState(1738);
				((TransformClauseContext)_localctx).recordReader = match(STRING);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SelectClauseContext extends ParserRuleContext {
		public HintContext hint;
		public List<HintContext> hints = new ArrayList<HintContext>();
		public TerminalNode SELECT() { return getToken(SqlBaseParser.SELECT, 0); }
		public NamedExpressionSeqContext namedExpressionSeq() {
			return getRuleContext(NamedExpressionSeqContext.class,0);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public List<HintContext> hint() {
			return getRuleContexts(HintContext.class);
		}
		public HintContext hint(int i) {
			return getRuleContext(HintContext.class,i);
		}
		public SelectClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selectClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSelectClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSelectClause(this);
		}
	}

	public final SelectClauseContext selectClause() throws RecognitionException {
		SelectClauseContext _localctx = new SelectClauseContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_selectClause);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1741);
			match(SELECT);
			setState(1745);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,212,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1742);
					((SelectClauseContext)_localctx).hint = hint();
					((SelectClauseContext)_localctx).hints.add(((SelectClauseContext)_localctx).hint);
					}
					} 
				}
				setState(1747);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,212,_ctx);
			}
			setState(1749);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,213,_ctx) ) {
			case 1:
				{
				setState(1748);
				setQuantifier();
				}
				break;
			}
			setState(1751);
			namedExpressionSeq();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SetClauseContext extends ParserRuleContext {
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public AssignmentListContext assignmentList() {
			return getRuleContext(AssignmentListContext.class,0);
		}
		public SetClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetClause(this);
		}
	}

	public final SetClauseContext setClause() throws RecognitionException {
		SetClauseContext _localctx = new SetClauseContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_setClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1753);
			match(SET);
			setState(1754);
			assignmentList();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MatchedClauseContext extends ParserRuleContext {
		public BooleanExpressionContext matchedCond;
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode MATCHED() { return getToken(SqlBaseParser.MATCHED, 0); }
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public MatchedActionContext matchedAction() {
			return getRuleContext(MatchedActionContext.class,0);
		}
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public MatchedClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_matchedClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMatchedClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMatchedClause(this);
		}
	}

	public final MatchedClauseContext matchedClause() throws RecognitionException {
		MatchedClauseContext _localctx = new MatchedClauseContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_matchedClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1756);
			match(WHEN);
			setState(1757);
			match(MATCHED);
			setState(1760);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AND) {
				{
				setState(1758);
				match(AND);
				setState(1759);
				((MatchedClauseContext)_localctx).matchedCond = booleanExpression(0);
				}
			}

			setState(1762);
			match(THEN);
			setState(1763);
			matchedAction();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NotMatchedClauseContext extends ParserRuleContext {
		public BooleanExpressionContext notMatchedCond;
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode MATCHED() { return getToken(SqlBaseParser.MATCHED, 0); }
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public NotMatchedActionContext notMatchedAction() {
			return getRuleContext(NotMatchedActionContext.class,0);
		}
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public NotMatchedClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_notMatchedClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNotMatchedClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNotMatchedClause(this);
		}
	}

	public final NotMatchedClauseContext notMatchedClause() throws RecognitionException {
		NotMatchedClauseContext _localctx = new NotMatchedClauseContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_notMatchedClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1765);
			match(WHEN);
			setState(1766);
			match(NOT);
			setState(1767);
			match(MATCHED);
			setState(1770);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AND) {
				{
				setState(1768);
				match(AND);
				setState(1769);
				((NotMatchedClauseContext)_localctx).notMatchedCond = booleanExpression(0);
				}
			}

			setState(1772);
			match(THEN);
			setState(1773);
			notMatchedAction();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MatchedActionContext extends ParserRuleContext {
		public TerminalNode DELETE() { return getToken(SqlBaseParser.DELETE, 0); }
		public TerminalNode UPDATE() { return getToken(SqlBaseParser.UPDATE, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public AssignmentListContext assignmentList() {
			return getRuleContext(AssignmentListContext.class,0);
		}
		public MatchedActionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_matchedAction; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMatchedAction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMatchedAction(this);
		}
	}

	public final MatchedActionContext matchedAction() throws RecognitionException {
		MatchedActionContext _localctx = new MatchedActionContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_matchedAction);
		try {
			setState(1782);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,216,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1775);
				match(DELETE);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1776);
				match(UPDATE);
				setState(1777);
				match(SET);
				setState(1778);
				match(ASTERISK);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1779);
				match(UPDATE);
				setState(1780);
				match(SET);
				setState(1781);
				assignmentList();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NotMatchedActionContext extends ParserRuleContext {
		public MultipartIdentifierListContext columns;
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public TerminalNode VALUES() { return getToken(SqlBaseParser.VALUES, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public MultipartIdentifierListContext multipartIdentifierList() {
			return getRuleContext(MultipartIdentifierListContext.class,0);
		}
		public NotMatchedActionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_notMatchedAction; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNotMatchedAction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNotMatchedAction(this);
		}
	}

	public final NotMatchedActionContext notMatchedAction() throws RecognitionException {
		NotMatchedActionContext _localctx = new NotMatchedActionContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_notMatchedAction);
		int _la;
		try {
			setState(1802);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,218,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1784);
				match(INSERT);
				setState(1785);
				match(ASTERISK);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1786);
				match(INSERT);
				setState(1787);
				match(T__1);
				setState(1788);
				((NotMatchedActionContext)_localctx).columns = multipartIdentifierList();
				setState(1789);
				match(T__2);
				setState(1790);
				match(VALUES);
				setState(1791);
				match(T__1);
				setState(1792);
				expression();
				setState(1797);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1793);
					match(T__3);
					setState(1794);
					expression();
					}
					}
					setState(1799);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1800);
				match(T__2);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AssignmentListContext extends ParserRuleContext {
		public List<AssignmentContext> assignment() {
			return getRuleContexts(AssignmentContext.class);
		}
		public AssignmentContext assignment(int i) {
			return getRuleContext(AssignmentContext.class,i);
		}
		public AssignmentListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_assignmentList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAssignmentList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAssignmentList(this);
		}
	}

	public final AssignmentListContext assignmentList() throws RecognitionException {
		AssignmentListContext _localctx = new AssignmentListContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_assignmentList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1804);
			assignment();
			setState(1809);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(1805);
				match(T__3);
				setState(1806);
				assignment();
				}
				}
				setState(1811);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AssignmentContext extends ParserRuleContext {
		public MultipartIdentifierContext key;
		public ExpressionContext value;
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public AssignmentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_assignment; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAssignment(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAssignment(this);
		}
	}

	public final AssignmentContext assignment() throws RecognitionException {
		AssignmentContext _localctx = new AssignmentContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_assignment);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1812);
			((AssignmentContext)_localctx).key = multipartIdentifier();
			setState(1813);
			match(EQ);
			setState(1814);
			((AssignmentContext)_localctx).value = expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WhereClauseContext extends ParserRuleContext {
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public WhereClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_whereClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWhereClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWhereClause(this);
		}
	}

	public final WhereClauseContext whereClause() throws RecognitionException {
		WhereClauseContext _localctx = new WhereClauseContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_whereClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1816);
			match(WHERE);
			setState(1817);
			booleanExpression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class HavingClauseContext extends ParserRuleContext {
		public TerminalNode HAVING() { return getToken(SqlBaseParser.HAVING, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public HavingClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_havingClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterHavingClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitHavingClause(this);
		}
	}

	public final HavingClauseContext havingClause() throws RecognitionException {
		HavingClauseContext _localctx = new HavingClauseContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_havingClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1819);
			match(HAVING);
			setState(1820);
			booleanExpression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class HintContext extends ParserRuleContext {
		public HintStatementContext hintStatement;
		public List<HintStatementContext> hintStatements = new ArrayList<HintStatementContext>();
		public List<HintStatementContext> hintStatement() {
			return getRuleContexts(HintStatementContext.class);
		}
		public HintStatementContext hintStatement(int i) {
			return getRuleContext(HintStatementContext.class,i);
		}
		public HintContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_hint; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterHint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitHint(this);
		}
	}

	public final HintContext hint() throws RecognitionException {
		HintContext _localctx = new HintContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_hint);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1822);
			match(T__5);
			setState(1823);
			((HintContext)_localctx).hintStatement = hintStatement();
			((HintContext)_localctx).hintStatements.add(((HintContext)_localctx).hintStatement);
			setState(1830);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,221,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1825);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,220,_ctx) ) {
					case 1:
						{
						setState(1824);
						match(T__3);
						}
						break;
					}
					setState(1827);
					((HintContext)_localctx).hintStatement = hintStatement();
					((HintContext)_localctx).hintStatements.add(((HintContext)_localctx).hintStatement);
					}
					} 
				}
				setState(1832);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,221,_ctx);
			}
			setState(1833);
			match(T__6);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class HintStatementContext extends ParserRuleContext {
		public IdentifierContext hintName;
		public PrimaryExpressionContext primaryExpression;
		public List<PrimaryExpressionContext> parameters = new ArrayList<PrimaryExpressionContext>();
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<PrimaryExpressionContext> primaryExpression() {
			return getRuleContexts(PrimaryExpressionContext.class);
		}
		public PrimaryExpressionContext primaryExpression(int i) {
			return getRuleContext(PrimaryExpressionContext.class,i);
		}
		public HintStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_hintStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterHintStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitHintStatement(this);
		}
	}

	public final HintStatementContext hintStatement() throws RecognitionException {
		HintStatementContext _localctx = new HintStatementContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_hintStatement);
		int _la;
		try {
			setState(1848);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,223,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1835);
				((HintStatementContext)_localctx).hintName = identifier();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1836);
				((HintStatementContext)_localctx).hintName = identifier();
				setState(1837);
				match(T__1);
				setState(1838);
				((HintStatementContext)_localctx).primaryExpression = primaryExpression(0);
				((HintStatementContext)_localctx).parameters.add(((HintStatementContext)_localctx).primaryExpression);
				setState(1843);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1839);
					match(T__3);
					setState(1840);
					((HintStatementContext)_localctx).primaryExpression = primaryExpression(0);
					((HintStatementContext)_localctx).parameters.add(((HintStatementContext)_localctx).primaryExpression);
					}
					}
					setState(1845);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1846);
				match(T__2);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FromClauseContext extends ParserRuleContext {
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public List<RelationContext> relation() {
			return getRuleContexts(RelationContext.class);
		}
		public RelationContext relation(int i) {
			return getRuleContext(RelationContext.class,i);
		}
		public List<LateralViewContext> lateralView() {
			return getRuleContexts(LateralViewContext.class);
		}
		public LateralViewContext lateralView(int i) {
			return getRuleContext(LateralViewContext.class,i);
		}
		public PivotClauseContext pivotClause() {
			return getRuleContext(PivotClauseContext.class,0);
		}
		public FromClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fromClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFromClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFromClause(this);
		}
	}

	public final FromClauseContext fromClause() throws RecognitionException {
		FromClauseContext _localctx = new FromClauseContext(_ctx, getState());
		enterRule(_localctx, 118, RULE_fromClause);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1850);
			match(FROM);
			setState(1851);
			relation();
			setState(1856);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,224,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1852);
					match(T__3);
					setState(1853);
					relation();
					}
					} 
				}
				setState(1858);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,224,_ctx);
			}
			setState(1862);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,225,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1859);
					lateralView();
					}
					} 
				}
				setState(1864);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,225,_ctx);
			}
			setState(1866);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,226,_ctx) ) {
			case 1:
				{
				setState(1865);
				pivotClause();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AggregationClauseContext extends ParserRuleContext {
		public ExpressionContext expression;
		public List<ExpressionContext> groupingExpressions = new ArrayList<ExpressionContext>();
		public Token kind;
		public TerminalNode GROUP() { return getToken(SqlBaseParser.GROUP, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode SETS() { return getToken(SqlBaseParser.SETS, 0); }
		public List<GroupingSetContext> groupingSet() {
			return getRuleContexts(GroupingSetContext.class);
		}
		public GroupingSetContext groupingSet(int i) {
			return getRuleContext(GroupingSetContext.class,i);
		}
		public TerminalNode ROLLUP() { return getToken(SqlBaseParser.ROLLUP, 0); }
		public TerminalNode CUBE() { return getToken(SqlBaseParser.CUBE, 0); }
		public TerminalNode GROUPING() { return getToken(SqlBaseParser.GROUPING, 0); }
		public AggregationClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aggregationClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAggregationClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAggregationClause(this);
		}
	}

	public final AggregationClauseContext aggregationClause() throws RecognitionException {
		AggregationClauseContext _localctx = new AggregationClauseContext(_ctx, getState());
		enterRule(_localctx, 120, RULE_aggregationClause);
		int _la;
		try {
			int _alt;
			setState(1912);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,231,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1868);
				match(GROUP);
				setState(1869);
				match(BY);
				setState(1870);
				((AggregationClauseContext)_localctx).expression = expression();
				((AggregationClauseContext)_localctx).groupingExpressions.add(((AggregationClauseContext)_localctx).expression);
				setState(1875);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,227,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1871);
						match(T__3);
						setState(1872);
						((AggregationClauseContext)_localctx).expression = expression();
						((AggregationClauseContext)_localctx).groupingExpressions.add(((AggregationClauseContext)_localctx).expression);
						}
						} 
					}
					setState(1877);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,227,_ctx);
				}
				setState(1895);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,229,_ctx) ) {
				case 1:
					{
					setState(1878);
					match(WITH);
					setState(1879);
					((AggregationClauseContext)_localctx).kind = match(ROLLUP);
					}
					break;
				case 2:
					{
					setState(1880);
					match(WITH);
					setState(1881);
					((AggregationClauseContext)_localctx).kind = match(CUBE);
					}
					break;
				case 3:
					{
					setState(1882);
					((AggregationClauseContext)_localctx).kind = match(GROUPING);
					setState(1883);
					match(SETS);
					setState(1884);
					match(T__1);
					setState(1885);
					groupingSet();
					setState(1890);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1886);
						match(T__3);
						setState(1887);
						groupingSet();
						}
						}
						setState(1892);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(1893);
					match(T__2);
					}
					break;
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1897);
				match(GROUP);
				setState(1898);
				match(BY);
				setState(1899);
				((AggregationClauseContext)_localctx).kind = match(GROUPING);
				setState(1900);
				match(SETS);
				setState(1901);
				match(T__1);
				setState(1902);
				groupingSet();
				setState(1907);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1903);
					match(T__3);
					setState(1904);
					groupingSet();
					}
					}
					setState(1909);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1910);
				match(T__2);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GroupingSetContext extends ParserRuleContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public GroupingSetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupingSet; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGroupingSet(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGroupingSet(this);
		}
	}

	public final GroupingSetContext groupingSet() throws RecognitionException {
		GroupingSetContext _localctx = new GroupingSetContext(_ctx, getState());
		enterRule(_localctx, 122, RULE_groupingSet);
		int _la;
		try {
			setState(1927);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,234,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1914);
				match(T__1);
				setState(1923);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,233,_ctx) ) {
				case 1:
					{
					setState(1915);
					expression();
					setState(1920);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1916);
						match(T__3);
						setState(1917);
						expression();
						}
						}
						setState(1922);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
					break;
				}
				setState(1925);
				match(T__2);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1926);
				expression();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PivotClauseContext extends ParserRuleContext {
		public NamedExpressionSeqContext aggregates;
		public PivotValueContext pivotValue;
		public List<PivotValueContext> pivotValues = new ArrayList<PivotValueContext>();
		public TerminalNode PIVOT() { return getToken(SqlBaseParser.PIVOT, 0); }
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public PivotColumnContext pivotColumn() {
			return getRuleContext(PivotColumnContext.class,0);
		}
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public NamedExpressionSeqContext namedExpressionSeq() {
			return getRuleContext(NamedExpressionSeqContext.class,0);
		}
		public List<PivotValueContext> pivotValue() {
			return getRuleContexts(PivotValueContext.class);
		}
		public PivotValueContext pivotValue(int i) {
			return getRuleContext(PivotValueContext.class,i);
		}
		public PivotClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pivotClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPivotClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPivotClause(this);
		}
	}

	public final PivotClauseContext pivotClause() throws RecognitionException {
		PivotClauseContext _localctx = new PivotClauseContext(_ctx, getState());
		enterRule(_localctx, 124, RULE_pivotClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1929);
			match(PIVOT);
			setState(1930);
			match(T__1);
			setState(1931);
			((PivotClauseContext)_localctx).aggregates = namedExpressionSeq();
			setState(1932);
			match(FOR);
			setState(1933);
			pivotColumn();
			setState(1934);
			match(IN);
			setState(1935);
			match(T__1);
			setState(1936);
			((PivotClauseContext)_localctx).pivotValue = pivotValue();
			((PivotClauseContext)_localctx).pivotValues.add(((PivotClauseContext)_localctx).pivotValue);
			setState(1941);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(1937);
				match(T__3);
				setState(1938);
				((PivotClauseContext)_localctx).pivotValue = pivotValue();
				((PivotClauseContext)_localctx).pivotValues.add(((PivotClauseContext)_localctx).pivotValue);
				}
				}
				setState(1943);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1944);
			match(T__2);
			setState(1945);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PivotColumnContext extends ParserRuleContext {
		public IdentifierContext identifier;
		public List<IdentifierContext> identifiers = new ArrayList<IdentifierContext>();
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public PivotColumnContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pivotColumn; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPivotColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPivotColumn(this);
		}
	}

	public final PivotColumnContext pivotColumn() throws RecognitionException {
		PivotColumnContext _localctx = new PivotColumnContext(_ctx, getState());
		enterRule(_localctx, 126, RULE_pivotColumn);
		int _la;
		try {
			setState(1959);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,237,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1947);
				((PivotColumnContext)_localctx).identifier = identifier();
				((PivotColumnContext)_localctx).identifiers.add(((PivotColumnContext)_localctx).identifier);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1948);
				match(T__1);
				setState(1949);
				((PivotColumnContext)_localctx).identifier = identifier();
				((PivotColumnContext)_localctx).identifiers.add(((PivotColumnContext)_localctx).identifier);
				setState(1954);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1950);
					match(T__3);
					setState(1951);
					((PivotColumnContext)_localctx).identifier = identifier();
					((PivotColumnContext)_localctx).identifiers.add(((PivotColumnContext)_localctx).identifier);
					}
					}
					setState(1956);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1957);
				match(T__2);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PivotValueContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public PivotValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pivotValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPivotValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPivotValue(this);
		}
	}

	public final PivotValueContext pivotValue() throws RecognitionException {
		PivotValueContext _localctx = new PivotValueContext(_ctx, getState());
		enterRule(_localctx, 128, RULE_pivotValue);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1961);
			expression();
			setState(1966);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,239,_ctx) ) {
			case 1:
				{
				setState(1963);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,238,_ctx) ) {
				case 1:
					{
					setState(1962);
					match(AS);
					}
					break;
				}
				setState(1965);
				identifier();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LateralViewContext extends ParserRuleContext {
		public IdentifierContext tblName;
		public IdentifierContext identifier;
		public List<IdentifierContext> colName = new ArrayList<IdentifierContext>();
		public TerminalNode LATERAL() { return getToken(SqlBaseParser.LATERAL, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode OUTER() { return getToken(SqlBaseParser.OUTER, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public LateralViewContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_lateralView; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLateralView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLateralView(this);
		}
	}

	public final LateralViewContext lateralView() throws RecognitionException {
		LateralViewContext _localctx = new LateralViewContext(_ctx, getState());
		enterRule(_localctx, 130, RULE_lateralView);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1968);
			match(LATERAL);
			setState(1969);
			match(VIEW);
			setState(1971);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,240,_ctx) ) {
			case 1:
				{
				setState(1970);
				match(OUTER);
				}
				break;
			}
			setState(1973);
			qualifiedName();
			setState(1974);
			match(T__1);
			setState(1983);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,242,_ctx) ) {
			case 1:
				{
				setState(1975);
				expression();
				setState(1980);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1976);
					match(T__3);
					setState(1977);
					expression();
					}
					}
					setState(1982);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			}
			setState(1985);
			match(T__2);
			setState(1986);
			((LateralViewContext)_localctx).tblName = identifier();
			setState(1998);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,245,_ctx) ) {
			case 1:
				{
				setState(1988);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,243,_ctx) ) {
				case 1:
					{
					setState(1987);
					match(AS);
					}
					break;
				}
				setState(1990);
				((LateralViewContext)_localctx).identifier = identifier();
				((LateralViewContext)_localctx).colName.add(((LateralViewContext)_localctx).identifier);
				setState(1995);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,244,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1991);
						match(T__3);
						setState(1992);
						((LateralViewContext)_localctx).identifier = identifier();
						((LateralViewContext)_localctx).colName.add(((LateralViewContext)_localctx).identifier);
						}
						} 
					}
					setState(1997);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,244,_ctx);
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SetQuantifierContext extends ParserRuleContext {
		public TerminalNode DISTINCT() { return getToken(SqlBaseParser.DISTINCT, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public SetQuantifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setQuantifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetQuantifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetQuantifier(this);
		}
	}

	public final SetQuantifierContext setQuantifier() throws RecognitionException {
		SetQuantifierContext _localctx = new SetQuantifierContext(_ctx, getState());
		enterRule(_localctx, 132, RULE_setQuantifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2000);
			_la = _input.LA(1);
			if ( !(_la==ALL || _la==DISTINCT) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RelationContext extends ParserRuleContext {
		public RelationPrimaryContext relationPrimary() {
			return getRuleContext(RelationPrimaryContext.class,0);
		}
		public List<JoinRelationContext> joinRelation() {
			return getRuleContexts(JoinRelationContext.class);
		}
		public JoinRelationContext joinRelation(int i) {
			return getRuleContext(JoinRelationContext.class,i);
		}
		public RelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRelation(this);
		}
	}

	public final RelationContext relation() throws RecognitionException {
		RelationContext _localctx = new RelationContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_relation);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2002);
			relationPrimary();
			setState(2006);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,246,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2003);
					joinRelation();
					}
					} 
				}
				setState(2008);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,246,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinRelationContext extends ParserRuleContext {
		public RelationPrimaryContext right;
		public TerminalNode JOIN() { return getToken(SqlBaseParser.JOIN, 0); }
		public RelationPrimaryContext relationPrimary() {
			return getRuleContext(RelationPrimaryContext.class,0);
		}
		public JoinTypeContext joinType() {
			return getRuleContext(JoinTypeContext.class,0);
		}
		public JoinCriteriaContext joinCriteria() {
			return getRuleContext(JoinCriteriaContext.class,0);
		}
		public TerminalNode NATURAL() { return getToken(SqlBaseParser.NATURAL, 0); }
		public JoinRelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinRelation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinRelation(this);
		}
	}

	public final JoinRelationContext joinRelation() throws RecognitionException {
		JoinRelationContext _localctx = new JoinRelationContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_joinRelation);
		try {
			setState(2020);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ANTI:
			case CROSS:
			case FULL:
			case INNER:
			case JOIN:
			case LEFT:
			case RIGHT:
			case SEMI:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(2009);
				joinType();
				}
				setState(2010);
				match(JOIN);
				setState(2011);
				((JoinRelationContext)_localctx).right = relationPrimary();
				setState(2013);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,247,_ctx) ) {
				case 1:
					{
					setState(2012);
					joinCriteria();
					}
					break;
				}
				}
				break;
			case NATURAL:
				enterOuterAlt(_localctx, 2);
				{
				setState(2015);
				match(NATURAL);
				setState(2016);
				joinType();
				setState(2017);
				match(JOIN);
				setState(2018);
				((JoinRelationContext)_localctx).right = relationPrimary();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinTypeContext extends ParserRuleContext {
		public TerminalNode INNER() { return getToken(SqlBaseParser.INNER, 0); }
		public TerminalNode CROSS() { return getToken(SqlBaseParser.CROSS, 0); }
		public TerminalNode LEFT() { return getToken(SqlBaseParser.LEFT, 0); }
		public TerminalNode OUTER() { return getToken(SqlBaseParser.OUTER, 0); }
		public TerminalNode SEMI() { return getToken(SqlBaseParser.SEMI, 0); }
		public TerminalNode RIGHT() { return getToken(SqlBaseParser.RIGHT, 0); }
		public TerminalNode FULL() { return getToken(SqlBaseParser.FULL, 0); }
		public TerminalNode ANTI() { return getToken(SqlBaseParser.ANTI, 0); }
		public JoinTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinType(this);
		}
	}

	public final JoinTypeContext joinType() throws RecognitionException {
		JoinTypeContext _localctx = new JoinTypeContext(_ctx, getState());
		enterRule(_localctx, 138, RULE_joinType);
		int _la;
		try {
			setState(2046);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,255,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2023);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INNER) {
					{
					setState(2022);
					match(INNER);
					}
				}

				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2025);
				match(CROSS);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2026);
				match(LEFT);
				setState(2028);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(2027);
					match(OUTER);
					}
				}

				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(2031);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LEFT) {
					{
					setState(2030);
					match(LEFT);
					}
				}

				setState(2033);
				match(SEMI);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(2034);
				match(RIGHT);
				setState(2036);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(2035);
					match(OUTER);
					}
				}

				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(2038);
				match(FULL);
				setState(2040);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(2039);
					match(OUTER);
					}
				}

				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(2043);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LEFT) {
					{
					setState(2042);
					match(LEFT);
					}
				}

				setState(2045);
				match(ANTI);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinCriteriaContext extends ParserRuleContext {
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public JoinCriteriaContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinCriteria; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinCriteria(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinCriteria(this);
		}
	}

	public final JoinCriteriaContext joinCriteria() throws RecognitionException {
		JoinCriteriaContext _localctx = new JoinCriteriaContext(_ctx, getState());
		enterRule(_localctx, 140, RULE_joinCriteria);
		try {
			setState(2052);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ON:
				enterOuterAlt(_localctx, 1);
				{
				setState(2048);
				match(ON);
				setState(2049);
				booleanExpression(0);
				}
				break;
			case USING:
				enterOuterAlt(_localctx, 2);
				{
				setState(2050);
				match(USING);
				setState(2051);
				identifierList();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SampleContext extends ParserRuleContext {
		public TerminalNode TABLESAMPLE() { return getToken(SqlBaseParser.TABLESAMPLE, 0); }
		public SampleMethodContext sampleMethod() {
			return getRuleContext(SampleMethodContext.class,0);
		}
		public SampleContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sample; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSample(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSample(this);
		}
	}

	public final SampleContext sample() throws RecognitionException {
		SampleContext _localctx = new SampleContext(_ctx, getState());
		enterRule(_localctx, 142, RULE_sample);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2054);
			match(TABLESAMPLE);
			setState(2055);
			match(T__1);
			setState(2057);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,257,_ctx) ) {
			case 1:
				{
				setState(2056);
				sampleMethod();
				}
				break;
			}
			setState(2059);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SampleMethodContext extends ParserRuleContext {
		public SampleMethodContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sampleMethod; }
	 
		public SampleMethodContext() { }
		public void copyFrom(SampleMethodContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class SampleByRowsContext extends SampleMethodContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public SampleByRowsContext(SampleMethodContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSampleByRows(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSampleByRows(this);
		}
	}
	public static class SampleByPercentileContext extends SampleMethodContext {
		public Token negativeSign;
		public Token percentage;
		public TerminalNode PERCENTLIT() { return getToken(SqlBaseParser.PERCENTLIT, 0); }
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public SampleByPercentileContext(SampleMethodContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSampleByPercentile(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSampleByPercentile(this);
		}
	}
	public static class SampleByBucketContext extends SampleMethodContext {
		public Token sampleType;
		public Token numerator;
		public Token denominator;
		public TerminalNode OUT() { return getToken(SqlBaseParser.OUT, 0); }
		public TerminalNode OF() { return getToken(SqlBaseParser.OF, 0); }
		public TerminalNode BUCKET() { return getToken(SqlBaseParser.BUCKET, 0); }
		public List<TerminalNode> INTEGER_VALUE() { return getTokens(SqlBaseParser.INTEGER_VALUE); }
		public TerminalNode INTEGER_VALUE(int i) {
			return getToken(SqlBaseParser.INTEGER_VALUE, i);
		}
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public SampleByBucketContext(SampleMethodContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSampleByBucket(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSampleByBucket(this);
		}
	}
	public static class SampleByBytesContext extends SampleMethodContext {
		public ExpressionContext bytes;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SampleByBytesContext(SampleMethodContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSampleByBytes(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSampleByBytes(this);
		}
	}

	public final SampleMethodContext sampleMethod() throws RecognitionException {
		SampleMethodContext _localctx = new SampleMethodContext(_ctx, getState());
		enterRule(_localctx, 144, RULE_sampleMethod);
		int _la;
		try {
			setState(2085);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,261,_ctx) ) {
			case 1:
				_localctx = new SampleByPercentileContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2062);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2061);
					((SampleByPercentileContext)_localctx).negativeSign = match(MINUS);
					}
				}

				setState(2064);
				((SampleByPercentileContext)_localctx).percentage = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==INTEGER_VALUE || _la==DECIMAL_VALUE) ) {
					((SampleByPercentileContext)_localctx).percentage = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(2065);
				match(PERCENTLIT);
				}
				break;
			case 2:
				_localctx = new SampleByRowsContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2066);
				expression();
				setState(2067);
				match(ROWS);
				}
				break;
			case 3:
				_localctx = new SampleByBucketContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2069);
				((SampleByBucketContext)_localctx).sampleType = match(BUCKET);
				setState(2070);
				((SampleByBucketContext)_localctx).numerator = match(INTEGER_VALUE);
				setState(2071);
				match(OUT);
				setState(2072);
				match(OF);
				setState(2073);
				((SampleByBucketContext)_localctx).denominator = match(INTEGER_VALUE);
				setState(2082);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ON) {
					{
					setState(2074);
					match(ON);
					setState(2080);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,259,_ctx) ) {
					case 1:
						{
						setState(2075);
						identifier();
						}
						break;
					case 2:
						{
						setState(2076);
						qualifiedName();
						setState(2077);
						match(T__1);
						setState(2078);
						match(T__2);
						}
						break;
					}
					}
				}

				}
				break;
			case 4:
				_localctx = new SampleByBytesContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2084);
				((SampleByBytesContext)_localctx).bytes = expression();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierListContext extends ParserRuleContext {
		public IdentifierSeqContext identifierSeq() {
			return getRuleContext(IdentifierSeqContext.class,0);
		}
		public IdentifierListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIdentifierList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIdentifierList(this);
		}
	}

	public final IdentifierListContext identifierList() throws RecognitionException {
		IdentifierListContext _localctx = new IdentifierListContext(_ctx, getState());
		enterRule(_localctx, 146, RULE_identifierList);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2087);
			match(T__1);
			setState(2088);
			identifierSeq();
			setState(2089);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierSeqContext extends ParserRuleContext {
		public ErrorCapturingIdentifierContext errorCapturingIdentifier;
		public List<ErrorCapturingIdentifierContext> ident = new ArrayList<ErrorCapturingIdentifierContext>();
		public List<ErrorCapturingIdentifierContext> errorCapturingIdentifier() {
			return getRuleContexts(ErrorCapturingIdentifierContext.class);
		}
		public ErrorCapturingIdentifierContext errorCapturingIdentifier(int i) {
			return getRuleContext(ErrorCapturingIdentifierContext.class,i);
		}
		public IdentifierSeqContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierSeq; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIdentifierSeq(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIdentifierSeq(this);
		}
	}

	public final IdentifierSeqContext identifierSeq() throws RecognitionException {
		IdentifierSeqContext _localctx = new IdentifierSeqContext(_ctx, getState());
		enterRule(_localctx, 148, RULE_identifierSeq);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2091);
			((IdentifierSeqContext)_localctx).errorCapturingIdentifier = errorCapturingIdentifier();
			((IdentifierSeqContext)_localctx).ident.add(((IdentifierSeqContext)_localctx).errorCapturingIdentifier);
			setState(2096);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,262,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2092);
					match(T__3);
					setState(2093);
					((IdentifierSeqContext)_localctx).errorCapturingIdentifier = errorCapturingIdentifier();
					((IdentifierSeqContext)_localctx).ident.add(((IdentifierSeqContext)_localctx).errorCapturingIdentifier);
					}
					} 
				}
				setState(2098);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,262,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OrderedIdentifierListContext extends ParserRuleContext {
		public List<OrderedIdentifierContext> orderedIdentifier() {
			return getRuleContexts(OrderedIdentifierContext.class);
		}
		public OrderedIdentifierContext orderedIdentifier(int i) {
			return getRuleContext(OrderedIdentifierContext.class,i);
		}
		public OrderedIdentifierListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_orderedIdentifierList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterOrderedIdentifierList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitOrderedIdentifierList(this);
		}
	}

	public final OrderedIdentifierListContext orderedIdentifierList() throws RecognitionException {
		OrderedIdentifierListContext _localctx = new OrderedIdentifierListContext(_ctx, getState());
		enterRule(_localctx, 150, RULE_orderedIdentifierList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2099);
			match(T__1);
			setState(2100);
			orderedIdentifier();
			setState(2105);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(2101);
				match(T__3);
				setState(2102);
				orderedIdentifier();
				}
				}
				setState(2107);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(2108);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OrderedIdentifierContext extends ParserRuleContext {
		public ErrorCapturingIdentifierContext ident;
		public Token ordering;
		public ErrorCapturingIdentifierContext errorCapturingIdentifier() {
			return getRuleContext(ErrorCapturingIdentifierContext.class,0);
		}
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public OrderedIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_orderedIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterOrderedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitOrderedIdentifier(this);
		}
	}

	public final OrderedIdentifierContext orderedIdentifier() throws RecognitionException {
		OrderedIdentifierContext _localctx = new OrderedIdentifierContext(_ctx, getState());
		enterRule(_localctx, 152, RULE_orderedIdentifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2110);
			((OrderedIdentifierContext)_localctx).ident = errorCapturingIdentifier();
			setState(2112);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ASC || _la==DESC) {
				{
				setState(2111);
				((OrderedIdentifierContext)_localctx).ordering = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
					((OrderedIdentifierContext)_localctx).ordering = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierCommentListContext extends ParserRuleContext {
		public List<IdentifierCommentContext> identifierComment() {
			return getRuleContexts(IdentifierCommentContext.class);
		}
		public IdentifierCommentContext identifierComment(int i) {
			return getRuleContext(IdentifierCommentContext.class,i);
		}
		public IdentifierCommentListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierCommentList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIdentifierCommentList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIdentifierCommentList(this);
		}
	}

	public final IdentifierCommentListContext identifierCommentList() throws RecognitionException {
		IdentifierCommentListContext _localctx = new IdentifierCommentListContext(_ctx, getState());
		enterRule(_localctx, 154, RULE_identifierCommentList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2114);
			match(T__1);
			setState(2115);
			identifierComment();
			setState(2120);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(2116);
				match(T__3);
				setState(2117);
				identifierComment();
				}
				}
				setState(2122);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(2123);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierCommentContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public CommentSpecContext commentSpec() {
			return getRuleContext(CommentSpecContext.class,0);
		}
		public IdentifierCommentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierComment; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIdentifierComment(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIdentifierComment(this);
		}
	}

	public final IdentifierCommentContext identifierComment() throws RecognitionException {
		IdentifierCommentContext _localctx = new IdentifierCommentContext(_ctx, getState());
		enterRule(_localctx, 156, RULE_identifierComment);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2125);
			identifier();
			setState(2127);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(2126);
				commentSpec();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RelationPrimaryContext extends ParserRuleContext {
		public RelationPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relationPrimary; }
	 
		public RelationPrimaryContext() { }
		public void copyFrom(RelationPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class TableValuedFunctionContext extends RelationPrimaryContext {
		public FunctionTableContext functionTable() {
			return getRuleContext(FunctionTableContext.class,0);
		}
		public TableValuedFunctionContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableValuedFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableValuedFunction(this);
		}
	}
	public static class InlineTableDefault2Context extends RelationPrimaryContext {
		public InlineTableContext inlineTable() {
			return getRuleContext(InlineTableContext.class,0);
		}
		public InlineTableDefault2Context(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInlineTableDefault2(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInlineTableDefault2(this);
		}
	}
	public static class AliasedRelationContext extends RelationPrimaryContext {
		public RelationContext relation() {
			return getRuleContext(RelationContext.class,0);
		}
		public TableAliasContext tableAlias() {
			return getRuleContext(TableAliasContext.class,0);
		}
		public SampleContext sample() {
			return getRuleContext(SampleContext.class,0);
		}
		public AliasedRelationContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAliasedRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAliasedRelation(this);
		}
	}
	public static class AliasedQueryContext extends RelationPrimaryContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TableAliasContext tableAlias() {
			return getRuleContext(TableAliasContext.class,0);
		}
		public SampleContext sample() {
			return getRuleContext(SampleContext.class,0);
		}
		public AliasedQueryContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAliasedQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAliasedQuery(this);
		}
	}
	public static class TableNameContext extends RelationPrimaryContext {
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TableAliasContext tableAlias() {
			return getRuleContext(TableAliasContext.class,0);
		}
		public SampleContext sample() {
			return getRuleContext(SampleContext.class,0);
		}
		public TableNameContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableName(this);
		}
	}

	public final RelationPrimaryContext relationPrimary() throws RecognitionException {
		RelationPrimaryContext _localctx = new RelationPrimaryContext(_ctx, getState());
		enterRule(_localctx, 158, RULE_relationPrimary);
		try {
			setState(2153);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,270,_ctx) ) {
			case 1:
				_localctx = new TableNameContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2129);
				multipartIdentifier();
				setState(2131);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,267,_ctx) ) {
				case 1:
					{
					setState(2130);
					sample();
					}
					break;
				}
				setState(2133);
				tableAlias();
				}
				break;
			case 2:
				_localctx = new AliasedQueryContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2135);
				match(T__1);
				setState(2136);
				query();
				setState(2137);
				match(T__2);
				setState(2139);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,268,_ctx) ) {
				case 1:
					{
					setState(2138);
					sample();
					}
					break;
				}
				setState(2141);
				tableAlias();
				}
				break;
			case 3:
				_localctx = new AliasedRelationContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2143);
				match(T__1);
				setState(2144);
				relation();
				setState(2145);
				match(T__2);
				setState(2147);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,269,_ctx) ) {
				case 1:
					{
					setState(2146);
					sample();
					}
					break;
				}
				setState(2149);
				tableAlias();
				}
				break;
			case 4:
				_localctx = new InlineTableDefault2Context(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2151);
				inlineTable();
				}
				break;
			case 5:
				_localctx = new TableValuedFunctionContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(2152);
				functionTable();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class InlineTableContext extends ParserRuleContext {
		public TerminalNode VALUES() { return getToken(SqlBaseParser.VALUES, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TableAliasContext tableAlias() {
			return getRuleContext(TableAliasContext.class,0);
		}
		public InlineTableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_inlineTable; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInlineTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInlineTable(this);
		}
	}

	public final InlineTableContext inlineTable() throws RecognitionException {
		InlineTableContext _localctx = new InlineTableContext(_ctx, getState());
		enterRule(_localctx, 160, RULE_inlineTable);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2155);
			match(VALUES);
			setState(2156);
			expression();
			setState(2161);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,271,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2157);
					match(T__3);
					setState(2158);
					expression();
					}
					} 
				}
				setState(2163);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,271,_ctx);
			}
			setState(2164);
			tableAlias();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FunctionTableContext extends ParserRuleContext {
		public ErrorCapturingIdentifierContext funcName;
		public TableAliasContext tableAlias() {
			return getRuleContext(TableAliasContext.class,0);
		}
		public ErrorCapturingIdentifierContext errorCapturingIdentifier() {
			return getRuleContext(ErrorCapturingIdentifierContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public FunctionTableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_functionTable; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFunctionTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFunctionTable(this);
		}
	}

	public final FunctionTableContext functionTable() throws RecognitionException {
		FunctionTableContext _localctx = new FunctionTableContext(_ctx, getState());
		enterRule(_localctx, 162, RULE_functionTable);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2166);
			((FunctionTableContext)_localctx).funcName = errorCapturingIdentifier();
			setState(2167);
			match(T__1);
			setState(2176);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,273,_ctx) ) {
			case 1:
				{
				setState(2168);
				expression();
				setState(2173);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(2169);
					match(T__3);
					setState(2170);
					expression();
					}
					}
					setState(2175);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			}
			setState(2178);
			match(T__2);
			setState(2179);
			tableAlias();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TableAliasContext extends ParserRuleContext {
		public StrictIdentifierContext strictIdentifier() {
			return getRuleContext(StrictIdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TableAliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableAlias; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableAlias(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableAlias(this);
		}
	}

	public final TableAliasContext tableAlias() throws RecognitionException {
		TableAliasContext _localctx = new TableAliasContext(_ctx, getState());
		enterRule(_localctx, 164, RULE_tableAlias);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2188);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,276,_ctx) ) {
			case 1:
				{
				setState(2182);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,274,_ctx) ) {
				case 1:
					{
					setState(2181);
					match(AS);
					}
					break;
				}
				setState(2184);
				strictIdentifier();
				setState(2186);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,275,_ctx) ) {
				case 1:
					{
					setState(2185);
					identifierList();
					}
					break;
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RowFormatContext extends ParserRuleContext {
		public RowFormatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rowFormat; }
	 
		public RowFormatContext() { }
		public void copyFrom(RowFormatContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class RowFormatSerdeContext extends RowFormatContext {
		public Token name;
		public TablePropertyListContext props;
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode SERDE() { return getToken(SqlBaseParser.SERDE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlBaseParser.SERDEPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public RowFormatSerdeContext(RowFormatContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRowFormatSerde(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRowFormatSerde(this);
		}
	}
	public static class RowFormatDelimitedContext extends RowFormatContext {
		public Token fieldsTerminatedBy;
		public Token escapedBy;
		public Token collectionItemsTerminatedBy;
		public Token keysTerminatedBy;
		public Token linesSeparatedBy;
		public Token nullDefinedAs;
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode DELIMITED() { return getToken(SqlBaseParser.DELIMITED, 0); }
		public TerminalNode FIELDS() { return getToken(SqlBaseParser.FIELDS, 0); }
		public List<TerminalNode> TERMINATED() { return getTokens(SqlBaseParser.TERMINATED); }
		public TerminalNode TERMINATED(int i) {
			return getToken(SqlBaseParser.TERMINATED, i);
		}
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public TerminalNode COLLECTION() { return getToken(SqlBaseParser.COLLECTION, 0); }
		public TerminalNode ITEMS() { return getToken(SqlBaseParser.ITEMS, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode KEYS() { return getToken(SqlBaseParser.KEYS, 0); }
		public TerminalNode LINES() { return getToken(SqlBaseParser.LINES, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode DEFINED() { return getToken(SqlBaseParser.DEFINED, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public TerminalNode ESCAPED() { return getToken(SqlBaseParser.ESCAPED, 0); }
		public RowFormatDelimitedContext(RowFormatContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRowFormatDelimited(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRowFormatDelimited(this);
		}
	}

	public final RowFormatContext rowFormat() throws RecognitionException {
		RowFormatContext _localctx = new RowFormatContext(_ctx, getState());
		enterRule(_localctx, 166, RULE_rowFormat);
		try {
			setState(2239);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,284,_ctx) ) {
			case 1:
				_localctx = new RowFormatSerdeContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2190);
				match(ROW);
				setState(2191);
				match(FORMAT);
				setState(2192);
				match(SERDE);
				setState(2193);
				((RowFormatSerdeContext)_localctx).name = match(STRING);
				setState(2197);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,277,_ctx) ) {
				case 1:
					{
					setState(2194);
					match(WITH);
					setState(2195);
					match(SERDEPROPERTIES);
					setState(2196);
					((RowFormatSerdeContext)_localctx).props = tablePropertyList();
					}
					break;
				}
				}
				break;
			case 2:
				_localctx = new RowFormatDelimitedContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2199);
				match(ROW);
				setState(2200);
				match(FORMAT);
				setState(2201);
				match(DELIMITED);
				setState(2211);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,279,_ctx) ) {
				case 1:
					{
					setState(2202);
					match(FIELDS);
					setState(2203);
					match(TERMINATED);
					setState(2204);
					match(BY);
					setState(2205);
					((RowFormatDelimitedContext)_localctx).fieldsTerminatedBy = match(STRING);
					setState(2209);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,278,_ctx) ) {
					case 1:
						{
						setState(2206);
						match(ESCAPED);
						setState(2207);
						match(BY);
						setState(2208);
						((RowFormatDelimitedContext)_localctx).escapedBy = match(STRING);
						}
						break;
					}
					}
					break;
				}
				setState(2218);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,280,_ctx) ) {
				case 1:
					{
					setState(2213);
					match(COLLECTION);
					setState(2214);
					match(ITEMS);
					setState(2215);
					match(TERMINATED);
					setState(2216);
					match(BY);
					setState(2217);
					((RowFormatDelimitedContext)_localctx).collectionItemsTerminatedBy = match(STRING);
					}
					break;
				}
				setState(2225);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,281,_ctx) ) {
				case 1:
					{
					setState(2220);
					match(MAP);
					setState(2221);
					match(KEYS);
					setState(2222);
					match(TERMINATED);
					setState(2223);
					match(BY);
					setState(2224);
					((RowFormatDelimitedContext)_localctx).keysTerminatedBy = match(STRING);
					}
					break;
				}
				setState(2231);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,282,_ctx) ) {
				case 1:
					{
					setState(2227);
					match(LINES);
					setState(2228);
					match(TERMINATED);
					setState(2229);
					match(BY);
					setState(2230);
					((RowFormatDelimitedContext)_localctx).linesSeparatedBy = match(STRING);
					}
					break;
				}
				setState(2237);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,283,_ctx) ) {
				case 1:
					{
					setState(2233);
					match(NULL);
					setState(2234);
					match(DEFINED);
					setState(2235);
					match(AS);
					setState(2236);
					((RowFormatDelimitedContext)_localctx).nullDefinedAs = match(STRING);
					}
					break;
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MultipartIdentifierListContext extends ParserRuleContext {
		public List<MultipartIdentifierContext> multipartIdentifier() {
			return getRuleContexts(MultipartIdentifierContext.class);
		}
		public MultipartIdentifierContext multipartIdentifier(int i) {
			return getRuleContext(MultipartIdentifierContext.class,i);
		}
		public MultipartIdentifierListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multipartIdentifierList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMultipartIdentifierList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMultipartIdentifierList(this);
		}
	}

	public final MultipartIdentifierListContext multipartIdentifierList() throws RecognitionException {
		MultipartIdentifierListContext _localctx = new MultipartIdentifierListContext(_ctx, getState());
		enterRule(_localctx, 168, RULE_multipartIdentifierList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2241);
			multipartIdentifier();
			setState(2246);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(2242);
				match(T__3);
				setState(2243);
				multipartIdentifier();
				}
				}
				setState(2248);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MultipartIdentifierContext extends ParserRuleContext {
		public ErrorCapturingIdentifierContext errorCapturingIdentifier;
		public List<ErrorCapturingIdentifierContext> parts = new ArrayList<ErrorCapturingIdentifierContext>();
		public List<ErrorCapturingIdentifierContext> errorCapturingIdentifier() {
			return getRuleContexts(ErrorCapturingIdentifierContext.class);
		}
		public ErrorCapturingIdentifierContext errorCapturingIdentifier(int i) {
			return getRuleContext(ErrorCapturingIdentifierContext.class,i);
		}
		public MultipartIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multipartIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMultipartIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMultipartIdentifier(this);
		}
	}

	public final MultipartIdentifierContext multipartIdentifier() throws RecognitionException {
		MultipartIdentifierContext _localctx = new MultipartIdentifierContext(_ctx, getState());
		enterRule(_localctx, 170, RULE_multipartIdentifier);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2249);
			((MultipartIdentifierContext)_localctx).errorCapturingIdentifier = errorCapturingIdentifier();
			((MultipartIdentifierContext)_localctx).parts.add(((MultipartIdentifierContext)_localctx).errorCapturingIdentifier);
			setState(2254);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,286,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2250);
					match(T__4);
					setState(2251);
					((MultipartIdentifierContext)_localctx).errorCapturingIdentifier = errorCapturingIdentifier();
					((MultipartIdentifierContext)_localctx).parts.add(((MultipartIdentifierContext)_localctx).errorCapturingIdentifier);
					}
					} 
				}
				setState(2256);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,286,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TableIdentifierContext extends ParserRuleContext {
		public ErrorCapturingIdentifierContext db;
		public ErrorCapturingIdentifierContext table;
		public List<ErrorCapturingIdentifierContext> errorCapturingIdentifier() {
			return getRuleContexts(ErrorCapturingIdentifierContext.class);
		}
		public ErrorCapturingIdentifierContext errorCapturingIdentifier(int i) {
			return getRuleContext(ErrorCapturingIdentifierContext.class,i);
		}
		public TableIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableIdentifier(this);
		}
	}

	public final TableIdentifierContext tableIdentifier() throws RecognitionException {
		TableIdentifierContext _localctx = new TableIdentifierContext(_ctx, getState());
		enterRule(_localctx, 172, RULE_tableIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2260);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,287,_ctx) ) {
			case 1:
				{
				setState(2257);
				((TableIdentifierContext)_localctx).db = errorCapturingIdentifier();
				setState(2258);
				match(T__4);
				}
				break;
			}
			setState(2262);
			((TableIdentifierContext)_localctx).table = errorCapturingIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FunctionIdentifierContext extends ParserRuleContext {
		public ErrorCapturingIdentifierContext db;
		public ErrorCapturingIdentifierContext function;
		public List<ErrorCapturingIdentifierContext> errorCapturingIdentifier() {
			return getRuleContexts(ErrorCapturingIdentifierContext.class);
		}
		public ErrorCapturingIdentifierContext errorCapturingIdentifier(int i) {
			return getRuleContext(ErrorCapturingIdentifierContext.class,i);
		}
		public FunctionIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_functionIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFunctionIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFunctionIdentifier(this);
		}
	}

	public final FunctionIdentifierContext functionIdentifier() throws RecognitionException {
		FunctionIdentifierContext _localctx = new FunctionIdentifierContext(_ctx, getState());
		enterRule(_localctx, 174, RULE_functionIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2267);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,288,_ctx) ) {
			case 1:
				{
				setState(2264);
				((FunctionIdentifierContext)_localctx).db = errorCapturingIdentifier();
				setState(2265);
				match(T__4);
				}
				break;
			}
			setState(2269);
			((FunctionIdentifierContext)_localctx).function = errorCapturingIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NamedExpressionContext extends ParserRuleContext {
		public ErrorCapturingIdentifierContext name;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public ErrorCapturingIdentifierContext errorCapturingIdentifier() {
			return getRuleContext(ErrorCapturingIdentifierContext.class,0);
		}
		public NamedExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNamedExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNamedExpression(this);
		}
	}

	public final NamedExpressionContext namedExpression() throws RecognitionException {
		NamedExpressionContext _localctx = new NamedExpressionContext(_ctx, getState());
		enterRule(_localctx, 176, RULE_namedExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2271);
			expression();
			setState(2279);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,291,_ctx) ) {
			case 1:
				{
				setState(2273);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,289,_ctx) ) {
				case 1:
					{
					setState(2272);
					match(AS);
					}
					break;
				}
				setState(2277);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,290,_ctx) ) {
				case 1:
					{
					setState(2275);
					((NamedExpressionContext)_localctx).name = errorCapturingIdentifier();
					}
					break;
				case 2:
					{
					setState(2276);
					identifierList();
					}
					break;
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NamedExpressionSeqContext extends ParserRuleContext {
		public List<NamedExpressionContext> namedExpression() {
			return getRuleContexts(NamedExpressionContext.class);
		}
		public NamedExpressionContext namedExpression(int i) {
			return getRuleContext(NamedExpressionContext.class,i);
		}
		public NamedExpressionSeqContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedExpressionSeq; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNamedExpressionSeq(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNamedExpressionSeq(this);
		}
	}

	public final NamedExpressionSeqContext namedExpressionSeq() throws RecognitionException {
		NamedExpressionSeqContext _localctx = new NamedExpressionSeqContext(_ctx, getState());
		enterRule(_localctx, 178, RULE_namedExpressionSeq);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2281);
			namedExpression();
			setState(2286);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,292,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2282);
					match(T__3);
					setState(2283);
					namedExpression();
					}
					} 
				}
				setState(2288);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,292,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TransformListContext extends ParserRuleContext {
		public TransformContext transform;
		public List<TransformContext> transforms = new ArrayList<TransformContext>();
		public List<TransformContext> transform() {
			return getRuleContexts(TransformContext.class);
		}
		public TransformContext transform(int i) {
			return getRuleContext(TransformContext.class,i);
		}
		public TransformListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_transformList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTransformList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTransformList(this);
		}
	}

	public final TransformListContext transformList() throws RecognitionException {
		TransformListContext _localctx = new TransformListContext(_ctx, getState());
		enterRule(_localctx, 180, RULE_transformList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2289);
			match(T__1);
			setState(2290);
			((TransformListContext)_localctx).transform = transform();
			((TransformListContext)_localctx).transforms.add(((TransformListContext)_localctx).transform);
			setState(2295);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(2291);
				match(T__3);
				setState(2292);
				((TransformListContext)_localctx).transform = transform();
				((TransformListContext)_localctx).transforms.add(((TransformListContext)_localctx).transform);
				}
				}
				setState(2297);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(2298);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TransformContext extends ParserRuleContext {
		public TransformContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_transform; }
	 
		public TransformContext() { }
		public void copyFrom(TransformContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class IdentityTransformContext extends TransformContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public IdentityTransformContext(TransformContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIdentityTransform(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIdentityTransform(this);
		}
	}
	public static class ApplyTransformContext extends TransformContext {
		public IdentifierContext transformName;
		public TransformArgumentContext transformArgument;
		public List<TransformArgumentContext> argument = new ArrayList<TransformArgumentContext>();
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<TransformArgumentContext> transformArgument() {
			return getRuleContexts(TransformArgumentContext.class);
		}
		public TransformArgumentContext transformArgument(int i) {
			return getRuleContext(TransformArgumentContext.class,i);
		}
		public ApplyTransformContext(TransformContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterApplyTransform(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitApplyTransform(this);
		}
	}

	public final TransformContext transform() throws RecognitionException {
		TransformContext _localctx = new TransformContext(_ctx, getState());
		enterRule(_localctx, 182, RULE_transform);
		int _la;
		try {
			setState(2313);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,295,_ctx) ) {
			case 1:
				_localctx = new IdentityTransformContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2300);
				qualifiedName();
				}
				break;
			case 2:
				_localctx = new ApplyTransformContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2301);
				((ApplyTransformContext)_localctx).transformName = identifier();
				setState(2302);
				match(T__1);
				setState(2303);
				((ApplyTransformContext)_localctx).transformArgument = transformArgument();
				((ApplyTransformContext)_localctx).argument.add(((ApplyTransformContext)_localctx).transformArgument);
				setState(2308);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(2304);
					match(T__3);
					setState(2305);
					((ApplyTransformContext)_localctx).transformArgument = transformArgument();
					((ApplyTransformContext)_localctx).argument.add(((ApplyTransformContext)_localctx).transformArgument);
					}
					}
					setState(2310);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(2311);
				match(T__2);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TransformArgumentContext extends ParserRuleContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public TransformArgumentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_transformArgument; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTransformArgument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTransformArgument(this);
		}
	}

	public final TransformArgumentContext transformArgument() throws RecognitionException {
		TransformArgumentContext _localctx = new TransformArgumentContext(_ctx, getState());
		enterRule(_localctx, 184, RULE_transformArgument);
		try {
			setState(2317);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,296,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2315);
				qualifiedName();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2316);
				constant();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionContext extends ParserRuleContext {
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExpression(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
		enterRule(_localctx, 186, RULE_expression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2319);
			booleanExpression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BooleanExpressionContext extends ParserRuleContext {
		public BooleanExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanExpression; }
	 
		public BooleanExpressionContext() { }
		public void copyFrom(BooleanExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class LogicalNotContext extends BooleanExpressionContext {
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public LogicalNotContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLogicalNot(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLogicalNot(this);
		}
	}
	public static class PredicatedContext extends BooleanExpressionContext {
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public PredicateContext predicate() {
			return getRuleContext(PredicateContext.class,0);
		}
		public PredicatedContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPredicated(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPredicated(this);
		}
	}
	public static class ExistsContext extends BooleanExpressionContext {
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public ExistsContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExists(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExists(this);
		}
	}
	public static class LogicalBinaryContext extends BooleanExpressionContext {
		public BooleanExpressionContext left;
		public Token operator;
		public BooleanExpressionContext right;
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public LogicalBinaryContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLogicalBinary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLogicalBinary(this);
		}
	}

	public final BooleanExpressionContext booleanExpression() throws RecognitionException {
		return booleanExpression(0);
	}

	private BooleanExpressionContext booleanExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		BooleanExpressionContext _localctx = new BooleanExpressionContext(_ctx, _parentState);
		BooleanExpressionContext _prevctx = _localctx;
		int _startState = 188;
		enterRecursionRule(_localctx, 188, RULE_booleanExpression, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2333);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,298,_ctx) ) {
			case 1:
				{
				_localctx = new LogicalNotContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(2322);
				match(NOT);
				setState(2323);
				booleanExpression(5);
				}
				break;
			case 2:
				{
				_localctx = new ExistsContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2324);
				match(EXISTS);
				setState(2325);
				match(T__1);
				setState(2326);
				query();
				setState(2327);
				match(T__2);
				}
				break;
			case 3:
				{
				_localctx = new PredicatedContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2329);
				valueExpression(0);
				setState(2331);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,297,_ctx) ) {
				case 1:
					{
					setState(2330);
					predicate();
					}
					break;
				}
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(2343);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,300,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(2341);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,299,_ctx) ) {
					case 1:
						{
						_localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
						((LogicalBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
						setState(2335);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(2336);
						((LogicalBinaryContext)_localctx).operator = match(AND);
						setState(2337);
						((LogicalBinaryContext)_localctx).right = booleanExpression(3);
						}
						break;
					case 2:
						{
						_localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
						((LogicalBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
						setState(2338);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(2339);
						((LogicalBinaryContext)_localctx).operator = match(OR);
						setState(2340);
						((LogicalBinaryContext)_localctx).right = booleanExpression(2);
						}
						break;
					}
					} 
				}
				setState(2345);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,300,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class PredicateContext extends ParserRuleContext {
		public Token kind;
		public ValueExpressionContext lower;
		public ValueExpressionContext upper;
		public ValueExpressionContext pattern;
		public Token quantifier;
		public Token escapeChar;
		public ValueExpressionContext right;
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode RLIKE() { return getToken(SqlBaseParser.RLIKE, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode ANY() { return getToken(SqlBaseParser.ANY, 0); }
		public TerminalNode SOME() { return getToken(SqlBaseParser.SOME, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode ESCAPE() { return getToken(SqlBaseParser.ESCAPE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode TRUE() { return getToken(SqlBaseParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(SqlBaseParser.FALSE, 0); }
		public TerminalNode UNKNOWN() { return getToken(SqlBaseParser.UNKNOWN, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode DISTINCT() { return getToken(SqlBaseParser.DISTINCT, 0); }
		public PredicateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicate; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPredicate(this);
		}
	}

	public final PredicateContext predicate() throws RecognitionException {
		PredicateContext _localctx = new PredicateContext(_ctx, getState());
		enterRule(_localctx, 190, RULE_predicate);
		int _la;
		try {
			setState(2428);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,314,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2347);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2346);
					match(NOT);
					}
				}

				setState(2349);
				((PredicateContext)_localctx).kind = match(BETWEEN);
				setState(2350);
				((PredicateContext)_localctx).lower = valueExpression(0);
				setState(2351);
				match(AND);
				setState(2352);
				((PredicateContext)_localctx).upper = valueExpression(0);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2355);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2354);
					match(NOT);
					}
				}

				setState(2357);
				((PredicateContext)_localctx).kind = match(IN);
				setState(2358);
				match(T__1);
				setState(2359);
				expression();
				setState(2364);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(2360);
					match(T__3);
					setState(2361);
					expression();
					}
					}
					setState(2366);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(2367);
				match(T__2);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2370);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2369);
					match(NOT);
					}
				}

				setState(2372);
				((PredicateContext)_localctx).kind = match(IN);
				setState(2373);
				match(T__1);
				setState(2374);
				query();
				setState(2375);
				match(T__2);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(2378);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2377);
					match(NOT);
					}
				}

				setState(2380);
				((PredicateContext)_localctx).kind = match(RLIKE);
				setState(2381);
				((PredicateContext)_localctx).pattern = valueExpression(0);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(2383);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2382);
					match(NOT);
					}
				}

				setState(2385);
				((PredicateContext)_localctx).kind = match(LIKE);
				setState(2386);
				((PredicateContext)_localctx).quantifier = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ALL || _la==ANY || _la==SOME) ) {
					((PredicateContext)_localctx).quantifier = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(2400);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,308,_ctx) ) {
				case 1:
					{
					setState(2387);
					match(T__1);
					setState(2388);
					match(T__2);
					}
					break;
				case 2:
					{
					setState(2389);
					match(T__1);
					setState(2390);
					expression();
					setState(2395);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(2391);
						match(T__3);
						setState(2392);
						expression();
						}
						}
						setState(2397);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(2398);
					match(T__2);
					}
					break;
				}
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(2403);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2402);
					match(NOT);
					}
				}

				setState(2405);
				((PredicateContext)_localctx).kind = match(LIKE);
				setState(2406);
				((PredicateContext)_localctx).pattern = valueExpression(0);
				setState(2409);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,310,_ctx) ) {
				case 1:
					{
					setState(2407);
					match(ESCAPE);
					setState(2408);
					((PredicateContext)_localctx).escapeChar = match(STRING);
					}
					break;
				}
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(2411);
				match(IS);
				setState(2413);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2412);
					match(NOT);
					}
				}

				setState(2415);
				((PredicateContext)_localctx).kind = match(NULL);
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(2416);
				match(IS);
				setState(2418);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2417);
					match(NOT);
					}
				}

				setState(2420);
				((PredicateContext)_localctx).kind = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==FALSE || _la==TRUE || _la==UNKNOWN) ) {
					((PredicateContext)_localctx).kind = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(2421);
				match(IS);
				setState(2423);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2422);
					match(NOT);
					}
				}

				setState(2425);
				((PredicateContext)_localctx).kind = match(DISTINCT);
				setState(2426);
				match(FROM);
				setState(2427);
				((PredicateContext)_localctx).right = valueExpression(0);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ValueExpressionContext extends ParserRuleContext {
		public ValueExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_valueExpression; }
	 
		public ValueExpressionContext() { }
		public void copyFrom(ValueExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ValueExpressionDefaultContext extends ValueExpressionContext {
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public ValueExpressionDefaultContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterValueExpressionDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitValueExpressionDefault(this);
		}
	}
	public static class ComparisonContext extends ValueExpressionContext {
		public ValueExpressionContext left;
		public ValueExpressionContext right;
		public ComparisonOperatorContext comparisonOperator() {
			return getRuleContext(ComparisonOperatorContext.class,0);
		}
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public ComparisonContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComparison(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComparison(this);
		}
	}
	public static class ArithmeticBinaryContext extends ValueExpressionContext {
		public ValueExpressionContext left;
		public Token operator;
		public ValueExpressionContext right;
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public TerminalNode SLASH() { return getToken(SqlBaseParser.SLASH, 0); }
		public TerminalNode PERCENT() { return getToken(SqlBaseParser.PERCENT, 0); }
		public TerminalNode DIV() { return getToken(SqlBaseParser.DIV, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TerminalNode CONCAT_PIPE() { return getToken(SqlBaseParser.CONCAT_PIPE, 0); }
		public TerminalNode AMPERSAND() { return getToken(SqlBaseParser.AMPERSAND, 0); }
		public TerminalNode HAT() { return getToken(SqlBaseParser.HAT, 0); }
		public TerminalNode PIPE() { return getToken(SqlBaseParser.PIPE, 0); }
		public ArithmeticBinaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArithmeticBinary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArithmeticBinary(this);
		}
	}
	public static class ArithmeticUnaryContext extends ValueExpressionContext {
		public Token operator;
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode TILDE() { return getToken(SqlBaseParser.TILDE, 0); }
		public ArithmeticUnaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArithmeticUnary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArithmeticUnary(this);
		}
	}

	public final ValueExpressionContext valueExpression() throws RecognitionException {
		return valueExpression(0);
	}

	private ValueExpressionContext valueExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ValueExpressionContext _localctx = new ValueExpressionContext(_ctx, _parentState);
		ValueExpressionContext _prevctx = _localctx;
		int _startState = 192;
		enterRecursionRule(_localctx, 192, RULE_valueExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2434);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,315,_ctx) ) {
			case 1:
				{
				_localctx = new ValueExpressionDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(2431);
				primaryExpression(0);
				}
				break;
			case 2:
				{
				_localctx = new ArithmeticUnaryContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2432);
				((ArithmeticUnaryContext)_localctx).operator = _input.LT(1);
				_la = _input.LA(1);
				if ( !(((((_la - 272)) & ~0x3f) == 0 && ((1L << (_la - 272)) & ((1L << (PLUS - 272)) | (1L << (MINUS - 272)) | (1L << (TILDE - 272)))) != 0)) ) {
					((ArithmeticUnaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(2433);
				valueExpression(7);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(2457);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,317,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(2455);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,316,_ctx) ) {
					case 1:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2436);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(2437);
						((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(((((_la - 274)) & ~0x3f) == 0 && ((1L << (_la - 274)) & ((1L << (ASTERISK - 274)) | (1L << (SLASH - 274)) | (1L << (PERCENT - 274)) | (1L << (DIV - 274)))) != 0)) ) {
							((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(2438);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(7);
						}
						break;
					case 2:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2439);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(2440);
						((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(((((_la - 272)) & ~0x3f) == 0 && ((1L << (_la - 272)) & ((1L << (PLUS - 272)) | (1L << (MINUS - 272)) | (1L << (CONCAT_PIPE - 272)))) != 0)) ) {
							((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(2441);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(6);
						}
						break;
					case 3:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2442);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(2443);
						((ArithmeticBinaryContext)_localctx).operator = match(AMPERSAND);
						setState(2444);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(5);
						}
						break;
					case 4:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2445);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(2446);
						((ArithmeticBinaryContext)_localctx).operator = match(HAT);
						setState(2447);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(4);
						}
						break;
					case 5:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2448);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(2449);
						((ArithmeticBinaryContext)_localctx).operator = match(PIPE);
						setState(2450);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(3);
						}
						break;
					case 6:
						{
						_localctx = new ComparisonContext(new ValueExpressionContext(_parentctx, _parentState));
						((ComparisonContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2451);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(2452);
						comparisonOperator();
						setState(2453);
						((ComparisonContext)_localctx).right = valueExpression(2);
						}
						break;
					}
					} 
				}
				setState(2459);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,317,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class PrimaryExpressionContext extends ParserRuleContext {
		public PrimaryExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_primaryExpression; }
	 
		public PrimaryExpressionContext() { }
		public void copyFrom(PrimaryExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class StructContext extends PrimaryExpressionContext {
		public NamedExpressionContext namedExpression;
		public List<NamedExpressionContext> argument = new ArrayList<NamedExpressionContext>();
		public TerminalNode STRUCT() { return getToken(SqlBaseParser.STRUCT, 0); }
		public List<NamedExpressionContext> namedExpression() {
			return getRuleContexts(NamedExpressionContext.class);
		}
		public NamedExpressionContext namedExpression(int i) {
			return getRuleContext(NamedExpressionContext.class,i);
		}
		public StructContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStruct(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStruct(this);
		}
	}
	public static class DereferenceContext extends PrimaryExpressionContext {
		public PrimaryExpressionContext base;
		public IdentifierContext fieldName;
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DereferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDereference(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDereference(this);
		}
	}
	public static class SimpleCaseContext extends PrimaryExpressionContext {
		public ExpressionContext value;
		public ExpressionContext elseExpression;
		public TerminalNode CASE() { return getToken(SqlBaseParser.CASE, 0); }
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<WhenClauseContext> whenClause() {
			return getRuleContexts(WhenClauseContext.class);
		}
		public WhenClauseContext whenClause(int i) {
			return getRuleContext(WhenClauseContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(SqlBaseParser.ELSE, 0); }
		public SimpleCaseContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSimpleCase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSimpleCase(this);
		}
	}
	public static class ColumnReferenceContext extends PrimaryExpressionContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColumnReferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColumnReference(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColumnReference(this);
		}
	}
	public static class RowConstructorContext extends PrimaryExpressionContext {
		public List<NamedExpressionContext> namedExpression() {
			return getRuleContexts(NamedExpressionContext.class);
		}
		public NamedExpressionContext namedExpression(int i) {
			return getRuleContext(NamedExpressionContext.class,i);
		}
		public RowConstructorContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRowConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRowConstructor(this);
		}
	}
	public static class LastContext extends PrimaryExpressionContext {
		public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode IGNORE() { return getToken(SqlBaseParser.IGNORE, 0); }
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public LastContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLast(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLast(this);
		}
	}
	public static class StarContext extends PrimaryExpressionContext {
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public StarContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStar(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStar(this);
		}
	}
	public static class OverlayContext extends PrimaryExpressionContext {
		public ValueExpressionContext input;
		public ValueExpressionContext replace;
		public ValueExpressionContext position;
		public ValueExpressionContext length;
		public TerminalNode OVERLAY() { return getToken(SqlBaseParser.OVERLAY, 0); }
		public TerminalNode PLACING() { return getToken(SqlBaseParser.PLACING, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public OverlayContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterOverlay(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitOverlay(this);
		}
	}
	public static class SubscriptContext extends PrimaryExpressionContext {
		public PrimaryExpressionContext value;
		public ValueExpressionContext index;
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public SubscriptContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubscript(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubscript(this);
		}
	}
	public static class SubqueryExpressionContext extends PrimaryExpressionContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public SubqueryExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubqueryExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubqueryExpression(this);
		}
	}
	public static class SubstringContext extends PrimaryExpressionContext {
		public ValueExpressionContext str;
		public ValueExpressionContext pos;
		public ValueExpressionContext len;
		public TerminalNode SUBSTR() { return getToken(SqlBaseParser.SUBSTR, 0); }
		public TerminalNode SUBSTRING() { return getToken(SqlBaseParser.SUBSTRING, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public SubstringContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubstring(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubstring(this);
		}
	}
	public static class CurrentDatetimeContext extends PrimaryExpressionContext {
		public Token name;
		public TerminalNode CURRENT_DATE() { return getToken(SqlBaseParser.CURRENT_DATE, 0); }
		public TerminalNode CURRENT_TIMESTAMP() { return getToken(SqlBaseParser.CURRENT_TIMESTAMP, 0); }
		public CurrentDatetimeContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCurrentDatetime(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCurrentDatetime(this);
		}
	}
	public static class CastContext extends PrimaryExpressionContext {
		public TerminalNode CAST() { return getToken(SqlBaseParser.CAST, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public CastContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCast(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCast(this);
		}
	}
	public static class ConstantDefaultContext extends PrimaryExpressionContext {
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public ConstantDefaultContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterConstantDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitConstantDefault(this);
		}
	}
	public static class LambdaContext extends PrimaryExpressionContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public LambdaContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLambda(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLambda(this);
		}
	}
	public static class ParenthesizedExpressionContext extends PrimaryExpressionContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public ParenthesizedExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterParenthesizedExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitParenthesizedExpression(this);
		}
	}
	public static class ExtractContext extends PrimaryExpressionContext {
		public IdentifierContext field;
		public ValueExpressionContext source;
		public TerminalNode EXTRACT() { return getToken(SqlBaseParser.EXTRACT, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public ExtractContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExtract(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExtract(this);
		}
	}
	public static class TrimContext extends PrimaryExpressionContext {
		public Token trimOption;
		public ValueExpressionContext trimStr;
		public ValueExpressionContext srcStr;
		public TerminalNode TRIM() { return getToken(SqlBaseParser.TRIM, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode BOTH() { return getToken(SqlBaseParser.BOTH, 0); }
		public TerminalNode LEADING() { return getToken(SqlBaseParser.LEADING, 0); }
		public TerminalNode TRAILING() { return getToken(SqlBaseParser.TRAILING, 0); }
		public TrimContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTrim(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTrim(this);
		}
	}
	public static class FunctionCallContext extends PrimaryExpressionContext {
		public ExpressionContext expression;
		public List<ExpressionContext> argument = new ArrayList<ExpressionContext>();
		public BooleanExpressionContext where;
		public FunctionNameContext functionName() {
			return getRuleContext(FunctionNameContext.class,0);
		}
		public TerminalNode FILTER() { return getToken(SqlBaseParser.FILTER, 0); }
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public TerminalNode OVER() { return getToken(SqlBaseParser.OVER, 0); }
		public WindowSpecContext windowSpec() {
			return getRuleContext(WindowSpecContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public FunctionCallContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFunctionCall(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFunctionCall(this);
		}
	}
	public static class SearchedCaseContext extends PrimaryExpressionContext {
		public ExpressionContext elseExpression;
		public TerminalNode CASE() { return getToken(SqlBaseParser.CASE, 0); }
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public List<WhenClauseContext> whenClause() {
			return getRuleContexts(WhenClauseContext.class);
		}
		public WhenClauseContext whenClause(int i) {
			return getRuleContext(WhenClauseContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(SqlBaseParser.ELSE, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SearchedCaseContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSearchedCase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSearchedCase(this);
		}
	}
	public static class PositionContext extends PrimaryExpressionContext {
		public ValueExpressionContext substr;
		public ValueExpressionContext str;
		public TerminalNode POSITION() { return getToken(SqlBaseParser.POSITION, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public PositionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPosition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPosition(this);
		}
	}
	public static class FirstContext extends PrimaryExpressionContext {
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode IGNORE() { return getToken(SqlBaseParser.IGNORE, 0); }
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public FirstContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFirst(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFirst(this);
		}
	}

	public final PrimaryExpressionContext primaryExpression() throws RecognitionException {
		return primaryExpression(0);
	}

	private PrimaryExpressionContext primaryExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		PrimaryExpressionContext _localctx = new PrimaryExpressionContext(_ctx, _parentState);
		PrimaryExpressionContext _prevctx = _localctx;
		int _startState = 194;
		enterRecursionRule(_localctx, 194, RULE_primaryExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2644);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,337,_ctx) ) {
			case 1:
				{
				_localctx = new CurrentDatetimeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(2461);
				((CurrentDatetimeContext)_localctx).name = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==CURRENT_DATE || _la==CURRENT_TIMESTAMP) ) {
					((CurrentDatetimeContext)_localctx).name = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case 2:
				{
				_localctx = new SearchedCaseContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2462);
				match(CASE);
				setState(2464); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(2463);
					whenClause();
					}
					}
					setState(2466); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(2470);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(2468);
					match(ELSE);
					setState(2469);
					((SearchedCaseContext)_localctx).elseExpression = expression();
					}
				}

				setState(2472);
				match(END);
				}
				break;
			case 3:
				{
				_localctx = new SimpleCaseContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2474);
				match(CASE);
				setState(2475);
				((SimpleCaseContext)_localctx).value = expression();
				setState(2477); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(2476);
					whenClause();
					}
					}
					setState(2479); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(2483);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(2481);
					match(ELSE);
					setState(2482);
					((SimpleCaseContext)_localctx).elseExpression = expression();
					}
				}

				setState(2485);
				match(END);
				}
				break;
			case 4:
				{
				_localctx = new CastContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2487);
				match(CAST);
				setState(2488);
				match(T__1);
				setState(2489);
				expression();
				setState(2490);
				match(AS);
				setState(2491);
				dataType();
				setState(2492);
				match(T__2);
				}
				break;
			case 5:
				{
				_localctx = new StructContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2494);
				match(STRUCT);
				setState(2495);
				match(T__1);
				setState(2504);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,323,_ctx) ) {
				case 1:
					{
					setState(2496);
					((StructContext)_localctx).namedExpression = namedExpression();
					((StructContext)_localctx).argument.add(((StructContext)_localctx).namedExpression);
					setState(2501);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(2497);
						match(T__3);
						setState(2498);
						((StructContext)_localctx).namedExpression = namedExpression();
						((StructContext)_localctx).argument.add(((StructContext)_localctx).namedExpression);
						}
						}
						setState(2503);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
					break;
				}
				setState(2506);
				match(T__2);
				}
				break;
			case 6:
				{
				_localctx = new FirstContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2507);
				match(FIRST);
				setState(2508);
				match(T__1);
				setState(2509);
				expression();
				setState(2512);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IGNORE) {
					{
					setState(2510);
					match(IGNORE);
					setState(2511);
					match(NULLS);
					}
				}

				setState(2514);
				match(T__2);
				}
				break;
			case 7:
				{
				_localctx = new LastContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2516);
				match(LAST);
				setState(2517);
				match(T__1);
				setState(2518);
				expression();
				setState(2521);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IGNORE) {
					{
					setState(2519);
					match(IGNORE);
					setState(2520);
					match(NULLS);
					}
				}

				setState(2523);
				match(T__2);
				}
				break;
			case 8:
				{
				_localctx = new PositionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2525);
				match(POSITION);
				setState(2526);
				match(T__1);
				setState(2527);
				((PositionContext)_localctx).substr = valueExpression(0);
				setState(2528);
				match(IN);
				setState(2529);
				((PositionContext)_localctx).str = valueExpression(0);
				setState(2530);
				match(T__2);
				}
				break;
			case 9:
				{
				_localctx = new ConstantDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2532);
				constant();
				}
				break;
			case 10:
				{
				_localctx = new StarContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2533);
				match(ASTERISK);
				}
				break;
			case 11:
				{
				_localctx = new StarContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2534);
				qualifiedName();
				setState(2535);
				match(T__4);
				setState(2536);
				match(ASTERISK);
				}
				break;
			case 12:
				{
				_localctx = new RowConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2538);
				match(T__1);
				setState(2539);
				namedExpression();
				setState(2542); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(2540);
					match(T__3);
					setState(2541);
					namedExpression();
					}
					}
					setState(2544); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==T__3 );
				setState(2546);
				match(T__2);
				}
				break;
			case 13:
				{
				_localctx = new SubqueryExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2548);
				match(T__1);
				setState(2549);
				query();
				setState(2550);
				match(T__2);
				}
				break;
			case 14:
				{
				_localctx = new FunctionCallContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2552);
				functionName();
				setState(2553);
				match(T__1);
				setState(2565);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,329,_ctx) ) {
				case 1:
					{
					setState(2555);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,327,_ctx) ) {
					case 1:
						{
						setState(2554);
						setQuantifier();
						}
						break;
					}
					setState(2557);
					((FunctionCallContext)_localctx).expression = expression();
					((FunctionCallContext)_localctx).argument.add(((FunctionCallContext)_localctx).expression);
					setState(2562);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(2558);
						match(T__3);
						setState(2559);
						((FunctionCallContext)_localctx).expression = expression();
						((FunctionCallContext)_localctx).argument.add(((FunctionCallContext)_localctx).expression);
						}
						}
						setState(2564);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
					break;
				}
				setState(2567);
				match(T__2);
				setState(2574);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,330,_ctx) ) {
				case 1:
					{
					setState(2568);
					match(FILTER);
					setState(2569);
					match(T__1);
					setState(2570);
					match(WHERE);
					setState(2571);
					((FunctionCallContext)_localctx).where = booleanExpression(0);
					setState(2572);
					match(T__2);
					}
					break;
				}
				setState(2578);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,331,_ctx) ) {
				case 1:
					{
					setState(2576);
					match(OVER);
					setState(2577);
					windowSpec();
					}
					break;
				}
				}
				break;
			case 15:
				{
				_localctx = new LambdaContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2580);
				identifier();
				setState(2581);
				match(T__7);
				setState(2582);
				expression();
				}
				break;
			case 16:
				{
				_localctx = new LambdaContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2584);
				match(T__1);
				setState(2585);
				identifier();
				setState(2588); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(2586);
					match(T__3);
					setState(2587);
					identifier();
					}
					}
					setState(2590); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==T__3 );
				setState(2592);
				match(T__2);
				setState(2593);
				match(T__7);
				setState(2594);
				expression();
				}
				break;
			case 17:
				{
				_localctx = new ColumnReferenceContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2596);
				identifier();
				}
				break;
			case 18:
				{
				_localctx = new ParenthesizedExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2597);
				match(T__1);
				setState(2598);
				expression();
				setState(2599);
				match(T__2);
				}
				break;
			case 19:
				{
				_localctx = new ExtractContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2601);
				match(EXTRACT);
				setState(2602);
				match(T__1);
				setState(2603);
				((ExtractContext)_localctx).field = identifier();
				setState(2604);
				match(FROM);
				setState(2605);
				((ExtractContext)_localctx).source = valueExpression(0);
				setState(2606);
				match(T__2);
				}
				break;
			case 20:
				{
				_localctx = new SubstringContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2608);
				_la = _input.LA(1);
				if ( !(_la==SUBSTR || _la==SUBSTRING) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(2609);
				match(T__1);
				setState(2610);
				((SubstringContext)_localctx).str = valueExpression(0);
				setState(2611);
				_la = _input.LA(1);
				if ( !(_la==T__3 || _la==FROM) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(2612);
				((SubstringContext)_localctx).pos = valueExpression(0);
				setState(2615);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__3 || _la==FOR) {
					{
					setState(2613);
					_la = _input.LA(1);
					if ( !(_la==T__3 || _la==FOR) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(2614);
					((SubstringContext)_localctx).len = valueExpression(0);
					}
				}

				setState(2617);
				match(T__2);
				}
				break;
			case 21:
				{
				_localctx = new TrimContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2619);
				match(TRIM);
				setState(2620);
				match(T__1);
				setState(2622);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,334,_ctx) ) {
				case 1:
					{
					setState(2621);
					((TrimContext)_localctx).trimOption = _input.LT(1);
					_la = _input.LA(1);
					if ( !(_la==BOTH || _la==LEADING || _la==TRAILING) ) {
						((TrimContext)_localctx).trimOption = (Token)_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					break;
				}
				setState(2625);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,335,_ctx) ) {
				case 1:
					{
					setState(2624);
					((TrimContext)_localctx).trimStr = valueExpression(0);
					}
					break;
				}
				setState(2627);
				match(FROM);
				setState(2628);
				((TrimContext)_localctx).srcStr = valueExpression(0);
				setState(2629);
				match(T__2);
				}
				break;
			case 22:
				{
				_localctx = new OverlayContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2631);
				match(OVERLAY);
				setState(2632);
				match(T__1);
				setState(2633);
				((OverlayContext)_localctx).input = valueExpression(0);
				setState(2634);
				match(PLACING);
				setState(2635);
				((OverlayContext)_localctx).replace = valueExpression(0);
				setState(2636);
				match(FROM);
				setState(2637);
				((OverlayContext)_localctx).position = valueExpression(0);
				setState(2640);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FOR) {
					{
					setState(2638);
					match(FOR);
					setState(2639);
					((OverlayContext)_localctx).length = valueExpression(0);
					}
				}

				setState(2642);
				match(T__2);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(2656);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,339,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(2654);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,338,_ctx) ) {
					case 1:
						{
						_localctx = new SubscriptContext(new PrimaryExpressionContext(_parentctx, _parentState));
						((SubscriptContext)_localctx).value = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
						setState(2646);
						if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
						setState(2647);
						match(T__8);
						setState(2648);
						((SubscriptContext)_localctx).index = valueExpression(0);
						setState(2649);
						match(T__9);
						}
						break;
					case 2:
						{
						_localctx = new DereferenceContext(new PrimaryExpressionContext(_parentctx, _parentState));
						((DereferenceContext)_localctx).base = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
						setState(2651);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(2652);
						match(T__4);
						setState(2653);
						((DereferenceContext)_localctx).fieldName = identifier();
						}
						break;
					}
					} 
				}
				setState(2658);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,339,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class ConstantContext extends ParserRuleContext {
		public ConstantContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constant; }
	 
		public ConstantContext() { }
		public void copyFrom(ConstantContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class NullLiteralContext extends ConstantContext {
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public NullLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNullLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNullLiteral(this);
		}
	}
	public static class StringLiteralContext extends ConstantContext {
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public StringLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStringLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStringLiteral(this);
		}
	}
	public static class TypeConstructorContext extends ConstantContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TypeConstructorContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTypeConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTypeConstructor(this);
		}
	}
	public static class IntervalLiteralContext extends ConstantContext {
		public IntervalContext interval() {
			return getRuleContext(IntervalContext.class,0);
		}
		public IntervalLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntervalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntervalLiteral(this);
		}
	}
	public static class NumericLiteralContext extends ConstantContext {
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public NumericLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNumericLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNumericLiteral(this);
		}
	}
	public static class BooleanLiteralContext extends ConstantContext {
		public BooleanValueContext booleanValue() {
			return getRuleContext(BooleanValueContext.class,0);
		}
		public BooleanLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBooleanLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBooleanLiteral(this);
		}
	}

	public final ConstantContext constant() throws RecognitionException {
		ConstantContext _localctx = new ConstantContext(_ctx, getState());
		enterRule(_localctx, 196, RULE_constant);
		try {
			int _alt;
			setState(2671);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,341,_ctx) ) {
			case 1:
				_localctx = new NullLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2659);
				match(NULL);
				}
				break;
			case 2:
				_localctx = new IntervalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2660);
				interval();
				}
				break;
			case 3:
				_localctx = new TypeConstructorContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2661);
				identifier();
				setState(2662);
				match(STRING);
				}
				break;
			case 4:
				_localctx = new NumericLiteralContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2664);
				number();
				}
				break;
			case 5:
				_localctx = new BooleanLiteralContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(2665);
				booleanValue();
				}
				break;
			case 6:
				_localctx = new StringLiteralContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(2667); 
				_errHandler.sync(this);
				_alt = 1;
				do {
					switch (_alt) {
					case 1:
						{
						{
						setState(2666);
						match(STRING);
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					setState(2669); 
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,340,_ctx);
				} while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ComparisonOperatorContext extends ParserRuleContext {
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public TerminalNode NEQ() { return getToken(SqlBaseParser.NEQ, 0); }
		public TerminalNode NEQJ() { return getToken(SqlBaseParser.NEQJ, 0); }
		public TerminalNode LT() { return getToken(SqlBaseParser.LT, 0); }
		public TerminalNode LTE() { return getToken(SqlBaseParser.LTE, 0); }
		public TerminalNode GT() { return getToken(SqlBaseParser.GT, 0); }
		public TerminalNode GTE() { return getToken(SqlBaseParser.GTE, 0); }
		public TerminalNode NSEQ() { return getToken(SqlBaseParser.NSEQ, 0); }
		public ComparisonOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comparisonOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComparisonOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComparisonOperator(this);
		}
	}

	public final ComparisonOperatorContext comparisonOperator() throws RecognitionException {
		ComparisonOperatorContext _localctx = new ComparisonOperatorContext(_ctx, getState());
		enterRule(_localctx, 198, RULE_comparisonOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2673);
			_la = _input.LA(1);
			if ( !(((((_la - 264)) & ~0x3f) == 0 && ((1L << (_la - 264)) & ((1L << (EQ - 264)) | (1L << (NSEQ - 264)) | (1L << (NEQ - 264)) | (1L << (NEQJ - 264)) | (1L << (LT - 264)) | (1L << (LTE - 264)) | (1L << (GT - 264)) | (1L << (GTE - 264)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ArithmeticOperatorContext extends ParserRuleContext {
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public TerminalNode SLASH() { return getToken(SqlBaseParser.SLASH, 0); }
		public TerminalNode PERCENT() { return getToken(SqlBaseParser.PERCENT, 0); }
		public TerminalNode DIV() { return getToken(SqlBaseParser.DIV, 0); }
		public TerminalNode TILDE() { return getToken(SqlBaseParser.TILDE, 0); }
		public TerminalNode AMPERSAND() { return getToken(SqlBaseParser.AMPERSAND, 0); }
		public TerminalNode PIPE() { return getToken(SqlBaseParser.PIPE, 0); }
		public TerminalNode CONCAT_PIPE() { return getToken(SqlBaseParser.CONCAT_PIPE, 0); }
		public TerminalNode HAT() { return getToken(SqlBaseParser.HAT, 0); }
		public ArithmeticOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_arithmeticOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArithmeticOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArithmeticOperator(this);
		}
	}

	public final ArithmeticOperatorContext arithmeticOperator() throws RecognitionException {
		ArithmeticOperatorContext _localctx = new ArithmeticOperatorContext(_ctx, getState());
		enterRule(_localctx, 200, RULE_arithmeticOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2675);
			_la = _input.LA(1);
			if ( !(((((_la - 272)) & ~0x3f) == 0 && ((1L << (_la - 272)) & ((1L << (PLUS - 272)) | (1L << (MINUS - 272)) | (1L << (ASTERISK - 272)) | (1L << (SLASH - 272)) | (1L << (PERCENT - 272)) | (1L << (DIV - 272)) | (1L << (TILDE - 272)) | (1L << (AMPERSAND - 272)) | (1L << (PIPE - 272)) | (1L << (CONCAT_PIPE - 272)) | (1L << (HAT - 272)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PredicateOperatorContext extends ParserRuleContext {
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public PredicateOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicateOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPredicateOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPredicateOperator(this);
		}
	}

	public final PredicateOperatorContext predicateOperator() throws RecognitionException {
		PredicateOperatorContext _localctx = new PredicateOperatorContext(_ctx, getState());
		enterRule(_localctx, 202, RULE_predicateOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2677);
			_la = _input.LA(1);
			if ( !(_la==AND || ((((_la - 113)) & ~0x3f) == 0 && ((1L << (_la - 113)) & ((1L << (IN - 113)) | (1L << (NOT - 113)) | (1L << (OR - 113)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BooleanValueContext extends ParserRuleContext {
		public TerminalNode TRUE() { return getToken(SqlBaseParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(SqlBaseParser.FALSE, 0); }
		public BooleanValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBooleanValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBooleanValue(this);
		}
	}

	public final BooleanValueContext booleanValue() throws RecognitionException {
		BooleanValueContext _localctx = new BooleanValueContext(_ctx, getState());
		enterRule(_localctx, 204, RULE_booleanValue);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2679);
			_la = _input.LA(1);
			if ( !(_la==FALSE || _la==TRUE) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IntervalContext extends ParserRuleContext {
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public ErrorCapturingMultiUnitsIntervalContext errorCapturingMultiUnitsInterval() {
			return getRuleContext(ErrorCapturingMultiUnitsIntervalContext.class,0);
		}
		public ErrorCapturingUnitToUnitIntervalContext errorCapturingUnitToUnitInterval() {
			return getRuleContext(ErrorCapturingUnitToUnitIntervalContext.class,0);
		}
		public IntervalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_interval; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInterval(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInterval(this);
		}
	}

	public final IntervalContext interval() throws RecognitionException {
		IntervalContext _localctx = new IntervalContext(_ctx, getState());
		enterRule(_localctx, 206, RULE_interval);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2681);
			match(INTERVAL);
			setState(2684);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,342,_ctx) ) {
			case 1:
				{
				setState(2682);
				errorCapturingMultiUnitsInterval();
				}
				break;
			case 2:
				{
				setState(2683);
				errorCapturingUnitToUnitInterval();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ErrorCapturingMultiUnitsIntervalContext extends ParserRuleContext {
		public MultiUnitsIntervalContext multiUnitsInterval() {
			return getRuleContext(MultiUnitsIntervalContext.class,0);
		}
		public UnitToUnitIntervalContext unitToUnitInterval() {
			return getRuleContext(UnitToUnitIntervalContext.class,0);
		}
		public ErrorCapturingMultiUnitsIntervalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_errorCapturingMultiUnitsInterval; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterErrorCapturingMultiUnitsInterval(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitErrorCapturingMultiUnitsInterval(this);
		}
	}

	public final ErrorCapturingMultiUnitsIntervalContext errorCapturingMultiUnitsInterval() throws RecognitionException {
		ErrorCapturingMultiUnitsIntervalContext _localctx = new ErrorCapturingMultiUnitsIntervalContext(_ctx, getState());
		enterRule(_localctx, 208, RULE_errorCapturingMultiUnitsInterval);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2686);
			multiUnitsInterval();
			setState(2688);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,343,_ctx) ) {
			case 1:
				{
				setState(2687);
				unitToUnitInterval();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MultiUnitsIntervalContext extends ParserRuleContext {
		public List<IntervalValueContext> intervalValue() {
			return getRuleContexts(IntervalValueContext.class);
		}
		public IntervalValueContext intervalValue(int i) {
			return getRuleContext(IntervalValueContext.class,i);
		}
		public List<IntervalUnitContext> intervalUnit() {
			return getRuleContexts(IntervalUnitContext.class);
		}
		public IntervalUnitContext intervalUnit(int i) {
			return getRuleContext(IntervalUnitContext.class,i);
		}
		public MultiUnitsIntervalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multiUnitsInterval; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMultiUnitsInterval(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMultiUnitsInterval(this);
		}
	}

	public final MultiUnitsIntervalContext multiUnitsInterval() throws RecognitionException {
		MultiUnitsIntervalContext _localctx = new MultiUnitsIntervalContext(_ctx, getState());
		enterRule(_localctx, 210, RULE_multiUnitsInterval);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2693); 
			_errHandler.sync(this);
			_alt = 1;
			do {
				switch (_alt) {
				case 1:
					{
					{
					setState(2690);
					intervalValue();
					setState(2691);
					intervalUnit();
					}
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(2695); 
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,344,_ctx);
			} while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ErrorCapturingUnitToUnitIntervalContext extends ParserRuleContext {
		public UnitToUnitIntervalContext body;
		public MultiUnitsIntervalContext error1;
		public UnitToUnitIntervalContext error2;
		public List<UnitToUnitIntervalContext> unitToUnitInterval() {
			return getRuleContexts(UnitToUnitIntervalContext.class);
		}
		public UnitToUnitIntervalContext unitToUnitInterval(int i) {
			return getRuleContext(UnitToUnitIntervalContext.class,i);
		}
		public MultiUnitsIntervalContext multiUnitsInterval() {
			return getRuleContext(MultiUnitsIntervalContext.class,0);
		}
		public ErrorCapturingUnitToUnitIntervalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_errorCapturingUnitToUnitInterval; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterErrorCapturingUnitToUnitInterval(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitErrorCapturingUnitToUnitInterval(this);
		}
	}

	public final ErrorCapturingUnitToUnitIntervalContext errorCapturingUnitToUnitInterval() throws RecognitionException {
		ErrorCapturingUnitToUnitIntervalContext _localctx = new ErrorCapturingUnitToUnitIntervalContext(_ctx, getState());
		enterRule(_localctx, 212, RULE_errorCapturingUnitToUnitInterval);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2697);
			((ErrorCapturingUnitToUnitIntervalContext)_localctx).body = unitToUnitInterval();
			setState(2700);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,345,_ctx) ) {
			case 1:
				{
				setState(2698);
				((ErrorCapturingUnitToUnitIntervalContext)_localctx).error1 = multiUnitsInterval();
				}
				break;
			case 2:
				{
				setState(2699);
				((ErrorCapturingUnitToUnitIntervalContext)_localctx).error2 = unitToUnitInterval();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class UnitToUnitIntervalContext extends ParserRuleContext {
		public IntervalValueContext value;
		public IntervalUnitContext from_;
		public IntervalUnitContext to;
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public IntervalValueContext intervalValue() {
			return getRuleContext(IntervalValueContext.class,0);
		}
		public List<IntervalUnitContext> intervalUnit() {
			return getRuleContexts(IntervalUnitContext.class);
		}
		public IntervalUnitContext intervalUnit(int i) {
			return getRuleContext(IntervalUnitContext.class,i);
		}
		public UnitToUnitIntervalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unitToUnitInterval; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnitToUnitInterval(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnitToUnitInterval(this);
		}
	}

	public final UnitToUnitIntervalContext unitToUnitInterval() throws RecognitionException {
		UnitToUnitIntervalContext _localctx = new UnitToUnitIntervalContext(_ctx, getState());
		enterRule(_localctx, 214, RULE_unitToUnitInterval);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2702);
			((UnitToUnitIntervalContext)_localctx).value = intervalValue();
			setState(2703);
			((UnitToUnitIntervalContext)_localctx).from_ = intervalUnit();
			setState(2704);
			match(TO);
			setState(2705);
			((UnitToUnitIntervalContext)_localctx).to = intervalUnit();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IntervalValueContext extends ParserRuleContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public IntervalValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_intervalValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntervalValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntervalValue(this);
		}
	}

	public final IntervalValueContext intervalValue() throws RecognitionException {
		IntervalValueContext _localctx = new IntervalValueContext(_ctx, getState());
		enterRule(_localctx, 216, RULE_intervalValue);
		int _la;
		try {
			setState(2712);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case PLUS:
			case MINUS:
			case INTEGER_VALUE:
			case DECIMAL_VALUE:
				enterOuterAlt(_localctx, 1);
				{
				setState(2708);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PLUS || _la==MINUS) {
					{
					setState(2707);
					_la = _input.LA(1);
					if ( !(_la==PLUS || _la==MINUS) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(2710);
				_la = _input.LA(1);
				if ( !(_la==INTEGER_VALUE || _la==DECIMAL_VALUE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case STRING:
				enterOuterAlt(_localctx, 2);
				{
				setState(2711);
				match(STRING);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IntervalUnitContext extends ParserRuleContext {
		public TerminalNode DAY() { return getToken(SqlBaseParser.DAY, 0); }
		public TerminalNode HOUR() { return getToken(SqlBaseParser.HOUR, 0); }
		public TerminalNode MINUTE() { return getToken(SqlBaseParser.MINUTE, 0); }
		public TerminalNode MONTH() { return getToken(SqlBaseParser.MONTH, 0); }
		public TerminalNode SECOND() { return getToken(SqlBaseParser.SECOND, 0); }
		public TerminalNode YEAR() { return getToken(SqlBaseParser.YEAR, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public IntervalUnitContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_intervalUnit; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntervalUnit(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntervalUnit(this);
		}
	}

	public final IntervalUnitContext intervalUnit() throws RecognitionException {
		IntervalUnitContext _localctx = new IntervalUnitContext(_ctx, getState());
		enterRule(_localctx, 218, RULE_intervalUnit);
		try {
			setState(2721);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,348,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2714);
				match(DAY);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2715);
				match(HOUR);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2716);
				match(MINUTE);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(2717);
				match(MONTH);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(2718);
				match(SECOND);
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(2719);
				match(YEAR);
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(2720);
				identifier();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColPositionContext extends ParserRuleContext {
		public Token position;
		public ErrorCapturingIdentifierContext afterCol;
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public TerminalNode AFTER() { return getToken(SqlBaseParser.AFTER, 0); }
		public ErrorCapturingIdentifierContext errorCapturingIdentifier() {
			return getRuleContext(ErrorCapturingIdentifierContext.class,0);
		}
		public ColPositionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colPosition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColPosition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColPosition(this);
		}
	}

	public final ColPositionContext colPosition() throws RecognitionException {
		ColPositionContext _localctx = new ColPositionContext(_ctx, getState());
		enterRule(_localctx, 220, RULE_colPosition);
		try {
			setState(2726);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FIRST:
				enterOuterAlt(_localctx, 1);
				{
				setState(2723);
				((ColPositionContext)_localctx).position = match(FIRST);
				}
				break;
			case AFTER:
				enterOuterAlt(_localctx, 2);
				{
				setState(2724);
				((ColPositionContext)_localctx).position = match(AFTER);
				setState(2725);
				((ColPositionContext)_localctx).afterCol = errorCapturingIdentifier();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DataTypeContext extends ParserRuleContext {
		public DataTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dataType; }
	 
		public DataTypeContext() { }
		public void copyFrom(DataTypeContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ComplexDataTypeContext extends DataTypeContext {
		public Token complex;
		public List<DataTypeContext> dataType() {
			return getRuleContexts(DataTypeContext.class);
		}
		public DataTypeContext dataType(int i) {
			return getRuleContext(DataTypeContext.class,i);
		}
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode STRUCT() { return getToken(SqlBaseParser.STRUCT, 0); }
		public TerminalNode NEQ() { return getToken(SqlBaseParser.NEQ, 0); }
		public ComplexColTypeListContext complexColTypeList() {
			return getRuleContext(ComplexColTypeListContext.class,0);
		}
		public ComplexDataTypeContext(DataTypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComplexDataType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComplexDataType(this);
		}
	}
	public static class PrimitiveDataTypeContext extends DataTypeContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<TerminalNode> INTEGER_VALUE() { return getTokens(SqlBaseParser.INTEGER_VALUE); }
		public TerminalNode INTEGER_VALUE(int i) {
			return getToken(SqlBaseParser.INTEGER_VALUE, i);
		}
		public PrimitiveDataTypeContext(DataTypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPrimitiveDataType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPrimitiveDataType(this);
		}
	}

	public final DataTypeContext dataType() throws RecognitionException {
		DataTypeContext _localctx = new DataTypeContext(_ctx, getState());
		enterRule(_localctx, 222, RULE_dataType);
		int _la;
		try {
			setState(2762);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,354,_ctx) ) {
			case 1:
				_localctx = new ComplexDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2728);
				((ComplexDataTypeContext)_localctx).complex = match(ARRAY);
				setState(2729);
				match(LT);
				setState(2730);
				dataType();
				setState(2731);
				match(GT);
				}
				break;
			case 2:
				_localctx = new ComplexDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2733);
				((ComplexDataTypeContext)_localctx).complex = match(MAP);
				setState(2734);
				match(LT);
				setState(2735);
				dataType();
				setState(2736);
				match(T__3);
				setState(2737);
				dataType();
				setState(2738);
				match(GT);
				}
				break;
			case 3:
				_localctx = new ComplexDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2740);
				((ComplexDataTypeContext)_localctx).complex = match(STRUCT);
				setState(2747);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case LT:
					{
					setState(2741);
					match(LT);
					setState(2743);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,350,_ctx) ) {
					case 1:
						{
						setState(2742);
						complexColTypeList();
						}
						break;
					}
					setState(2745);
					match(GT);
					}
					break;
				case NEQ:
					{
					setState(2746);
					match(NEQ);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			case 4:
				_localctx = new PrimitiveDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2749);
				identifier();
				setState(2760);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,353,_ctx) ) {
				case 1:
					{
					setState(2750);
					match(T__1);
					setState(2751);
					match(INTEGER_VALUE);
					setState(2756);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(2752);
						match(T__3);
						setState(2753);
						match(INTEGER_VALUE);
						}
						}
						setState(2758);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(2759);
					match(T__2);
					}
					break;
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QualifiedColTypeWithPositionListContext extends ParserRuleContext {
		public List<QualifiedColTypeWithPositionContext> qualifiedColTypeWithPosition() {
			return getRuleContexts(QualifiedColTypeWithPositionContext.class);
		}
		public QualifiedColTypeWithPositionContext qualifiedColTypeWithPosition(int i) {
			return getRuleContext(QualifiedColTypeWithPositionContext.class,i);
		}
		public QualifiedColTypeWithPositionListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_qualifiedColTypeWithPositionList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQualifiedColTypeWithPositionList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQualifiedColTypeWithPositionList(this);
		}
	}

	public final QualifiedColTypeWithPositionListContext qualifiedColTypeWithPositionList() throws RecognitionException {
		QualifiedColTypeWithPositionListContext _localctx = new QualifiedColTypeWithPositionListContext(_ctx, getState());
		enterRule(_localctx, 224, RULE_qualifiedColTypeWithPositionList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2764);
			qualifiedColTypeWithPosition();
			setState(2769);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(2765);
				match(T__3);
				setState(2766);
				qualifiedColTypeWithPosition();
				}
				}
				setState(2771);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QualifiedColTypeWithPositionContext extends ParserRuleContext {
		public MultipartIdentifierContext name;
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public CommentSpecContext commentSpec() {
			return getRuleContext(CommentSpecContext.class,0);
		}
		public ColPositionContext colPosition() {
			return getRuleContext(ColPositionContext.class,0);
		}
		public QualifiedColTypeWithPositionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_qualifiedColTypeWithPosition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQualifiedColTypeWithPosition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQualifiedColTypeWithPosition(this);
		}
	}

	public final QualifiedColTypeWithPositionContext qualifiedColTypeWithPosition() throws RecognitionException {
		QualifiedColTypeWithPositionContext _localctx = new QualifiedColTypeWithPositionContext(_ctx, getState());
		enterRule(_localctx, 226, RULE_qualifiedColTypeWithPosition);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2772);
			((QualifiedColTypeWithPositionContext)_localctx).name = multipartIdentifier();
			setState(2773);
			dataType();
			setState(2776);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==NOT) {
				{
				setState(2774);
				match(NOT);
				setState(2775);
				match(NULL);
				}
			}

			setState(2779);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(2778);
				commentSpec();
				}
			}

			setState(2782);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AFTER || _la==FIRST) {
				{
				setState(2781);
				colPosition();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColTypeListContext extends ParserRuleContext {
		public List<ColTypeContext> colType() {
			return getRuleContexts(ColTypeContext.class);
		}
		public ColTypeContext colType(int i) {
			return getRuleContext(ColTypeContext.class,i);
		}
		public ColTypeListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colTypeList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColTypeList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColTypeList(this);
		}
	}

	public final ColTypeListContext colTypeList() throws RecognitionException {
		ColTypeListContext _localctx = new ColTypeListContext(_ctx, getState());
		enterRule(_localctx, 228, RULE_colTypeList);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2784);
			colType();
			setState(2789);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,359,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2785);
					match(T__3);
					setState(2786);
					colType();
					}
					} 
				}
				setState(2791);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,359,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColTypeContext extends ParserRuleContext {
		public ErrorCapturingIdentifierContext colName;
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public ErrorCapturingIdentifierContext errorCapturingIdentifier() {
			return getRuleContext(ErrorCapturingIdentifierContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public CommentSpecContext commentSpec() {
			return getRuleContext(CommentSpecContext.class,0);
		}
		public ColTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColType(this);
		}
	}

	public final ColTypeContext colType() throws RecognitionException {
		ColTypeContext _localctx = new ColTypeContext(_ctx, getState());
		enterRule(_localctx, 230, RULE_colType);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2792);
			((ColTypeContext)_localctx).colName = errorCapturingIdentifier();
			setState(2793);
			dataType();
			setState(2796);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,360,_ctx) ) {
			case 1:
				{
				setState(2794);
				match(NOT);
				setState(2795);
				match(NULL);
				}
				break;
			}
			setState(2799);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,361,_ctx) ) {
			case 1:
				{
				setState(2798);
				commentSpec();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ComplexColTypeListContext extends ParserRuleContext {
		public List<ComplexColTypeContext> complexColType() {
			return getRuleContexts(ComplexColTypeContext.class);
		}
		public ComplexColTypeContext complexColType(int i) {
			return getRuleContext(ComplexColTypeContext.class,i);
		}
		public ComplexColTypeListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_complexColTypeList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComplexColTypeList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComplexColTypeList(this);
		}
	}

	public final ComplexColTypeListContext complexColTypeList() throws RecognitionException {
		ComplexColTypeListContext _localctx = new ComplexColTypeListContext(_ctx, getState());
		enterRule(_localctx, 232, RULE_complexColTypeList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2801);
			complexColType();
			setState(2806);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(2802);
				match(T__3);
				setState(2803);
				complexColType();
				}
				}
				setState(2808);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ComplexColTypeContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public CommentSpecContext commentSpec() {
			return getRuleContext(CommentSpecContext.class,0);
		}
		public ComplexColTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_complexColType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComplexColType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComplexColType(this);
		}
	}

	public final ComplexColTypeContext complexColType() throws RecognitionException {
		ComplexColTypeContext _localctx = new ComplexColTypeContext(_ctx, getState());
		enterRule(_localctx, 234, RULE_complexColType);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2809);
			identifier();
			setState(2810);
			match(T__10);
			setState(2811);
			dataType();
			setState(2814);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==NOT) {
				{
				setState(2812);
				match(NOT);
				setState(2813);
				match(NULL);
				}
			}

			setState(2817);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(2816);
				commentSpec();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WhenClauseContext extends ParserRuleContext {
		public ExpressionContext condition;
		public ExpressionContext result;
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public WhenClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_whenClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWhenClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWhenClause(this);
		}
	}

	public final WhenClauseContext whenClause() throws RecognitionException {
		WhenClauseContext _localctx = new WhenClauseContext(_ctx, getState());
		enterRule(_localctx, 236, RULE_whenClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2819);
			match(WHEN);
			setState(2820);
			((WhenClauseContext)_localctx).condition = expression();
			setState(2821);
			match(THEN);
			setState(2822);
			((WhenClauseContext)_localctx).result = expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WindowClauseContext extends ParserRuleContext {
		public TerminalNode WINDOW() { return getToken(SqlBaseParser.WINDOW, 0); }
		public List<NamedWindowContext> namedWindow() {
			return getRuleContexts(NamedWindowContext.class);
		}
		public NamedWindowContext namedWindow(int i) {
			return getRuleContext(NamedWindowContext.class,i);
		}
		public WindowClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWindowClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWindowClause(this);
		}
	}

	public final WindowClauseContext windowClause() throws RecognitionException {
		WindowClauseContext _localctx = new WindowClauseContext(_ctx, getState());
		enterRule(_localctx, 238, RULE_windowClause);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2824);
			match(WINDOW);
			setState(2825);
			namedWindow();
			setState(2830);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,365,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2826);
					match(T__3);
					setState(2827);
					namedWindow();
					}
					} 
				}
				setState(2832);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,365,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NamedWindowContext extends ParserRuleContext {
		public ErrorCapturingIdentifierContext name;
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public WindowSpecContext windowSpec() {
			return getRuleContext(WindowSpecContext.class,0);
		}
		public ErrorCapturingIdentifierContext errorCapturingIdentifier() {
			return getRuleContext(ErrorCapturingIdentifierContext.class,0);
		}
		public NamedWindowContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedWindow; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNamedWindow(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNamedWindow(this);
		}
	}

	public final NamedWindowContext namedWindow() throws RecognitionException {
		NamedWindowContext _localctx = new NamedWindowContext(_ctx, getState());
		enterRule(_localctx, 240, RULE_namedWindow);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2833);
			((NamedWindowContext)_localctx).name = errorCapturingIdentifier();
			setState(2834);
			match(AS);
			setState(2835);
			windowSpec();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WindowSpecContext extends ParserRuleContext {
		public WindowSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowSpec; }
	 
		public WindowSpecContext() { }
		public void copyFrom(WindowSpecContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class WindowRefContext extends WindowSpecContext {
		public ErrorCapturingIdentifierContext name;
		public ErrorCapturingIdentifierContext errorCapturingIdentifier() {
			return getRuleContext(ErrorCapturingIdentifierContext.class,0);
		}
		public WindowRefContext(WindowSpecContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWindowRef(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWindowRef(this);
		}
	}
	public static class WindowDefContext extends WindowSpecContext {
		public ExpressionContext expression;
		public List<ExpressionContext> partition = new ArrayList<ExpressionContext>();
		public TerminalNode CLUSTER() { return getToken(SqlBaseParser.CLUSTER, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public WindowFrameContext windowFrame() {
			return getRuleContext(WindowFrameContext.class,0);
		}
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public TerminalNode DISTRIBUTE() { return getToken(SqlBaseParser.DISTRIBUTE, 0); }
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public TerminalNode SORT() { return getToken(SqlBaseParser.SORT, 0); }
		public WindowDefContext(WindowSpecContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWindowDef(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWindowDef(this);
		}
	}

	public final WindowSpecContext windowSpec() throws RecognitionException {
		WindowSpecContext _localctx = new WindowSpecContext(_ctx, getState());
		enterRule(_localctx, 242, RULE_windowSpec);
		int _la;
		try {
			setState(2883);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,373,_ctx) ) {
			case 1:
				_localctx = new WindowRefContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2837);
				((WindowRefContext)_localctx).name = errorCapturingIdentifier();
				}
				break;
			case 2:
				_localctx = new WindowRefContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2838);
				match(T__1);
				setState(2839);
				((WindowRefContext)_localctx).name = errorCapturingIdentifier();
				setState(2840);
				match(T__2);
				}
				break;
			case 3:
				_localctx = new WindowDefContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2842);
				match(T__1);
				setState(2877);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case CLUSTER:
					{
					setState(2843);
					match(CLUSTER);
					setState(2844);
					match(BY);
					setState(2845);
					((WindowDefContext)_localctx).expression = expression();
					((WindowDefContext)_localctx).partition.add(((WindowDefContext)_localctx).expression);
					setState(2850);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(2846);
						match(T__3);
						setState(2847);
						((WindowDefContext)_localctx).expression = expression();
						((WindowDefContext)_localctx).partition.add(((WindowDefContext)_localctx).expression);
						}
						}
						setState(2852);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
					break;
				case T__2:
				case DISTRIBUTE:
				case ORDER:
				case PARTITION:
				case RANGE:
				case ROWS:
				case SORT:
					{
					setState(2863);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==DISTRIBUTE || _la==PARTITION) {
						{
						setState(2853);
						_la = _input.LA(1);
						if ( !(_la==DISTRIBUTE || _la==PARTITION) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(2854);
						match(BY);
						setState(2855);
						((WindowDefContext)_localctx).expression = expression();
						((WindowDefContext)_localctx).partition.add(((WindowDefContext)_localctx).expression);
						setState(2860);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==T__3) {
							{
							{
							setState(2856);
							match(T__3);
							setState(2857);
							((WindowDefContext)_localctx).expression = expression();
							((WindowDefContext)_localctx).partition.add(((WindowDefContext)_localctx).expression);
							}
							}
							setState(2862);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						}
					}

					setState(2875);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ORDER || _la==SORT) {
						{
						setState(2865);
						_la = _input.LA(1);
						if ( !(_la==ORDER || _la==SORT) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(2866);
						match(BY);
						setState(2867);
						sortItem();
						setState(2872);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==T__3) {
							{
							{
							setState(2868);
							match(T__3);
							setState(2869);
							sortItem();
							}
							}
							setState(2874);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						}
					}

					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(2880);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==RANGE || _la==ROWS) {
					{
					setState(2879);
					windowFrame();
					}
				}

				setState(2882);
				match(T__2);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WindowFrameContext extends ParserRuleContext {
		public Token frameType;
		public FrameBoundContext start;
		public FrameBoundContext end;
		public TerminalNode RANGE() { return getToken(SqlBaseParser.RANGE, 0); }
		public List<FrameBoundContext> frameBound() {
			return getRuleContexts(FrameBoundContext.class);
		}
		public FrameBoundContext frameBound(int i) {
			return getRuleContext(FrameBoundContext.class,i);
		}
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public WindowFrameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowFrame; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWindowFrame(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWindowFrame(this);
		}
	}

	public final WindowFrameContext windowFrame() throws RecognitionException {
		WindowFrameContext _localctx = new WindowFrameContext(_ctx, getState());
		enterRule(_localctx, 244, RULE_windowFrame);
		try {
			setState(2901);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,374,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2885);
				((WindowFrameContext)_localctx).frameType = match(RANGE);
				setState(2886);
				((WindowFrameContext)_localctx).start = frameBound();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2887);
				((WindowFrameContext)_localctx).frameType = match(ROWS);
				setState(2888);
				((WindowFrameContext)_localctx).start = frameBound();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2889);
				((WindowFrameContext)_localctx).frameType = match(RANGE);
				setState(2890);
				match(BETWEEN);
				setState(2891);
				((WindowFrameContext)_localctx).start = frameBound();
				setState(2892);
				match(AND);
				setState(2893);
				((WindowFrameContext)_localctx).end = frameBound();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(2895);
				((WindowFrameContext)_localctx).frameType = match(ROWS);
				setState(2896);
				match(BETWEEN);
				setState(2897);
				((WindowFrameContext)_localctx).start = frameBound();
				setState(2898);
				match(AND);
				setState(2899);
				((WindowFrameContext)_localctx).end = frameBound();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FrameBoundContext extends ParserRuleContext {
		public Token boundType;
		public TerminalNode UNBOUNDED() { return getToken(SqlBaseParser.UNBOUNDED, 0); }
		public TerminalNode PRECEDING() { return getToken(SqlBaseParser.PRECEDING, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlBaseParser.FOLLOWING, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public FrameBoundContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_frameBound; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFrameBound(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFrameBound(this);
		}
	}

	public final FrameBoundContext frameBound() throws RecognitionException {
		FrameBoundContext _localctx = new FrameBoundContext(_ctx, getState());
		enterRule(_localctx, 246, RULE_frameBound);
		int _la;
		try {
			setState(2910);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,375,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2903);
				match(UNBOUNDED);
				setState(2904);
				((FrameBoundContext)_localctx).boundType = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==FOLLOWING || _la==PRECEDING) ) {
					((FrameBoundContext)_localctx).boundType = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2905);
				((FrameBoundContext)_localctx).boundType = match(CURRENT);
				setState(2906);
				match(ROW);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2907);
				expression();
				setState(2908);
				((FrameBoundContext)_localctx).boundType = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==FOLLOWING || _la==PRECEDING) ) {
					((FrameBoundContext)_localctx).boundType = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QualifiedNameListContext extends ParserRuleContext {
		public List<QualifiedNameContext> qualifiedName() {
			return getRuleContexts(QualifiedNameContext.class);
		}
		public QualifiedNameContext qualifiedName(int i) {
			return getRuleContext(QualifiedNameContext.class,i);
		}
		public QualifiedNameListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_qualifiedNameList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQualifiedNameList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQualifiedNameList(this);
		}
	}

	public final QualifiedNameListContext qualifiedNameList() throws RecognitionException {
		QualifiedNameListContext _localctx = new QualifiedNameListContext(_ctx, getState());
		enterRule(_localctx, 248, RULE_qualifiedNameList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2912);
			qualifiedName();
			setState(2917);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(2913);
				match(T__3);
				setState(2914);
				qualifiedName();
				}
				}
				setState(2919);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FunctionNameContext extends ParserRuleContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode FILTER() { return getToken(SqlBaseParser.FILTER, 0); }
		public TerminalNode LEFT() { return getToken(SqlBaseParser.LEFT, 0); }
		public TerminalNode RIGHT() { return getToken(SqlBaseParser.RIGHT, 0); }
		public FunctionNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_functionName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFunctionName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFunctionName(this);
		}
	}

	public final FunctionNameContext functionName() throws RecognitionException {
		FunctionNameContext _localctx = new FunctionNameContext(_ctx, getState());
		enterRule(_localctx, 250, RULE_functionName);
		try {
			setState(2924);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,377,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2920);
				qualifiedName();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2921);
				match(FILTER);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2922);
				match(LEFT);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(2923);
				match(RIGHT);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QualifiedNameContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public QualifiedNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_qualifiedName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQualifiedName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQualifiedName(this);
		}
	}

	public final QualifiedNameContext qualifiedName() throws RecognitionException {
		QualifiedNameContext _localctx = new QualifiedNameContext(_ctx, getState());
		enterRule(_localctx, 252, RULE_qualifiedName);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2926);
			identifier();
			setState(2931);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,378,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2927);
					match(T__4);
					setState(2928);
					identifier();
					}
					} 
				}
				setState(2933);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,378,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ErrorCapturingIdentifierContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ErrorCapturingIdentifierExtraContext errorCapturingIdentifierExtra() {
			return getRuleContext(ErrorCapturingIdentifierExtraContext.class,0);
		}
		public ErrorCapturingIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_errorCapturingIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterErrorCapturingIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitErrorCapturingIdentifier(this);
		}
	}

	public final ErrorCapturingIdentifierContext errorCapturingIdentifier() throws RecognitionException {
		ErrorCapturingIdentifierContext _localctx = new ErrorCapturingIdentifierContext(_ctx, getState());
		enterRule(_localctx, 254, RULE_errorCapturingIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2934);
			identifier();
			setState(2935);
			errorCapturingIdentifierExtra();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ErrorCapturingIdentifierExtraContext extends ParserRuleContext {
		public ErrorCapturingIdentifierExtraContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_errorCapturingIdentifierExtra; }
	 
		public ErrorCapturingIdentifierExtraContext() { }
		public void copyFrom(ErrorCapturingIdentifierExtraContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ErrorIdentContext extends ErrorCapturingIdentifierExtraContext {
		public List<TerminalNode> MINUS() { return getTokens(SqlBaseParser.MINUS); }
		public TerminalNode MINUS(int i) {
			return getToken(SqlBaseParser.MINUS, i);
		}
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public ErrorIdentContext(ErrorCapturingIdentifierExtraContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterErrorIdent(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitErrorIdent(this);
		}
	}
	public static class RealIdentContext extends ErrorCapturingIdentifierExtraContext {
		public RealIdentContext(ErrorCapturingIdentifierExtraContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRealIdent(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRealIdent(this);
		}
	}

	public final ErrorCapturingIdentifierExtraContext errorCapturingIdentifierExtra() throws RecognitionException {
		ErrorCapturingIdentifierExtraContext _localctx = new ErrorCapturingIdentifierExtraContext(_ctx, getState());
		enterRule(_localctx, 256, RULE_errorCapturingIdentifierExtra);
		try {
			int _alt;
			setState(2944);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,380,_ctx) ) {
			case 1:
				_localctx = new ErrorIdentContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2939); 
				_errHandler.sync(this);
				_alt = 1;
				do {
					switch (_alt) {
					case 1:
						{
						{
						setState(2937);
						match(MINUS);
						setState(2938);
						identifier();
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					setState(2941); 
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,379,_ctx);
				} while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
				}
				break;
			case 2:
				_localctx = new RealIdentContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierContext extends ParserRuleContext {
		public StrictIdentifierContext strictIdentifier() {
			return getRuleContext(StrictIdentifierContext.class,0);
		}
		public StrictNonReservedContext strictNonReserved() {
			return getRuleContext(StrictNonReservedContext.class,0);
		}
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIdentifier(this);
		}
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 258, RULE_identifier);
		try {
			setState(2949);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,381,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2946);
				strictIdentifier();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2947);
				if (!(not self.SQL_standard_keyword_behavior)) throw new FailedPredicateException(this, "not self.SQL_standard_keyword_behavior");
				setState(2948);
				strictNonReserved();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StrictIdentifierContext extends ParserRuleContext {
		public StrictIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_strictIdentifier; }
	 
		public StrictIdentifierContext() { }
		public void copyFrom(StrictIdentifierContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class QuotedIdentifierAlternativeContext extends StrictIdentifierContext {
		public QuotedIdentifierContext quotedIdentifier() {
			return getRuleContext(QuotedIdentifierContext.class,0);
		}
		public QuotedIdentifierAlternativeContext(StrictIdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuotedIdentifierAlternative(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuotedIdentifierAlternative(this);
		}
	}
	public static class UnquotedIdentifierContext extends StrictIdentifierContext {
		public TerminalNode IDENTIFIER() { return getToken(SqlBaseParser.IDENTIFIER, 0); }
		public AnsiNonReservedContext ansiNonReserved() {
			return getRuleContext(AnsiNonReservedContext.class,0);
		}
		public NonReservedContext nonReserved() {
			return getRuleContext(NonReservedContext.class,0);
		}
		public UnquotedIdentifierContext(StrictIdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnquotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnquotedIdentifier(this);
		}
	}

	public final StrictIdentifierContext strictIdentifier() throws RecognitionException {
		StrictIdentifierContext _localctx = new StrictIdentifierContext(_ctx, getState());
		enterRule(_localctx, 260, RULE_strictIdentifier);
		try {
			setState(2957);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,382,_ctx) ) {
			case 1:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2951);
				match(IDENTIFIER);
				}
				break;
			case 2:
				_localctx = new QuotedIdentifierAlternativeContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2952);
				quotedIdentifier();
				}
				break;
			case 3:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2953);
				if (!(self.SQL_standard_keyword_behavior)) throw new FailedPredicateException(this, "self.SQL_standard_keyword_behavior");
				setState(2954);
				ansiNonReserved();
				}
				break;
			case 4:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2955);
				if (!(not self.SQL_standard_keyword_behavior)) throw new FailedPredicateException(this, "not self.SQL_standard_keyword_behavior");
				setState(2956);
				nonReserved();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QuotedIdentifierContext extends ParserRuleContext {
		public TerminalNode BACKQUOTED_IDENTIFIER() { return getToken(SqlBaseParser.BACKQUOTED_IDENTIFIER, 0); }
		public QuotedIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_quotedIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuotedIdentifier(this);
		}
	}

	public final QuotedIdentifierContext quotedIdentifier() throws RecognitionException {
		QuotedIdentifierContext _localctx = new QuotedIdentifierContext(_ctx, getState());
		enterRule(_localctx, 262, RULE_quotedIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2959);
			match(BACKQUOTED_IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NumberContext extends ParserRuleContext {
		public NumberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_number; }
	 
		public NumberContext() { }
		public void copyFrom(NumberContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class DecimalLiteralContext extends NumberContext {
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public DecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDecimalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDecimalLiteral(this);
		}
	}
	public static class BigIntLiteralContext extends NumberContext {
		public TerminalNode BIGINT_LITERAL() { return getToken(SqlBaseParser.BIGINT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public BigIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBigIntLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBigIntLiteral(this);
		}
	}
	public static class TinyIntLiteralContext extends NumberContext {
		public TerminalNode TINYINT_LITERAL() { return getToken(SqlBaseParser.TINYINT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TinyIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTinyIntLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTinyIntLiteral(this);
		}
	}
	public static class LegacyDecimalLiteralContext extends NumberContext {
		public TerminalNode EXPONENT_VALUE() { return getToken(SqlBaseParser.EXPONENT_VALUE, 0); }
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public LegacyDecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLegacyDecimalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLegacyDecimalLiteral(this);
		}
	}
	public static class BigDecimalLiteralContext extends NumberContext {
		public TerminalNode BIGDECIMAL_LITERAL() { return getToken(SqlBaseParser.BIGDECIMAL_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public BigDecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBigDecimalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBigDecimalLiteral(this);
		}
	}
	public static class ExponentLiteralContext extends NumberContext {
		public TerminalNode EXPONENT_VALUE() { return getToken(SqlBaseParser.EXPONENT_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public ExponentLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExponentLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExponentLiteral(this);
		}
	}
	public static class DoubleLiteralContext extends NumberContext {
		public TerminalNode DOUBLE_LITERAL() { return getToken(SqlBaseParser.DOUBLE_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public DoubleLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDoubleLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDoubleLiteral(this);
		}
	}
	public static class IntegerLiteralContext extends NumberContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public IntegerLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntegerLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntegerLiteral(this);
		}
	}
	public static class SmallIntLiteralContext extends NumberContext {
		public TerminalNode SMALLINT_LITERAL() { return getToken(SqlBaseParser.SMALLINT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public SmallIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSmallIntLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSmallIntLiteral(this);
		}
	}

	public final NumberContext number() throws RecognitionException {
		NumberContext _localctx = new NumberContext(_ctx, getState());
		enterRule(_localctx, 264, RULE_number);
		int _la;
		try {
			setState(3000);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,392,_ctx) ) {
			case 1:
				_localctx = new ExponentLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2961);
				if (!(not self.legacy_exponent_literal_as_decimal_enabled)) throw new FailedPredicateException(this, "not self.legacy_exponent_literal_as_decimal_enabled");
				setState(2963);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2962);
					match(MINUS);
					}
				}

				setState(2965);
				match(EXPONENT_VALUE);
				}
				break;
			case 2:
				_localctx = new DecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2966);
				if (!(not self.legacy_exponent_literal_as_decimal_enabled)) throw new FailedPredicateException(this, "not self.legacy_exponent_literal_as_decimal_enabled");
				setState(2968);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2967);
					match(MINUS);
					}
				}

				setState(2970);
				match(DECIMAL_VALUE);
				}
				break;
			case 3:
				_localctx = new LegacyDecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2971);
				if (!(self.legacy_exponent_literal_as_decimal_enabled)) throw new FailedPredicateException(this, "self.legacy_exponent_literal_as_decimal_enabled");
				setState(2973);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2972);
					match(MINUS);
					}
				}

				setState(2975);
				_la = _input.LA(1);
				if ( !(_la==EXPONENT_VALUE || _la==DECIMAL_VALUE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case 4:
				_localctx = new IntegerLiteralContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2977);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2976);
					match(MINUS);
					}
				}

				setState(2979);
				match(INTEGER_VALUE);
				}
				break;
			case 5:
				_localctx = new BigIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(2981);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2980);
					match(MINUS);
					}
				}

				setState(2983);
				match(BIGINT_LITERAL);
				}
				break;
			case 6:
				_localctx = new SmallIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(2985);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2984);
					match(MINUS);
					}
				}

				setState(2987);
				match(SMALLINT_LITERAL);
				}
				break;
			case 7:
				_localctx = new TinyIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(2989);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2988);
					match(MINUS);
					}
				}

				setState(2991);
				match(TINYINT_LITERAL);
				}
				break;
			case 8:
				_localctx = new DoubleLiteralContext(_localctx);
				enterOuterAlt(_localctx, 8);
				{
				setState(2993);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2992);
					match(MINUS);
					}
				}

				setState(2995);
				match(DOUBLE_LITERAL);
				}
				break;
			case 9:
				_localctx = new BigDecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 9);
				{
				setState(2997);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2996);
					match(MINUS);
					}
				}

				setState(2999);
				match(BIGDECIMAL_LITERAL);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AlterColumnActionContext extends ParserRuleContext {
		public Token setOrDrop;
		public TerminalNode TYPE() { return getToken(SqlBaseParser.TYPE, 0); }
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public CommentSpecContext commentSpec() {
			return getRuleContext(CommentSpecContext.class,0);
		}
		public ColPositionContext colPosition() {
			return getRuleContext(ColPositionContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public AlterColumnActionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alterColumnAction; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAlterColumnAction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAlterColumnAction(this);
		}
	}

	public final AlterColumnActionContext alterColumnAction() throws RecognitionException {
		AlterColumnActionContext _localctx = new AlterColumnActionContext(_ctx, getState());
		enterRule(_localctx, 266, RULE_alterColumnAction);
		int _la;
		try {
			setState(3009);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case TYPE:
				enterOuterAlt(_localctx, 1);
				{
				setState(3002);
				match(TYPE);
				setState(3003);
				dataType();
				}
				break;
			case COMMENT:
				enterOuterAlt(_localctx, 2);
				{
				setState(3004);
				commentSpec();
				}
				break;
			case AFTER:
			case FIRST:
				enterOuterAlt(_localctx, 3);
				{
				setState(3005);
				colPosition();
				}
				break;
			case DROP:
			case SET:
				enterOuterAlt(_localctx, 4);
				{
				setState(3006);
				((AlterColumnActionContext)_localctx).setOrDrop = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==DROP || _la==SET) ) {
					((AlterColumnActionContext)_localctx).setOrDrop = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(3007);
				match(NOT);
				setState(3008);
				match(NULL);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AnsiNonReservedContext extends ParserRuleContext {
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode AFTER() { return getToken(SqlBaseParser.AFTER, 0); }
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
		public TerminalNode ARCHIVE() { return getToken(SqlBaseParser.ARCHIVE, 0); }
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode AT() { return getToken(SqlBaseParser.AT, 0); }
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public TerminalNode BUCKET() { return getToken(SqlBaseParser.BUCKET, 0); }
		public TerminalNode BUCKETS() { return getToken(SqlBaseParser.BUCKETS, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public TerminalNode CACHE() { return getToken(SqlBaseParser.CACHE, 0); }
		public TerminalNode CASCADE() { return getToken(SqlBaseParser.CASCADE, 0); }
		public TerminalNode CHANGE() { return getToken(SqlBaseParser.CHANGE, 0); }
		public TerminalNode CLEAR() { return getToken(SqlBaseParser.CLEAR, 0); }
		public TerminalNode CLUSTER() { return getToken(SqlBaseParser.CLUSTER, 0); }
		public TerminalNode CLUSTERED() { return getToken(SqlBaseParser.CLUSTERED, 0); }
		public TerminalNode CODEGEN() { return getToken(SqlBaseParser.CODEGEN, 0); }
		public TerminalNode COLLECTION() { return getToken(SqlBaseParser.COLLECTION, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode COMMIT() { return getToken(SqlBaseParser.COMMIT, 0); }
		public TerminalNode COMPACT() { return getToken(SqlBaseParser.COMPACT, 0); }
		public TerminalNode COMPACTIONS() { return getToken(SqlBaseParser.COMPACTIONS, 0); }
		public TerminalNode COMPUTE() { return getToken(SqlBaseParser.COMPUTE, 0); }
		public TerminalNode CONCATENATE() { return getToken(SqlBaseParser.CONCATENATE, 0); }
		public TerminalNode COST() { return getToken(SqlBaseParser.COST, 0); }
		public TerminalNode CUBE() { return getToken(SqlBaseParser.CUBE, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public TerminalNode DATA() { return getToken(SqlBaseParser.DATA, 0); }
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public TerminalNode DATABASES() { return getToken(SqlBaseParser.DATABASES, 0); }
		public TerminalNode DBPROPERTIES() { return getToken(SqlBaseParser.DBPROPERTIES, 0); }
		public TerminalNode DEFINED() { return getToken(SqlBaseParser.DEFINED, 0); }
		public TerminalNode DELETE() { return getToken(SqlBaseParser.DELETE, 0); }
		public TerminalNode DELIMITED() { return getToken(SqlBaseParser.DELIMITED, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode DFS() { return getToken(SqlBaseParser.DFS, 0); }
		public TerminalNode DIRECTORIES() { return getToken(SqlBaseParser.DIRECTORIES, 0); }
		public TerminalNode DIRECTORY() { return getToken(SqlBaseParser.DIRECTORY, 0); }
		public TerminalNode DISTRIBUTE() { return getToken(SqlBaseParser.DISTRIBUTE, 0); }
		public TerminalNode DIV() { return getToken(SqlBaseParser.DIV, 0); }
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode ESCAPED() { return getToken(SqlBaseParser.ESCAPED, 0); }
		public TerminalNode EXCHANGE() { return getToken(SqlBaseParser.EXCHANGE, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
		public TerminalNode EXPORT() { return getToken(SqlBaseParser.EXPORT, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public TerminalNode EXTERNAL() { return getToken(SqlBaseParser.EXTERNAL, 0); }
		public TerminalNode EXTRACT() { return getToken(SqlBaseParser.EXTRACT, 0); }
		public TerminalNode FIELDS() { return getToken(SqlBaseParser.FIELDS, 0); }
		public TerminalNode FILEFORMAT() { return getToken(SqlBaseParser.FILEFORMAT, 0); }
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlBaseParser.FOLLOWING, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode FORMATTED() { return getToken(SqlBaseParser.FORMATTED, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
		public TerminalNode GLOBAL() { return getToken(SqlBaseParser.GLOBAL, 0); }
		public TerminalNode GROUPING() { return getToken(SqlBaseParser.GROUPING, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode IGNORE() { return getToken(SqlBaseParser.IGNORE, 0); }
		public TerminalNode IMPORT() { return getToken(SqlBaseParser.IMPORT, 0); }
		public TerminalNode INDEX() { return getToken(SqlBaseParser.INDEX, 0); }
		public TerminalNode INDEXES() { return getToken(SqlBaseParser.INDEXES, 0); }
		public TerminalNode INPATH() { return getToken(SqlBaseParser.INPATH, 0); }
		public TerminalNode INPUTFORMAT() { return getToken(SqlBaseParser.INPUTFORMAT, 0); }
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public TerminalNode ITEMS() { return getToken(SqlBaseParser.ITEMS, 0); }
		public TerminalNode KEYS() { return getToken(SqlBaseParser.KEYS, 0); }
		public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
		public TerminalNode LATERAL() { return getToken(SqlBaseParser.LATERAL, 0); }
		public TerminalNode LAZY() { return getToken(SqlBaseParser.LAZY, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode LIMIT() { return getToken(SqlBaseParser.LIMIT, 0); }
		public TerminalNode LINES() { return getToken(SqlBaseParser.LINES, 0); }
		public TerminalNode LIST() { return getToken(SqlBaseParser.LIST, 0); }
		public TerminalNode LOAD() { return getToken(SqlBaseParser.LOAD, 0); }
		public TerminalNode LOCAL() { return getToken(SqlBaseParser.LOCAL, 0); }
		public TerminalNode LOCATION() { return getToken(SqlBaseParser.LOCATION, 0); }
		public TerminalNode LOCK() { return getToken(SqlBaseParser.LOCK, 0); }
		public TerminalNode LOCKS() { return getToken(SqlBaseParser.LOCKS, 0); }
		public TerminalNode LOGICAL() { return getToken(SqlBaseParser.LOGICAL, 0); }
		public TerminalNode MACRO() { return getToken(SqlBaseParser.MACRO, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode MATCHED() { return getToken(SqlBaseParser.MATCHED, 0); }
		public TerminalNode MERGE() { return getToken(SqlBaseParser.MERGE, 0); }
		public TerminalNode MSCK() { return getToken(SqlBaseParser.MSCK, 0); }
		public TerminalNode NAMESPACE() { return getToken(SqlBaseParser.NAMESPACE, 0); }
		public TerminalNode NAMESPACES() { return getToken(SqlBaseParser.NAMESPACES, 0); }
		public TerminalNode NO() { return getToken(SqlBaseParser.NO, 0); }
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public TerminalNode OF() { return getToken(SqlBaseParser.OF, 0); }
		public TerminalNode OPTION() { return getToken(SqlBaseParser.OPTION, 0); }
		public TerminalNode OPTIONS() { return getToken(SqlBaseParser.OPTIONS, 0); }
		public TerminalNode OUT() { return getToken(SqlBaseParser.OUT, 0); }
		public TerminalNode OUTPUTFORMAT() { return getToken(SqlBaseParser.OUTPUTFORMAT, 0); }
		public TerminalNode OVER() { return getToken(SqlBaseParser.OVER, 0); }
		public TerminalNode OVERLAY() { return getToken(SqlBaseParser.OVERLAY, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlBaseParser.OVERWRITE, 0); }
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public TerminalNode PARTITIONED() { return getToken(SqlBaseParser.PARTITIONED, 0); }
		public TerminalNode PARTITIONS() { return getToken(SqlBaseParser.PARTITIONS, 0); }
		public TerminalNode PERCENTLIT() { return getToken(SqlBaseParser.PERCENTLIT, 0); }
		public TerminalNode PIVOT() { return getToken(SqlBaseParser.PIVOT, 0); }
		public TerminalNode PLACING() { return getToken(SqlBaseParser.PLACING, 0); }
		public TerminalNode POSITION() { return getToken(SqlBaseParser.POSITION, 0); }
		public TerminalNode PRECEDING() { return getToken(SqlBaseParser.PRECEDING, 0); }
		public TerminalNode PRINCIPALS() { return getToken(SqlBaseParser.PRINCIPALS, 0); }
		public TerminalNode PROPERTIES() { return getToken(SqlBaseParser.PROPERTIES, 0); }
		public TerminalNode PURGE() { return getToken(SqlBaseParser.PURGE, 0); }
		public TerminalNode QUERY() { return getToken(SqlBaseParser.QUERY, 0); }
		public TerminalNode RANGE() { return getToken(SqlBaseParser.RANGE, 0); }
		public TerminalNode RECORDREADER() { return getToken(SqlBaseParser.RECORDREADER, 0); }
		public TerminalNode RECORDWRITER() { return getToken(SqlBaseParser.RECORDWRITER, 0); }
		public TerminalNode RECOVER() { return getToken(SqlBaseParser.RECOVER, 0); }
		public TerminalNode REDUCE() { return getToken(SqlBaseParser.REDUCE, 0); }
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode REPAIR() { return getToken(SqlBaseParser.REPAIR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode RESET() { return getToken(SqlBaseParser.RESET, 0); }
		public TerminalNode RESTRICT() { return getToken(SqlBaseParser.RESTRICT, 0); }
		public TerminalNode REVOKE() { return getToken(SqlBaseParser.REVOKE, 0); }
		public TerminalNode RLIKE() { return getToken(SqlBaseParser.RLIKE, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public TerminalNode ROLES() { return getToken(SqlBaseParser.ROLES, 0); }
		public TerminalNode ROLLBACK() { return getToken(SqlBaseParser.ROLLBACK, 0); }
		public TerminalNode ROLLUP() { return getToken(SqlBaseParser.ROLLUP, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public TerminalNode SEPARATED() { return getToken(SqlBaseParser.SEPARATED, 0); }
		public TerminalNode SERDE() { return getToken(SqlBaseParser.SERDE, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlBaseParser.SERDEPROPERTIES, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode SETS() { return getToken(SqlBaseParser.SETS, 0); }
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode SKEWED() { return getToken(SqlBaseParser.SKEWED, 0); }
		public TerminalNode SORT() { return getToken(SqlBaseParser.SORT, 0); }
		public TerminalNode SORTED() { return getToken(SqlBaseParser.SORTED, 0); }
		public TerminalNode START() { return getToken(SqlBaseParser.START, 0); }
		public TerminalNode STATISTICS() { return getToken(SqlBaseParser.STATISTICS, 0); }
		public TerminalNode STORED() { return getToken(SqlBaseParser.STORED, 0); }
		public TerminalNode STRATIFY() { return getToken(SqlBaseParser.STRATIFY, 0); }
		public TerminalNode STRUCT() { return getToken(SqlBaseParser.STRUCT, 0); }
		public TerminalNode SUBSTR() { return getToken(SqlBaseParser.SUBSTR, 0); }
		public TerminalNode SUBSTRING() { return getToken(SqlBaseParser.SUBSTRING, 0); }
		public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
		public TerminalNode TABLESAMPLE() { return getToken(SqlBaseParser.TABLESAMPLE, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode TERMINATED() { return getToken(SqlBaseParser.TERMINATED, 0); }
		public TerminalNode TOUCH() { return getToken(SqlBaseParser.TOUCH, 0); }
		public TerminalNode TRANSACTION() { return getToken(SqlBaseParser.TRANSACTION, 0); }
		public TerminalNode TRANSACTIONS() { return getToken(SqlBaseParser.TRANSACTIONS, 0); }
		public TerminalNode TRANSFORM() { return getToken(SqlBaseParser.TRANSFORM, 0); }
		public TerminalNode TRIM() { return getToken(SqlBaseParser.TRIM, 0); }
		public TerminalNode TRUE() { return getToken(SqlBaseParser.TRUE, 0); }
		public TerminalNode TRUNCATE() { return getToken(SqlBaseParser.TRUNCATE, 0); }
		public TerminalNode UNARCHIVE() { return getToken(SqlBaseParser.UNARCHIVE, 0); }
		public TerminalNode UNBOUNDED() { return getToken(SqlBaseParser.UNBOUNDED, 0); }
		public TerminalNode UNCACHE() { return getToken(SqlBaseParser.UNCACHE, 0); }
		public TerminalNode UNLOCK() { return getToken(SqlBaseParser.UNLOCK, 0); }
		public TerminalNode UNSET() { return getToken(SqlBaseParser.UNSET, 0); }
		public TerminalNode UPDATE() { return getToken(SqlBaseParser.UPDATE, 0); }
		public TerminalNode USE() { return getToken(SqlBaseParser.USE, 0); }
		public TerminalNode VALUES() { return getToken(SqlBaseParser.VALUES, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode VIEWS() { return getToken(SqlBaseParser.VIEWS, 0); }
		public TerminalNode WINDOW() { return getToken(SqlBaseParser.WINDOW, 0); }
		public AnsiNonReservedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ansiNonReserved; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAnsiNonReserved(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAnsiNonReserved(this);
		}
	}

	public final AnsiNonReservedContext ansiNonReserved() throws RecognitionException {
		AnsiNonReservedContext _localctx = new AnsiNonReservedContext(_ctx, getState());
		enterRule(_localctx, 268, RULE_ansiNonReserved);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3011);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ADD) | (1L << AFTER) | (1L << ALTER) | (1L << ANALYZE) | (1L << ARCHIVE) | (1L << ARRAY) | (1L << ASC) | (1L << AT) | (1L << BETWEEN) | (1L << BUCKET) | (1L << BUCKETS) | (1L << BY) | (1L << CACHE) | (1L << CASCADE) | (1L << CHANGE) | (1L << CLEAR) | (1L << CLUSTER) | (1L << CLUSTERED) | (1L << CODEGEN) | (1L << COLLECTION) | (1L << COLUMNS) | (1L << COMMENT) | (1L << COMMIT) | (1L << COMPACT) | (1L << COMPACTIONS) | (1L << COMPUTE) | (1L << CONCATENATE) | (1L << COST) | (1L << CUBE) | (1L << CURRENT) | (1L << DATA) | (1L << DATABASE) | (1L << DATABASES))) != 0) || ((((_la - 65)) & ~0x3f) == 0 && ((1L << (_la - 65)) & ((1L << (DBPROPERTIES - 65)) | (1L << (DEFINED - 65)) | (1L << (DELETE - 65)) | (1L << (DELIMITED - 65)) | (1L << (DESC - 65)) | (1L << (DESCRIBE - 65)) | (1L << (DFS - 65)) | (1L << (DIRECTORIES - 65)) | (1L << (DIRECTORY - 65)) | (1L << (DISTRIBUTE - 65)) | (1L << (DROP - 65)) | (1L << (ESCAPED - 65)) | (1L << (EXCHANGE - 65)) | (1L << (EXISTS - 65)) | (1L << (EXPLAIN - 65)) | (1L << (EXPORT - 65)) | (1L << (EXTENDED - 65)) | (1L << (EXTERNAL - 65)) | (1L << (EXTRACT - 65)) | (1L << (FIELDS - 65)) | (1L << (FILEFORMAT - 65)) | (1L << (FIRST - 65)) | (1L << (FOLLOWING - 65)) | (1L << (FORMAT - 65)) | (1L << (FORMATTED - 65)) | (1L << (FUNCTION - 65)) | (1L << (FUNCTIONS - 65)) | (1L << (GLOBAL - 65)) | (1L << (GROUPING - 65)) | (1L << (IF - 65)) | (1L << (IGNORE - 65)) | (1L << (IMPORT - 65)) | (1L << (INDEX - 65)) | (1L << (INDEXES - 65)) | (1L << (INPATH - 65)) | (1L << (INPUTFORMAT - 65)) | (1L << (INSERT - 65)) | (1L << (INTERVAL - 65)) | (1L << (ITEMS - 65)) | (1L << (KEYS - 65)) | (1L << (LAST - 65)) | (1L << (LATERAL - 65)))) != 0) || ((((_la - 129)) & ~0x3f) == 0 && ((1L << (_la - 129)) & ((1L << (LAZY - 129)) | (1L << (LIKE - 129)) | (1L << (LIMIT - 129)) | (1L << (LINES - 129)) | (1L << (LIST - 129)) | (1L << (LOAD - 129)) | (1L << (LOCAL - 129)) | (1L << (LOCATION - 129)) | (1L << (LOCK - 129)) | (1L << (LOCKS - 129)) | (1L << (LOGICAL - 129)) | (1L << (MACRO - 129)) | (1L << (MAP - 129)) | (1L << (MATCHED - 129)) | (1L << (MERGE - 129)) | (1L << (MSCK - 129)) | (1L << (NAMESPACE - 129)) | (1L << (NAMESPACES - 129)) | (1L << (NO - 129)) | (1L << (NULLS - 129)) | (1L << (OF - 129)) | (1L << (OPTION - 129)) | (1L << (OPTIONS - 129)) | (1L << (OUT - 129)) | (1L << (OUTPUTFORMAT - 129)) | (1L << (OVER - 129)) | (1L << (OVERLAY - 129)) | (1L << (OVERWRITE - 129)) | (1L << (PARTITION - 129)) | (1L << (PARTITIONED - 129)) | (1L << (PARTITIONS - 129)) | (1L << (PERCENTLIT - 129)) | (1L << (PIVOT - 129)) | (1L << (PLACING - 129)) | (1L << (POSITION - 129)) | (1L << (PRECEDING - 129)) | (1L << (PRINCIPALS - 129)) | (1L << (PROPERTIES - 129)) | (1L << (PURGE - 129)) | (1L << (QUERY - 129)) | (1L << (RANGE - 129)) | (1L << (RECORDREADER - 129)) | (1L << (RECORDWRITER - 129)) | (1L << (RECOVER - 129)) | (1L << (REDUCE - 129)) | (1L << (REFRESH - 129)) | (1L << (RENAME - 129)) | (1L << (REPAIR - 129)) | (1L << (REPLACE - 129)))) != 0) || ((((_la - 193)) & ~0x3f) == 0 && ((1L << (_la - 193)) & ((1L << (RESET - 193)) | (1L << (RESTRICT - 193)) | (1L << (REVOKE - 193)) | (1L << (RLIKE - 193)) | (1L << (ROLE - 193)) | (1L << (ROLES - 193)) | (1L << (ROLLBACK - 193)) | (1L << (ROLLUP - 193)) | (1L << (ROW - 193)) | (1L << (ROWS - 193)) | (1L << (SCHEMA - 193)) | (1L << (SEPARATED - 193)) | (1L << (SERDE - 193)) | (1L << (SERDEPROPERTIES - 193)) | (1L << (SET - 193)) | (1L << (SETS - 193)) | (1L << (SHOW - 193)) | (1L << (SKEWED - 193)) | (1L << (SORT - 193)) | (1L << (SORTED - 193)) | (1L << (START - 193)) | (1L << (STATISTICS - 193)) | (1L << (STORED - 193)) | (1L << (STRATIFY - 193)) | (1L << (STRUCT - 193)) | (1L << (SUBSTR - 193)) | (1L << (SUBSTRING - 193)) | (1L << (TABLES - 193)) | (1L << (TABLESAMPLE - 193)) | (1L << (TBLPROPERTIES - 193)) | (1L << (TEMPORARY - 193)) | (1L << (TERMINATED - 193)) | (1L << (TOUCH - 193)) | (1L << (TRANSACTION - 193)) | (1L << (TRANSACTIONS - 193)) | (1L << (TRANSFORM - 193)) | (1L << (TRIM - 193)) | (1L << (TRUE - 193)) | (1L << (TRUNCATE - 193)) | (1L << (UNARCHIVE - 193)) | (1L << (UNBOUNDED - 193)) | (1L << (UNCACHE - 193)) | (1L << (UNLOCK - 193)) | (1L << (UNSET - 193)) | (1L << (UPDATE - 193)) | (1L << (USE - 193)) | (1L << (VALUES - 193)))) != 0) || ((((_la - 257)) & ~0x3f) == 0 && ((1L << (_la - 257)) & ((1L << (VIEW - 257)) | (1L << (VIEWS - 257)) | (1L << (WINDOW - 257)) | (1L << (DIV - 257)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StrictNonReservedContext extends ParserRuleContext {
		public TerminalNode ANTI() { return getToken(SqlBaseParser.ANTI, 0); }
		public TerminalNode CROSS() { return getToken(SqlBaseParser.CROSS, 0); }
		public TerminalNode EXCEPT() { return getToken(SqlBaseParser.EXCEPT, 0); }
		public TerminalNode FULL() { return getToken(SqlBaseParser.FULL, 0); }
		public TerminalNode INNER() { return getToken(SqlBaseParser.INNER, 0); }
		public TerminalNode INTERSECT() { return getToken(SqlBaseParser.INTERSECT, 0); }
		public TerminalNode JOIN() { return getToken(SqlBaseParser.JOIN, 0); }
		public TerminalNode LEFT() { return getToken(SqlBaseParser.LEFT, 0); }
		public TerminalNode NATURAL() { return getToken(SqlBaseParser.NATURAL, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TerminalNode RIGHT() { return getToken(SqlBaseParser.RIGHT, 0); }
		public TerminalNode SEMI() { return getToken(SqlBaseParser.SEMI, 0); }
		public TerminalNode SETMINUS() { return getToken(SqlBaseParser.SETMINUS, 0); }
		public TerminalNode UNION() { return getToken(SqlBaseParser.UNION, 0); }
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public StrictNonReservedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_strictNonReserved; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStrictNonReserved(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStrictNonReserved(this);
		}
	}

	public final StrictNonReservedContext strictNonReserved() throws RecognitionException {
		StrictNonReservedContext _localctx = new StrictNonReservedContext(_ctx, getState());
		enterRule(_localctx, 270, RULE_strictNonReserved);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3013);
			_la = _input.LA(1);
			if ( !(((((_la - 18)) & ~0x3f) == 0 && ((1L << (_la - 18)) & ((1L << (ANTI - 18)) | (1L << (CROSS - 18)) | (1L << (EXCEPT - 18)))) != 0) || ((((_la - 101)) & ~0x3f) == 0 && ((1L << (_la - 101)) & ((1L << (FULL - 101)) | (1L << (INNER - 101)) | (1L << (INTERSECT - 101)) | (1L << (JOIN - 101)) | (1L << (LEFT - 101)) | (1L << (NATURAL - 101)) | (1L << (ON - 101)))) != 0) || ((((_la - 196)) & ~0x3f) == 0 && ((1L << (_la - 196)) & ((1L << (RIGHT - 196)) | (1L << (SEMI - 196)) | (1L << (SETMINUS - 196)) | (1L << (UNION - 196)) | (1L << (USING - 196)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NonReservedContext extends ParserRuleContext {
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode AFTER() { return getToken(SqlBaseParser.AFTER, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public TerminalNode ANY() { return getToken(SqlBaseParser.ANY, 0); }
		public TerminalNode ARCHIVE() { return getToken(SqlBaseParser.ARCHIVE, 0); }
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode AT() { return getToken(SqlBaseParser.AT, 0); }
		public TerminalNode AUTHORIZATION() { return getToken(SqlBaseParser.AUTHORIZATION, 0); }
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public TerminalNode BOTH() { return getToken(SqlBaseParser.BOTH, 0); }
		public TerminalNode BUCKET() { return getToken(SqlBaseParser.BUCKET, 0); }
		public TerminalNode BUCKETS() { return getToken(SqlBaseParser.BUCKETS, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public TerminalNode CACHE() { return getToken(SqlBaseParser.CACHE, 0); }
		public TerminalNode CASCADE() { return getToken(SqlBaseParser.CASCADE, 0); }
		public TerminalNode CASE() { return getToken(SqlBaseParser.CASE, 0); }
		public TerminalNode CAST() { return getToken(SqlBaseParser.CAST, 0); }
		public TerminalNode CHANGE() { return getToken(SqlBaseParser.CHANGE, 0); }
		public TerminalNode CHECK() { return getToken(SqlBaseParser.CHECK, 0); }
		public TerminalNode CLEAR() { return getToken(SqlBaseParser.CLEAR, 0); }
		public TerminalNode CLUSTER() { return getToken(SqlBaseParser.CLUSTER, 0); }
		public TerminalNode CLUSTERED() { return getToken(SqlBaseParser.CLUSTERED, 0); }
		public TerminalNode CODEGEN() { return getToken(SqlBaseParser.CODEGEN, 0); }
		public TerminalNode COLLATE() { return getToken(SqlBaseParser.COLLATE, 0); }
		public TerminalNode COLLECTION() { return getToken(SqlBaseParser.COLLECTION, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode COMMIT() { return getToken(SqlBaseParser.COMMIT, 0); }
		public TerminalNode COMPACT() { return getToken(SqlBaseParser.COMPACT, 0); }
		public TerminalNode COMPACTIONS() { return getToken(SqlBaseParser.COMPACTIONS, 0); }
		public TerminalNode COMPUTE() { return getToken(SqlBaseParser.COMPUTE, 0); }
		public TerminalNode CONCATENATE() { return getToken(SqlBaseParser.CONCATENATE, 0); }
		public TerminalNode CONSTRAINT() { return getToken(SqlBaseParser.CONSTRAINT, 0); }
		public TerminalNode COST() { return getToken(SqlBaseParser.COST, 0); }
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode CUBE() { return getToken(SqlBaseParser.CUBE, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public TerminalNode CURRENT_DATE() { return getToken(SqlBaseParser.CURRENT_DATE, 0); }
		public TerminalNode CURRENT_TIME() { return getToken(SqlBaseParser.CURRENT_TIME, 0); }
		public TerminalNode CURRENT_TIMESTAMP() { return getToken(SqlBaseParser.CURRENT_TIMESTAMP, 0); }
		public TerminalNode CURRENT_USER() { return getToken(SqlBaseParser.CURRENT_USER, 0); }
		public TerminalNode DATA() { return getToken(SqlBaseParser.DATA, 0); }
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public TerminalNode DATABASES() { return getToken(SqlBaseParser.DATABASES, 0); }
		public TerminalNode DAY() { return getToken(SqlBaseParser.DAY, 0); }
		public TerminalNode DBPROPERTIES() { return getToken(SqlBaseParser.DBPROPERTIES, 0); }
		public TerminalNode DEFINED() { return getToken(SqlBaseParser.DEFINED, 0); }
		public TerminalNode DELETE() { return getToken(SqlBaseParser.DELETE, 0); }
		public TerminalNode DELIMITED() { return getToken(SqlBaseParser.DELIMITED, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode DFS() { return getToken(SqlBaseParser.DFS, 0); }
		public TerminalNode DIRECTORIES() { return getToken(SqlBaseParser.DIRECTORIES, 0); }
		public TerminalNode DIRECTORY() { return getToken(SqlBaseParser.DIRECTORY, 0); }
		public TerminalNode DISTINCT() { return getToken(SqlBaseParser.DISTINCT, 0); }
		public TerminalNode DISTRIBUTE() { return getToken(SqlBaseParser.DISTRIBUTE, 0); }
		public TerminalNode DIV() { return getToken(SqlBaseParser.DIV, 0); }
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode ELSE() { return getToken(SqlBaseParser.ELSE, 0); }
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public TerminalNode ESCAPE() { return getToken(SqlBaseParser.ESCAPE, 0); }
		public TerminalNode ESCAPED() { return getToken(SqlBaseParser.ESCAPED, 0); }
		public TerminalNode EXCHANGE() { return getToken(SqlBaseParser.EXCHANGE, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
		public TerminalNode EXPORT() { return getToken(SqlBaseParser.EXPORT, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public TerminalNode EXTERNAL() { return getToken(SqlBaseParser.EXTERNAL, 0); }
		public TerminalNode EXTRACT() { return getToken(SqlBaseParser.EXTRACT, 0); }
		public TerminalNode FALSE() { return getToken(SqlBaseParser.FALSE, 0); }
		public TerminalNode FETCH() { return getToken(SqlBaseParser.FETCH, 0); }
		public TerminalNode FILTER() { return getToken(SqlBaseParser.FILTER, 0); }
		public TerminalNode FIELDS() { return getToken(SqlBaseParser.FIELDS, 0); }
		public TerminalNode FILEFORMAT() { return getToken(SqlBaseParser.FILEFORMAT, 0); }
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlBaseParser.FOLLOWING, 0); }
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public TerminalNode FOREIGN() { return getToken(SqlBaseParser.FOREIGN, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode FORMATTED() { return getToken(SqlBaseParser.FORMATTED, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
		public TerminalNode GLOBAL() { return getToken(SqlBaseParser.GLOBAL, 0); }
		public TerminalNode GRANT() { return getToken(SqlBaseParser.GRANT, 0); }
		public TerminalNode GROUP() { return getToken(SqlBaseParser.GROUP, 0); }
		public TerminalNode GROUPING() { return getToken(SqlBaseParser.GROUPING, 0); }
		public TerminalNode HAVING() { return getToken(SqlBaseParser.HAVING, 0); }
		public TerminalNode HOUR() { return getToken(SqlBaseParser.HOUR, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode IGNORE() { return getToken(SqlBaseParser.IGNORE, 0); }
		public TerminalNode IMPORT() { return getToken(SqlBaseParser.IMPORT, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public TerminalNode INDEX() { return getToken(SqlBaseParser.INDEX, 0); }
		public TerminalNode INDEXES() { return getToken(SqlBaseParser.INDEXES, 0); }
		public TerminalNode INPATH() { return getToken(SqlBaseParser.INPATH, 0); }
		public TerminalNode INPUTFORMAT() { return getToken(SqlBaseParser.INPUTFORMAT, 0); }
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public TerminalNode ITEMS() { return getToken(SqlBaseParser.ITEMS, 0); }
		public TerminalNode KEYS() { return getToken(SqlBaseParser.KEYS, 0); }
		public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
		public TerminalNode LATERAL() { return getToken(SqlBaseParser.LATERAL, 0); }
		public TerminalNode LAZY() { return getToken(SqlBaseParser.LAZY, 0); }
		public TerminalNode LEADING() { return getToken(SqlBaseParser.LEADING, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode LIMIT() { return getToken(SqlBaseParser.LIMIT, 0); }
		public TerminalNode LINES() { return getToken(SqlBaseParser.LINES, 0); }
		public TerminalNode LIST() { return getToken(SqlBaseParser.LIST, 0); }
		public TerminalNode LOAD() { return getToken(SqlBaseParser.LOAD, 0); }
		public TerminalNode LOCAL() { return getToken(SqlBaseParser.LOCAL, 0); }
		public TerminalNode LOCATION() { return getToken(SqlBaseParser.LOCATION, 0); }
		public TerminalNode LOCK() { return getToken(SqlBaseParser.LOCK, 0); }
		public TerminalNode LOCKS() { return getToken(SqlBaseParser.LOCKS, 0); }
		public TerminalNode LOGICAL() { return getToken(SqlBaseParser.LOGICAL, 0); }
		public TerminalNode MACRO() { return getToken(SqlBaseParser.MACRO, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode MATCHED() { return getToken(SqlBaseParser.MATCHED, 0); }
		public TerminalNode MERGE() { return getToken(SqlBaseParser.MERGE, 0); }
		public TerminalNode MINUTE() { return getToken(SqlBaseParser.MINUTE, 0); }
		public TerminalNode MONTH() { return getToken(SqlBaseParser.MONTH, 0); }
		public TerminalNode MSCK() { return getToken(SqlBaseParser.MSCK, 0); }
		public TerminalNode NAMESPACE() { return getToken(SqlBaseParser.NAMESPACE, 0); }
		public TerminalNode NAMESPACES() { return getToken(SqlBaseParser.NAMESPACES, 0); }
		public TerminalNode NO() { return getToken(SqlBaseParser.NO, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public TerminalNode OF() { return getToken(SqlBaseParser.OF, 0); }
		public TerminalNode ONLY() { return getToken(SqlBaseParser.ONLY, 0); }
		public TerminalNode OPTION() { return getToken(SqlBaseParser.OPTION, 0); }
		public TerminalNode OPTIONS() { return getToken(SqlBaseParser.OPTIONS, 0); }
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public TerminalNode OUT() { return getToken(SqlBaseParser.OUT, 0); }
		public TerminalNode OUTER() { return getToken(SqlBaseParser.OUTER, 0); }
		public TerminalNode OUTPUTFORMAT() { return getToken(SqlBaseParser.OUTPUTFORMAT, 0); }
		public TerminalNode OVER() { return getToken(SqlBaseParser.OVER, 0); }
		public TerminalNode OVERLAPS() { return getToken(SqlBaseParser.OVERLAPS, 0); }
		public TerminalNode OVERLAY() { return getToken(SqlBaseParser.OVERLAY, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlBaseParser.OVERWRITE, 0); }
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public TerminalNode PARTITIONED() { return getToken(SqlBaseParser.PARTITIONED, 0); }
		public TerminalNode PARTITIONS() { return getToken(SqlBaseParser.PARTITIONS, 0); }
		public TerminalNode PERCENTLIT() { return getToken(SqlBaseParser.PERCENTLIT, 0); }
		public TerminalNode PIVOT() { return getToken(SqlBaseParser.PIVOT, 0); }
		public TerminalNode PLACING() { return getToken(SqlBaseParser.PLACING, 0); }
		public TerminalNode POSITION() { return getToken(SqlBaseParser.POSITION, 0); }
		public TerminalNode PRECEDING() { return getToken(SqlBaseParser.PRECEDING, 0); }
		public TerminalNode PRIMARY() { return getToken(SqlBaseParser.PRIMARY, 0); }
		public TerminalNode PRINCIPALS() { return getToken(SqlBaseParser.PRINCIPALS, 0); }
		public TerminalNode PROPERTIES() { return getToken(SqlBaseParser.PROPERTIES, 0); }
		public TerminalNode PURGE() { return getToken(SqlBaseParser.PURGE, 0); }
		public TerminalNode QUERY() { return getToken(SqlBaseParser.QUERY, 0); }
		public TerminalNode RANGE() { return getToken(SqlBaseParser.RANGE, 0); }
		public TerminalNode RECORDREADER() { return getToken(SqlBaseParser.RECORDREADER, 0); }
		public TerminalNode RECORDWRITER() { return getToken(SqlBaseParser.RECORDWRITER, 0); }
		public TerminalNode RECOVER() { return getToken(SqlBaseParser.RECOVER, 0); }
		public TerminalNode REDUCE() { return getToken(SqlBaseParser.REDUCE, 0); }
		public TerminalNode REFERENCES() { return getToken(SqlBaseParser.REFERENCES, 0); }
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode REPAIR() { return getToken(SqlBaseParser.REPAIR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode RESET() { return getToken(SqlBaseParser.RESET, 0); }
		public TerminalNode RESTRICT() { return getToken(SqlBaseParser.RESTRICT, 0); }
		public TerminalNode REVOKE() { return getToken(SqlBaseParser.REVOKE, 0); }
		public TerminalNode RLIKE() { return getToken(SqlBaseParser.RLIKE, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public TerminalNode ROLES() { return getToken(SqlBaseParser.ROLES, 0); }
		public TerminalNode ROLLBACK() { return getToken(SqlBaseParser.ROLLBACK, 0); }
		public TerminalNode ROLLUP() { return getToken(SqlBaseParser.ROLLUP, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public TerminalNode SECOND() { return getToken(SqlBaseParser.SECOND, 0); }
		public TerminalNode SELECT() { return getToken(SqlBaseParser.SELECT, 0); }
		public TerminalNode SEPARATED() { return getToken(SqlBaseParser.SEPARATED, 0); }
		public TerminalNode SERDE() { return getToken(SqlBaseParser.SERDE, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlBaseParser.SERDEPROPERTIES, 0); }
		public TerminalNode SESSION_USER() { return getToken(SqlBaseParser.SESSION_USER, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode SETS() { return getToken(SqlBaseParser.SETS, 0); }
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode SKEWED() { return getToken(SqlBaseParser.SKEWED, 0); }
		public TerminalNode SOME() { return getToken(SqlBaseParser.SOME, 0); }
		public TerminalNode SORT() { return getToken(SqlBaseParser.SORT, 0); }
		public TerminalNode SORTED() { return getToken(SqlBaseParser.SORTED, 0); }
		public TerminalNode START() { return getToken(SqlBaseParser.START, 0); }
		public TerminalNode STATISTICS() { return getToken(SqlBaseParser.STATISTICS, 0); }
		public TerminalNode STORED() { return getToken(SqlBaseParser.STORED, 0); }
		public TerminalNode STRATIFY() { return getToken(SqlBaseParser.STRATIFY, 0); }
		public TerminalNode STRUCT() { return getToken(SqlBaseParser.STRUCT, 0); }
		public TerminalNode SUBSTR() { return getToken(SqlBaseParser.SUBSTR, 0); }
		public TerminalNode SUBSTRING() { return getToken(SqlBaseParser.SUBSTRING, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
		public TerminalNode TABLESAMPLE() { return getToken(SqlBaseParser.TABLESAMPLE, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode TERMINATED() { return getToken(SqlBaseParser.TERMINATED, 0); }
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public TerminalNode TOUCH() { return getToken(SqlBaseParser.TOUCH, 0); }
		public TerminalNode TRAILING() { return getToken(SqlBaseParser.TRAILING, 0); }
		public TerminalNode TRANSACTION() { return getToken(SqlBaseParser.TRANSACTION, 0); }
		public TerminalNode TRANSACTIONS() { return getToken(SqlBaseParser.TRANSACTIONS, 0); }
		public TerminalNode TRANSFORM() { return getToken(SqlBaseParser.TRANSFORM, 0); }
		public TerminalNode TRIM() { return getToken(SqlBaseParser.TRIM, 0); }
		public TerminalNode TRUE() { return getToken(SqlBaseParser.TRUE, 0); }
		public TerminalNode TRUNCATE() { return getToken(SqlBaseParser.TRUNCATE, 0); }
		public TerminalNode TYPE() { return getToken(SqlBaseParser.TYPE, 0); }
		public TerminalNode UNARCHIVE() { return getToken(SqlBaseParser.UNARCHIVE, 0); }
		public TerminalNode UNBOUNDED() { return getToken(SqlBaseParser.UNBOUNDED, 0); }
		public TerminalNode UNCACHE() { return getToken(SqlBaseParser.UNCACHE, 0); }
		public TerminalNode UNIQUE() { return getToken(SqlBaseParser.UNIQUE, 0); }
		public TerminalNode UNKNOWN() { return getToken(SqlBaseParser.UNKNOWN, 0); }
		public TerminalNode UNLOCK() { return getToken(SqlBaseParser.UNLOCK, 0); }
		public TerminalNode UNSET() { return getToken(SqlBaseParser.UNSET, 0); }
		public TerminalNode UPDATE() { return getToken(SqlBaseParser.UPDATE, 0); }
		public TerminalNode USE() { return getToken(SqlBaseParser.USE, 0); }
		public TerminalNode USER() { return getToken(SqlBaseParser.USER, 0); }
		public TerminalNode VALUES() { return getToken(SqlBaseParser.VALUES, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode VIEWS() { return getToken(SqlBaseParser.VIEWS, 0); }
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public TerminalNode WINDOW() { return getToken(SqlBaseParser.WINDOW, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode YEAR() { return getToken(SqlBaseParser.YEAR, 0); }
		public NonReservedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nonReserved; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNonReserved(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNonReserved(this);
		}
	}

	public final NonReservedContext nonReserved() throws RecognitionException {
		NonReservedContext _localctx = new NonReservedContext(_ctx, getState());
		enterRule(_localctx, 272, RULE_nonReserved);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3015);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ADD) | (1L << AFTER) | (1L << ALL) | (1L << ALTER) | (1L << ANALYZE) | (1L << AND) | (1L << ANY) | (1L << ARCHIVE) | (1L << ARRAY) | (1L << AS) | (1L << ASC) | (1L << AT) | (1L << AUTHORIZATION) | (1L << BETWEEN) | (1L << BOTH) | (1L << BUCKET) | (1L << BUCKETS) | (1L << BY) | (1L << CACHE) | (1L << CASCADE) | (1L << CASE) | (1L << CAST) | (1L << CHANGE) | (1L << CHECK) | (1L << CLEAR) | (1L << CLUSTER) | (1L << CLUSTERED) | (1L << CODEGEN) | (1L << COLLATE) | (1L << COLLECTION) | (1L << COLUMN) | (1L << COLUMNS) | (1L << COMMENT) | (1L << COMMIT) | (1L << COMPACT) | (1L << COMPACTIONS) | (1L << COMPUTE) | (1L << CONCATENATE) | (1L << CONSTRAINT) | (1L << COST) | (1L << CREATE) | (1L << CUBE) | (1L << CURRENT) | (1L << CURRENT_DATE) | (1L << CURRENT_TIME) | (1L << CURRENT_TIMESTAMP) | (1L << CURRENT_USER) | (1L << DATA) | (1L << DATABASE) | (1L << DATABASES))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (DAY - 64)) | (1L << (DBPROPERTIES - 64)) | (1L << (DEFINED - 64)) | (1L << (DELETE - 64)) | (1L << (DELIMITED - 64)) | (1L << (DESC - 64)) | (1L << (DESCRIBE - 64)) | (1L << (DFS - 64)) | (1L << (DIRECTORIES - 64)) | (1L << (DIRECTORY - 64)) | (1L << (DISTINCT - 64)) | (1L << (DISTRIBUTE - 64)) | (1L << (DROP - 64)) | (1L << (ELSE - 64)) | (1L << (END - 64)) | (1L << (ESCAPE - 64)) | (1L << (ESCAPED - 64)) | (1L << (EXCHANGE - 64)) | (1L << (EXISTS - 64)) | (1L << (EXPLAIN - 64)) | (1L << (EXPORT - 64)) | (1L << (EXTENDED - 64)) | (1L << (EXTERNAL - 64)) | (1L << (EXTRACT - 64)) | (1L << (FALSE - 64)) | (1L << (FETCH - 64)) | (1L << (FIELDS - 64)) | (1L << (FILTER - 64)) | (1L << (FILEFORMAT - 64)) | (1L << (FIRST - 64)) | (1L << (FOLLOWING - 64)) | (1L << (FOR - 64)) | (1L << (FOREIGN - 64)) | (1L << (FORMAT - 64)) | (1L << (FORMATTED - 64)) | (1L << (FROM - 64)) | (1L << (FUNCTION - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (GLOBAL - 64)) | (1L << (GRANT - 64)) | (1L << (GROUP - 64)) | (1L << (GROUPING - 64)) | (1L << (HAVING - 64)) | (1L << (HOUR - 64)) | (1L << (IF - 64)) | (1L << (IGNORE - 64)) | (1L << (IMPORT - 64)) | (1L << (IN - 64)) | (1L << (INDEX - 64)) | (1L << (INDEXES - 64)) | (1L << (INPATH - 64)) | (1L << (INPUTFORMAT - 64)) | (1L << (INSERT - 64)) | (1L << (INTERVAL - 64)) | (1L << (INTO - 64)) | (1L << (IS - 64)) | (1L << (ITEMS - 64)) | (1L << (KEYS - 64)) | (1L << (LAST - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (LATERAL - 128)) | (1L << (LAZY - 128)) | (1L << (LEADING - 128)) | (1L << (LIKE - 128)) | (1L << (LIMIT - 128)) | (1L << (LINES - 128)) | (1L << (LIST - 128)) | (1L << (LOAD - 128)) | (1L << (LOCAL - 128)) | (1L << (LOCATION - 128)) | (1L << (LOCK - 128)) | (1L << (LOCKS - 128)) | (1L << (LOGICAL - 128)) | (1L << (MACRO - 128)) | (1L << (MAP - 128)) | (1L << (MATCHED - 128)) | (1L << (MERGE - 128)) | (1L << (MINUTE - 128)) | (1L << (MONTH - 128)) | (1L << (MSCK - 128)) | (1L << (NAMESPACE - 128)) | (1L << (NAMESPACES - 128)) | (1L << (NO - 128)) | (1L << (NOT - 128)) | (1L << (NULL - 128)) | (1L << (NULLS - 128)) | (1L << (OF - 128)) | (1L << (ONLY - 128)) | (1L << (OPTION - 128)) | (1L << (OPTIONS - 128)) | (1L << (OR - 128)) | (1L << (ORDER - 128)) | (1L << (OUT - 128)) | (1L << (OUTER - 128)) | (1L << (OUTPUTFORMAT - 128)) | (1L << (OVER - 128)) | (1L << (OVERLAPS - 128)) | (1L << (OVERLAY - 128)) | (1L << (OVERWRITE - 128)) | (1L << (PARTITION - 128)) | (1L << (PARTITIONED - 128)) | (1L << (PARTITIONS - 128)) | (1L << (PERCENTLIT - 128)) | (1L << (PIVOT - 128)) | (1L << (PLACING - 128)) | (1L << (POSITION - 128)) | (1L << (PRECEDING - 128)) | (1L << (PRIMARY - 128)) | (1L << (PRINCIPALS - 128)) | (1L << (PROPERTIES - 128)) | (1L << (PURGE - 128)) | (1L << (QUERY - 128)) | (1L << (RANGE - 128)) | (1L << (RECORDREADER - 128)) | (1L << (RECORDWRITER - 128)) | (1L << (RECOVER - 128)) | (1L << (REDUCE - 128)) | (1L << (REFERENCES - 128)) | (1L << (REFRESH - 128)) | (1L << (RENAME - 128)) | (1L << (REPAIR - 128)))) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & ((1L << (REPLACE - 192)) | (1L << (RESET - 192)) | (1L << (RESTRICT - 192)) | (1L << (REVOKE - 192)) | (1L << (RLIKE - 192)) | (1L << (ROLE - 192)) | (1L << (ROLES - 192)) | (1L << (ROLLBACK - 192)) | (1L << (ROLLUP - 192)) | (1L << (ROW - 192)) | (1L << (ROWS - 192)) | (1L << (SCHEMA - 192)) | (1L << (SECOND - 192)) | (1L << (SELECT - 192)) | (1L << (SEPARATED - 192)) | (1L << (SERDE - 192)) | (1L << (SERDEPROPERTIES - 192)) | (1L << (SESSION_USER - 192)) | (1L << (SET - 192)) | (1L << (SETS - 192)) | (1L << (SHOW - 192)) | (1L << (SKEWED - 192)) | (1L << (SOME - 192)) | (1L << (SORT - 192)) | (1L << (SORTED - 192)) | (1L << (START - 192)) | (1L << (STATISTICS - 192)) | (1L << (STORED - 192)) | (1L << (STRATIFY - 192)) | (1L << (STRUCT - 192)) | (1L << (SUBSTR - 192)) | (1L << (SUBSTRING - 192)) | (1L << (TABLE - 192)) | (1L << (TABLES - 192)) | (1L << (TABLESAMPLE - 192)) | (1L << (TBLPROPERTIES - 192)) | (1L << (TEMPORARY - 192)) | (1L << (TERMINATED - 192)) | (1L << (THEN - 192)) | (1L << (TO - 192)) | (1L << (TOUCH - 192)) | (1L << (TRAILING - 192)) | (1L << (TRANSACTION - 192)) | (1L << (TRANSACTIONS - 192)) | (1L << (TRANSFORM - 192)) | (1L << (TRIM - 192)) | (1L << (TRUE - 192)) | (1L << (TRUNCATE - 192)) | (1L << (TYPE - 192)) | (1L << (UNARCHIVE - 192)) | (1L << (UNBOUNDED - 192)) | (1L << (UNCACHE - 192)) | (1L << (UNIQUE - 192)) | (1L << (UNKNOWN - 192)) | (1L << (UNLOCK - 192)) | (1L << (UNSET - 192)) | (1L << (UPDATE - 192)) | (1L << (USE - 192)) | (1L << (USER - 192)))) != 0) || ((((_la - 256)) & ~0x3f) == 0 && ((1L << (_la - 256)) & ((1L << (VALUES - 256)) | (1L << (VIEW - 256)) | (1L << (VIEWS - 256)) | (1L << (WHEN - 256)) | (1L << (WHERE - 256)) | (1L << (WINDOW - 256)) | (1L << (WITH - 256)) | (1L << (YEAR - 256)) | (1L << (DIV - 256)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 7:
			return statement_sempred((StatementContext)_localctx, predIndex);
		case 40:
			return queryTerm_sempred((QueryTermContext)_localctx, predIndex);
		case 94:
			return booleanExpression_sempred((BooleanExpressionContext)_localctx, predIndex);
		case 96:
			return valueExpression_sempred((ValueExpressionContext)_localctx, predIndex);
		case 97:
			return primaryExpression_sempred((PrimaryExpressionContext)_localctx, predIndex);
		case 129:
			return identifier_sempred((IdentifierContext)_localctx, predIndex);
		case 130:
			return strictIdentifier_sempred((StrictIdentifierContext)_localctx, predIndex);
		case 132:
			return number_sempred((NumberContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean statement_sempred(StatementContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return not self.legacy_create_hive_table_by_default_enabled;
		case 1:
			return self.legacy_create_hive_table_by_default_enabled;
		}
		return true;
	}
	private boolean queryTerm_sempred(QueryTermContext _localctx, int predIndex) {
		switch (predIndex) {
		case 2:
			return precpred(_ctx, 3);
		case 3:
			return self.legacy_setops_precedence_enbled;
		case 4:
			return precpred(_ctx, 2);
		case 5:
			return not self.legacy_setops_precedence_enbled;
		case 6:
			return precpred(_ctx, 1);
		case 7:
			return not self.legacy_setops_precedence_enbled;
		}
		return true;
	}
	private boolean booleanExpression_sempred(BooleanExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 8:
			return precpred(_ctx, 2);
		case 9:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean valueExpression_sempred(ValueExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 10:
			return precpred(_ctx, 6);
		case 11:
			return precpred(_ctx, 5);
		case 12:
			return precpred(_ctx, 4);
		case 13:
			return precpred(_ctx, 3);
		case 14:
			return precpred(_ctx, 2);
		case 15:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean primaryExpression_sempred(PrimaryExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 16:
			return precpred(_ctx, 8);
		case 17:
			return precpred(_ctx, 6);
		}
		return true;
	}
	private boolean identifier_sempred(IdentifierContext _localctx, int predIndex) {
		switch (predIndex) {
		case 18:
			return not self.SQL_standard_keyword_behavior;
		}
		return true;
	}
	private boolean strictIdentifier_sempred(StrictIdentifierContext _localctx, int predIndex) {
		switch (predIndex) {
		case 19:
			return self.SQL_standard_keyword_behavior;
		case 20:
			return not self.SQL_standard_keyword_behavior;
		}
		return true;
	}
	private boolean number_sempred(NumberContext _localctx, int predIndex) {
		switch (predIndex) {
		case 21:
			return not self.legacy_exponent_literal_as_decimal_enabled;
		case 22:
			return not self.legacy_exponent_literal_as_decimal_enabled;
		case 23:
			return self.legacy_exponent_literal_as_decimal_enabled;
		}
		return true;
	}

	private static final int _serializedATNSegments = 2;
	private static final String _serializedATNSegment0 =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\u012b\u0bcc\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
		"\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t="+
		"\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4I"+
		"\tI\4J\tJ\4K\tK\4L\tL\4M\tM\4N\tN\4O\tO\4P\tP\4Q\tQ\4R\tR\4S\tS\4T\tT"+
		"\4U\tU\4V\tV\4W\tW\4X\tX\4Y\tY\4Z\tZ\4[\t[\4\\\t\\\4]\t]\4^\t^\4_\t_\4"+
		"`\t`\4a\ta\4b\tb\4c\tc\4d\td\4e\te\4f\tf\4g\tg\4h\th\4i\ti\4j\tj\4k\t"+
		"k\4l\tl\4m\tm\4n\tn\4o\to\4p\tp\4q\tq\4r\tr\4s\ts\4t\tt\4u\tu\4v\tv\4"+
		"w\tw\4x\tx\4y\ty\4z\tz\4{\t{\4|\t|\4}\t}\4~\t~\4\177\t\177\4\u0080\t\u0080"+
		"\4\u0081\t\u0081\4\u0082\t\u0082\4\u0083\t\u0083\4\u0084\t\u0084\4\u0085"+
		"\t\u0085\4\u0086\t\u0086\4\u0087\t\u0087\4\u0088\t\u0088\4\u0089\t\u0089"+
		"\4\u008a\t\u008a\3\2\3\2\7\2\u0117\n\2\f\2\16\2\u011a\13\2\3\2\3\2\3\3"+
		"\3\3\3\3\3\4\3\4\3\4\3\5\3\5\3\5\3\6\3\6\3\6\3\7\3\7\3\7\3\b\3\b\3\b\3"+
		"\t\3\t\5\t\u0132\n\t\3\t\3\t\3\t\5\t\u0137\n\t\3\t\3\t\3\t\3\t\3\t\3\t"+
		"\5\t\u013f\n\t\3\t\3\t\3\t\3\t\3\t\3\t\7\t\u0147\n\t\f\t\16\t\u014a\13"+
		"\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t"+
		"\5\t\u015d\n\t\3\t\3\t\5\t\u0161\n\t\3\t\3\t\3\t\3\t\5\t\u0167\n\t\3\t"+
		"\5\t\u016a\n\t\3\t\5\t\u016d\n\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u0175\n\t"+
		"\3\t\5\t\u0178\n\t\3\t\3\t\5\t\u017c\n\t\3\t\5\t\u017f\n\t\3\t\3\t\3\t"+
		"\3\t\3\t\3\t\5\t\u0187\n\t\3\t\3\t\3\t\5\t\u018c\n\t\3\t\5\t\u018f\n\t"+
		"\3\t\3\t\3\t\3\t\3\t\5\t\u0196\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t"+
		"\3\t\5\t\u01a2\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\7\t\u01ab\n\t\f\t\16\t"+
		"\u01ae\13\t\3\t\5\t\u01b1\n\t\3\t\5\t\u01b4\n\t\3\t\3\t\3\t\3\t\3\t\5"+
		"\t\u01bb\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\7\t\u01c6\n\t\f\t\16"+
		"\t\u01c9\13\t\3\t\3\t\3\t\3\t\3\t\5\t\u01d0\n\t\3\t\3\t\3\t\5\t\u01d5"+
		"\n\t\3\t\5\t\u01d8\n\t\3\t\3\t\3\t\3\t\5\t\u01de\n\t\3\t\3\t\3\t\3\t\3"+
		"\t\3\t\3\t\3\t\3\t\5\t\u01e9\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3"+
		"\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t"+
		"\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3"+
		"\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t"+
		"\5\t\u0229\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u0232\n\t\3\t\3\t\5\t\u0236"+
		"\n\t\3\t\3\t\3\t\3\t\5\t\u023c\n\t\3\t\3\t\5\t\u0240\n\t\3\t\3\t\3\t\5"+
		"\t\u0245\n\t\3\t\3\t\3\t\3\t\5\t\u024b\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t"+
		"\3\t\3\t\3\t\5\t\u0257\n\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u025f\n\t\3\t\3"+
		"\t\3\t\3\t\5\t\u0265\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5"+
		"\t\u0272\n\t\3\t\6\t\u0275\n\t\r\t\16\t\u0276\3\t\3\t\3\t\3\t\3\t\3\t"+
		"\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u0287\n\t\3\t\3\t\3\t\7\t\u028c\n"+
		"\t\f\t\16\t\u028f\13\t\3\t\5\t\u0292\n\t\3\t\3\t\3\t\3\t\5\t\u0298\n\t"+
		"\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u02a7\n\t\3\t"+
		"\3\t\5\t\u02ab\n\t\3\t\3\t\3\t\3\t\5\t\u02b1\n\t\3\t\3\t\3\t\3\t\5\t\u02b7"+
		"\n\t\3\t\5\t\u02ba\n\t\3\t\5\t\u02bd\n\t\3\t\3\t\3\t\3\t\5\t\u02c3\n\t"+
		"\3\t\3\t\5\t\u02c7\n\t\3\t\3\t\3\t\3\t\3\t\3\t\7\t\u02cf\n\t\f\t\16\t"+
		"\u02d2\13\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u02da\n\t\3\t\5\t\u02dd\n\t\3"+
		"\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u02e6\n\t\3\t\3\t\3\t\5\t\u02eb\n\t\3\t"+
		"\3\t\3\t\3\t\5\t\u02f1\n\t\3\t\3\t\3\t\3\t\3\t\5\t\u02f8\n\t\3\t\5\t\u02fb"+
		"\n\t\3\t\3\t\3\t\3\t\5\t\u0301\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\7\t\u030a"+
		"\n\t\f\t\16\t\u030d\13\t\5\t\u030f\n\t\3\t\3\t\5\t\u0313\n\t\3\t\3\t\3"+
		"\t\5\t\u0318\n\t\3\t\3\t\3\t\5\t\u031d\n\t\3\t\3\t\3\t\3\t\3\t\5\t\u0324"+
		"\n\t\3\t\5\t\u0327\n\t\3\t\5\t\u032a\n\t\3\t\3\t\3\t\3\t\3\t\5\t\u0331"+
		"\n\t\3\t\3\t\3\t\5\t\u0336\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u033f\n"+
		"\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u0347\n\t\3\t\3\t\3\t\3\t\5\t\u034d\n\t"+
		"\3\t\5\t\u0350\n\t\3\t\5\t\u0353\n\t\3\t\3\t\3\t\3\t\5\t\u0359\n\t\3\t"+
		"\3\t\5\t\u035d\n\t\3\t\3\t\5\t\u0361\n\t\3\t\3\t\5\t\u0365\n\t\5\t\u0367"+
		"\n\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u036f\n\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t"+
		"\u0377\n\t\3\t\3\t\3\t\3\t\5\t\u037d\n\t\3\t\3\t\3\t\3\t\5\t\u0383\n\t"+
		"\3\t\5\t\u0386\n\t\3\t\3\t\5\t\u038a\n\t\3\t\5\t\u038d\n\t\3\t\3\t\5\t"+
		"\u0391\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t"+
		"\3\t\3\t\3\t\3\t\3\t\3\t\7\t\u03a8\n\t\f\t\16\t\u03ab\13\t\5\t\u03ad\n"+
		"\t\3\t\3\t\5\t\u03b1\n\t\3\t\3\t\3\t\3\t\5\t\u03b7\n\t\3\t\5\t\u03ba\n"+
		"\t\3\t\5\t\u03bd\n\t\3\t\3\t\3\t\3\t\5\t\u03c3\n\t\3\t\3\t\3\t\3\t\3\t"+
		"\3\t\5\t\u03cb\n\t\3\t\3\t\3\t\5\t\u03d0\n\t\3\t\3\t\3\t\3\t\5\t\u03d6"+
		"\n\t\3\t\3\t\3\t\3\t\5\t\u03dc\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\7\t"+
		"\u03e6\n\t\f\t\16\t\u03e9\13\t\5\t\u03eb\n\t\3\t\3\t\3\t\7\t\u03f0\n\t"+
		"\f\t\16\t\u03f3\13\t\3\t\3\t\7\t\u03f7\n\t\f\t\16\t\u03fa\13\t\3\t\3\t"+
		"\3\t\7\t\u03ff\n\t\f\t\16\t\u0402\13\t\5\t\u0404\n\t\3\n\3\n\3\n\3\n\3"+
		"\n\3\n\5\n\u040c\n\n\3\n\3\n\5\n\u0410\n\n\3\n\3\n\3\n\3\n\3\n\5\n\u0417"+
		"\n\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3"+
		"\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n"+
		"\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3"+
		"\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n"+
		"\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3"+
		"\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n"+
		"\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\5\n\u048b\n\n\3\n\3\n\3\n\3\n"+
		"\3\n\3\n\5\n\u0493\n\n\3\n\3\n\3\n\3\n\3\n\3\n\5\n\u049b\n\n\3\n\3\n\3"+
		"\n\3\n\3\n\3\n\3\n\5\n\u04a4\n\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\5\n\u04ae"+
		"\n\n\3\13\3\13\5\13\u04b2\n\13\3\13\5\13\u04b5\n\13\3\13\3\13\3\13\3\13"+
		"\5\13\u04bb\n\13\3\13\3\13\3\f\3\f\5\f\u04c1\n\f\3\f\3\f\3\f\3\f\3\r\3"+
		"\r\3\r\3\r\3\r\3\r\5\r\u04cd\n\r\3\r\3\r\3\r\3\r\3\16\3\16\3\16\3\16\3"+
		"\16\3\16\5\16\u04d9\n\16\3\16\3\16\3\16\5\16\u04de\n\16\3\17\3\17\3\17"+
		"\3\20\3\20\3\20\3\21\5\21\u04e7\n\21\3\21\3\21\3\21\3\22\3\22\3\22\5\22"+
		"\u04ef\n\22\3\22\3\22\3\22\3\22\3\22\5\22\u04f6\n\22\5\22\u04f8\n\22\3"+
		"\22\3\22\3\22\5\22\u04fd\n\22\3\22\3\22\5\22\u0501\n\22\3\22\3\22\3\22"+
		"\5\22\u0506\n\22\3\22\3\22\3\22\5\22\u050b\n\22\3\22\3\22\3\22\5\22\u0510"+
		"\n\22\3\22\5\22\u0513\n\22\3\22\3\22\3\22\5\22\u0518\n\22\3\22\3\22\5"+
		"\22\u051c\n\22\3\22\3\22\3\22\5\22\u0521\n\22\5\22\u0523\n\22\3\23\3\23"+
		"\5\23\u0527\n\23\3\24\3\24\3\24\3\24\3\24\7\24\u052e\n\24\f\24\16\24\u0531"+
		"\13\24\3\24\3\24\3\25\3\25\3\25\5\25\u0538\n\25\3\26\3\26\3\27\3\27\3"+
		"\27\3\27\3\27\5\27\u0541\n\27\3\30\3\30\3\30\7\30\u0546\n\30\f\30\16\30"+
		"\u0549\13\30\3\31\3\31\3\31\3\31\7\31\u054f\n\31\f\31\16\31\u0552\13\31"+
		"\3\32\3\32\5\32\u0556\n\32\3\32\5\32\u0559\n\32\3\32\3\32\3\32\3\32\3"+
		"\33\3\33\3\33\3\34\3\34\3\34\3\34\3\34\3\34\3\34\3\34\3\34\3\34\7\34\u056c"+
		"\n\34\f\34\16\34\u056f\13\34\3\35\3\35\3\35\3\35\7\35\u0575\n\35\f\35"+
		"\16\35\u0578\13\35\3\35\3\35\3\36\3\36\5\36\u057e\n\36\3\36\5\36\u0581"+
		"\n\36\3\37\3\37\3\37\7\37\u0586\n\37\f\37\16\37\u0589\13\37\3\37\5\37"+
		"\u058c\n\37\3 \3 \3 \3 \5 \u0592\n \3!\3!\3!\3!\7!\u0598\n!\f!\16!\u059b"+
		"\13!\3!\3!\3\"\3\"\3\"\3\"\7\"\u05a3\n\"\f\"\16\"\u05a6\13\"\3\"\3\"\3"+
		"#\3#\3#\3#\3#\3#\5#\u05b0\n#\3$\3$\3$\3$\3$\5$\u05b7\n$\3%\3%\3%\3%\5"+
		"%\u05bd\n%\3&\3&\3&\3\'\3\'\3\'\3\'\3\'\3\'\6\'\u05c8\n\'\r\'\16\'\u05c9"+
		"\3\'\3\'\3\'\3\'\3\'\5\'\u05d1\n\'\3\'\3\'\3\'\3\'\3\'\5\'\u05d8\n\'\3"+
		"\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\5\'\u05e4\n\'\3\'\3\'\3\'\3\'\7"+
		"\'\u05ea\n\'\f\'\16\'\u05ed\13\'\3\'\7\'\u05f0\n\'\f\'\16\'\u05f3\13\'"+
		"\5\'\u05f5\n\'\3(\3(\3(\3(\3(\7(\u05fc\n(\f(\16(\u05ff\13(\5(\u0601\n"+
		"(\3(\3(\3(\3(\3(\7(\u0608\n(\f(\16(\u060b\13(\5(\u060d\n(\3(\3(\3(\3("+
		"\3(\7(\u0614\n(\f(\16(\u0617\13(\5(\u0619\n(\3(\3(\3(\3(\3(\7(\u0620\n"+
		"(\f(\16(\u0623\13(\5(\u0625\n(\3(\5(\u0628\n(\3(\3(\3(\5(\u062d\n(\5("+
		"\u062f\n(\3)\3)\3)\3*\3*\3*\3*\3*\3*\3*\5*\u063b\n*\3*\3*\3*\3*\3*\5*"+
		"\u0642\n*\3*\3*\3*\3*\3*\5*\u0649\n*\3*\7*\u064c\n*\f*\16*\u064f\13*\3"+
		"+\3+\3+\3+\3+\3+\3+\3+\3+\5+\u065a\n+\3,\3,\5,\u065e\n,\3,\3,\5,\u0662"+
		"\n,\3-\3-\6-\u0666\n-\r-\16-\u0667\3.\3.\5.\u066c\n.\3.\3.\3.\3.\7.\u0672"+
		"\n.\f.\16.\u0675\13.\3.\5.\u0678\n.\3.\5.\u067b\n.\3.\5.\u067e\n.\3.\5"+
		".\u0681\n.\3.\3.\5.\u0685\n.\3/\3/\5/\u0689\n/\3/\5/\u068c\n/\3/\3/\5"+
		"/\u0690\n/\3/\7/\u0693\n/\f/\16/\u0696\13/\3/\5/\u0699\n/\3/\5/\u069c"+
		"\n/\3/\5/\u069f\n/\3/\5/\u06a2\n/\5/\u06a4\n/\3\60\3\60\3\60\3\60\3\60"+
		"\3\60\3\60\3\60\3\60\3\60\5\60\u06b0\n\60\3\60\5\60\u06b3\n\60\3\60\3"+
		"\60\5\60\u06b7\n\60\3\60\3\60\3\60\3\60\3\60\3\60\3\60\3\60\5\60\u06c1"+
		"\n\60\3\60\3\60\5\60\u06c5\n\60\5\60\u06c7\n\60\3\60\5\60\u06ca\n\60\3"+
		"\60\3\60\5\60\u06ce\n\60\3\61\3\61\7\61\u06d2\n\61\f\61\16\61\u06d5\13"+
		"\61\3\61\5\61\u06d8\n\61\3\61\3\61\3\62\3\62\3\62\3\63\3\63\3\63\3\63"+
		"\5\63\u06e3\n\63\3\63\3\63\3\63\3\64\3\64\3\64\3\64\3\64\5\64\u06ed\n"+
		"\64\3\64\3\64\3\64\3\65\3\65\3\65\3\65\3\65\3\65\3\65\5\65\u06f9\n\65"+
		"\3\66\3\66\3\66\3\66\3\66\3\66\3\66\3\66\3\66\3\66\3\66\7\66\u0706\n\66"+
		"\f\66\16\66\u0709\13\66\3\66\3\66\5\66\u070d\n\66\3\67\3\67\3\67\7\67"+
		"\u0712\n\67\f\67\16\67\u0715\13\67\38\38\38\38\39\39\39\3:\3:\3:\3;\3"+
		";\3;\5;\u0724\n;\3;\7;\u0727\n;\f;\16;\u072a\13;\3;\3;\3<\3<\3<\3<\3<"+
		"\3<\7<\u0734\n<\f<\16<\u0737\13<\3<\3<\5<\u073b\n<\3=\3=\3=\3=\7=\u0741"+
		"\n=\f=\16=\u0744\13=\3=\7=\u0747\n=\f=\16=\u074a\13=\3=\5=\u074d\n=\3"+
		">\3>\3>\3>\3>\7>\u0754\n>\f>\16>\u0757\13>\3>\3>\3>\3>\3>\3>\3>\3>\3>"+
		"\3>\7>\u0763\n>\f>\16>\u0766\13>\3>\3>\5>\u076a\n>\3>\3>\3>\3>\3>\3>\3"+
		">\3>\7>\u0774\n>\f>\16>\u0777\13>\3>\3>\5>\u077b\n>\3?\3?\3?\3?\7?\u0781"+
		"\n?\f?\16?\u0784\13?\5?\u0786\n?\3?\3?\5?\u078a\n?\3@\3@\3@\3@\3@\3@\3"+
		"@\3@\3@\3@\7@\u0796\n@\f@\16@\u0799\13@\3@\3@\3@\3A\3A\3A\3A\3A\7A\u07a3"+
		"\nA\fA\16A\u07a6\13A\3A\3A\5A\u07aa\nA\3B\3B\5B\u07ae\nB\3B\5B\u07b1\n"+
		"B\3C\3C\3C\5C\u07b6\nC\3C\3C\3C\3C\3C\7C\u07bd\nC\fC\16C\u07c0\13C\5C"+
		"\u07c2\nC\3C\3C\3C\5C\u07c7\nC\3C\3C\3C\7C\u07cc\nC\fC\16C\u07cf\13C\5"+
		"C\u07d1\nC\3D\3D\3E\3E\7E\u07d7\nE\fE\16E\u07da\13E\3F\3F\3F\3F\5F\u07e0"+
		"\nF\3F\3F\3F\3F\3F\5F\u07e7\nF\3G\5G\u07ea\nG\3G\3G\3G\5G\u07ef\nG\3G"+
		"\5G\u07f2\nG\3G\3G\3G\5G\u07f7\nG\3G\3G\5G\u07fb\nG\3G\5G\u07fe\nG\3G"+
		"\5G\u0801\nG\3H\3H\3H\3H\5H\u0807\nH\3I\3I\3I\5I\u080c\nI\3I\3I\3J\5J"+
		"\u0811\nJ\3J\3J\3J\3J\3J\3J\3J\3J\3J\3J\3J\3J\3J\3J\3J\3J\5J\u0823\nJ"+
		"\5J\u0825\nJ\3J\5J\u0828\nJ\3K\3K\3K\3K\3L\3L\3L\7L\u0831\nL\fL\16L\u0834"+
		"\13L\3M\3M\3M\3M\7M\u083a\nM\fM\16M\u083d\13M\3M\3M\3N\3N\5N\u0843\nN"+
		"\3O\3O\3O\3O\7O\u0849\nO\fO\16O\u084c\13O\3O\3O\3P\3P\5P\u0852\nP\3Q\3"+
		"Q\5Q\u0856\nQ\3Q\3Q\3Q\3Q\3Q\3Q\5Q\u085e\nQ\3Q\3Q\3Q\3Q\3Q\3Q\5Q\u0866"+
		"\nQ\3Q\3Q\3Q\3Q\5Q\u086c\nQ\3R\3R\3R\3R\7R\u0872\nR\fR\16R\u0875\13R\3"+
		"R\3R\3S\3S\3S\3S\3S\7S\u087e\nS\fS\16S\u0881\13S\5S\u0883\nS\3S\3S\3S"+
		"\3T\5T\u0889\nT\3T\3T\5T\u088d\nT\5T\u088f\nT\3U\3U\3U\3U\3U\3U\3U\5U"+
		"\u0898\nU\3U\3U\3U\3U\3U\3U\3U\3U\3U\3U\5U\u08a4\nU\5U\u08a6\nU\3U\3U"+
		"\3U\3U\3U\5U\u08ad\nU\3U\3U\3U\3U\3U\5U\u08b4\nU\3U\3U\3U\3U\5U\u08ba"+
		"\nU\3U\3U\3U\3U\5U\u08c0\nU\5U\u08c2\nU\3V\3V\3V\7V\u08c7\nV\fV\16V\u08ca"+
		"\13V\3W\3W\3W\7W\u08cf\nW\fW\16W\u08d2\13W\3X\3X\3X\5X\u08d7\nX\3X\3X"+
		"\3Y\3Y\3Y\5Y\u08de\nY\3Y\3Y\3Z\3Z\5Z\u08e4\nZ\3Z\3Z\5Z\u08e8\nZ\5Z\u08ea"+
		"\nZ\3[\3[\3[\7[\u08ef\n[\f[\16[\u08f2\13[\3\\\3\\\3\\\3\\\7\\\u08f8\n"+
		"\\\f\\\16\\\u08fb\13\\\3\\\3\\\3]\3]\3]\3]\3]\3]\7]\u0905\n]\f]\16]\u0908"+
		"\13]\3]\3]\5]\u090c\n]\3^\3^\5^\u0910\n^\3_\3_\3`\3`\3`\3`\3`\3`\3`\3"+
		"`\3`\3`\5`\u091e\n`\5`\u0920\n`\3`\3`\3`\3`\3`\3`\7`\u0928\n`\f`\16`\u092b"+
		"\13`\3a\5a\u092e\na\3a\3a\3a\3a\3a\3a\5a\u0936\na\3a\3a\3a\3a\3a\7a\u093d"+
		"\na\fa\16a\u0940\13a\3a\3a\3a\5a\u0945\na\3a\3a\3a\3a\3a\3a\5a\u094d\n"+
		"a\3a\3a\3a\5a\u0952\na\3a\3a\3a\3a\3a\3a\3a\3a\7a\u095c\na\fa\16a\u095f"+
		"\13a\3a\3a\5a\u0963\na\3a\5a\u0966\na\3a\3a\3a\3a\5a\u096c\na\3a\3a\5"+
		"a\u0970\na\3a\3a\3a\5a\u0975\na\3a\3a\3a\5a\u097a\na\3a\3a\3a\5a\u097f"+
		"\na\3b\3b\3b\3b\5b\u0985\nb\3b\3b\3b\3b\3b\3b\3b\3b\3b\3b\3b\3b\3b\3b"+
		"\3b\3b\3b\3b\3b\7b\u099a\nb\fb\16b\u099d\13b\3c\3c\3c\3c\6c\u09a3\nc\r"+
		"c\16c\u09a4\3c\3c\5c\u09a9\nc\3c\3c\3c\3c\3c\6c\u09b0\nc\rc\16c\u09b1"+
		"\3c\3c\5c\u09b6\nc\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\7c\u09c6"+
		"\nc\fc\16c\u09c9\13c\5c\u09cb\nc\3c\3c\3c\3c\3c\3c\5c\u09d3\nc\3c\3c\3"+
		"c\3c\3c\3c\3c\5c\u09dc\nc\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3"+
		"c\3c\3c\3c\3c\6c\u09f1\nc\rc\16c\u09f2\3c\3c\3c\3c\3c\3c\3c\3c\3c\5c\u09fe"+
		"\nc\3c\3c\3c\7c\u0a03\nc\fc\16c\u0a06\13c\5c\u0a08\nc\3c\3c\3c\3c\3c\3"+
		"c\3c\5c\u0a11\nc\3c\3c\5c\u0a15\nc\3c\3c\3c\3c\3c\3c\3c\3c\6c\u0a1f\n"+
		"c\rc\16c\u0a20\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3"+
		"c\3c\3c\3c\3c\5c\u0a3a\nc\3c\3c\3c\3c\3c\5c\u0a41\nc\3c\5c\u0a44\nc\3"+
		"c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\5c\u0a53\nc\3c\3c\5c\u0a57\nc\3"+
		"c\3c\3c\3c\3c\3c\3c\3c\7c\u0a61\nc\fc\16c\u0a64\13c\3d\3d\3d\3d\3d\3d"+
		"\3d\3d\6d\u0a6e\nd\rd\16d\u0a6f\5d\u0a72\nd\3e\3e\3f\3f\3g\3g\3h\3h\3"+
		"i\3i\3i\5i\u0a7f\ni\3j\3j\5j\u0a83\nj\3k\3k\3k\6k\u0a88\nk\rk\16k\u0a89"+
		"\3l\3l\3l\5l\u0a8f\nl\3m\3m\3m\3m\3m\3n\5n\u0a97\nn\3n\3n\5n\u0a9b\nn"+
		"\3o\3o\3o\3o\3o\3o\3o\5o\u0aa4\no\3p\3p\3p\5p\u0aa9\np\3q\3q\3q\3q\3q"+
		"\3q\3q\3q\3q\3q\3q\3q\3q\3q\3q\5q\u0aba\nq\3q\3q\5q\u0abe\nq\3q\3q\3q"+
		"\3q\3q\7q\u0ac5\nq\fq\16q\u0ac8\13q\3q\5q\u0acb\nq\5q\u0acd\nq\3r\3r\3"+
		"r\7r\u0ad2\nr\fr\16r\u0ad5\13r\3s\3s\3s\3s\5s\u0adb\ns\3s\5s\u0ade\ns"+
		"\3s\5s\u0ae1\ns\3t\3t\3t\7t\u0ae6\nt\ft\16t\u0ae9\13t\3u\3u\3u\3u\5u\u0aef"+
		"\nu\3u\5u\u0af2\nu\3v\3v\3v\7v\u0af7\nv\fv\16v\u0afa\13v\3w\3w\3w\3w\3"+
		"w\5w\u0b01\nw\3w\5w\u0b04\nw\3x\3x\3x\3x\3x\3y\3y\3y\3y\7y\u0b0f\ny\f"+
		"y\16y\u0b12\13y\3z\3z\3z\3z\3{\3{\3{\3{\3{\3{\3{\3{\3{\3{\3{\7{\u0b23"+
		"\n{\f{\16{\u0b26\13{\3{\3{\3{\3{\3{\7{\u0b2d\n{\f{\16{\u0b30\13{\5{\u0b32"+
		"\n{\3{\3{\3{\3{\3{\7{\u0b39\n{\f{\16{\u0b3c\13{\5{\u0b3e\n{\5{\u0b40\n"+
		"{\3{\5{\u0b43\n{\3{\5{\u0b46\n{\3|\3|\3|\3|\3|\3|\3|\3|\3|\3|\3|\3|\3"+
		"|\3|\3|\3|\5|\u0b58\n|\3}\3}\3}\3}\3}\3}\3}\5}\u0b61\n}\3~\3~\3~\7~\u0b66"+
		"\n~\f~\16~\u0b69\13~\3\177\3\177\3\177\3\177\5\177\u0b6f\n\177\3\u0080"+
		"\3\u0080\3\u0080\7\u0080\u0b74\n\u0080\f\u0080\16\u0080\u0b77\13\u0080"+
		"\3\u0081\3\u0081\3\u0081\3\u0082\3\u0082\6\u0082\u0b7e\n\u0082\r\u0082"+
		"\16\u0082\u0b7f\3\u0082\5\u0082\u0b83\n\u0082\3\u0083\3\u0083\3\u0083"+
		"\5\u0083\u0b88\n\u0083\3\u0084\3\u0084\3\u0084\3\u0084\3\u0084\3\u0084"+
		"\5\u0084\u0b90\n\u0084\3\u0085\3\u0085\3\u0086\3\u0086\5\u0086\u0b96\n"+
		"\u0086\3\u0086\3\u0086\3\u0086\5\u0086\u0b9b\n\u0086\3\u0086\3\u0086\3"+
		"\u0086\5\u0086\u0ba0\n\u0086\3\u0086\3\u0086\5\u0086\u0ba4\n\u0086\3\u0086"+
		"\3\u0086\5\u0086\u0ba8\n\u0086\3\u0086\3\u0086\5\u0086\u0bac\n\u0086\3"+
		"\u0086\3\u0086\5\u0086\u0bb0\n\u0086\3\u0086\3\u0086\5\u0086\u0bb4\n\u0086"+
		"\3\u0086\3\u0086\5\u0086\u0bb8\n\u0086\3\u0086\5\u0086\u0bbb\n\u0086\3"+
		"\u0087\3\u0087\3\u0087\3\u0087\3\u0087\3\u0087\3\u0087\5\u0087\u0bc4\n"+
		"\u0087\3\u0088\3\u0088\3\u0089\3\u0089\3\u008a\3\u008a\3\u008a\7\u03a9"+
		"\u03e7\u03f1\u03f8\u0400\6R\u00be\u00c2\u00c4\u008b\2\4\6\b\n\f\16\20"+
		"\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BDFHJLNPRTVXZ\\^`bdfhj"+
		"lnprtvxz|~\u0080\u0082\u0084\u0086\u0088\u008a\u008c\u008e\u0090\u0092"+
		"\u0094\u0096\u0098\u009a\u009c\u009e\u00a0\u00a2\u00a4\u00a6\u00a8\u00aa"+
		"\u00ac\u00ae\u00b0\u00b2\u00b4\u00b6\u00b8\u00ba\u00bc\u00be\u00c0\u00c2"+
		"\u00c4\u00c6\u00c8\u00ca\u00cc\u00ce\u00d0\u00d2\u00d4\u00d6\u00d8\u00da"+
		"\u00dc\u00de\u00e0\u00e2\u00e4\u00e6\u00e8\u00ea\u00ec\u00ee\u00f0\u00f2"+
		"\u00f4\u00f6\u00f8\u00fa\u00fc\u00fe\u0100\u0102\u0104\u0106\u0108\u010a"+
		"\u010c\u010e\u0110\u0112\2,\4\2CC\u00b6\u00b6\4\2\"\"\u00c4\u00c4\4\2"+
		"AA\u0098\u0098\4\2ffss\3\2-.\4\2\u00e5\u00e5\u0103\u0103\4\2\21\21%%\7"+
		"\2**\66\66XXee\u008f\u008f\3\2GH\4\2XXee\4\2\u009c\u009c\u011d\u011d\4"+
		"\2\16\16\u0089\u0089\5\2@@\u0097\u0097\u00ce\u00ce\6\2SSzz\u00d7\u00d7"+
		"\u00f9\u00f9\5\2SS\u00d7\u00d7\u00f9\u00f9\4\2\31\31GG\4\2``\u0081\u0081"+
		"\4\2\20\20LL\4\2\u0121\u0121\u0123\u0123\5\2\20\20\25\25\u00db\u00db\5"+
		"\2[[\u00f3\u00f3\u00fb\u00fb\4\2\u0112\u0113\u0118\u0118\3\2\u0114\u0117"+
		"\4\2\u0112\u0113\u011b\u011b\4\2;;==\3\2\u00e3\u00e4\4\2\6\6ff\4\2\6\6"+
		"bb\5\2\35\35\u0084\u0084\u00ee\u00ee\3\2\u010a\u0111\3\2\u0112\u011c\6"+
		"\2\23\23ss\u009b\u009b\u00a3\u00a3\4\2[[\u00f3\u00f3\3\2\u0112\u0113\4"+
		"\2MM\u00ac\u00ac\4\2\u00a4\u00a4\u00dc\u00dc\4\2aa\u00b3\u00b3\3\2\u0122"+
		"\u0123\4\2NN\u00d6\u00d6\65\2\16\17\21\22\26\27\31\32\34\34\36\"%%\'*"+
		",,.\64\66\669:?ACKMNRRTZ]]_adehjmmprtuwy{{~~\u0080\u0083\u0086\u0093\u0096"+
		"\u0098\u009a\u009a\u009d\u009e\u00a1\u00a2\u00a5\u00a5\u00a7\u00a8\u00aa"+
		"\u00b3\u00b5\u00bd\u00bf\u00c5\u00c7\u00ce\u00d2\u00d4\u00d6\u00d6\u00d8"+
		"\u00da\u00dc\u00e4\u00e6\u00ea\u00ed\u00ed\u00ef\u00f4\u00f6\u00f8\u00fc"+
		"\u00ff\u0102\u0104\u0107\u0107\u0117\u0117\21\2\24\2488SSggvvzz\177\177"+
		"\u0085\u0085\u0099\u0099\u009f\u009f\u00c6\u00c6\u00d1\u00d1\u00d7\u00d7"+
		"\u00f9\u00f9\u0101\u0101\23\2\16\23\25\679RTfhuwy{~\u0080\u0084\u0086"+
		"\u0098\u009a\u009e\u00a0\u00c5\u00c7\u00d0\u00d2\u00d6\u00d8\u00f8\u00fa"+
		"\u0100\u0102\u0109\u0117\u0117\2\u0da4\2\u0114\3\2\2\2\4\u011d\3\2\2\2"+
		"\6\u0120\3\2\2\2\b\u0123\3\2\2\2\n\u0126\3\2\2\2\f\u0129\3\2\2\2\16\u012c"+
		"\3\2\2\2\20\u0403\3\2\2\2\22\u04ad\3\2\2\2\24\u04af\3\2\2\2\26\u04c0\3"+
		"\2\2\2\30\u04c6\3\2\2\2\32\u04d2\3\2\2\2\34\u04df\3\2\2\2\36\u04e2\3\2"+
		"\2\2 \u04e6\3\2\2\2\"\u0522\3\2\2\2$\u0524\3\2\2\2&\u0528\3\2\2\2(\u0534"+
		"\3\2\2\2*\u0539\3\2\2\2,\u0540\3\2\2\2.\u0542\3\2\2\2\60\u054a\3\2\2\2"+
		"\62\u0553\3\2\2\2\64\u055e\3\2\2\2\66\u056d\3\2\2\28\u0570\3\2\2\2:\u057b"+
		"\3\2\2\2<\u058b\3\2\2\2>\u0591\3\2\2\2@\u0593\3\2\2\2B\u059e\3\2\2\2D"+
		"\u05af\3\2\2\2F\u05b6\3\2\2\2H\u05b8\3\2\2\2J\u05be\3\2\2\2L\u05f4\3\2"+
		"\2\2N\u0600\3\2\2\2P\u0630\3\2\2\2R\u0633\3\2\2\2T\u0659\3\2\2\2V\u065b"+
		"\3\2\2\2X\u0663\3\2\2\2Z\u0684\3\2\2\2\\\u06a3\3\2\2\2^\u06af\3\2\2\2"+
		"`\u06cf\3\2\2\2b\u06db\3\2\2\2d\u06de\3\2\2\2f\u06e7\3\2\2\2h\u06f8\3"+
		"\2\2\2j\u070c\3\2\2\2l\u070e\3\2\2\2n\u0716\3\2\2\2p\u071a\3\2\2\2r\u071d"+
		"\3\2\2\2t\u0720\3\2\2\2v\u073a\3\2\2\2x\u073c\3\2\2\2z\u077a\3\2\2\2|"+
		"\u0789\3\2\2\2~\u078b\3\2\2\2\u0080\u07a9\3\2\2\2\u0082\u07ab\3\2\2\2"+
		"\u0084\u07b2\3\2\2\2\u0086\u07d2\3\2\2\2\u0088\u07d4\3\2\2\2\u008a\u07e6"+
		"\3\2\2\2\u008c\u0800\3\2\2\2\u008e\u0806\3\2\2\2\u0090\u0808\3\2\2\2\u0092"+
		"\u0827\3\2\2\2\u0094\u0829\3\2\2\2\u0096\u082d\3\2\2\2\u0098\u0835\3\2"+
		"\2\2\u009a\u0840\3\2\2\2\u009c\u0844\3\2\2\2\u009e\u084f\3\2\2\2\u00a0"+
		"\u086b\3\2\2\2\u00a2\u086d\3\2\2\2\u00a4\u0878\3\2\2\2\u00a6\u088e\3\2"+
		"\2\2\u00a8\u08c1\3\2\2\2\u00aa\u08c3\3\2\2\2\u00ac\u08cb\3\2\2\2\u00ae"+
		"\u08d6\3\2\2\2\u00b0\u08dd\3\2\2\2\u00b2\u08e1\3\2\2\2\u00b4\u08eb\3\2"+
		"\2\2\u00b6\u08f3\3\2\2\2\u00b8\u090b\3\2\2\2\u00ba\u090f\3\2\2\2\u00bc"+
		"\u0911\3\2\2\2\u00be\u091f\3\2\2\2\u00c0\u097e\3\2\2\2\u00c2\u0984\3\2"+
		"\2\2\u00c4\u0a56\3\2\2\2\u00c6\u0a71\3\2\2\2\u00c8\u0a73\3\2\2\2\u00ca"+
		"\u0a75\3\2\2\2\u00cc\u0a77\3\2\2\2\u00ce\u0a79\3\2\2\2\u00d0\u0a7b\3\2"+
		"\2\2\u00d2\u0a80\3\2\2\2\u00d4\u0a87\3\2\2\2\u00d6\u0a8b\3\2\2\2\u00d8"+
		"\u0a90\3\2\2\2\u00da\u0a9a\3\2\2\2\u00dc\u0aa3\3\2\2\2\u00de\u0aa8\3\2"+
		"\2\2\u00e0\u0acc\3\2\2\2\u00e2\u0ace\3\2\2\2\u00e4\u0ad6\3\2\2\2\u00e6"+
		"\u0ae2\3\2\2\2\u00e8\u0aea\3\2\2\2\u00ea\u0af3\3\2\2\2\u00ec\u0afb\3\2"+
		"\2\2\u00ee\u0b05\3\2\2\2\u00f0\u0b0a\3\2\2\2\u00f2\u0b13\3\2\2\2\u00f4"+
		"\u0b45\3\2\2\2\u00f6\u0b57\3\2\2\2\u00f8\u0b60\3\2\2\2\u00fa\u0b62\3\2"+
		"\2\2\u00fc\u0b6e\3\2\2\2\u00fe\u0b70\3\2\2\2\u0100\u0b78\3\2\2\2\u0102"+
		"\u0b82\3\2\2\2\u0104\u0b87\3\2\2\2\u0106\u0b8f\3\2\2\2\u0108\u0b91\3\2"+
		"\2\2\u010a\u0bba\3\2\2\2\u010c\u0bc3\3\2\2\2\u010e\u0bc5\3\2\2\2\u0110"+
		"\u0bc7\3\2\2\2\u0112\u0bc9\3\2\2\2\u0114\u0118\5\20\t\2\u0115\u0117\7"+
		"\3\2\2\u0116\u0115\3\2\2\2\u0117\u011a\3\2\2\2\u0118\u0116\3\2\2\2\u0118"+
		"\u0119\3\2\2\2\u0119\u011b\3\2\2\2\u011a\u0118\3\2\2\2\u011b\u011c\7\2"+
		"\2\3\u011c\3\3\2\2\2\u011d\u011e\5\u00b2Z\2\u011e\u011f\7\2\2\3\u011f"+
		"\5\3\2\2\2\u0120\u0121\5\u00aeX\2\u0121\u0122\7\2\2\3\u0122\7\3\2\2\2"+
		"\u0123\u0124\5\u00acW\2\u0124\u0125\7\2\2\3\u0125\t\3\2\2\2\u0126\u0127"+
		"\5\u00b0Y\2\u0127\u0128\7\2\2\3\u0128\13\3\2\2\2\u0129\u012a\5\u00e0q"+
		"\2\u012a\u012b\7\2\2\3\u012b\r\3\2\2\2\u012c\u012d\5\u00e6t\2\u012d\u012e"+
		"\7\2\2\3\u012e\17\3\2\2\2\u012f\u0404\5 \21\2\u0130\u0132\5\60\31\2\u0131"+
		"\u0130\3\2\2\2\u0131\u0132\3\2\2\2\u0132\u0133\3\2\2\2\u0133\u0404\5L"+
		"\'\2\u0134\u0136\7\u00ff\2\2\u0135\u0137\7\u0097\2\2\u0136\u0135\3\2\2"+
		"\2\u0136\u0137\3\2\2\2\u0137\u0138\3\2\2\2\u0138\u0404\5\u00acW\2\u0139"+
		"\u013a\7\67\2\2\u013a\u013e\5*\26\2\u013b\u013c\7p\2\2\u013c\u013d\7\u009b"+
		"\2\2\u013d\u013f\7U\2\2\u013e\u013b\3\2\2\2\u013e\u013f\3\2\2\2\u013f"+
		"\u0140\3\2\2\2\u0140\u0148\5\u00acW\2\u0141\u0147\5\36\20\2\u0142\u0147"+
		"\5\34\17\2\u0143\u0144\7\u0108\2\2\u0144\u0145\t\2\2\2\u0145\u0147\58"+
		"\35\2\u0146\u0141\3\2\2\2\u0146\u0142\3\2\2\2\u0146\u0143\3\2\2\2\u0147"+
		"\u014a\3\2\2\2\u0148\u0146\3\2\2\2\u0148\u0149\3\2\2\2\u0149\u0404\3\2"+
		"\2\2\u014a\u0148\3\2\2\2\u014b\u014c\7\21\2\2\u014c\u014d\5*\26\2\u014d"+
		"\u014e\5\u00acW\2\u014e\u014f\7\u00d6\2\2\u014f\u0150\t\2\2\2\u0150\u0151"+
		"\58\35\2\u0151\u0404\3\2\2\2\u0152\u0153\7\21\2\2\u0153\u0154\5*\26\2"+
		"\u0154\u0155\5\u00acW\2\u0155\u0156\7\u00d6\2\2\u0156\u0157\5\34\17\2"+
		"\u0157\u0404\3\2\2\2\u0158\u0159\7N\2\2\u0159\u015c\5*\26\2\u015a\u015b"+
		"\7p\2\2\u015b\u015d\7U\2\2\u015c\u015a\3\2\2\2\u015c\u015d\3\2\2\2\u015d"+
		"\u015e\3\2\2\2\u015e\u0160\5\u00acW\2\u015f\u0161\t\3\2\2\u0160\u015f"+
		"\3\2\2\2\u0160\u0161\3\2\2\2\u0161\u0404\3\2\2\2\u0162\u0163\7\u00d9\2"+
		"\2\u0163\u0166\t\4\2\2\u0164\u0165\t\5\2\2\u0165\u0167\5\u00acW\2\u0166"+
		"\u0164\3\2\2\2\u0166\u0167\3\2\2\2\u0167\u016c\3\2\2\2\u0168\u016a\7\u0086"+
		"\2\2\u0169\u0168\3\2\2\2\u0169\u016a\3\2\2\2\u016a\u016b\3\2\2\2\u016b"+
		"\u016d\7\u011d\2\2\u016c\u0169\3\2\2\2\u016c\u016d\3\2\2\2\u016d\u0404"+
		"\3\2\2\2\u016e\u016f\6\t\2\2\u016f\u0174\5\24\13\2\u0170\u0171\7\4\2\2"+
		"\u0171\u0172\5\u00e6t\2\u0172\u0173\7\5\2\2\u0173\u0175\3\2\2\2\u0174"+
		"\u0170\3\2\2\2\u0174\u0175\3\2\2\2\u0175\u0177\3\2\2\2\u0176\u0178\5\64"+
		"\33\2\u0177\u0176\3\2\2\2\u0177\u0178\3\2\2\2\u0178\u0179\3\2\2\2\u0179"+
		"\u017e\5\66\34\2\u017a\u017c\7\30\2\2\u017b\u017a\3\2\2\2\u017b\u017c"+
		"\3\2\2\2\u017c\u017d\3\2\2\2\u017d\u017f\5 \21\2\u017e\u017b\3\2\2\2\u017e"+
		"\u017f\3\2\2\2\u017f\u0404\3\2\2\2\u0180\u0181\6\t\3\2\u0181\u0186\5\24"+
		"\13\2\u0182\u0183\7\4\2\2\u0183\u0184\5\u00e6t\2\u0184\u0185\7\5\2\2\u0185"+
		"\u0187\3\2\2\2\u0186\u0182\3\2\2\2\u0186\u0187\3\2\2\2\u0187\u0188\3\2"+
		"\2\2\u0188\u0189\5\64\33\2\u0189\u018e\5\66\34\2\u018a\u018c\7\30\2\2"+
		"\u018b\u018a\3\2\2\2\u018b\u018c\3\2\2\2\u018c\u018d\3\2\2\2\u018d\u018f"+
		"\5 \21\2\u018e\u018b\3\2\2\2\u018e\u018f\3\2\2\2\u018f\u0404\3\2\2\2\u0190"+
		"\u0195\5\24\13\2\u0191\u0192\7\4\2\2\u0192\u0193\5\u00e6t\2\u0193\u0194"+
		"\7\5\2\2\u0194\u0196\3\2\2\2\u0195\u0191\3\2\2\2\u0195\u0196\3\2\2\2\u0196"+
		"\u01ac\3\2\2\2\u0197\u01ab\5\36\20\2\u0198\u0199\7\u00ad\2\2\u0199\u019a"+
		"\7 \2\2\u019a\u019b\7\4\2\2\u019b\u019c\5\u00e6t\2\u019c\u019d\7\5\2\2"+
		"\u019d\u01a2\3\2\2\2\u019e\u019f\7\u00ad\2\2\u019f\u01a0\7 \2\2\u01a0"+
		"\u01a2\5\u0094K\2\u01a1\u0198\3\2\2\2\u01a1\u019e\3\2\2\2\u01a2\u01ab"+
		"\3\2\2\2\u01a3\u01ab\5\30\r\2\u01a4\u01ab\5\32\16\2\u01a5\u01ab\5\u00a8"+
		"U\2\u01a6\u01ab\5D#\2\u01a7\u01ab\5\34\17\2\u01a8\u01a9\7\u00e8\2\2\u01a9"+
		"\u01ab\58\35\2\u01aa\u0197\3\2\2\2\u01aa\u01a1\3\2\2\2\u01aa\u01a3\3\2"+
		"\2\2\u01aa\u01a4\3\2\2\2\u01aa\u01a5\3\2\2\2\u01aa\u01a6\3\2\2\2\u01aa"+
		"\u01a7\3\2\2\2\u01aa\u01a8\3\2\2\2\u01ab\u01ae\3\2\2\2\u01ac\u01aa\3\2"+
		"\2\2\u01ac\u01ad\3\2\2\2\u01ad\u01b3\3\2\2\2\u01ae\u01ac\3\2\2\2\u01af"+
		"\u01b1\7\30\2\2\u01b0\u01af\3\2\2\2\u01b0\u01b1\3\2\2\2\u01b1\u01b2\3"+
		"\2\2\2\u01b2\u01b4\5 \21\2\u01b3\u01b0\3\2\2\2\u01b3\u01b4\3\2\2\2\u01b4"+
		"\u0404\3\2\2\2\u01b5\u01b6\7\67\2\2\u01b6\u01ba\7\u00e5\2\2\u01b7\u01b8"+
		"\7p\2\2\u01b8\u01b9\7\u009b\2\2\u01b9\u01bb\7U\2\2\u01ba\u01b7\3\2\2\2"+
		"\u01ba\u01bb\3\2\2\2\u01bb\u01bc\3\2\2\2\u01bc\u01bd\5\u00aeX\2\u01bd"+
		"\u01be\7\u0086\2\2\u01be\u01c7\5\u00aeX\2\u01bf\u01c6\5\64\33\2\u01c0"+
		"\u01c6\5\u00a8U\2\u01c1\u01c6\5D#\2\u01c2\u01c6\5\34\17\2\u01c3\u01c4"+
		"\7\u00e8\2\2\u01c4\u01c6\58\35\2\u01c5\u01bf\3\2\2\2\u01c5\u01c0\3\2\2"+
		"\2\u01c5\u01c1\3\2\2\2\u01c5\u01c2\3\2\2\2\u01c5\u01c3\3\2\2\2\u01c6\u01c9"+
		"\3\2\2\2\u01c7\u01c5\3\2\2\2\u01c7\u01c8\3\2\2\2\u01c8\u0404\3\2\2\2\u01c9"+
		"\u01c7\3\2\2\2\u01ca\u01cf\5\26\f\2\u01cb\u01cc\7\4\2\2\u01cc\u01cd\5"+
		"\u00e6t\2\u01cd\u01ce\7\5\2\2\u01ce\u01d0\3\2\2\2\u01cf\u01cb\3\2\2\2"+
		"\u01cf\u01d0\3\2\2\2\u01d0\u01d1\3\2\2\2\u01d1\u01d2\5\64\33\2\u01d2\u01d7"+
		"\5\66\34\2\u01d3\u01d5\7\30\2\2\u01d4\u01d3\3\2\2\2\u01d4\u01d5\3\2\2"+
		"\2\u01d5\u01d6\3\2\2\2\u01d6\u01d8\5 \21\2\u01d7\u01d4\3\2\2\2\u01d7\u01d8"+
		"\3\2\2\2\u01d8\u0404\3\2\2\2\u01d9\u01da\7\22\2\2\u01da\u01db\7\u00e5"+
		"\2\2\u01db\u01dd\5\u00acW\2\u01dc\u01de\5&\24\2\u01dd\u01dc\3\2\2\2\u01dd"+
		"\u01de\3\2\2\2\u01de\u01df\3\2\2\2\u01df\u01e0\7\63\2\2\u01e0\u01e8\7"+
		"\u00df\2\2\u01e1\u01e9\5\u0104\u0083\2\u01e2\u01e3\7b\2\2\u01e3\u01e4"+
		"\7.\2\2\u01e4\u01e9\5\u0096L\2\u01e5\u01e6\7b\2\2\u01e6\u01e7\7\20\2\2"+
		"\u01e7\u01e9\7.\2\2\u01e8\u01e1\3\2\2\2\u01e8\u01e2\3\2\2\2\u01e8\u01e5"+
		"\3\2\2\2\u01e8\u01e9\3\2\2\2\u01e9\u0404\3\2\2\2\u01ea\u01eb\7\21\2\2"+
		"\u01eb\u01ec\7\u00e5\2\2\u01ec\u01ed\5\u00acW\2\u01ed\u01ee\7\16\2\2\u01ee"+
		"\u01ef\t\6\2\2\u01ef\u01f0\5\u00e2r\2\u01f0\u0404\3\2\2\2\u01f1\u01f2"+
		"\7\21\2\2\u01f2\u01f3\7\u00e5\2\2\u01f3\u01f4\5\u00acW\2\u01f4\u01f5\7"+
		"\16\2\2\u01f5\u01f6\t\6\2\2\u01f6\u01f7\7\4\2\2\u01f7\u01f8\5\u00e2r\2"+
		"\u01f8\u01f9\7\5\2\2\u01f9\u0404\3\2\2\2\u01fa\u01fb\7\21\2\2\u01fb\u01fc"+
		"\7\u00e5\2\2\u01fc\u01fd\5\u00acW\2\u01fd\u01fe\7\u00c0\2\2\u01fe\u01ff"+
		"\7-\2\2\u01ff\u0200\5\u00acW\2\u0200\u0201\7\u00ec\2\2\u0201\u0202\5\u0100"+
		"\u0081\2\u0202\u0404\3\2\2\2\u0203\u0204\7\21\2\2\u0204\u0205\7\u00e5"+
		"\2\2\u0205\u0206\5\u00acW\2\u0206\u0207\7N\2\2\u0207\u0208\t\6\2\2\u0208"+
		"\u0209\7\4\2\2\u0209\u020a\5\u00aaV\2\u020a\u020b\7\5\2\2\u020b\u0404"+
		"\3\2\2\2\u020c\u020d\7\21\2\2\u020d\u020e\7\u00e5\2\2\u020e\u020f\5\u00ac"+
		"W\2\u020f\u0210\7N\2\2\u0210\u0211\t\6\2\2\u0211\u0212\5\u00aaV\2\u0212"+
		"\u0404\3\2\2\2\u0213\u0214\7\21\2\2\u0214\u0215\t\7\2\2\u0215\u0216\5"+
		"\u00acW\2\u0216\u0217\7\u00c0\2\2\u0217\u0218\7\u00ec\2\2\u0218\u0219"+
		"\5\u00acW\2\u0219\u0404\3\2\2\2\u021a\u021b\7\21\2\2\u021b\u021c\t\7\2"+
		"\2\u021c\u021d\5\u00acW\2\u021d\u021e\7\u00d6\2\2\u021e\u021f\7\u00e8"+
		"\2\2\u021f\u0220\58\35\2\u0220\u0404\3\2\2\2\u0221\u0222\7\21\2\2\u0222"+
		"\u0223\t\7\2\2\u0223\u0224\5\u00acW\2\u0224\u0225\7\u00fd\2\2\u0225\u0228"+
		"\7\u00e8\2\2\u0226\u0227\7p\2\2\u0227\u0229\7U\2\2\u0228\u0226\3\2\2\2"+
		"\u0228\u0229\3\2\2\2\u0229\u022a\3\2\2\2\u022a\u022b\58\35\2\u022b\u0404"+
		"\3\2\2\2\u022c\u022d\7\21\2\2\u022d\u022e\7\u00e5\2\2\u022e\u022f\5\u00ac"+
		"W\2\u022f\u0231\t\b\2\2\u0230\u0232\7-\2\2\u0231\u0230\3\2\2\2\u0231\u0232"+
		"\3\2\2\2\u0232\u0233\3\2\2\2\u0233\u0235\5\u00acW\2\u0234\u0236\5\u010c"+
		"\u0087\2\u0235\u0234\3\2\2\2\u0235\u0236\3\2\2\2\u0236\u0404\3\2\2\2\u0237"+
		"\u0238\7\21\2\2\u0238\u0239\7\u00e5\2\2\u0239\u023b\5\u00acW\2\u023a\u023c"+
		"\5&\24\2\u023b\u023a\3\2\2\2\u023b\u023c\3\2\2\2\u023c\u023d\3\2\2\2\u023d"+
		"\u023f\7%\2\2\u023e\u0240\7-\2\2\u023f\u023e\3\2\2\2\u023f\u0240\3\2\2"+
		"\2\u0240\u0241\3\2\2\2\u0241\u0242\5\u00acW\2\u0242\u0244\5\u00e8u\2\u0243"+
		"\u0245\5\u00dep\2\u0244\u0243\3\2\2\2\u0244\u0245\3\2\2\2\u0245\u0404"+
		"\3\2\2\2\u0246\u0247\7\21\2\2\u0247\u0248\7\u00e5\2\2\u0248\u024a\5\u00ac"+
		"W\2\u0249\u024b\5&\24\2\u024a\u0249\3\2\2\2\u024a\u024b\3\2\2\2\u024b"+
		"\u024c\3\2\2\2\u024c\u024d\7\u00c2\2\2\u024d\u024e\7.\2\2\u024e\u024f"+
		"\7\4\2\2\u024f\u0250\5\u00e2r\2\u0250\u0251\7\5\2\2\u0251\u0404\3\2\2"+
		"\2\u0252\u0253\7\21\2\2\u0253\u0254\7\u00e5\2\2\u0254\u0256\5\u00acW\2"+
		"\u0255\u0257\5&\24\2\u0256\u0255\3\2\2\2\u0256\u0257\3\2\2\2\u0257\u0258"+
		"\3\2\2\2\u0258\u0259\7\u00d6\2\2\u0259\u025a\7\u00d3\2\2\u025a\u025e\7"+
		"\u011d\2\2\u025b\u025c\7\u0108\2\2\u025c\u025d\7\u00d4\2\2\u025d\u025f"+
		"\58\35\2\u025e\u025b\3\2\2\2\u025e\u025f\3\2\2\2\u025f\u0404\3\2\2\2\u0260"+
		"\u0261\7\21\2\2\u0261\u0262\7\u00e5\2\2\u0262\u0264\5\u00acW\2\u0263\u0265"+
		"\5&\24\2\u0264\u0263\3\2\2\2\u0264\u0265\3\2\2\2\u0265\u0266\3\2\2\2\u0266"+
		"\u0267\7\u00d6\2\2\u0267\u0268\7\u00d4\2\2\u0268\u0269\58\35\2\u0269\u0404"+
		"\3\2\2\2\u026a\u026b\7\21\2\2\u026b\u026c\t\7\2\2\u026c\u026d\5\u00ac"+
		"W\2\u026d\u0271\7\16\2\2\u026e\u026f\7p\2\2\u026f\u0270\7\u009b\2\2\u0270"+
		"\u0272\7U\2\2\u0271\u026e\3\2\2\2\u0271\u0272\3\2\2\2\u0272\u0274\3\2"+
		"\2\2\u0273\u0275\5$\23\2\u0274\u0273\3\2\2\2\u0275\u0276\3\2\2\2\u0276"+
		"\u0274\3\2\2\2\u0276\u0277\3\2\2\2\u0277\u0404\3\2\2\2\u0278\u0279\7\21"+
		"\2\2\u0279\u027a\7\u00e5\2\2\u027a\u027b\5\u00acW\2\u027b\u027c\5&\24"+
		"\2\u027c\u027d\7\u00c0\2\2\u027d\u027e\7\u00ec\2\2\u027e\u027f\5&\24\2"+
		"\u027f\u0404\3\2\2\2\u0280\u0281\7\21\2\2\u0281\u0282\t\7\2\2\u0282\u0283"+
		"\5\u00acW\2\u0283\u0286\7N\2\2\u0284\u0285\7p\2\2\u0285\u0287\7U\2\2\u0286"+
		"\u0284\3\2\2\2\u0286\u0287\3\2\2\2\u0287\u0288\3\2\2\2\u0288\u028d\5&"+
		"\24\2\u0289\u028a\7\6\2\2\u028a\u028c\5&\24\2\u028b\u0289\3\2\2\2\u028c"+
		"\u028f\3\2\2\2\u028d\u028b\3\2\2\2\u028d\u028e\3\2\2\2\u028e\u0291\3\2"+
		"\2\2\u028f\u028d\3\2\2\2\u0290\u0292\7\u00b7\2\2\u0291\u0290\3\2\2\2\u0291"+
		"\u0292\3\2\2\2\u0292\u0404\3\2\2\2\u0293\u0294\7\21\2\2\u0294\u0295\7"+
		"\u00e5\2\2\u0295\u0297\5\u00acW\2\u0296\u0298\5&\24\2\u0297\u0296\3\2"+
		"\2\2\u0297\u0298\3\2\2\2\u0298\u0299\3\2\2\2\u0299\u029a\7\u00d6\2\2\u029a"+
		"\u029b\5\34\17\2\u029b\u0404\3\2\2\2\u029c\u029d\7\21\2\2\u029d\u029e"+
		"\7\u00e5\2\2\u029e\u029f\5\u00acW\2\u029f\u02a0\7\u00bc\2\2\u02a0\u02a1"+
		"\7\u00ae\2\2\u02a1\u0404\3\2\2\2\u02a2\u02a3\7N\2\2\u02a3\u02a6\7\u00e5"+
		"\2\2\u02a4\u02a5\7p\2\2\u02a5\u02a7\7U\2\2\u02a6\u02a4\3\2\2\2\u02a6\u02a7"+
		"\3\2\2\2\u02a7\u02a8\3\2\2\2\u02a8\u02aa\5\u00acW\2\u02a9\u02ab\7\u00b7"+
		"\2\2\u02aa\u02a9\3\2\2\2\u02aa\u02ab\3\2\2\2\u02ab\u0404\3\2\2\2\u02ac"+
		"\u02ad\7N\2\2\u02ad\u02b0\7\u0103\2\2\u02ae\u02af\7p\2\2\u02af\u02b1\7"+
		"U\2\2\u02b0\u02ae\3\2\2\2\u02b0\u02b1\3\2\2\2\u02b1\u02b2\3\2\2\2\u02b2"+
		"\u0404\5\u00acW\2\u02b3\u02b6\7\67\2\2\u02b4\u02b5\7\u00a3\2\2\u02b5\u02b7"+
		"\7\u00c2\2\2\u02b6\u02b4\3\2\2\2\u02b6\u02b7\3\2\2\2\u02b7\u02bc\3\2\2"+
		"\2\u02b8\u02ba\7j\2\2\u02b9\u02b8\3\2\2\2\u02b9\u02ba\3\2\2\2\u02ba\u02bb"+
		"\3\2\2\2\u02bb\u02bd\7\u00e9\2\2\u02bc\u02b9\3\2\2\2\u02bc\u02bd\3\2\2"+
		"\2\u02bd\u02be\3\2\2\2\u02be\u02c2\7\u0103\2\2\u02bf\u02c0\7p\2\2\u02c0"+
		"\u02c1\7\u009b\2\2\u02c1\u02c3\7U\2\2\u02c2\u02bf\3\2\2\2\u02c2\u02c3"+
		"\3\2\2\2\u02c3\u02c4\3\2\2\2\u02c4\u02c6\5\u00acW\2\u02c5\u02c7\5\u009c"+
		"O\2\u02c6\u02c5\3\2\2\2\u02c6\u02c7\3\2\2\2\u02c7\u02d0\3\2\2\2\u02c8"+
		"\u02cf\5\36\20\2\u02c9\u02ca\7\u00ad\2\2\u02ca\u02cb\7\u009f\2\2\u02cb"+
		"\u02cf\5\u0094K\2\u02cc\u02cd\7\u00e8\2\2\u02cd\u02cf\58\35\2\u02ce\u02c8"+
		"\3\2\2\2\u02ce\u02c9\3\2\2\2\u02ce\u02cc\3\2\2\2\u02cf\u02d2\3\2\2\2\u02d0"+
		"\u02ce\3\2\2\2\u02d0\u02d1\3\2\2\2\u02d1\u02d3\3\2\2\2\u02d2\u02d0\3\2"+
		"\2\2\u02d3\u02d4\7\30\2\2\u02d4\u02d5\5 \21\2\u02d5\u0404\3\2\2\2\u02d6"+
		"\u02d9\7\67\2\2\u02d7\u02d8\7\u00a3\2\2\u02d8\u02da\7\u00c2\2\2\u02d9"+
		"\u02d7\3\2\2\2\u02d9\u02da\3\2\2\2\u02da\u02dc\3\2\2\2\u02db\u02dd\7j"+
		"\2\2\u02dc\u02db\3\2\2\2\u02dc\u02dd\3\2\2\2\u02dd\u02de\3\2\2\2\u02de"+
		"\u02df\7\u00e9\2\2\u02df\u02e0\7\u0103\2\2\u02e0\u02e5\5\u00aeX\2\u02e1"+
		"\u02e2\7\4\2\2\u02e2\u02e3\5\u00e6t\2\u02e3\u02e4\7\5\2\2\u02e4\u02e6"+
		"\3\2\2\2\u02e5\u02e1\3\2\2\2\u02e5\u02e6\3\2\2\2\u02e6\u02e7\3\2\2\2\u02e7"+
		"\u02ea\5\64\33\2\u02e8\u02e9\7\u00a2\2\2\u02e9\u02eb\58\35\2\u02ea\u02e8"+
		"\3\2\2\2\u02ea\u02eb\3\2\2\2\u02eb\u0404\3\2\2\2\u02ec\u02ed\7\21\2\2"+
		"\u02ed\u02ee\7\u0103\2\2\u02ee\u02f0\5\u00acW\2\u02ef\u02f1\7\30\2\2\u02f0"+
		"\u02ef\3\2\2\2\u02f0\u02f1\3\2\2\2\u02f1\u02f2\3\2\2\2\u02f2\u02f3\5 "+
		"\21\2\u02f3\u0404\3\2\2\2\u02f4\u02f7\7\67\2\2\u02f5\u02f6\7\u00a3\2\2"+
		"\u02f6\u02f8\7\u00c2\2\2\u02f7\u02f5\3\2\2\2\u02f7\u02f8\3\2\2\2\u02f8"+
		"\u02fa\3\2\2\2\u02f9\u02fb\7\u00e9\2\2\u02fa\u02f9\3\2\2\2\u02fa\u02fb"+
		"\3\2\2\2\u02fb\u02fc\3\2\2\2\u02fc\u0300\7h\2\2\u02fd\u02fe\7p\2\2\u02fe"+
		"\u02ff\7\u009b\2\2\u02ff\u0301\7U\2\2\u0300\u02fd\3\2\2\2\u0300\u0301"+
		"\3\2\2\2\u0301\u0302\3\2\2\2\u0302\u0303\5\u00acW\2\u0303\u0304\7\30\2"+
		"\2\u0304\u030e\7\u011d\2\2\u0305\u0306\7\u0101\2\2\u0306\u030b\5J&\2\u0307"+
		"\u0308\7\6\2\2\u0308\u030a\5J&\2\u0309\u0307\3\2\2\2\u030a\u030d\3\2\2"+
		"\2\u030b\u0309\3\2\2\2\u030b\u030c\3\2\2\2\u030c\u030f\3\2\2\2\u030d\u030b"+
		"\3\2\2\2\u030e\u0305\3\2\2\2\u030e\u030f\3\2\2\2\u030f\u0404\3\2\2\2\u0310"+
		"\u0312\7N\2\2\u0311\u0313\7\u00e9\2\2\u0312\u0311\3\2\2\2\u0312\u0313"+
		"\3\2\2\2\u0313\u0314\3\2\2\2\u0314\u0317\7h\2\2\u0315\u0316\7p\2\2\u0316"+
		"\u0318\7U\2\2\u0317\u0315\3\2\2\2\u0317\u0318\3\2\2\2\u0318\u0319\3\2"+
		"\2\2\u0319\u0404\5\u00acW\2\u031a\u031c\7V\2\2\u031b\u031d\t\t\2\2\u031c"+
		"\u031b\3\2\2\2\u031c\u031d\3\2\2\2\u031d\u031e\3\2\2\2\u031e\u0404\5\20"+
		"\t\2\u031f\u0320\7\u00d9\2\2\u0320\u0323\7\u00e6\2\2\u0321\u0322\t\5\2"+
		"\2\u0322\u0324\5\u00acW\2\u0323\u0321\3\2\2\2\u0323\u0324\3\2\2\2\u0324"+
		"\u0329\3\2\2\2\u0325\u0327\7\u0086\2\2\u0326\u0325\3\2\2\2\u0326\u0327"+
		"\3\2\2\2\u0327\u0328\3\2\2\2\u0328\u032a\7\u011d\2\2\u0329\u0326\3\2\2"+
		"\2\u0329\u032a\3\2\2\2\u032a\u0404\3\2\2\2\u032b\u032c\7\u00d9\2\2\u032c"+
		"\u032d\7\u00e5\2\2\u032d\u0330\7X\2\2\u032e\u032f\t\5\2\2\u032f\u0331"+
		"\5\u00acW\2\u0330\u032e\3\2\2\2\u0330\u0331\3\2\2\2\u0331\u0332\3\2\2"+
		"\2\u0332\u0333\7\u0086\2\2\u0333\u0335\7\u011d\2\2\u0334\u0336\5&\24\2"+
		"\u0335\u0334\3\2\2\2\u0335\u0336\3\2\2\2\u0336\u0404\3\2\2\2\u0337\u0338"+
		"\7\u00d9\2\2\u0338\u0339\7\u00e8\2\2\u0339\u033e\5\u00acW\2\u033a\u033b"+
		"\7\4\2\2\u033b\u033c\5<\37\2\u033c\u033d\7\5\2\2\u033d\u033f\3\2\2\2\u033e"+
		"\u033a\3\2\2\2\u033e\u033f\3\2\2\2\u033f\u0404\3\2\2\2\u0340\u0341\7\u00d9"+
		"\2\2\u0341\u0342\7.\2\2\u0342\u0343\t\5\2\2\u0343\u0346\5\u00acW\2\u0344"+
		"\u0345\t\5\2\2\u0345\u0347\5\u00acW\2\u0346\u0344\3\2\2\2\u0346\u0347"+
		"\3\2\2\2\u0347\u0404\3\2\2\2\u0348\u0349\7\u00d9\2\2\u0349\u034c\7\u0104"+
		"\2\2\u034a\u034b\t\5\2\2\u034b\u034d\5\u00acW\2\u034c\u034a\3\2\2\2\u034c"+
		"\u034d\3\2\2\2\u034d\u0352\3\2\2\2\u034e\u0350\7\u0086\2\2\u034f\u034e"+
		"\3\2\2\2\u034f\u0350\3\2\2\2\u0350\u0351\3\2\2\2\u0351\u0353\7\u011d\2"+
		"\2\u0352\u034f\3\2\2\2\u0352\u0353\3\2\2\2\u0353\u0404\3\2\2\2\u0354\u0355"+
		"\7\u00d9\2\2\u0355\u0356\7\u00ae\2\2\u0356\u0358\5\u00acW\2\u0357\u0359"+
		"\5&\24\2\u0358\u0357\3\2\2\2\u0358\u0359\3\2\2\2\u0359\u0404\3\2\2\2\u035a"+
		"\u035c\7\u00d9\2\2\u035b\u035d\5\u0104\u0083\2\u035c\u035b\3\2\2\2\u035c"+
		"\u035d\3\2\2\2\u035d\u035e\3\2\2\2\u035e\u0366\7i\2\2\u035f\u0361\7\u0086"+
		"\2\2\u0360\u035f\3\2\2\2\u0360\u0361\3\2\2\2\u0361\u0364\3\2\2\2\u0362"+
		"\u0365\5\u00acW\2\u0363\u0365\7\u011d\2\2\u0364\u0362\3\2\2\2\u0364\u0363"+
		"\3\2\2\2\u0365\u0367\3\2\2\2\u0366\u0360\3\2\2\2\u0366\u0367\3\2\2\2\u0367"+
		"\u0404\3\2\2\2\u0368\u0369\7\u00d9\2\2\u0369\u036a\7\67\2\2\u036a\u036b"+
		"\7\u00e5\2\2\u036b\u036e\5\u00acW\2\u036c\u036d\7\30\2\2\u036d\u036f\7"+
		"\u00d3\2\2\u036e\u036c\3\2\2\2\u036e\u036f\3\2\2\2\u036f\u0404\3\2\2\2"+
		"\u0370\u0371\7\u00d9\2\2\u0371\u0372\7:\2\2\u0372\u0404\7\u0097\2\2\u0373"+
		"\u0374\t\n\2\2\u0374\u0376\7h\2\2\u0375\u0377\7X\2\2\u0376\u0375\3\2\2"+
		"\2\u0376\u0377\3\2\2\2\u0377\u0378\3\2\2\2\u0378\u0404\5,\27\2\u0379\u037a"+
		"\t\n\2\2\u037a\u037c\5*\26\2\u037b\u037d\7X\2\2\u037c\u037b\3\2\2\2\u037c"+
		"\u037d\3\2\2\2\u037d\u037e\3\2\2\2\u037e\u037f\5\u00acW\2\u037f\u0404"+
		"\3\2\2\2\u0380\u0382\t\n\2\2\u0381\u0383\7\u00e5\2\2\u0382\u0381\3\2\2"+
		"\2\u0382\u0383\3\2\2\2\u0383\u0385\3\2\2\2\u0384\u0386\t\13\2\2\u0385"+
		"\u0384\3\2\2\2\u0385\u0386\3\2\2\2\u0386\u0387\3\2\2\2\u0387\u0389\5\u00ac"+
		"W\2\u0388\u038a\5&\24\2\u0389\u0388\3\2\2\2\u0389\u038a\3\2\2\2\u038a"+
		"\u038c\3\2\2\2\u038b\u038d\5.\30\2\u038c\u038b\3\2\2\2\u038c\u038d\3\2"+
		"\2\2\u038d\u0404\3\2\2\2\u038e\u0390\t\n\2\2\u038f\u0391\7\u00b8\2\2\u0390"+
		"\u038f\3\2\2\2\u0390\u0391\3\2\2\2\u0391\u0392\3\2\2\2\u0392\u0404\5 "+
		"\21\2\u0393\u0394\7/\2\2\u0394\u0395\7\u009f\2\2\u0395\u0396\5*\26\2\u0396"+
		"\u0397\5\u00acW\2\u0397\u0398\7}\2\2\u0398\u0399\t\f\2\2\u0399\u0404\3"+
		"\2\2\2\u039a\u039b\7/\2\2\u039b\u039c\7\u009f\2\2\u039c\u039d\7\u00e5"+
		"\2\2\u039d\u039e\5\u00acW\2\u039e\u039f\7}\2\2\u039f\u03a0\t\f\2\2\u03a0"+
		"\u0404\3\2\2\2\u03a1\u03a2\7\u00bf\2\2\u03a2\u03a3\7\u00e5\2\2\u03a3\u0404"+
		"\5\u00acW\2\u03a4\u03ac\7\u00bf\2\2\u03a5\u03ad\7\u011d\2\2\u03a6\u03a8"+
		"\13\2\2\2\u03a7\u03a6\3\2\2\2\u03a8\u03ab\3\2\2\2\u03a9\u03aa\3\2\2\2"+
		"\u03a9\u03a7\3\2\2\2\u03aa\u03ad\3\2\2\2\u03ab\u03a9\3\2\2\2\u03ac\u03a5"+
		"\3\2\2\2\u03ac\u03a9\3\2\2\2\u03ad\u0404\3\2\2\2\u03ae\u03b0\7!\2\2\u03af"+
		"\u03b1\7\u0083\2\2\u03b0\u03af\3\2\2\2\u03b0\u03b1\3\2\2\2\u03b1\u03b2"+
		"\3\2\2\2\u03b2\u03b3\7\u00e5\2\2\u03b3\u03b6\5\u00acW\2\u03b4\u03b5\7"+
		"\u00a2\2\2\u03b5\u03b7\58\35\2\u03b6\u03b4\3\2\2\2\u03b6\u03b7\3\2\2\2"+
		"\u03b7\u03bc\3\2\2\2\u03b8\u03ba\7\30\2\2\u03b9\u03b8\3\2\2\2\u03b9\u03ba"+
		"\3\2\2\2\u03ba\u03bb\3\2\2\2\u03bb\u03bd\5 \21\2\u03bc\u03b9\3\2\2\2\u03bc"+
		"\u03bd\3\2\2\2\u03bd\u0404\3\2\2\2\u03be\u03bf\7\u00f8\2\2\u03bf\u03c2"+
		"\7\u00e5\2\2\u03c0\u03c1\7p\2\2\u03c1\u03c3\7U\2\2\u03c2\u03c0\3\2\2\2"+
		"\u03c2\u03c3\3\2\2\2\u03c3\u03c4\3\2\2\2\u03c4\u0404\5\u00acW\2\u03c5"+
		"\u03c6\7\'\2\2\u03c6\u0404\7!\2\2\u03c7\u03c8\7\u008a\2\2\u03c8\u03ca"+
		"\7?\2\2\u03c9\u03cb\7\u008b\2\2\u03ca\u03c9\3\2\2\2\u03ca\u03cb\3\2\2"+
		"\2\u03cb\u03cc\3\2\2\2\u03cc\u03cd\7w\2\2\u03cd\u03cf\7\u011d\2\2\u03ce"+
		"\u03d0\7\u00ab\2\2\u03cf\u03ce\3\2\2\2\u03cf\u03d0\3\2\2\2\u03d0\u03d1"+
		"\3\2\2\2\u03d1\u03d2\7|\2\2\u03d2\u03d3\7\u00e5\2\2\u03d3\u03d5\5\u00ac"+
		"W\2\u03d4\u03d6\5&\24\2\u03d5\u03d4\3\2\2\2\u03d5\u03d6\3\2\2\2\u03d6"+
		"\u0404\3\2\2\2\u03d7\u03d8\7\u00f4\2\2\u03d8\u03d9\7\u00e5\2\2\u03d9\u03db"+
		"\5\u00acW\2\u03da\u03dc\5&\24\2\u03db\u03da\3\2\2\2\u03db\u03dc\3\2\2"+
		"\2\u03dc\u0404\3\2\2\2\u03dd\u03de\7\u0096\2\2\u03de\u03df\7\u00c1\2\2"+
		"\u03df\u03e0\7\u00e5\2\2\u03e0\u0404\5\u00acW\2\u03e1\u03e2\t\r\2\2\u03e2"+
		"\u03ea\5\u0104\u0083\2\u03e3\u03eb\7\u011d\2\2\u03e4\u03e6\13\2\2\2\u03e5"+
		"\u03e4\3\2\2\2\u03e6\u03e9\3\2\2\2\u03e7\u03e8\3\2\2\2\u03e7\u03e5\3\2"+
		"\2\2\u03e8\u03eb\3\2\2\2\u03e9\u03e7\3\2\2\2\u03ea\u03e3\3\2\2\2\u03ea"+
		"\u03e7\3\2\2\2\u03eb\u0404\3\2\2\2\u03ec\u03ed\7\u00d6\2\2\u03ed\u03f1"+
		"\7\u00c8\2\2\u03ee\u03f0\13\2\2\2\u03ef\u03ee\3\2\2\2\u03f0\u03f3\3\2"+
		"\2\2\u03f1\u03f2\3\2\2\2\u03f1\u03ef\3\2\2\2\u03f2\u0404\3\2\2\2\u03f3"+
		"\u03f1\3\2\2\2\u03f4\u03f8\7\u00d6\2\2\u03f5\u03f7\13\2\2\2\u03f6\u03f5"+
		"\3\2\2\2\u03f7\u03fa\3\2\2\2\u03f8\u03f9\3\2\2\2\u03f8\u03f6\3\2\2\2\u03f9"+
		"\u0404\3\2\2\2\u03fa\u03f8\3\2\2\2\u03fb\u0404\7\u00c3\2\2\u03fc\u0400"+
		"\5\22\n\2\u03fd\u03ff\13\2\2\2\u03fe\u03fd\3\2\2\2\u03ff\u0402\3\2\2\2"+
		"\u0400\u0401\3\2\2\2\u0400\u03fe\3\2\2\2\u0401\u0404\3\2\2\2\u0402\u0400"+
		"\3\2\2\2\u0403\u012f\3\2\2\2\u0403\u0131\3\2\2\2\u0403\u0134\3\2\2\2\u0403"+
		"\u0139\3\2\2\2\u0403\u014b\3\2\2\2\u0403\u0152\3\2\2\2\u0403\u0158\3\2"+
		"\2\2\u0403\u0162\3\2\2\2\u0403\u016e\3\2\2\2\u0403\u0180\3\2\2\2\u0403"+
		"\u0190\3\2\2\2\u0403\u01b5\3\2\2\2\u0403\u01ca\3\2\2\2\u0403\u01d9\3\2"+
		"\2\2\u0403\u01ea\3\2\2\2\u0403\u01f1\3\2\2\2\u0403\u01fa\3\2\2\2\u0403"+
		"\u0203\3\2\2\2\u0403\u020c\3\2\2\2\u0403\u0213\3\2\2\2\u0403\u021a\3\2"+
		"\2\2\u0403\u0221\3\2\2\2\u0403\u022c\3\2\2\2\u0403\u0237\3\2\2\2\u0403"+
		"\u0246\3\2\2\2\u0403\u0252\3\2\2\2\u0403\u0260\3\2\2\2\u0403\u026a\3\2"+
		"\2\2\u0403\u0278\3\2\2\2\u0403\u0280\3\2\2\2\u0403\u0293\3\2\2\2\u0403"+
		"\u029c\3\2\2\2\u0403\u02a2\3\2\2\2\u0403\u02ac\3\2\2\2\u0403\u02b3\3\2"+
		"\2\2\u0403\u02d6\3\2\2\2\u0403\u02ec\3\2\2\2\u0403\u02f4\3\2\2\2\u0403"+
		"\u0310\3\2\2\2\u0403\u031a\3\2\2\2\u0403\u031f\3\2\2\2\u0403\u032b\3\2"+
		"\2\2\u0403\u0337\3\2\2\2\u0403\u0340\3\2\2\2\u0403\u0348\3\2\2\2\u0403"+
		"\u0354\3\2\2\2\u0403\u035a\3\2\2\2\u0403\u0368\3\2\2\2\u0403\u0370\3\2"+
		"\2\2\u0403\u0373\3\2\2\2\u0403\u0379\3\2\2\2\u0403\u0380\3\2\2\2\u0403"+
		"\u038e\3\2\2\2\u0403\u0393\3\2\2\2\u0403\u039a\3\2\2\2\u0403\u03a1\3\2"+
		"\2\2\u0403\u03a4\3\2\2\2\u0403\u03ae\3\2\2\2\u0403\u03be\3\2\2\2\u0403"+
		"\u03c5\3\2\2\2\u0403\u03c7\3\2\2\2\u0403\u03d7\3\2\2\2\u0403\u03dd\3\2"+
		"\2\2\u0403\u03e1\3\2\2\2\u0403\u03ec\3\2\2\2\u0403\u03f4\3\2\2\2\u0403"+
		"\u03fb\3\2\2\2\u0403\u03fc\3\2\2\2\u0404\21\3\2\2\2\u0405\u0406\7\67\2"+
		"\2\u0406\u04ae\7\u00c8\2\2\u0407\u0408\7N\2\2\u0408\u04ae\7\u00c8\2\2"+
		"\u0409\u040b\7k\2\2\u040a\u040c\7\u00c8\2\2\u040b\u040a\3\2\2\2\u040b"+
		"\u040c\3\2\2\2\u040c\u04ae\3\2\2\2\u040d\u040f\7\u00c5\2\2\u040e\u0410"+
		"\7\u00c8\2\2\u040f\u040e\3\2\2\2\u040f\u0410\3\2\2\2\u0410\u04ae\3\2\2"+
		"\2\u0411\u0412\7\u00d9\2\2\u0412\u04ae\7k\2\2\u0413\u0414\7\u00d9\2\2"+
		"\u0414\u0416\7\u00c8\2\2\u0415\u0417\7k\2\2\u0416\u0415\3\2\2\2\u0416"+
		"\u0417\3\2\2\2\u0417\u04ae\3\2\2\2\u0418\u0419\7\u00d9\2\2\u0419\u04ae"+
		"\7\u00b5\2\2\u041a\u041b\7\u00d9\2\2\u041b\u04ae\7\u00c9\2\2\u041c\u041d"+
		"\7\u00d9\2\2\u041d\u041e\7:\2\2\u041e\u04ae\7\u00c9\2\2\u041f\u0420\7"+
		"W\2\2\u0420\u04ae\7\u00e5\2\2\u0421\u0422\7r\2\2\u0422\u04ae\7\u00e5\2"+
		"\2\u0423\u0424\7\u00d9\2\2\u0424\u04ae\7\62\2\2\u0425\u0426\7\u00d9\2"+
		"\2\u0426\u0427\7\67\2\2\u0427\u04ae\7\u00e5\2\2\u0428\u0429\7\u00d9\2"+
		"\2\u0429\u04ae\7\u00f0\2\2\u042a\u042b\7\u00d9\2\2\u042b\u04ae\7u\2\2"+
		"\u042c\u042d\7\u00d9\2\2\u042d\u04ae\7\u008e\2\2\u042e\u042f\7\67\2\2"+
		"\u042f\u04ae\7t\2\2\u0430\u0431\7N\2\2\u0431\u04ae\7t\2\2\u0432\u0433"+
		"\7\21\2\2\u0433\u04ae\7t\2\2\u0434\u0435\7\u008d\2\2\u0435\u04ae\7\u00e5"+
		"\2\2\u0436\u0437\7\u008d\2\2\u0437\u04ae\7@\2\2\u0438\u0439\7\u00fc\2"+
		"\2\u0439\u04ae\7\u00e5\2\2\u043a\u043b\7\u00fc\2\2\u043b\u04ae\7@\2\2"+
		"\u043c\u043d\7\67\2\2\u043d\u043e\7\u00e9\2\2\u043e\u04ae\7\u0090\2\2"+
		"\u043f\u0440\7N\2\2\u0440\u0441\7\u00e9\2\2\u0441\u04ae\7\u0090\2\2\u0442"+
		"\u0443\7\21\2\2\u0443\u0444\7\u00e5\2\2\u0444\u0445\5\u00aeX\2\u0445\u0446"+
		"\7\u009b\2\2\u0446\u0447\7)\2\2\u0447\u04ae\3\2\2\2\u0448\u0449\7\21\2"+
		"\2\u0449\u044a\7\u00e5\2\2\u044a\u044b\5\u00aeX\2\u044b\u044c\7)\2\2\u044c"+
		"\u044d\7 \2\2\u044d\u04ae\3\2\2\2\u044e\u044f\7\21\2\2\u044f\u0450\7\u00e5"+
		"\2\2\u0450\u0451\5\u00aeX\2\u0451\u0452\7\u009b\2\2\u0452\u0453\7\u00dd"+
		"\2\2\u0453\u04ae\3\2\2\2\u0454\u0455\7\21\2\2\u0455\u0456\7\u00e5\2\2"+
		"\u0456\u0457\5\u00aeX\2\u0457\u0458\7\u00da\2\2\u0458\u0459\7 \2\2\u0459"+
		"\u04ae\3\2\2\2\u045a\u045b\7\21\2\2\u045b\u045c\7\u00e5\2\2\u045c\u045d"+
		"\5\u00aeX\2\u045d\u045e\7\u009b\2\2\u045e\u045f\7\u00da\2\2\u045f\u04ae"+
		"\3\2\2\2\u0460\u0461\7\21\2\2\u0461\u0462\7\u00e5\2\2\u0462\u0463\5\u00ae"+
		"X\2\u0463\u0464\7\u009b\2\2\u0464\u0465\7\u00e0\2\2\u0465\u0466\7\30\2"+
		"\2\u0466\u0467\7J\2\2\u0467\u04ae\3\2\2\2\u0468\u0469\7\21\2\2\u0469\u046a"+
		"\7\u00e5\2\2\u046a\u046b\5\u00aeX\2\u046b\u046c\7\u00d6\2\2\u046c\u046d"+
		"\7\u00da\2\2\u046d\u046e\7\u008c\2\2\u046e\u04ae\3\2\2\2\u046f\u0470\7"+
		"\21\2\2\u0470\u0471\7\u00e5\2\2\u0471\u0472\5\u00aeX\2\u0472\u0473\7T"+
		"\2\2\u0473\u0474\7\u00ac\2\2\u0474\u04ae\3\2\2\2\u0475\u0476\7\21\2\2"+
		"\u0476\u0477\7\u00e5\2\2\u0477\u0478\5\u00aeX\2\u0478\u0479\7\26\2\2\u0479"+
		"\u047a\7\u00ac\2\2\u047a\u04ae\3\2\2\2\u047b\u047c\7\21\2\2\u047c\u047d"+
		"\7\u00e5\2\2\u047d\u047e\5\u00aeX\2\u047e\u047f\7\u00f6\2\2\u047f\u0480"+
		"\7\u00ac\2\2\u0480\u04ae\3\2\2\2\u0481\u0482\7\21\2\2\u0482\u0483\7\u00e5"+
		"\2\2\u0483\u0484\5\u00aeX\2\u0484\u0485\7\u00ed\2\2\u0485\u04ae\3\2\2"+
		"\2\u0486\u0487\7\21\2\2\u0487\u0488\7\u00e5\2\2\u0488\u048a\5\u00aeX\2"+
		"\u0489\u048b\5&\24\2\u048a\u0489\3\2\2\2\u048a\u048b\3\2\2\2\u048b\u048c"+
		"\3\2\2\2\u048c\u048d\7\61\2\2\u048d\u04ae\3\2\2\2\u048e\u048f\7\21\2\2"+
		"\u048f\u0490\7\u00e5\2\2\u0490\u0492\5\u00aeX\2\u0491\u0493\5&\24\2\u0492"+
		"\u0491\3\2\2\2\u0492\u0493\3\2\2\2\u0493\u0494\3\2\2\2\u0494\u0495\7\64"+
		"\2\2\u0495\u04ae\3\2\2\2\u0496\u0497\7\21\2\2\u0497\u0498\7\u00e5\2\2"+
		"\u0498\u049a\5\u00aeX\2\u0499\u049b\5&\24\2\u049a\u0499\3\2\2\2\u049a"+
		"\u049b\3\2\2\2\u049b\u049c\3\2\2\2\u049c\u049d\7\u00d6\2\2\u049d\u049e"+
		"\7_\2\2\u049e\u04ae\3\2\2\2\u049f\u04a0\7\21\2\2\u04a0\u04a1\7\u00e5\2"+
		"\2\u04a1\u04a3\5\u00aeX\2\u04a2\u04a4\5&\24\2\u04a3\u04a2\3\2\2\2\u04a3"+
		"\u04a4\3\2\2\2\u04a4\u04a5\3\2\2\2\u04a5\u04a6\7\u00c2\2\2\u04a6\u04a7"+
		"\7.\2\2\u04a7\u04ae\3\2\2\2\u04a8\u04a9\7\u00de\2\2\u04a9\u04ae\7\u00ef"+
		"\2\2\u04aa\u04ae\7\60\2\2\u04ab\u04ae\7\u00ca\2\2\u04ac\u04ae\7I\2\2\u04ad"+
		"\u0405\3\2\2\2\u04ad\u0407\3\2\2\2\u04ad\u0409\3\2\2\2\u04ad\u040d\3\2"+
		"\2\2\u04ad\u0411\3\2\2\2\u04ad\u0413\3\2\2\2\u04ad\u0418\3\2\2\2\u04ad"+
		"\u041a\3\2\2\2\u04ad\u041c\3\2\2\2\u04ad\u041f\3\2\2\2\u04ad\u0421\3\2"+
		"\2\2\u04ad\u0423\3\2\2\2\u04ad\u0425\3\2\2\2\u04ad\u0428\3\2\2\2\u04ad"+
		"\u042a\3\2\2\2\u04ad\u042c\3\2\2\2\u04ad\u042e\3\2\2\2\u04ad\u0430\3\2"+
		"\2\2\u04ad\u0432\3\2\2\2\u04ad\u0434\3\2\2\2\u04ad\u0436\3\2\2\2\u04ad"+
		"\u0438\3\2\2\2\u04ad\u043a\3\2\2\2\u04ad\u043c\3\2\2\2\u04ad\u043f\3\2"+
		"\2\2\u04ad\u0442\3\2\2\2\u04ad\u0448\3\2\2\2\u04ad\u044e\3\2\2\2\u04ad"+
		"\u0454\3\2\2\2\u04ad\u045a\3\2\2\2\u04ad\u0460\3\2\2\2\u04ad\u0468\3\2"+
		"\2\2\u04ad\u046f\3\2\2\2\u04ad\u0475\3\2\2\2\u04ad\u047b\3\2\2\2\u04ad"+
		"\u0481\3\2\2\2\u04ad\u0486\3\2\2\2\u04ad\u048e\3\2\2\2\u04ad\u0496\3\2"+
		"\2\2\u04ad\u049f\3\2\2\2\u04ad\u04a8\3\2\2\2\u04ad\u04aa\3\2\2\2\u04ad"+
		"\u04ab\3\2\2\2\u04ad\u04ac\3\2\2\2\u04ae\23\3\2\2\2\u04af\u04b1\7\67\2"+
		"\2\u04b0\u04b2\7\u00e9\2\2\u04b1\u04b0\3\2\2\2\u04b1\u04b2\3\2\2\2\u04b2"+
		"\u04b4\3\2\2\2\u04b3\u04b5\7Y\2\2\u04b4\u04b3\3\2\2\2\u04b4\u04b5\3\2"+
		"\2\2\u04b5\u04b6\3\2\2\2\u04b6\u04ba\7\u00e5\2\2\u04b7\u04b8\7p\2\2\u04b8"+
		"\u04b9\7\u009b\2\2\u04b9\u04bb\7U\2\2\u04ba\u04b7\3\2\2\2\u04ba\u04bb"+
		"\3\2\2\2\u04bb\u04bc\3\2\2\2\u04bc\u04bd\5\u00acW\2\u04bd\25\3\2\2\2\u04be"+
		"\u04bf\7\67\2\2\u04bf\u04c1\7\u00a3\2\2\u04c0\u04be\3\2\2\2\u04c0\u04c1"+
		"\3\2\2\2\u04c1\u04c2\3\2\2\2\u04c2\u04c3\7\u00c2\2\2\u04c3\u04c4\7\u00e5"+
		"\2\2\u04c4\u04c5\5\u00acW\2\u04c5\27\3\2\2\2\u04c6\u04c7\7)\2\2\u04c7"+
		"\u04c8\7 \2\2\u04c8\u04cc\5\u0094K\2\u04c9\u04ca\7\u00dd\2\2\u04ca\u04cb"+
		"\7 \2\2\u04cb\u04cd\5\u0098M\2\u04cc\u04c9\3\2\2\2\u04cc\u04cd\3\2\2\2"+
		"\u04cd\u04ce\3\2\2\2\u04ce\u04cf\7|\2\2\u04cf\u04d0\7\u0121\2\2\u04d0"+
		"\u04d1\7\37\2\2\u04d1\31\3\2\2\2\u04d2\u04d3\7\u00da\2\2\u04d3\u04d4\7"+
		" \2\2\u04d4\u04d5\5\u0094K\2\u04d5\u04d8\7\u009f\2\2\u04d6\u04d9\5@!\2"+
		"\u04d7\u04d9\5B\"\2\u04d8\u04d6\3\2\2\2\u04d8\u04d7\3\2\2\2\u04d9\u04dd"+
		"\3\2\2\2\u04da\u04db\7\u00e0\2\2\u04db\u04dc\7\30\2\2\u04dc\u04de\7J\2"+
		"\2\u04dd\u04da\3\2\2\2\u04dd\u04de\3\2\2\2\u04de\33\3\2\2\2\u04df\u04e0"+
		"\7\u008c\2\2\u04e0\u04e1\7\u011d\2\2\u04e1\35\3\2\2\2\u04e2\u04e3\7/\2"+
		"\2\u04e3\u04e4\7\u011d\2\2\u04e4\37\3\2\2\2\u04e5\u04e7\5\60\31\2\u04e6"+
		"\u04e5\3\2\2\2\u04e6\u04e7\3\2\2\2\u04e7\u04e8\3\2\2\2\u04e8\u04e9\5R"+
		"*\2\u04e9\u04ea\5N(\2\u04ea!\3\2\2\2\u04eb\u04ec\7y\2\2\u04ec\u04ee\7"+
		"\u00ab\2\2\u04ed\u04ef\7\u00e5\2\2\u04ee\u04ed\3\2\2\2\u04ee\u04ef\3\2"+
		"\2\2\u04ef\u04f0\3\2\2\2\u04f0\u04f7\5\u00acW\2\u04f1\u04f5\5&\24\2\u04f2"+
		"\u04f3\7p\2\2\u04f3\u04f4\7\u009b\2\2\u04f4\u04f6\7U\2\2\u04f5\u04f2\3"+
		"\2\2\2\u04f5\u04f6\3\2\2\2\u04f6\u04f8\3\2\2\2\u04f7\u04f1\3\2\2\2\u04f7"+
		"\u04f8\3\2\2\2\u04f8\u0523\3\2\2\2\u04f9\u04fa\7y\2\2\u04fa\u04fc\7|\2"+
		"\2\u04fb\u04fd\7\u00e5\2\2\u04fc\u04fb\3\2\2\2\u04fc\u04fd\3\2\2\2\u04fd"+
		"\u04fe\3\2\2\2\u04fe\u0500\5\u00acW\2\u04ff\u0501\5&\24\2\u0500\u04ff"+
		"\3\2\2\2\u0500\u0501\3\2\2\2\u0501\u0505\3\2\2\2\u0502\u0503\7p\2\2\u0503"+
		"\u0504\7\u009b\2\2\u0504\u0506\7U\2\2\u0505\u0502\3\2\2\2\u0505\u0506"+
		"\3\2\2\2\u0506\u0523\3\2\2\2\u0507\u0508\7y\2\2\u0508\u050a\7\u00ab\2"+
		"\2\u0509\u050b\7\u008b\2\2\u050a\u0509\3\2\2\2\u050a\u050b\3\2\2\2\u050b"+
		"\u050c\3\2\2\2\u050c\u050d\7K\2\2\u050d\u050f\7\u011d\2\2\u050e\u0510"+
		"\5\u00a8U\2\u050f\u050e\3\2\2\2\u050f\u0510\3\2\2\2\u0510\u0512\3\2\2"+
		"\2\u0511\u0513\5D#\2\u0512\u0511\3\2\2\2\u0512\u0513\3\2\2\2\u0513\u0523"+
		"\3\2\2\2\u0514\u0515\7y\2\2\u0515\u0517\7\u00ab\2\2\u0516\u0518\7\u008b"+
		"\2\2\u0517\u0516\3\2\2\2\u0517\u0518\3\2\2\2\u0518\u0519\3\2\2\2\u0519"+
		"\u051b\7K\2\2\u051a\u051c\7\u011d\2\2\u051b\u051a\3\2\2\2\u051b\u051c"+
		"\3\2\2\2\u051c\u051d\3\2\2\2\u051d\u0520\5\64\33\2\u051e\u051f\7\u00a2"+
		"\2\2\u051f\u0521\58\35\2\u0520\u051e\3\2\2\2\u0520\u0521\3\2\2\2\u0521"+
		"\u0523\3\2\2\2\u0522\u04eb\3\2\2\2\u0522\u04f9\3\2\2\2\u0522\u0507\3\2"+
		"\2\2\u0522\u0514\3\2\2\2\u0523#\3\2\2\2\u0524\u0526\5&\24\2\u0525\u0527"+
		"\5\34\17\2\u0526\u0525\3\2\2\2\u0526\u0527\3\2\2\2\u0527%\3\2\2\2\u0528"+
		"\u0529\7\u00ac\2\2\u0529\u052a\7\4\2\2\u052a\u052f\5(\25\2\u052b\u052c"+
		"\7\6\2\2\u052c\u052e\5(\25\2\u052d\u052b\3\2\2\2\u052e\u0531\3\2\2\2\u052f"+
		"\u052d\3\2\2\2\u052f\u0530\3\2\2\2\u0530\u0532\3\2\2\2\u0531\u052f\3\2"+
		"\2\2\u0532\u0533\7\5\2\2\u0533\'\3\2\2\2\u0534\u0537\5\u0104\u0083\2\u0535"+
		"\u0536\7\u010a\2\2\u0536\u0538\5\u00c6d\2\u0537\u0535\3\2\2\2\u0537\u0538"+
		"\3\2\2\2\u0538)\3\2\2\2\u0539\u053a\t\16\2\2\u053a+\3\2\2\2\u053b\u0541"+
		"\5\u00fe\u0080\2\u053c\u0541\7\u011d\2\2\u053d\u0541\5\u00c8e\2\u053e"+
		"\u0541\5\u00caf\2\u053f\u0541\5\u00ccg\2\u0540\u053b\3\2\2\2\u0540\u053c"+
		"\3\2\2\2\u0540\u053d\3\2\2\2\u0540\u053e\3\2\2\2\u0540\u053f\3\2\2\2\u0541"+
		"-\3\2\2\2\u0542\u0547\5\u0104\u0083\2\u0543\u0544\7\7\2\2\u0544\u0546"+
		"\5\u0104\u0083\2\u0545\u0543\3\2\2\2\u0546\u0549\3\2\2\2\u0547\u0545\3"+
		"\2\2\2\u0547\u0548\3\2\2\2\u0548/\3\2\2\2\u0549\u0547\3\2\2\2\u054a\u054b"+
		"\7\u0108\2\2\u054b\u0550\5\62\32\2\u054c\u054d\7\6\2\2\u054d\u054f\5\62"+
		"\32\2\u054e\u054c\3\2\2\2\u054f\u0552\3\2\2\2\u0550\u054e\3\2\2\2\u0550"+
		"\u0551\3\2\2\2\u0551\61\3\2\2\2\u0552\u0550\3\2\2\2\u0553\u0555\5\u0100"+
		"\u0081\2\u0554\u0556\5\u0094K\2\u0555\u0554\3\2\2\2\u0555\u0556\3\2\2"+
		"\2\u0556\u0558\3\2\2\2\u0557\u0559\7\30\2\2\u0558\u0557\3\2\2\2\u0558"+
		"\u0559\3\2\2\2\u0559\u055a\3\2\2\2\u055a\u055b\7\4\2\2\u055b\u055c\5 "+
		"\21\2\u055c\u055d\7\5\2\2\u055d\63\3\2\2\2\u055e\u055f\7\u0101\2\2\u055f"+
		"\u0560\5\u00acW\2\u0560\65\3\2\2\2\u0561\u0562\7\u00a2\2\2\u0562\u056c"+
		"\58\35\2\u0563\u0564\7\u00ad\2\2\u0564\u0565\7 \2\2\u0565\u056c\5\u00b6"+
		"\\\2\u0566\u056c\5\30\r\2\u0567\u056c\5\34\17\2\u0568\u056c\5\36\20\2"+
		"\u0569\u056a\7\u00e8\2\2\u056a\u056c\58\35\2\u056b\u0561\3\2\2\2\u056b"+
		"\u0563\3\2\2\2\u056b\u0566\3\2\2\2\u056b\u0567\3\2\2\2\u056b\u0568\3\2"+
		"\2\2\u056b\u0569\3\2\2\2\u056c\u056f\3\2\2\2\u056d\u056b\3\2\2\2\u056d"+
		"\u056e\3\2\2\2\u056e\67\3\2\2\2\u056f\u056d\3\2\2\2\u0570\u0571\7\4\2"+
		"\2\u0571\u0576\5:\36\2\u0572\u0573\7\6\2\2\u0573\u0575\5:\36\2\u0574\u0572"+
		"\3\2\2\2\u0575\u0578\3\2\2\2\u0576\u0574\3\2\2\2\u0576\u0577\3\2\2\2\u0577"+
		"\u0579\3\2\2\2\u0578\u0576\3\2\2\2\u0579\u057a\7\5\2\2\u057a9\3\2\2\2"+
		"\u057b\u0580\5<\37\2\u057c\u057e\7\u010a\2\2\u057d\u057c\3\2\2\2\u057d"+
		"\u057e\3\2\2\2\u057e\u057f\3\2\2\2\u057f\u0581\5> \2\u0580\u057d\3\2\2"+
		"\2\u0580\u0581\3\2\2\2\u0581;\3\2\2\2\u0582\u0587\5\u0104\u0083\2\u0583"+
		"\u0584\7\7\2\2\u0584\u0586\5\u0104\u0083\2\u0585\u0583\3\2\2\2\u0586\u0589"+
		"\3\2\2\2\u0587\u0585\3\2\2\2\u0587\u0588\3\2\2\2\u0588\u058c\3\2\2\2\u0589"+
		"\u0587\3\2\2\2\u058a\u058c\7\u011d\2\2\u058b\u0582\3\2\2\2\u058b\u058a"+
		"\3\2\2\2\u058c=\3\2\2\2\u058d\u0592\7\u0121\2\2\u058e\u0592\7\u0123\2"+
		"\2\u058f\u0592\5\u00ceh\2\u0590\u0592\7\u011d\2\2\u0591\u058d\3\2\2\2"+
		"\u0591\u058e\3\2\2\2\u0591\u058f\3\2\2\2\u0591\u0590\3\2\2\2\u0592?\3"+
		"\2\2\2\u0593\u0594\7\4\2\2\u0594\u0599\5\u00c6d\2\u0595\u0596\7\6\2\2"+
		"\u0596\u0598\5\u00c6d\2\u0597\u0595\3\2\2\2\u0598\u059b\3\2\2\2\u0599"+
		"\u0597\3\2\2\2\u0599\u059a\3\2\2\2\u059a\u059c\3\2\2\2\u059b\u0599\3\2"+
		"\2\2\u059c\u059d\7\5\2\2\u059dA\3\2\2\2\u059e\u059f\7\4\2\2\u059f\u05a4"+
		"\5@!\2\u05a0\u05a1\7\6\2\2\u05a1\u05a3\5@!\2\u05a2\u05a0\3\2\2\2\u05a3"+
		"\u05a6\3\2\2\2\u05a4\u05a2\3\2\2\2\u05a4\u05a5\3\2\2\2\u05a5\u05a7\3\2"+
		"\2\2\u05a6\u05a4\3\2\2\2\u05a7\u05a8\7\5\2\2\u05a8C\3\2\2\2\u05a9\u05aa"+
		"\7\u00e0\2\2\u05aa\u05ab\7\30\2\2\u05ab\u05b0\5F$\2\u05ac\u05ad\7\u00e0"+
		"\2\2\u05ad\u05ae\7 \2\2\u05ae\u05b0\5H%\2\u05af\u05a9\3\2\2\2\u05af\u05ac"+
		"\3\2\2\2\u05b0E\3\2\2\2\u05b1\u05b2\7x\2\2\u05b2\u05b3\7\u011d\2\2\u05b3"+
		"\u05b4\7\u00a7\2\2\u05b4\u05b7\7\u011d\2\2\u05b5\u05b7\5\u0104\u0083\2"+
		"\u05b6\u05b1\3\2\2\2\u05b6\u05b5\3\2\2\2\u05b7G\3\2\2\2\u05b8\u05bc\7"+
		"\u011d\2\2\u05b9\u05ba\7\u0108\2\2\u05ba\u05bb\7\u00d4\2\2\u05bb\u05bd"+
		"\58\35\2\u05bc\u05b9\3\2\2\2\u05bc\u05bd\3\2\2\2\u05bdI\3\2\2\2\u05be"+
		"\u05bf\5\u0104\u0083\2\u05bf\u05c0\7\u011d\2\2\u05c0K\3\2\2\2\u05c1\u05c2"+
		"\5\"\22\2\u05c2\u05c3\5R*\2\u05c3\u05c4\5N(\2\u05c4\u05f5\3\2\2\2\u05c5"+
		"\u05c7\5x=\2\u05c6\u05c8\5P)\2\u05c7\u05c6\3\2\2\2\u05c8\u05c9\3\2\2\2"+
		"\u05c9\u05c7\3\2\2\2\u05c9\u05ca\3\2\2\2\u05ca\u05f5\3\2\2\2\u05cb\u05cc"+
		"\7E\2\2\u05cc\u05cd\7f\2\2\u05cd\u05ce\5\u00acW\2\u05ce\u05d0\5\u00a6"+
		"T\2\u05cf\u05d1\5p9\2\u05d0\u05cf\3\2\2\2\u05d0\u05d1\3\2\2\2\u05d1\u05f5"+
		"\3\2\2\2\u05d2\u05d3\7\u00fe\2\2\u05d3\u05d4\5\u00acW\2\u05d4\u05d5\5"+
		"\u00a6T\2\u05d5\u05d7\5b\62\2\u05d6\u05d8\5p9\2\u05d7\u05d6\3\2\2\2\u05d7"+
		"\u05d8\3\2\2\2\u05d8\u05f5\3\2\2\2\u05d9\u05da\7\u0093\2\2\u05da\u05db"+
		"\7|\2\2\u05db\u05dc\5\u00acW\2\u05dc\u05dd\5\u00a6T\2\u05dd\u05e3\7\u0101"+
		"\2\2\u05de\u05e4\5\u00acW\2\u05df\u05e0\7\4\2\2\u05e0\u05e1\5 \21\2\u05e1"+
		"\u05e2\7\5\2\2\u05e2\u05e4\3\2\2\2\u05e3\u05de\3\2\2\2\u05e3\u05df\3\2"+
		"\2\2\u05e4\u05e5\3\2\2\2\u05e5\u05e6\5\u00a6T\2\u05e6\u05e7\7\u009f\2"+
		"\2\u05e7\u05eb\5\u00be`\2\u05e8\u05ea\5d\63\2\u05e9\u05e8\3\2\2\2\u05ea"+
		"\u05ed\3\2\2\2\u05eb\u05e9\3\2\2\2\u05eb\u05ec\3\2\2\2\u05ec\u05f1\3\2"+
		"\2\2\u05ed\u05eb\3\2\2\2\u05ee\u05f0\5f\64\2\u05ef\u05ee\3\2\2\2\u05f0"+
		"\u05f3\3\2\2\2\u05f1\u05ef\3\2\2\2\u05f1\u05f2\3\2\2\2\u05f2\u05f5\3\2"+
		"\2\2\u05f3\u05f1\3\2\2\2\u05f4\u05c1\3\2\2\2\u05f4\u05c5\3\2\2\2\u05f4"+
		"\u05cb\3\2\2\2\u05f4\u05d2\3\2\2\2\u05f4\u05d9\3\2\2\2\u05f5M\3\2\2\2"+
		"\u05f6\u05f7\7\u00a4\2\2\u05f7\u05f8\7 \2\2\u05f8\u05fd\5V,\2\u05f9\u05fa"+
		"\7\6\2\2\u05fa\u05fc\5V,\2\u05fb\u05f9\3\2\2\2\u05fc\u05ff\3\2\2\2\u05fd"+
		"\u05fb\3\2\2\2\u05fd\u05fe\3\2\2\2\u05fe\u0601\3\2\2\2\u05ff\u05fd\3\2"+
		"\2\2\u0600\u05f6\3\2\2\2\u0600\u0601\3\2\2\2\u0601\u060c\3\2\2\2\u0602"+
		"\u0603\7(\2\2\u0603\u0604\7 \2\2\u0604\u0609\5\u00bc_\2\u0605\u0606\7"+
		"\6\2\2\u0606\u0608\5\u00bc_\2\u0607\u0605\3\2\2\2\u0608\u060b\3\2\2\2"+
		"\u0609\u0607\3\2\2\2\u0609\u060a\3\2\2\2\u060a\u060d\3\2\2\2\u060b\u0609"+
		"\3\2\2\2\u060c\u0602\3\2\2\2\u060c\u060d\3\2\2\2\u060d\u0618\3\2\2\2\u060e"+
		"\u060f\7M\2\2\u060f\u0610\7 \2\2\u0610\u0615\5\u00bc_\2\u0611\u0612\7"+
		"\6\2\2\u0612\u0614\5\u00bc_\2\u0613\u0611\3\2\2\2\u0614\u0617\3\2\2\2"+
		"\u0615\u0613\3\2\2\2\u0615\u0616\3\2\2\2\u0616\u0619\3\2\2\2\u0617\u0615"+
		"\3\2\2\2\u0618\u060e\3\2\2\2\u0618\u0619\3\2\2\2\u0619\u0624\3\2\2\2\u061a"+
		"\u061b\7\u00dc\2\2\u061b\u061c\7 \2\2\u061c\u0621\5V,\2\u061d\u061e\7"+
		"\6\2\2\u061e\u0620\5V,\2\u061f\u061d\3\2\2\2\u0620\u0623\3\2\2\2\u0621"+
		"\u061f\3\2\2\2\u0621\u0622\3\2\2\2\u0622\u0625\3\2\2\2\u0623\u0621\3\2"+
		"\2\2\u0624\u061a\3\2\2\2\u0624\u0625\3\2\2\2\u0625\u0627\3\2\2\2\u0626"+
		"\u0628\5\u00f0y\2\u0627\u0626\3\2\2\2\u0627\u0628\3\2\2\2\u0628\u062e"+
		"\3\2\2\2\u0629\u062c\7\u0087\2\2\u062a\u062d\7\20\2\2\u062b\u062d\5\u00bc"+
		"_\2\u062c\u062a\3\2\2\2\u062c\u062b\3\2\2\2\u062d\u062f\3\2\2\2\u062e"+
		"\u0629\3\2\2\2\u062e\u062f\3\2\2\2\u062fO\3\2\2\2\u0630\u0631\5\"\22\2"+
		"\u0631\u0632\5Z.\2\u0632Q\3\2\2\2\u0633\u0634\b*\1\2\u0634\u0635\5T+\2"+
		"\u0635\u064d\3\2\2\2\u0636\u0637\f\5\2\2\u0637\u0638\6*\5\2\u0638\u063a"+
		"\t\17\2\2\u0639\u063b\5\u0086D\2\u063a\u0639\3\2\2\2\u063a\u063b\3\2\2"+
		"\2\u063b\u063c\3\2\2\2\u063c\u064c\5R*\6\u063d\u063e\f\4\2\2\u063e\u063f"+
		"\6*\7\2\u063f\u0641\7z\2\2\u0640\u0642\5\u0086D\2\u0641\u0640\3\2\2\2"+
		"\u0641\u0642\3\2\2\2\u0642\u0643\3\2\2\2\u0643\u064c\5R*\5\u0644\u0645"+
		"\f\3\2\2\u0645\u0646\6*\t\2\u0646\u0648\t\20\2\2\u0647\u0649\5\u0086D"+
		"\2\u0648\u0647\3\2\2\2\u0648\u0649\3\2\2\2\u0649\u064a\3\2\2\2\u064a\u064c"+
		"\5R*\4\u064b\u0636\3\2\2\2\u064b\u063d\3\2\2\2\u064b\u0644\3\2\2\2\u064c"+
		"\u064f\3\2\2\2\u064d\u064b\3\2\2\2\u064d\u064e\3\2\2\2\u064eS\3\2\2\2"+
		"\u064f\u064d\3\2\2\2\u0650\u065a\5\\/\2\u0651\u065a\5X-\2\u0652\u0653"+
		"\7\u00e5\2\2\u0653\u065a\5\u00acW\2\u0654\u065a\5\u00a2R\2\u0655\u0656"+
		"\7\4\2\2\u0656\u0657\5 \21\2\u0657\u0658\7\5\2\2\u0658\u065a\3\2\2\2\u0659"+
		"\u0650\3\2\2\2\u0659\u0651\3\2\2\2\u0659\u0652\3\2\2\2\u0659\u0654\3\2"+
		"\2\2\u0659\u0655\3\2\2\2\u065aU\3\2\2\2\u065b\u065d\5\u00bc_\2\u065c\u065e"+
		"\t\21\2\2\u065d\u065c\3\2\2\2\u065d\u065e\3\2\2\2\u065e\u0661\3\2\2\2"+
		"\u065f\u0660\7\u009d\2\2\u0660\u0662\t\22\2\2\u0661\u065f\3\2\2\2\u0661"+
		"\u0662\3\2\2\2\u0662W\3\2\2\2\u0663\u0665\5x=\2\u0664\u0666\5Z.\2\u0665"+
		"\u0664\3\2\2\2\u0666\u0667\3\2\2\2\u0667\u0665\3\2\2\2\u0667\u0668\3\2"+
		"\2\2\u0668Y\3\2\2\2\u0669\u066b\5^\60\2\u066a\u066c\5p9\2\u066b\u066a"+
		"\3\2\2\2\u066b\u066c\3\2\2\2\u066c\u066d\3\2\2\2\u066d\u066e\5N(\2\u066e"+
		"\u0685\3\2\2\2\u066f\u0673\5`\61\2\u0670\u0672\5\u0084C\2\u0671\u0670"+
		"\3\2\2\2\u0672\u0675\3\2\2\2\u0673\u0671\3\2\2\2\u0673\u0674\3\2\2\2\u0674"+
		"\u0677\3\2\2\2\u0675\u0673\3\2\2\2\u0676\u0678\5p9\2\u0677\u0676\3\2\2"+
		"\2\u0677\u0678\3\2\2\2\u0678\u067a\3\2\2\2\u0679\u067b\5z>\2\u067a\u0679"+
		"\3\2\2\2\u067a\u067b\3\2\2\2\u067b\u067d\3\2\2\2\u067c\u067e\5r:\2\u067d"+
		"\u067c\3\2\2\2\u067d\u067e\3\2\2\2\u067e\u0680\3\2\2\2\u067f\u0681\5\u00f0"+
		"y\2\u0680\u067f\3\2\2\2\u0680\u0681\3\2\2\2\u0681\u0682\3\2\2\2\u0682"+
		"\u0683\5N(\2\u0683\u0685\3\2\2\2\u0684\u0669\3\2\2\2\u0684\u066f\3\2\2"+
		"\2\u0685[\3\2\2\2\u0686\u0688\5^\60\2\u0687\u0689\5x=\2\u0688\u0687\3"+
		"\2\2\2\u0688\u0689\3\2\2\2\u0689\u068b\3\2\2\2\u068a\u068c\5p9\2\u068b"+
		"\u068a\3\2\2\2\u068b\u068c\3\2\2\2\u068c\u06a4\3\2\2\2\u068d\u068f\5`"+
		"\61\2\u068e\u0690\5x=\2\u068f\u068e\3\2\2\2\u068f\u0690\3\2\2\2\u0690"+
		"\u0694\3\2\2\2\u0691\u0693\5\u0084C\2\u0692\u0691\3\2\2\2\u0693\u0696"+
		"\3\2\2\2\u0694\u0692\3\2\2\2\u0694\u0695\3\2\2\2\u0695\u0698\3\2\2\2\u0696"+
		"\u0694\3\2\2\2\u0697\u0699\5p9\2\u0698\u0697\3\2\2\2\u0698\u0699\3\2\2"+
		"\2\u0699\u069b\3\2\2\2\u069a\u069c\5z>\2\u069b\u069a\3\2\2\2\u069b\u069c"+
		"\3\2\2\2\u069c\u069e\3\2\2\2\u069d\u069f\5r:\2\u069e\u069d\3\2\2\2\u069e"+
		"\u069f\3\2\2\2\u069f\u06a1\3\2\2\2\u06a0\u06a2\5\u00f0y\2\u06a1\u06a0"+
		"\3\2\2\2\u06a1\u06a2\3\2\2\2\u06a2\u06a4\3\2\2\2\u06a3\u0686\3\2\2\2\u06a3"+
		"\u068d\3\2\2\2\u06a4]\3\2\2\2\u06a5\u06a6\7\u00d0\2\2\u06a6\u06a7\7\u00f1"+
		"\2\2\u06a7\u06a8\7\4\2\2\u06a8\u06a9\5\u00b4[\2\u06a9\u06aa\7\5\2\2\u06aa"+
		"\u06b0\3\2\2\2\u06ab\u06ac\7\u0091\2\2\u06ac\u06b0\5\u00b4[\2\u06ad\u06ae"+
		"\7\u00bd\2\2\u06ae\u06b0\5\u00b4[\2\u06af\u06a5\3\2\2\2\u06af\u06ab\3"+
		"\2\2\2\u06af\u06ad\3\2\2\2\u06b0\u06b2\3\2\2\2\u06b1\u06b3\5\u00a8U\2"+
		"\u06b2\u06b1\3\2\2\2\u06b2\u06b3\3\2\2\2\u06b3\u06b6\3\2\2\2\u06b4\u06b5"+
		"\7\u00bb\2\2\u06b5\u06b7\7\u011d\2\2\u06b6\u06b4\3\2\2\2\u06b6\u06b7\3"+
		"\2\2\2\u06b7\u06b8\3\2\2\2\u06b8\u06b9\7\u0101\2\2\u06b9\u06c6\7\u011d"+
		"\2\2\u06ba\u06c4\7\30\2\2\u06bb\u06c5\5\u0096L\2\u06bc\u06c5\5\u00e6t"+
		"\2\u06bd\u06c0\7\4\2\2\u06be\u06c1\5\u0096L\2\u06bf\u06c1\5\u00e6t\2\u06c0"+
		"\u06be\3\2\2\2\u06c0\u06bf\3\2\2\2\u06c1\u06c2\3\2\2\2\u06c2\u06c3\7\5"+
		"\2\2\u06c3\u06c5\3\2\2\2\u06c4\u06bb\3\2\2\2\u06c4\u06bc\3\2\2\2\u06c4"+
		"\u06bd\3\2\2\2\u06c5\u06c7\3\2\2\2\u06c6\u06ba\3\2\2\2\u06c6\u06c7\3\2"+
		"\2\2\u06c7\u06c9\3\2\2\2\u06c8\u06ca\5\u00a8U\2\u06c9\u06c8\3\2\2\2\u06c9"+
		"\u06ca\3\2\2\2\u06ca\u06cd\3\2\2\2\u06cb\u06cc\7\u00ba\2\2\u06cc\u06ce"+
		"\7\u011d\2\2\u06cd\u06cb\3\2\2\2\u06cd\u06ce\3\2\2\2\u06ce_\3\2\2\2\u06cf"+
		"\u06d3\7\u00d0\2\2\u06d0\u06d2\5t;\2\u06d1\u06d0\3\2\2\2\u06d2\u06d5\3"+
		"\2\2\2\u06d3\u06d1\3\2\2\2\u06d3\u06d4\3\2\2\2\u06d4\u06d7\3\2\2\2\u06d5"+
		"\u06d3\3\2\2\2\u06d6\u06d8\5\u0086D\2\u06d7\u06d6\3\2\2\2\u06d7\u06d8"+
		"\3\2\2\2\u06d8\u06d9\3\2\2\2\u06d9\u06da\5\u00b4[\2\u06daa\3\2\2\2\u06db"+
		"\u06dc\7\u00d6\2\2\u06dc\u06dd\5l\67\2\u06ddc\3\2\2\2\u06de\u06df\7\u0105"+
		"\2\2\u06df\u06e2\7\u0092\2\2\u06e0\u06e1\7\23\2\2\u06e1\u06e3\5\u00be"+
		"`\2\u06e2\u06e0\3\2\2\2\u06e2\u06e3\3\2\2\2\u06e3\u06e4\3\2\2\2\u06e4"+
		"\u06e5\7\u00eb\2\2\u06e5\u06e6\5h\65\2\u06e6e\3\2\2\2\u06e7\u06e8\7\u0105"+
		"\2\2\u06e8\u06e9\7\u009b\2\2\u06e9\u06ec\7\u0092\2\2\u06ea\u06eb\7\23"+
		"\2\2\u06eb\u06ed\5\u00be`\2\u06ec\u06ea\3\2\2\2\u06ec\u06ed\3\2\2\2\u06ed"+
		"\u06ee\3\2\2\2\u06ee\u06ef\7\u00eb\2\2\u06ef\u06f0\5j\66\2\u06f0g\3\2"+
		"\2\2\u06f1\u06f9\7E\2\2\u06f2\u06f3\7\u00fe\2\2\u06f3\u06f4\7\u00d6\2"+
		"\2\u06f4\u06f9\7\u0114\2\2\u06f5\u06f6\7\u00fe\2\2\u06f6\u06f7\7\u00d6"+
		"\2\2\u06f7\u06f9\5l\67\2\u06f8\u06f1\3\2\2\2\u06f8\u06f2\3\2\2\2\u06f8"+
		"\u06f5\3\2\2\2\u06f9i\3\2\2\2\u06fa\u06fb\7y\2\2\u06fb\u070d\7\u0114\2"+
		"\2\u06fc\u06fd\7y\2\2\u06fd\u06fe\7\4\2\2\u06fe\u06ff\5\u00aaV\2\u06ff"+
		"\u0700\7\5\2\2\u0700\u0701\7\u0102\2\2\u0701\u0702\7\4\2\2\u0702\u0707"+
		"\5\u00bc_\2\u0703\u0704\7\6\2\2\u0704\u0706\5\u00bc_\2\u0705\u0703\3\2"+
		"\2\2\u0706\u0709\3\2\2\2\u0707\u0705\3\2\2\2\u0707\u0708\3\2\2\2\u0708"+
		"\u070a\3\2\2\2\u0709\u0707\3\2\2\2\u070a\u070b\7\5\2\2\u070b\u070d\3\2"+
		"\2\2\u070c\u06fa\3\2\2\2\u070c\u06fc\3\2\2\2\u070dk\3\2\2\2\u070e\u0713"+
		"\5n8\2\u070f\u0710\7\6\2\2\u0710\u0712\5n8\2\u0711\u070f\3\2\2\2\u0712"+
		"\u0715\3\2\2\2\u0713\u0711\3\2\2\2\u0713\u0714\3\2\2\2\u0714m\3\2\2\2"+
		"\u0715\u0713\3\2\2\2\u0716\u0717\5\u00acW\2\u0717\u0718\7\u010a\2\2\u0718"+
		"\u0719\5\u00bc_\2\u0719o\3\2\2\2\u071a\u071b\7\u0106\2\2\u071b\u071c\5"+
		"\u00be`\2\u071cq\3\2\2\2\u071d\u071e\7n\2\2\u071e\u071f\5\u00be`\2\u071f"+
		"s\3\2\2\2\u0720\u0721\7\b\2\2\u0721\u0728\5v<\2\u0722\u0724\7\6\2\2\u0723"+
		"\u0722\3\2\2\2\u0723\u0724\3\2\2\2\u0724\u0725\3\2\2\2\u0725\u0727\5v"+
		"<\2\u0726\u0723\3\2\2\2\u0727\u072a\3\2\2\2\u0728\u0726\3\2\2\2\u0728"+
		"\u0729\3\2\2\2\u0729\u072b\3\2\2\2\u072a\u0728\3\2\2\2\u072b\u072c\7\t"+
		"\2\2\u072cu\3\2\2\2\u072d\u073b\5\u0104\u0083\2\u072e\u072f\5\u0104\u0083"+
		"\2\u072f\u0730\7\4\2\2\u0730\u0735\5\u00c4c\2\u0731\u0732\7\6\2\2\u0732"+
		"\u0734\5\u00c4c\2\u0733\u0731\3\2\2\2\u0734\u0737\3\2\2\2\u0735\u0733"+
		"\3\2\2\2\u0735\u0736\3\2\2\2\u0736\u0738\3\2\2\2\u0737\u0735\3\2\2\2\u0738"+
		"\u0739\7\5\2\2\u0739\u073b\3\2\2\2\u073a\u072d\3\2\2\2\u073a\u072e\3\2"+
		"\2\2\u073bw\3\2\2\2\u073c\u073d\7f\2\2\u073d\u0742\5\u0088E\2\u073e\u073f"+
		"\7\6\2\2\u073f\u0741\5\u0088E\2\u0740\u073e\3\2\2\2\u0741\u0744\3\2\2"+
		"\2\u0742\u0740\3\2\2\2\u0742\u0743\3\2\2\2\u0743\u0748\3\2\2\2\u0744\u0742"+
		"\3\2\2\2\u0745\u0747\5\u0084C\2\u0746\u0745\3\2\2\2\u0747\u074a\3\2\2"+
		"\2\u0748\u0746\3\2\2\2\u0748\u0749\3\2\2\2\u0749\u074c\3\2\2\2\u074a\u0748"+
		"\3\2\2\2\u074b\u074d\5~@\2\u074c\u074b\3\2\2\2\u074c\u074d\3\2\2\2\u074d"+
		"y\3\2\2\2\u074e\u074f\7l\2\2\u074f\u0750\7 \2\2\u0750\u0755\5\u00bc_\2"+
		"\u0751\u0752\7\6\2\2\u0752\u0754\5\u00bc_\2\u0753\u0751\3\2\2\2\u0754"+
		"\u0757\3\2\2\2\u0755\u0753\3\2\2\2\u0755\u0756\3\2\2\2\u0756\u0769\3\2"+
		"\2\2\u0757\u0755\3\2\2\2\u0758\u0759\7\u0108\2\2\u0759\u076a\7\u00cb\2"+
		"\2\u075a\u075b\7\u0108\2\2\u075b\u076a\79\2\2\u075c\u075d\7m\2\2\u075d"+
		"\u075e\7\u00d8\2\2\u075e\u075f\7\4\2\2\u075f\u0764\5|?\2\u0760\u0761\7"+
		"\6\2\2\u0761\u0763\5|?\2\u0762\u0760\3\2\2\2\u0763\u0766\3\2\2\2\u0764"+
		"\u0762\3\2\2\2\u0764\u0765\3\2\2\2\u0765\u0767\3\2\2\2\u0766\u0764\3\2"+
		"\2\2\u0767\u0768\7\5\2\2\u0768\u076a\3\2\2\2\u0769\u0758\3\2\2\2\u0769"+
		"\u075a\3\2\2\2\u0769\u075c\3\2\2\2\u0769\u076a\3\2\2\2\u076a\u077b\3\2"+
		"\2\2\u076b\u076c\7l\2\2\u076c\u076d\7 \2\2\u076d\u076e\7m\2\2\u076e\u076f"+
		"\7\u00d8\2\2\u076f\u0770\7\4\2\2\u0770\u0775\5|?\2\u0771\u0772\7\6\2\2"+
		"\u0772\u0774\5|?\2\u0773\u0771\3\2\2\2\u0774\u0777\3\2\2\2\u0775\u0773"+
		"\3\2\2\2\u0775\u0776\3\2\2\2\u0776\u0778\3\2\2\2\u0777\u0775\3\2\2\2\u0778"+
		"\u0779\7\5\2\2\u0779\u077b\3\2\2\2\u077a\u074e\3\2\2\2\u077a\u076b\3\2"+
		"\2\2\u077b{\3\2\2\2\u077c\u0785\7\4\2\2\u077d\u0782\5\u00bc_\2\u077e\u077f"+
		"\7\6\2\2\u077f\u0781\5\u00bc_\2\u0780\u077e\3\2\2\2\u0781\u0784\3\2\2"+
		"\2\u0782\u0780\3\2\2\2\u0782\u0783\3\2\2\2\u0783\u0786\3\2\2\2\u0784\u0782"+
		"\3\2\2\2\u0785\u077d\3\2\2\2\u0785\u0786\3\2\2\2\u0786\u0787\3\2\2\2\u0787"+
		"\u078a\7\5\2\2\u0788\u078a\5\u00bc_\2\u0789\u077c\3\2\2\2\u0789\u0788"+
		"\3\2\2\2\u078a}\3\2\2\2\u078b\u078c\7\u00b0\2\2\u078c\u078d\7\4\2\2\u078d"+
		"\u078e\5\u00b4[\2\u078e\u078f\7b\2\2\u078f\u0790\5\u0080A\2\u0790\u0791"+
		"\7s\2\2\u0791\u0792\7\4\2\2\u0792\u0797\5\u0082B\2\u0793\u0794\7\6\2\2"+
		"\u0794\u0796\5\u0082B\2\u0795\u0793\3\2\2\2\u0796\u0799\3\2\2\2\u0797"+
		"\u0795\3\2\2\2\u0797\u0798\3\2\2\2\u0798\u079a\3\2\2\2\u0799\u0797\3\2"+
		"\2\2\u079a\u079b\7\5\2\2\u079b\u079c\7\5\2\2\u079c\177\3\2\2\2\u079d\u07aa"+
		"\5\u0104\u0083\2\u079e\u079f\7\4\2\2\u079f\u07a4\5\u0104\u0083\2\u07a0"+
		"\u07a1\7\6\2\2\u07a1\u07a3\5\u0104\u0083\2\u07a2\u07a0\3\2\2\2\u07a3\u07a6"+
		"\3\2\2\2\u07a4\u07a2\3\2\2\2\u07a4\u07a5\3\2\2\2\u07a5\u07a7\3\2\2\2\u07a6"+
		"\u07a4\3\2\2\2\u07a7\u07a8\7\5\2\2\u07a8\u07aa\3\2\2\2\u07a9\u079d\3\2"+
		"\2\2\u07a9\u079e\3\2\2\2\u07aa\u0081\3\2\2\2\u07ab\u07b0\5\u00bc_\2\u07ac"+
		"\u07ae\7\30\2\2\u07ad\u07ac\3\2\2\2\u07ad\u07ae\3\2\2\2\u07ae\u07af\3"+
		"\2\2\2\u07af\u07b1\5\u0104\u0083\2\u07b0\u07ad\3\2\2\2\u07b0\u07b1\3\2"+
		"\2\2\u07b1\u0083\3\2\2\2\u07b2\u07b3\7\u0082\2\2\u07b3\u07b5\7\u0103\2"+
		"\2\u07b4\u07b6\7\u00a6\2\2\u07b5\u07b4\3\2\2\2\u07b5\u07b6\3\2\2\2\u07b6"+
		"\u07b7\3\2\2\2\u07b7\u07b8\5\u00fe\u0080\2\u07b8\u07c1\7\4\2\2\u07b9\u07be"+
		"\5\u00bc_\2\u07ba\u07bb\7\6\2\2\u07bb\u07bd\5\u00bc_\2\u07bc\u07ba\3\2"+
		"\2\2\u07bd\u07c0\3\2\2\2\u07be\u07bc\3\2\2\2\u07be\u07bf\3\2\2\2\u07bf"+
		"\u07c2\3\2\2\2\u07c0\u07be\3\2\2\2\u07c1\u07b9\3\2\2\2\u07c1\u07c2\3\2"+
		"\2\2\u07c2\u07c3\3\2\2\2\u07c3\u07c4\7\5\2\2\u07c4\u07d0\5\u0104\u0083"+
		"\2\u07c5\u07c7\7\30\2\2\u07c6\u07c5\3\2\2\2\u07c6\u07c7\3\2\2\2\u07c7"+
		"\u07c8\3\2\2\2\u07c8\u07cd\5\u0104\u0083\2\u07c9\u07ca\7\6\2\2\u07ca\u07cc"+
		"\5\u0104\u0083\2\u07cb\u07c9\3\2\2\2\u07cc\u07cf\3\2\2\2\u07cd\u07cb\3"+
		"\2\2\2\u07cd\u07ce\3\2\2\2\u07ce\u07d1\3\2\2\2\u07cf\u07cd\3\2\2\2\u07d0"+
		"\u07c6\3\2\2\2\u07d0\u07d1\3\2\2\2\u07d1\u0085\3\2\2\2\u07d2\u07d3\t\23"+
		"\2\2\u07d3\u0087\3\2\2\2\u07d4\u07d8\5\u00a0Q\2\u07d5\u07d7\5\u008aF\2"+
		"\u07d6\u07d5\3\2\2\2\u07d7\u07da\3\2\2\2\u07d8\u07d6\3\2\2\2\u07d8\u07d9"+
		"\3\2\2\2\u07d9\u0089\3\2\2\2\u07da\u07d8\3\2\2\2\u07db\u07dc\5\u008cG"+
		"\2\u07dc\u07dd\7\177\2\2\u07dd\u07df\5\u00a0Q\2\u07de\u07e0\5\u008eH\2"+
		"\u07df\u07de\3\2\2\2\u07df\u07e0\3\2\2\2\u07e0\u07e7\3\2\2\2\u07e1\u07e2"+
		"\7\u0099\2\2\u07e2\u07e3\5\u008cG\2\u07e3\u07e4\7\177\2\2\u07e4\u07e5"+
		"\5\u00a0Q\2\u07e5\u07e7\3\2\2\2\u07e6\u07db\3\2\2\2\u07e6\u07e1\3\2\2"+
		"\2\u07e7\u008b\3\2\2\2\u07e8\u07ea\7v\2\2\u07e9\u07e8\3\2\2\2\u07e9\u07ea"+
		"\3\2\2\2\u07ea\u0801\3\2\2\2\u07eb\u0801\78\2\2\u07ec\u07ee\7\u0085\2"+
		"\2\u07ed\u07ef\7\u00a6\2\2\u07ee\u07ed\3\2\2\2\u07ee\u07ef\3\2\2\2\u07ef"+
		"\u0801\3\2\2\2\u07f0\u07f2\7\u0085\2\2\u07f1\u07f0\3\2\2\2\u07f1\u07f2"+
		"\3\2\2\2\u07f2\u07f3\3\2\2\2\u07f3\u0801\7\u00d1\2\2\u07f4\u07f6\7\u00c6"+
		"\2\2\u07f5\u07f7\7\u00a6\2\2\u07f6\u07f5\3\2\2\2\u07f6\u07f7\3\2\2\2\u07f7"+
		"\u0801\3\2\2\2\u07f8\u07fa\7g\2\2\u07f9\u07fb\7\u00a6\2\2\u07fa\u07f9"+
		"\3\2\2\2\u07fa\u07fb\3\2\2\2\u07fb\u0801\3\2\2\2\u07fc\u07fe\7\u0085\2"+
		"\2\u07fd\u07fc\3\2\2\2\u07fd\u07fe\3\2\2\2\u07fe\u07ff\3\2\2\2\u07ff\u0801"+
		"\7\24\2\2\u0800\u07e9\3\2\2\2\u0800\u07eb\3\2\2\2\u0800\u07ec\3\2\2\2"+
		"\u0800\u07f1\3\2\2\2\u0800\u07f4\3\2\2\2\u0800\u07f8\3\2\2\2\u0800\u07fd"+
		"\3\2\2\2\u0801\u008d\3\2\2\2\u0802\u0803\7\u009f\2\2\u0803\u0807\5\u00be"+
		"`\2\u0804\u0805\7\u0101\2\2\u0805\u0807\5\u0094K\2\u0806\u0802\3\2\2\2"+
		"\u0806\u0804\3\2\2\2\u0807\u008f\3\2\2\2\u0808\u0809\7\u00e7\2\2\u0809"+
		"\u080b\7\4\2\2\u080a\u080c\5\u0092J\2\u080b\u080a\3\2\2\2\u080b\u080c"+
		"\3\2\2\2\u080c\u080d\3\2\2\2\u080d\u080e\7\5\2\2\u080e\u0091\3\2\2\2\u080f"+
		"\u0811\7\u0113\2\2\u0810\u080f\3\2\2\2\u0810\u0811\3\2\2\2\u0811\u0812"+
		"\3\2\2\2\u0812\u0813\t\24\2\2\u0813\u0828\7\u00af\2\2\u0814\u0815\5\u00bc"+
		"_\2\u0815\u0816\7\u00cd\2\2\u0816\u0828\3\2\2\2\u0817\u0818\7\36\2\2\u0818"+
		"\u0819\7\u0121\2\2\u0819\u081a\7\u00a5\2\2\u081a\u081b\7\u009e\2\2\u081b"+
		"\u0824\7\u0121\2\2\u081c\u0822\7\u009f\2\2\u081d\u0823\5\u0104\u0083\2"+
		"\u081e\u081f\5\u00fe\u0080\2\u081f\u0820\7\4\2\2\u0820\u0821\7\5\2\2\u0821"+
		"\u0823\3\2\2\2\u0822\u081d\3\2\2\2\u0822\u081e\3\2\2\2\u0823\u0825\3\2"+
		"\2\2\u0824\u081c\3\2\2\2\u0824\u0825\3\2\2\2\u0825\u0828\3\2\2\2\u0826"+
		"\u0828\5\u00bc_\2\u0827\u0810\3\2\2\2\u0827\u0814\3\2\2\2\u0827\u0817"+
		"\3\2\2\2\u0827\u0826\3\2\2\2\u0828\u0093\3\2\2\2\u0829\u082a\7\4\2\2\u082a"+
		"\u082b\5\u0096L\2\u082b\u082c\7\5\2\2\u082c\u0095\3\2\2\2\u082d\u0832"+
		"\5\u0100\u0081\2\u082e\u082f\7\6\2\2\u082f\u0831\5\u0100\u0081\2\u0830"+
		"\u082e\3\2\2\2\u0831\u0834\3\2\2\2\u0832\u0830\3\2\2\2\u0832\u0833\3\2"+
		"\2\2\u0833\u0097\3\2\2\2\u0834\u0832\3\2\2\2\u0835\u0836\7\4\2\2\u0836"+
		"\u083b\5\u009aN\2\u0837\u0838\7\6\2\2\u0838\u083a\5\u009aN\2\u0839\u0837"+
		"\3\2\2\2\u083a\u083d\3\2\2\2\u083b\u0839\3\2\2\2\u083b\u083c\3\2\2\2\u083c"+
		"\u083e\3\2\2\2\u083d\u083b\3\2\2\2\u083e\u083f\7\5\2\2\u083f\u0099\3\2"+
		"\2\2\u0840\u0842\5\u0100\u0081\2\u0841\u0843\t\21\2\2\u0842\u0841\3\2"+
		"\2\2\u0842\u0843\3\2\2\2\u0843\u009b\3\2\2\2\u0844\u0845\7\4\2\2\u0845"+
		"\u084a\5\u009eP\2\u0846\u0847\7\6\2\2\u0847\u0849\5\u009eP\2\u0848\u0846"+
		"\3\2\2\2\u0849\u084c\3\2\2\2\u084a\u0848\3\2\2\2\u084a\u084b\3\2\2\2\u084b"+
		"\u084d\3\2\2\2\u084c\u084a\3\2\2\2\u084d\u084e\7\5\2\2\u084e\u009d\3\2"+
		"\2\2\u084f\u0851\5\u0104\u0083\2\u0850\u0852\5\36\20\2\u0851\u0850\3\2"+
		"\2\2\u0851\u0852\3\2\2\2\u0852\u009f\3\2\2\2\u0853\u0855\5\u00acW\2\u0854"+
		"\u0856\5\u0090I\2\u0855\u0854\3\2\2\2\u0855\u0856\3\2\2\2\u0856\u0857"+
		"\3\2\2\2\u0857\u0858\5\u00a6T\2\u0858\u086c\3\2\2\2\u0859\u085a\7\4\2"+
		"\2\u085a\u085b\5 \21\2\u085b\u085d\7\5\2\2\u085c\u085e\5\u0090I\2\u085d"+
		"\u085c\3\2\2\2\u085d\u085e\3\2\2\2\u085e\u085f\3\2\2\2\u085f\u0860\5\u00a6"+
		"T\2\u0860\u086c\3\2\2\2\u0861\u0862\7\4\2\2\u0862\u0863\5\u0088E\2\u0863"+
		"\u0865\7\5\2\2\u0864\u0866\5\u0090I\2\u0865\u0864\3\2\2\2\u0865\u0866"+
		"\3\2\2\2\u0866\u0867\3\2\2\2\u0867\u0868\5\u00a6T\2\u0868\u086c\3\2\2"+
		"\2\u0869\u086c\5\u00a2R\2\u086a\u086c\5\u00a4S\2\u086b\u0853\3\2\2\2\u086b"+
		"\u0859\3\2\2\2\u086b\u0861\3\2\2\2\u086b\u0869\3\2\2\2\u086b\u086a\3\2"+
		"\2\2\u086c\u00a1\3\2\2\2\u086d\u086e\7\u0102\2\2\u086e\u0873\5\u00bc_"+
		"\2\u086f\u0870\7\6\2\2\u0870\u0872\5\u00bc_\2\u0871\u086f\3\2\2\2\u0872"+
		"\u0875\3\2\2\2\u0873\u0871\3\2\2\2\u0873\u0874\3\2\2\2\u0874\u0876\3\2"+
		"\2\2\u0875\u0873\3\2\2\2\u0876\u0877\5\u00a6T\2\u0877\u00a3\3\2\2\2\u0878"+
		"\u0879\5\u0100\u0081\2\u0879\u0882\7\4\2\2\u087a\u087f\5\u00bc_\2\u087b"+
		"\u087c\7\6\2\2\u087c\u087e\5\u00bc_\2\u087d\u087b\3";
	private static final String _serializedATNSegment1 =
		"\2\2\2\u087e\u0881\3\2\2\2\u087f\u087d\3\2\2\2\u087f\u0880\3\2\2\2\u0880"+
		"\u0883\3\2\2\2\u0881\u087f\3\2\2\2\u0882\u087a\3\2\2\2\u0882\u0883\3\2"+
		"\2\2\u0883\u0884\3\2\2\2\u0884\u0885\7\5\2\2\u0885\u0886\5\u00a6T\2\u0886"+
		"\u00a5\3\2\2\2\u0887\u0889\7\30\2\2\u0888\u0887\3\2\2\2\u0888\u0889\3"+
		"\2\2\2\u0889\u088a\3\2\2\2\u088a\u088c\5\u0106\u0084\2\u088b\u088d\5\u0094"+
		"K\2\u088c\u088b\3\2\2\2\u088c\u088d\3\2\2\2\u088d\u088f\3\2\2\2\u088e"+
		"\u0888\3\2\2\2\u088e\u088f\3\2\2\2\u088f\u00a7\3\2\2\2\u0890\u0891\7\u00cc"+
		"\2\2\u0891\u0892\7d\2\2\u0892\u0893\7\u00d3\2\2\u0893\u0897\7\u011d\2"+
		"\2\u0894\u0895\7\u0108\2\2\u0895\u0896\7\u00d4\2\2\u0896\u0898\58\35\2"+
		"\u0897\u0894\3\2\2\2\u0897\u0898\3\2\2\2\u0898\u08c2\3\2\2\2\u0899\u089a"+
		"\7\u00cc\2\2\u089a\u089b\7d\2\2\u089b\u08a5\7F\2\2\u089c\u089d\7]\2\2"+
		"\u089d\u089e\7\u00ea\2\2\u089e\u089f\7 \2\2\u089f\u08a3\7\u011d\2\2\u08a0"+
		"\u08a1\7R\2\2\u08a1\u08a2\7 \2\2\u08a2\u08a4\7\u011d\2\2\u08a3\u08a0\3"+
		"\2\2\2\u08a3\u08a4\3\2\2\2\u08a4\u08a6\3\2\2\2\u08a5\u089c\3\2\2\2\u08a5"+
		"\u08a6\3\2\2\2\u08a6\u08ac\3\2\2\2\u08a7\u08a8\7,\2\2\u08a8\u08a9\7~\2"+
		"\2\u08a9\u08aa\7\u00ea\2\2\u08aa\u08ab\7 \2\2\u08ab\u08ad\7\u011d\2\2"+
		"\u08ac\u08a7\3\2\2\2\u08ac\u08ad\3\2\2\2\u08ad\u08b3\3\2\2\2\u08ae\u08af"+
		"\7\u0091\2\2\u08af\u08b0\7\u0080\2\2\u08b0\u08b1\7\u00ea\2\2\u08b1\u08b2"+
		"\7 \2\2\u08b2\u08b4\7\u011d\2\2\u08b3\u08ae\3\2\2\2\u08b3\u08b4\3\2\2"+
		"\2\u08b4\u08b9\3\2\2\2\u08b5\u08b6\7\u0088\2\2\u08b6\u08b7\7\u00ea\2\2"+
		"\u08b7\u08b8\7 \2\2\u08b8\u08ba\7\u011d\2\2\u08b9\u08b5\3\2\2\2\u08b9"+
		"\u08ba\3\2\2\2\u08ba\u08bf\3\2\2\2\u08bb\u08bc\7\u009c\2\2\u08bc\u08bd"+
		"\7D\2\2\u08bd\u08be\7\30\2\2\u08be\u08c0\7\u011d\2\2\u08bf\u08bb\3\2\2"+
		"\2\u08bf\u08c0\3\2\2\2\u08c0\u08c2\3\2\2\2\u08c1\u0890\3\2\2\2\u08c1\u0899"+
		"\3\2\2\2\u08c2\u00a9\3\2\2\2\u08c3\u08c8\5\u00acW\2\u08c4\u08c5\7\6\2"+
		"\2\u08c5\u08c7\5\u00acW\2\u08c6\u08c4\3\2\2\2\u08c7\u08ca\3\2\2\2\u08c8"+
		"\u08c6\3\2\2\2\u08c8\u08c9\3\2\2\2\u08c9\u00ab\3\2\2\2\u08ca\u08c8\3\2"+
		"\2\2\u08cb\u08d0\5\u0100\u0081\2\u08cc\u08cd\7\7\2\2\u08cd\u08cf\5\u0100"+
		"\u0081\2\u08ce\u08cc\3\2\2\2\u08cf\u08d2\3\2\2\2\u08d0\u08ce\3\2\2\2\u08d0"+
		"\u08d1\3\2\2\2\u08d1\u00ad\3\2\2\2\u08d2\u08d0\3\2\2\2\u08d3\u08d4\5\u0100"+
		"\u0081\2\u08d4\u08d5\7\7\2\2\u08d5\u08d7\3\2\2\2\u08d6\u08d3\3\2\2\2\u08d6"+
		"\u08d7\3\2\2\2\u08d7\u08d8\3\2\2\2\u08d8\u08d9\5\u0100\u0081\2\u08d9\u00af"+
		"\3\2\2\2\u08da\u08db\5\u0100\u0081\2\u08db\u08dc\7\7\2\2\u08dc\u08de\3"+
		"\2\2\2\u08dd\u08da\3\2\2\2\u08dd\u08de\3\2\2\2\u08de\u08df\3\2\2\2\u08df"+
		"\u08e0\5\u0100\u0081\2\u08e0\u00b1\3\2\2\2\u08e1\u08e9\5\u00bc_\2\u08e2"+
		"\u08e4\7\30\2\2\u08e3\u08e2\3\2\2\2\u08e3\u08e4\3\2\2\2\u08e4\u08e7\3"+
		"\2\2\2\u08e5\u08e8\5\u0100\u0081\2\u08e6\u08e8\5\u0094K\2\u08e7\u08e5"+
		"\3\2\2\2\u08e7\u08e6\3\2\2\2\u08e8\u08ea\3\2\2\2\u08e9\u08e3\3\2\2\2\u08e9"+
		"\u08ea\3\2\2\2\u08ea\u00b3\3\2\2\2\u08eb\u08f0\5\u00b2Z\2\u08ec\u08ed"+
		"\7\6\2\2\u08ed\u08ef\5\u00b2Z\2\u08ee\u08ec\3\2\2\2\u08ef\u08f2\3\2\2"+
		"\2\u08f0\u08ee\3\2\2\2\u08f0\u08f1\3\2\2\2\u08f1\u00b5\3\2\2\2\u08f2\u08f0"+
		"\3\2\2\2\u08f3\u08f4\7\4\2\2\u08f4\u08f9\5\u00b8]\2\u08f5\u08f6\7\6\2"+
		"\2\u08f6\u08f8\5\u00b8]\2\u08f7\u08f5\3\2\2\2\u08f8\u08fb\3\2\2\2\u08f9"+
		"\u08f7\3\2\2\2\u08f9\u08fa\3\2\2\2\u08fa\u08fc\3\2\2\2\u08fb\u08f9\3\2"+
		"\2\2\u08fc\u08fd\7\5\2\2\u08fd\u00b7\3\2\2\2\u08fe\u090c\5\u00fe\u0080"+
		"\2\u08ff\u0900\5\u0104\u0083\2\u0900\u0901\7\4\2\2\u0901\u0906\5\u00ba"+
		"^\2\u0902\u0903\7\6\2\2\u0903\u0905\5\u00ba^\2\u0904\u0902\3\2\2\2\u0905"+
		"\u0908\3\2\2\2\u0906\u0904\3\2\2\2\u0906\u0907\3\2\2\2\u0907\u0909\3\2"+
		"\2\2\u0908\u0906\3\2\2\2\u0909\u090a\7\5\2\2\u090a\u090c\3\2\2\2\u090b"+
		"\u08fe\3\2\2\2\u090b\u08ff\3\2\2\2\u090c\u00b9\3\2\2\2\u090d\u0910\5\u00fe"+
		"\u0080\2\u090e\u0910\5\u00c6d\2\u090f\u090d\3\2\2\2\u090f\u090e\3\2\2"+
		"\2\u0910\u00bb\3\2\2\2\u0911\u0912\5\u00be`\2\u0912\u00bd\3\2\2\2\u0913"+
		"\u0914\b`\1\2\u0914\u0915\7\u009b\2\2\u0915\u0920\5\u00be`\7\u0916\u0917"+
		"\7U\2\2\u0917\u0918\7\4\2\2\u0918\u0919\5 \21\2\u0919\u091a\7\5\2\2\u091a"+
		"\u0920\3\2\2\2\u091b\u091d\5\u00c2b\2\u091c\u091e\5\u00c0a\2\u091d\u091c"+
		"\3\2\2\2\u091d\u091e\3\2\2\2\u091e\u0920\3\2\2\2\u091f\u0913\3\2\2\2\u091f"+
		"\u0916\3\2\2\2\u091f\u091b\3\2\2\2\u0920\u0929\3\2\2\2\u0921\u0922\f\4"+
		"\2\2\u0922\u0923\7\23\2\2\u0923\u0928\5\u00be`\5\u0924\u0925\f\3\2\2\u0925"+
		"\u0926\7\u00a3\2\2\u0926\u0928\5\u00be`\4\u0927\u0921\3\2\2\2\u0927\u0924"+
		"\3\2\2\2\u0928\u092b\3\2\2\2\u0929\u0927\3\2\2\2\u0929\u092a\3\2\2\2\u092a"+
		"\u00bf\3\2\2\2\u092b\u0929\3\2\2\2\u092c\u092e\7\u009b\2\2\u092d\u092c"+
		"\3\2\2\2\u092d\u092e\3\2\2\2\u092e\u092f\3\2\2\2\u092f\u0930\7\34\2\2"+
		"\u0930\u0931\5\u00c2b\2\u0931\u0932\7\23\2\2\u0932\u0933\5\u00c2b\2\u0933"+
		"\u097f\3\2\2\2\u0934\u0936\7\u009b\2\2\u0935\u0934\3\2\2\2\u0935\u0936"+
		"\3\2\2\2\u0936\u0937\3\2\2\2\u0937\u0938\7s\2\2\u0938\u0939\7\4\2\2\u0939"+
		"\u093e\5\u00bc_\2\u093a\u093b\7\6\2\2\u093b\u093d\5\u00bc_\2\u093c\u093a"+
		"\3\2\2\2\u093d\u0940\3\2\2\2\u093e\u093c\3\2\2\2\u093e\u093f\3\2\2\2\u093f"+
		"\u0941\3\2\2\2\u0940\u093e\3\2\2\2\u0941\u0942\7\5\2\2\u0942\u097f\3\2"+
		"\2\2\u0943\u0945\7\u009b\2\2\u0944\u0943\3\2\2\2\u0944\u0945\3\2\2\2\u0945"+
		"\u0946\3\2\2\2\u0946\u0947\7s\2\2\u0947\u0948\7\4\2\2\u0948\u0949\5 \21"+
		"\2\u0949\u094a\7\5\2\2\u094a\u097f\3\2\2\2\u094b\u094d\7\u009b\2\2\u094c"+
		"\u094b\3\2\2\2\u094c\u094d\3\2\2\2\u094d\u094e\3\2\2\2\u094e\u094f\7\u00c7"+
		"\2\2\u094f\u097f\5\u00c2b\2\u0950\u0952\7\u009b\2\2\u0951\u0950\3\2\2"+
		"\2\u0951\u0952\3\2\2\2\u0952\u0953\3\2\2\2\u0953\u0954\7\u0086\2\2\u0954"+
		"\u0962\t\25\2\2\u0955\u0956\7\4\2\2\u0956\u0963\7\5\2\2\u0957\u0958\7"+
		"\4\2\2\u0958\u095d\5\u00bc_\2\u0959\u095a\7\6\2\2\u095a\u095c\5\u00bc"+
		"_\2\u095b\u0959\3\2\2\2\u095c\u095f\3\2\2\2\u095d\u095b\3\2\2\2\u095d"+
		"\u095e\3\2\2\2\u095e\u0960\3\2\2\2\u095f\u095d\3\2\2\2\u0960\u0961\7\5"+
		"\2\2\u0961\u0963\3\2\2\2\u0962\u0955\3\2\2\2\u0962\u0957\3\2\2\2\u0963"+
		"\u097f\3\2\2\2\u0964\u0966\7\u009b\2\2\u0965\u0964\3\2\2\2\u0965\u0966"+
		"\3\2\2\2\u0966\u0967\3\2\2\2\u0967\u0968\7\u0086\2\2\u0968\u096b\5\u00c2"+
		"b\2\u0969\u096a\7Q\2\2\u096a\u096c\7\u011d\2\2\u096b\u0969\3\2\2\2\u096b"+
		"\u096c\3\2\2\2\u096c\u097f\3\2\2\2\u096d\u096f\7}\2\2\u096e\u0970\7\u009b"+
		"\2\2\u096f\u096e\3\2\2\2\u096f\u0970\3\2\2\2\u0970\u0971\3\2\2\2\u0971"+
		"\u097f\7\u009c\2\2\u0972\u0974\7}\2\2\u0973\u0975\7\u009b\2\2\u0974\u0973"+
		"\3\2\2\2\u0974\u0975\3\2\2\2\u0975\u0976\3\2\2\2\u0976\u097f\t\26\2\2"+
		"\u0977\u0979\7}\2\2\u0978\u097a\7\u009b\2\2\u0979\u0978\3\2\2\2\u0979"+
		"\u097a\3\2\2\2\u097a\u097b\3\2\2\2\u097b\u097c\7L\2\2\u097c\u097d\7f\2"+
		"\2\u097d\u097f\5\u00c2b\2\u097e\u092d\3\2\2\2\u097e\u0935\3\2\2\2\u097e"+
		"\u0944\3\2\2\2\u097e\u094c\3\2\2\2\u097e\u0951\3\2\2\2\u097e\u0965\3\2"+
		"\2\2\u097e\u096d\3\2\2\2\u097e\u0972\3\2\2\2\u097e\u0977\3\2\2\2\u097f"+
		"\u00c1\3\2\2\2\u0980\u0981\bb\1\2\u0981\u0985\5\u00c4c\2\u0982\u0983\t"+
		"\27\2\2\u0983\u0985\5\u00c2b\t\u0984\u0980\3\2\2\2\u0984\u0982\3\2\2\2"+
		"\u0985\u099b\3\2\2\2\u0986\u0987\f\b\2\2\u0987\u0988\t\30\2\2\u0988\u099a"+
		"\5\u00c2b\t\u0989\u098a\f\7\2\2\u098a\u098b\t\31\2\2\u098b\u099a\5\u00c2"+
		"b\b\u098c\u098d\f\6\2\2\u098d\u098e\7\u0119\2\2\u098e\u099a\5\u00c2b\7"+
		"\u098f\u0990\f\5\2\2\u0990\u0991\7\u011c\2\2\u0991\u099a\5\u00c2b\6\u0992"+
		"\u0993\f\4\2\2\u0993\u0994\7\u011a\2\2\u0994\u099a\5\u00c2b\5\u0995\u0996"+
		"\f\3\2\2\u0996\u0997\5\u00c8e\2\u0997\u0998\5\u00c2b\4\u0998\u099a\3\2"+
		"\2\2\u0999\u0986\3\2\2\2\u0999\u0989\3\2\2\2\u0999\u098c\3\2\2\2\u0999"+
		"\u098f\3\2\2\2\u0999\u0992\3\2\2\2\u0999\u0995\3\2\2\2\u099a\u099d\3\2"+
		"\2\2\u099b\u0999\3\2\2\2\u099b\u099c\3\2\2\2\u099c\u00c3\3\2\2\2\u099d"+
		"\u099b\3\2\2\2\u099e\u099f\bc\1\2\u099f\u0a57\t\32\2\2\u09a0\u09a2\7#"+
		"\2\2\u09a1\u09a3\5\u00eex\2\u09a2\u09a1\3\2\2\2\u09a3\u09a4\3\2\2\2\u09a4"+
		"\u09a2\3\2\2\2\u09a4\u09a5\3\2\2\2\u09a5\u09a8\3\2\2\2\u09a6\u09a7\7O"+
		"\2\2\u09a7\u09a9\5\u00bc_\2\u09a8\u09a6\3\2\2\2\u09a8\u09a9\3\2\2\2\u09a9"+
		"\u09aa\3\2\2\2\u09aa\u09ab\7P\2\2\u09ab\u0a57\3\2\2\2\u09ac\u09ad\7#\2"+
		"\2\u09ad\u09af\5\u00bc_\2\u09ae\u09b0\5\u00eex\2\u09af\u09ae\3\2\2\2\u09b0"+
		"\u09b1\3\2\2\2\u09b1\u09af\3\2\2\2\u09b1\u09b2\3\2\2\2\u09b2\u09b5\3\2"+
		"\2\2\u09b3\u09b4\7O\2\2\u09b4\u09b6\5\u00bc_\2\u09b5\u09b3\3\2\2\2\u09b5"+
		"\u09b6\3\2\2\2\u09b6\u09b7\3\2\2\2\u09b7\u09b8\7P\2\2\u09b8\u0a57\3\2"+
		"\2\2\u09b9\u09ba\7$\2\2\u09ba\u09bb\7\4\2\2\u09bb\u09bc\5\u00bc_\2\u09bc"+
		"\u09bd\7\30\2\2\u09bd\u09be\5\u00e0q\2\u09be\u09bf\7\5\2\2\u09bf\u0a57"+
		"\3\2\2\2\u09c0\u09c1\7\u00e2\2\2\u09c1\u09ca\7\4\2\2\u09c2\u09c7\5\u00b2"+
		"Z\2\u09c3\u09c4\7\6\2\2\u09c4\u09c6\5\u00b2Z\2\u09c5\u09c3\3\2\2\2\u09c6"+
		"\u09c9\3\2\2\2\u09c7\u09c5\3\2\2\2\u09c7\u09c8\3\2\2\2\u09c8\u09cb\3\2"+
		"\2\2\u09c9\u09c7\3\2\2\2\u09ca\u09c2\3\2\2\2\u09ca\u09cb\3\2\2\2\u09cb"+
		"\u09cc\3\2\2\2\u09cc\u0a57\7\5\2\2\u09cd\u09ce\7`\2\2\u09ce\u09cf\7\4"+
		"\2\2\u09cf\u09d2\5\u00bc_\2\u09d0\u09d1\7q\2\2\u09d1\u09d3\7\u009d\2\2"+
		"\u09d2\u09d0\3\2\2\2\u09d2\u09d3\3\2\2\2\u09d3\u09d4\3\2\2\2\u09d4\u09d5"+
		"\7\5\2\2\u09d5\u0a57\3\2\2\2\u09d6\u09d7\7\u0081\2\2\u09d7\u09d8\7\4\2"+
		"\2\u09d8\u09db\5\u00bc_\2\u09d9\u09da\7q\2\2\u09da\u09dc\7\u009d\2\2\u09db"+
		"\u09d9\3\2\2\2\u09db\u09dc\3\2\2\2\u09dc\u09dd\3\2\2\2\u09dd\u09de\7\5"+
		"\2\2\u09de\u0a57\3\2\2\2\u09df\u09e0\7\u00b2\2\2\u09e0\u09e1\7\4\2\2\u09e1"+
		"\u09e2\5\u00c2b\2\u09e2\u09e3\7s\2\2\u09e3\u09e4\5\u00c2b\2\u09e4\u09e5"+
		"\7\5\2\2\u09e5\u0a57\3\2\2\2\u09e6\u0a57\5\u00c6d\2\u09e7\u0a57\7\u0114"+
		"\2\2\u09e8\u09e9\5\u00fe\u0080\2\u09e9\u09ea\7\7\2\2\u09ea\u09eb\7\u0114"+
		"\2\2\u09eb\u0a57\3\2\2\2\u09ec\u09ed\7\4\2\2\u09ed\u09f0\5\u00b2Z\2\u09ee"+
		"\u09ef\7\6\2\2\u09ef\u09f1\5\u00b2Z\2\u09f0\u09ee\3\2\2\2\u09f1\u09f2"+
		"\3\2\2\2\u09f2\u09f0\3\2\2\2\u09f2\u09f3\3\2\2\2\u09f3\u09f4\3\2\2\2\u09f4"+
		"\u09f5\7\5\2\2\u09f5\u0a57\3\2\2\2\u09f6\u09f7\7\4\2\2\u09f7\u09f8\5 "+
		"\21\2\u09f8\u09f9\7\5\2\2\u09f9\u0a57\3\2\2\2\u09fa\u09fb\5\u00fc\177"+
		"\2\u09fb\u0a07\7\4\2\2\u09fc\u09fe\5\u0086D\2\u09fd\u09fc\3\2\2\2\u09fd"+
		"\u09fe\3\2\2\2\u09fe\u09ff\3\2\2\2\u09ff\u0a04\5\u00bc_\2\u0a00\u0a01"+
		"\7\6\2\2\u0a01\u0a03\5\u00bc_\2\u0a02\u0a00\3\2\2\2\u0a03\u0a06\3\2\2"+
		"\2\u0a04\u0a02\3\2\2\2\u0a04\u0a05\3\2\2\2\u0a05\u0a08\3\2\2\2\u0a06\u0a04"+
		"\3\2\2\2\u0a07\u09fd\3\2\2\2\u0a07\u0a08\3\2\2\2\u0a08\u0a09\3\2\2\2\u0a09"+
		"\u0a10\7\5\2\2\u0a0a\u0a0b\7^\2\2\u0a0b\u0a0c\7\4\2\2\u0a0c\u0a0d\7\u0106"+
		"\2\2\u0a0d\u0a0e\5\u00be`\2\u0a0e\u0a0f\7\5\2\2\u0a0f\u0a11\3\2\2\2\u0a10"+
		"\u0a0a\3\2\2\2\u0a10\u0a11\3\2\2\2\u0a11\u0a14\3\2\2\2\u0a12\u0a13\7\u00a8"+
		"\2\2\u0a13\u0a15\5\u00f4{\2\u0a14\u0a12\3\2\2\2\u0a14\u0a15\3\2\2\2\u0a15"+
		"\u0a57\3\2\2\2\u0a16\u0a17\5\u0104\u0083\2\u0a17\u0a18\7\n\2\2\u0a18\u0a19"+
		"\5\u00bc_\2\u0a19\u0a57\3\2\2\2\u0a1a\u0a1b\7\4\2\2\u0a1b\u0a1e\5\u0104"+
		"\u0083\2\u0a1c\u0a1d\7\6\2\2\u0a1d\u0a1f\5\u0104\u0083\2\u0a1e\u0a1c\3"+
		"\2\2\2\u0a1f\u0a20\3\2\2\2\u0a20\u0a1e\3\2\2\2\u0a20\u0a21\3\2\2\2\u0a21"+
		"\u0a22\3\2\2\2\u0a22\u0a23\7\5\2\2\u0a23\u0a24\7\n\2\2\u0a24\u0a25\5\u00bc"+
		"_\2\u0a25\u0a57\3\2\2\2\u0a26\u0a57\5\u0104\u0083\2\u0a27\u0a28\7\4\2"+
		"\2\u0a28\u0a29\5\u00bc_\2\u0a29\u0a2a\7\5\2\2\u0a2a\u0a57\3\2\2\2\u0a2b"+
		"\u0a2c\7Z\2\2\u0a2c\u0a2d\7\4\2\2\u0a2d\u0a2e\5\u0104\u0083\2\u0a2e\u0a2f"+
		"\7f\2\2\u0a2f\u0a30\5\u00c2b\2\u0a30\u0a31\7\5\2\2\u0a31\u0a57\3\2\2\2"+
		"\u0a32\u0a33\t\33\2\2\u0a33\u0a34\7\4\2\2\u0a34\u0a35\5\u00c2b\2\u0a35"+
		"\u0a36\t\34\2\2\u0a36\u0a39\5\u00c2b\2\u0a37\u0a38\t\35\2\2\u0a38\u0a3a"+
		"\5\u00c2b\2\u0a39\u0a37\3\2\2\2\u0a39\u0a3a\3\2\2\2\u0a3a\u0a3b\3\2\2"+
		"\2\u0a3b\u0a3c\7\5\2\2\u0a3c\u0a57\3\2\2\2\u0a3d\u0a3e\7\u00f2\2\2\u0a3e"+
		"\u0a40\7\4\2\2\u0a3f\u0a41\t\36\2\2\u0a40\u0a3f\3\2\2\2\u0a40\u0a41\3"+
		"\2\2\2\u0a41\u0a43\3\2\2\2\u0a42\u0a44\5\u00c2b\2\u0a43\u0a42\3\2\2\2"+
		"\u0a43\u0a44\3\2\2\2\u0a44\u0a45\3\2\2\2\u0a45\u0a46\7f\2\2\u0a46\u0a47"+
		"\5\u00c2b\2\u0a47\u0a48\7\5\2\2\u0a48\u0a57\3\2\2\2\u0a49\u0a4a\7\u00aa"+
		"\2\2\u0a4a\u0a4b\7\4\2\2\u0a4b\u0a4c\5\u00c2b\2\u0a4c\u0a4d\7\u00b1\2"+
		"\2\u0a4d\u0a4e\5\u00c2b\2\u0a4e\u0a4f\7f\2\2\u0a4f\u0a52\5\u00c2b\2\u0a50"+
		"\u0a51\7b\2\2\u0a51\u0a53\5\u00c2b\2\u0a52\u0a50\3\2\2\2\u0a52\u0a53\3"+
		"\2\2\2\u0a53\u0a54\3\2\2\2\u0a54\u0a55\7\5\2\2\u0a55\u0a57\3\2\2\2\u0a56"+
		"\u099e\3\2\2\2\u0a56\u09a0\3\2\2\2\u0a56\u09ac\3\2\2\2\u0a56\u09b9\3\2"+
		"\2\2\u0a56\u09c0\3\2\2\2\u0a56\u09cd\3\2\2\2\u0a56\u09d6\3\2\2\2\u0a56"+
		"\u09df\3\2\2\2\u0a56\u09e6\3\2\2\2\u0a56\u09e7\3\2\2\2\u0a56\u09e8\3\2"+
		"\2\2\u0a56\u09ec\3\2\2\2\u0a56\u09f6\3\2\2\2\u0a56\u09fa\3\2\2\2\u0a56"+
		"\u0a16\3\2\2\2\u0a56\u0a1a\3\2\2\2\u0a56\u0a26\3\2\2\2\u0a56\u0a27\3\2"+
		"\2\2\u0a56\u0a2b\3\2\2\2\u0a56\u0a32\3\2\2\2\u0a56\u0a3d\3\2\2\2\u0a56"+
		"\u0a49\3\2\2\2\u0a57\u0a62\3\2\2\2\u0a58\u0a59\f\n\2\2\u0a59\u0a5a\7\13"+
		"\2\2\u0a5a\u0a5b\5\u00c2b\2\u0a5b\u0a5c\7\f\2\2\u0a5c\u0a61\3\2\2\2\u0a5d"+
		"\u0a5e\f\b\2\2\u0a5e\u0a5f\7\7\2\2\u0a5f\u0a61\5\u0104\u0083\2\u0a60\u0a58"+
		"\3\2\2\2\u0a60\u0a5d\3\2\2\2\u0a61\u0a64\3\2\2\2\u0a62\u0a60\3\2\2\2\u0a62"+
		"\u0a63\3\2\2\2\u0a63\u00c5\3\2\2\2\u0a64\u0a62\3\2\2\2\u0a65\u0a72\7\u009c"+
		"\2\2\u0a66\u0a72\5\u00d0i\2\u0a67\u0a68\5\u0104\u0083\2\u0a68\u0a69\7"+
		"\u011d\2\2\u0a69\u0a72\3\2\2\2\u0a6a\u0a72\5\u010a\u0086\2\u0a6b\u0a72"+
		"\5\u00ceh\2\u0a6c\u0a6e\7\u011d\2\2\u0a6d\u0a6c\3\2\2\2\u0a6e\u0a6f\3"+
		"\2\2\2\u0a6f\u0a6d\3\2\2\2\u0a6f\u0a70\3\2\2\2\u0a70\u0a72\3\2\2\2\u0a71"+
		"\u0a65\3\2\2\2\u0a71\u0a66\3\2\2\2\u0a71\u0a67\3\2\2\2\u0a71\u0a6a\3\2"+
		"\2\2\u0a71\u0a6b\3\2\2\2\u0a71\u0a6d\3\2\2\2\u0a72\u00c7\3\2\2\2\u0a73"+
		"\u0a74\t\37\2\2\u0a74\u00c9\3\2\2\2\u0a75\u0a76\t \2\2\u0a76\u00cb\3\2"+
		"\2\2\u0a77\u0a78\t!\2\2\u0a78\u00cd\3\2\2\2\u0a79\u0a7a\t\"\2\2\u0a7a"+
		"\u00cf\3\2\2\2\u0a7b\u0a7e\7{\2\2\u0a7c\u0a7f\5\u00d2j\2\u0a7d\u0a7f\5"+
		"\u00d6l\2\u0a7e\u0a7c\3\2\2\2\u0a7e\u0a7d\3\2\2\2\u0a7e\u0a7f\3\2\2\2"+
		"\u0a7f\u00d1\3\2\2\2\u0a80\u0a82\5\u00d4k\2\u0a81\u0a83\5\u00d8m\2\u0a82"+
		"\u0a81\3\2\2\2\u0a82\u0a83\3\2\2\2\u0a83\u00d3\3\2\2\2\u0a84\u0a85\5\u00da"+
		"n\2\u0a85\u0a86\5\u00dco\2\u0a86\u0a88\3\2\2\2\u0a87\u0a84\3\2\2\2\u0a88"+
		"\u0a89\3\2\2\2\u0a89\u0a87\3\2\2\2\u0a89\u0a8a\3\2\2\2\u0a8a\u00d5\3\2"+
		"\2\2\u0a8b\u0a8e\5\u00d8m\2\u0a8c\u0a8f\5\u00d4k\2\u0a8d\u0a8f\5\u00d8"+
		"m\2\u0a8e\u0a8c\3\2\2\2\u0a8e\u0a8d\3\2\2\2\u0a8e\u0a8f\3\2\2\2\u0a8f"+
		"\u00d7\3\2\2\2\u0a90\u0a91\5\u00dan\2\u0a91\u0a92\5\u00dco\2\u0a92\u0a93"+
		"\7\u00ec\2\2\u0a93\u0a94\5\u00dco\2\u0a94\u00d9\3\2\2\2\u0a95\u0a97\t"+
		"#\2\2\u0a96\u0a95\3\2\2\2\u0a96\u0a97\3\2\2\2\u0a97\u0a98\3\2\2\2\u0a98"+
		"\u0a9b\t\24\2\2\u0a99\u0a9b\7\u011d\2\2\u0a9a\u0a96\3\2\2\2\u0a9a\u0a99"+
		"\3\2\2\2\u0a9b\u00db\3\2\2\2\u0a9c\u0aa4\7B\2\2\u0a9d\u0aa4\7o\2\2\u0a9e"+
		"\u0aa4\7\u0094\2\2\u0a9f\u0aa4\7\u0095\2\2\u0aa0\u0aa4\7\u00cf\2\2\u0aa1"+
		"\u0aa4\7\u0109\2\2\u0aa2\u0aa4\5\u0104\u0083\2\u0aa3\u0a9c\3\2\2\2\u0aa3"+
		"\u0a9d\3\2\2\2\u0aa3\u0a9e\3\2\2\2\u0aa3\u0a9f\3\2\2\2\u0aa3\u0aa0\3\2"+
		"\2\2\u0aa3\u0aa1\3\2\2\2\u0aa3\u0aa2\3\2\2\2\u0aa4\u00dd\3\2\2\2\u0aa5"+
		"\u0aa9\7`\2\2\u0aa6\u0aa7\7\17\2\2\u0aa7\u0aa9\5\u0100\u0081\2\u0aa8\u0aa5"+
		"\3\2\2\2\u0aa8\u0aa6\3\2\2\2\u0aa9\u00df\3\2\2\2\u0aaa\u0aab\7\27\2\2"+
		"\u0aab\u0aac\7\u010e\2\2\u0aac\u0aad\5\u00e0q\2\u0aad\u0aae\7\u0110\2"+
		"\2\u0aae\u0acd\3\2\2\2\u0aaf\u0ab0\7\u0091\2\2\u0ab0\u0ab1\7\u010e\2\2"+
		"\u0ab1\u0ab2\5\u00e0q\2\u0ab2\u0ab3\7\6\2\2\u0ab3\u0ab4\5\u00e0q\2\u0ab4"+
		"\u0ab5\7\u0110\2\2\u0ab5\u0acd\3\2\2\2\u0ab6\u0abd\7\u00e2\2\2\u0ab7\u0ab9"+
		"\7\u010e\2\2\u0ab8\u0aba\5\u00eav\2\u0ab9\u0ab8\3\2\2\2\u0ab9\u0aba\3"+
		"\2\2\2\u0aba\u0abb\3\2\2\2\u0abb\u0abe\7\u0110\2\2\u0abc\u0abe\7\u010c"+
		"\2\2\u0abd\u0ab7\3\2\2\2\u0abd\u0abc\3\2\2\2\u0abe\u0acd\3\2\2\2\u0abf"+
		"\u0aca\5\u0104\u0083\2\u0ac0\u0ac1\7\4\2\2\u0ac1\u0ac6\7\u0121\2\2\u0ac2"+
		"\u0ac3\7\6\2\2\u0ac3\u0ac5\7\u0121\2\2\u0ac4\u0ac2\3\2\2\2\u0ac5\u0ac8"+
		"\3\2\2\2\u0ac6\u0ac4\3\2\2\2\u0ac6\u0ac7\3\2\2\2\u0ac7\u0ac9\3\2\2\2\u0ac8"+
		"\u0ac6\3\2\2\2\u0ac9\u0acb\7\5\2\2\u0aca\u0ac0\3\2\2\2\u0aca\u0acb\3\2"+
		"\2\2\u0acb\u0acd\3\2\2\2\u0acc\u0aaa\3\2\2\2\u0acc\u0aaf\3\2\2\2\u0acc"+
		"\u0ab6\3\2\2\2\u0acc\u0abf\3\2\2\2\u0acd\u00e1\3\2\2\2\u0ace\u0ad3\5\u00e4"+
		"s\2\u0acf\u0ad0\7\6\2\2\u0ad0\u0ad2\5\u00e4s\2\u0ad1\u0acf\3\2\2\2\u0ad2"+
		"\u0ad5\3\2\2\2\u0ad3\u0ad1\3\2\2\2\u0ad3\u0ad4\3\2\2\2\u0ad4\u00e3\3\2"+
		"\2\2\u0ad5\u0ad3\3\2\2\2\u0ad6\u0ad7\5\u00acW\2\u0ad7\u0ada\5\u00e0q\2"+
		"\u0ad8\u0ad9\7\u009b\2\2\u0ad9\u0adb\7\u009c\2\2\u0ada\u0ad8\3\2\2\2\u0ada"+
		"\u0adb\3\2\2\2\u0adb\u0add\3\2\2\2\u0adc\u0ade\5\36\20\2\u0add\u0adc\3"+
		"\2\2\2\u0add\u0ade\3\2\2\2\u0ade\u0ae0\3\2\2\2\u0adf\u0ae1\5\u00dep\2"+
		"\u0ae0\u0adf\3\2\2\2\u0ae0\u0ae1\3\2\2\2\u0ae1\u00e5\3\2\2\2\u0ae2\u0ae7"+
		"\5\u00e8u\2\u0ae3\u0ae4\7\6\2\2\u0ae4\u0ae6\5\u00e8u\2\u0ae5\u0ae3\3\2"+
		"\2\2\u0ae6\u0ae9\3\2\2\2\u0ae7\u0ae5\3\2\2\2\u0ae7\u0ae8\3\2\2\2\u0ae8"+
		"\u00e7\3\2\2\2\u0ae9\u0ae7\3\2\2\2\u0aea\u0aeb\5\u0100\u0081\2\u0aeb\u0aee"+
		"\5\u00e0q\2\u0aec\u0aed\7\u009b\2\2\u0aed\u0aef\7\u009c\2\2\u0aee\u0aec"+
		"\3\2\2\2\u0aee\u0aef\3\2\2\2\u0aef\u0af1\3\2\2\2\u0af0\u0af2\5\36\20\2"+
		"\u0af1\u0af0\3\2\2\2\u0af1\u0af2\3\2\2\2\u0af2\u00e9\3\2\2\2\u0af3\u0af8"+
		"\5\u00ecw\2\u0af4\u0af5\7\6\2\2\u0af5\u0af7\5\u00ecw\2\u0af6\u0af4\3\2"+
		"\2\2\u0af7\u0afa\3\2\2\2\u0af8\u0af6\3\2\2\2\u0af8\u0af9\3\2\2\2\u0af9"+
		"\u00eb\3\2\2\2\u0afa\u0af8\3\2\2\2\u0afb\u0afc\5\u0104\u0083\2\u0afc\u0afd"+
		"\7\r\2\2\u0afd\u0b00\5\u00e0q\2\u0afe\u0aff\7\u009b\2\2\u0aff\u0b01\7"+
		"\u009c\2\2\u0b00\u0afe\3\2\2\2\u0b00\u0b01\3\2\2\2\u0b01\u0b03\3\2\2\2"+
		"\u0b02\u0b04\5\36\20\2\u0b03\u0b02\3\2\2\2\u0b03\u0b04\3\2\2\2\u0b04\u00ed"+
		"\3\2\2\2\u0b05\u0b06\7\u0105\2\2\u0b06\u0b07\5\u00bc_\2\u0b07\u0b08\7"+
		"\u00eb\2\2\u0b08\u0b09\5\u00bc_\2\u0b09\u00ef\3\2\2\2\u0b0a\u0b0b\7\u0107"+
		"\2\2\u0b0b\u0b10\5\u00f2z\2\u0b0c\u0b0d\7\6\2\2\u0b0d\u0b0f\5\u00f2z\2"+
		"\u0b0e\u0b0c\3\2\2\2\u0b0f\u0b12\3\2\2\2\u0b10\u0b0e\3\2\2\2\u0b10\u0b11"+
		"\3\2\2\2\u0b11\u00f1\3\2\2\2\u0b12\u0b10\3\2\2\2\u0b13\u0b14\5\u0100\u0081"+
		"\2\u0b14\u0b15\7\30\2\2\u0b15\u0b16\5\u00f4{\2\u0b16\u00f3\3\2\2\2\u0b17"+
		"\u0b46\5\u0100\u0081\2\u0b18\u0b19\7\4\2\2\u0b19\u0b1a\5\u0100\u0081\2"+
		"\u0b1a\u0b1b\7\5\2\2\u0b1b\u0b46\3\2\2\2\u0b1c\u0b3f\7\4\2\2\u0b1d\u0b1e"+
		"\7(\2\2\u0b1e\u0b1f\7 \2\2\u0b1f\u0b24\5\u00bc_\2\u0b20\u0b21\7\6\2\2"+
		"\u0b21\u0b23\5\u00bc_\2\u0b22\u0b20\3\2\2\2\u0b23\u0b26\3\2\2\2\u0b24"+
		"\u0b22\3\2\2\2\u0b24\u0b25\3\2\2\2\u0b25\u0b40\3\2\2\2\u0b26\u0b24\3\2"+
		"\2\2\u0b27\u0b28\t$\2\2\u0b28\u0b29\7 \2\2\u0b29\u0b2e\5\u00bc_\2\u0b2a"+
		"\u0b2b\7\6\2\2\u0b2b\u0b2d\5\u00bc_\2\u0b2c\u0b2a\3\2\2\2\u0b2d\u0b30"+
		"\3\2\2\2\u0b2e\u0b2c\3\2\2\2\u0b2e\u0b2f\3\2\2\2\u0b2f\u0b32\3\2\2\2\u0b30"+
		"\u0b2e\3\2\2\2\u0b31\u0b27\3\2\2\2\u0b31\u0b32\3\2\2\2\u0b32\u0b3d\3\2"+
		"\2\2\u0b33\u0b34\t%\2\2\u0b34\u0b35\7 \2\2\u0b35\u0b3a\5V,\2\u0b36\u0b37"+
		"\7\6\2\2\u0b37\u0b39\5V,\2\u0b38\u0b36\3\2\2\2\u0b39\u0b3c\3\2\2\2\u0b3a"+
		"\u0b38\3\2\2\2\u0b3a\u0b3b\3\2\2\2\u0b3b\u0b3e\3\2\2\2\u0b3c\u0b3a\3\2"+
		"\2\2\u0b3d\u0b33\3\2\2\2\u0b3d\u0b3e\3\2\2\2\u0b3e\u0b40\3\2\2\2\u0b3f"+
		"\u0b1d\3\2\2\2\u0b3f\u0b31\3\2\2\2\u0b40\u0b42\3\2\2\2\u0b41\u0b43\5\u00f6"+
		"|\2\u0b42\u0b41\3\2\2\2\u0b42\u0b43\3\2\2\2\u0b43\u0b44\3\2\2\2\u0b44"+
		"\u0b46\7\5\2\2\u0b45\u0b17\3\2\2\2\u0b45\u0b18\3\2\2\2\u0b45\u0b1c\3\2"+
		"\2\2\u0b46\u00f5\3\2\2\2\u0b47\u0b48\7\u00b9\2\2\u0b48\u0b58\5\u00f8}"+
		"\2\u0b49\u0b4a\7\u00cd\2\2\u0b4a\u0b58\5\u00f8}\2\u0b4b\u0b4c\7\u00b9"+
		"\2\2\u0b4c\u0b4d\7\34\2\2\u0b4d\u0b4e\5\u00f8}\2\u0b4e\u0b4f\7\23\2\2"+
		"\u0b4f\u0b50\5\u00f8}\2\u0b50\u0b58\3\2\2\2\u0b51\u0b52\7\u00cd\2\2\u0b52"+
		"\u0b53\7\34\2\2\u0b53\u0b54\5\u00f8}\2\u0b54\u0b55\7\23\2\2\u0b55\u0b56"+
		"\5\u00f8}\2\u0b56\u0b58\3\2\2\2\u0b57\u0b47\3\2\2\2\u0b57\u0b49\3\2\2"+
		"\2\u0b57\u0b4b\3\2\2\2\u0b57\u0b51\3\2\2\2\u0b58\u00f7\3\2\2\2\u0b59\u0b5a"+
		"\7\u00f7\2\2\u0b5a\u0b61\t&\2\2\u0b5b\u0b5c\7:\2\2\u0b5c\u0b61\7\u00cc"+
		"\2\2\u0b5d\u0b5e\5\u00bc_\2\u0b5e\u0b5f\t&\2\2\u0b5f\u0b61\3\2\2\2\u0b60"+
		"\u0b59\3\2\2\2\u0b60\u0b5b\3\2\2\2\u0b60\u0b5d\3\2\2\2\u0b61\u00f9\3\2"+
		"\2\2\u0b62\u0b67\5\u00fe\u0080\2\u0b63\u0b64\7\6\2\2\u0b64\u0b66\5\u00fe"+
		"\u0080\2\u0b65\u0b63\3\2\2\2\u0b66\u0b69\3\2\2\2\u0b67\u0b65\3\2\2\2\u0b67"+
		"\u0b68\3\2\2\2\u0b68\u00fb\3\2\2\2\u0b69\u0b67\3\2\2\2\u0b6a\u0b6f\5\u00fe"+
		"\u0080\2\u0b6b\u0b6f\7^\2\2\u0b6c\u0b6f\7\u0085\2\2\u0b6d\u0b6f\7\u00c6"+
		"\2\2\u0b6e\u0b6a\3\2\2\2\u0b6e\u0b6b\3\2\2\2\u0b6e\u0b6c\3\2\2\2\u0b6e"+
		"\u0b6d\3\2\2\2\u0b6f\u00fd\3\2\2\2\u0b70\u0b75\5\u0104\u0083\2\u0b71\u0b72"+
		"\7\7\2\2\u0b72\u0b74\5\u0104\u0083\2\u0b73\u0b71\3\2\2\2\u0b74\u0b77\3"+
		"\2\2\2\u0b75\u0b73\3\2\2\2\u0b75\u0b76\3\2\2\2\u0b76\u00ff\3\2\2\2\u0b77"+
		"\u0b75\3\2\2\2\u0b78\u0b79\5\u0104\u0083\2\u0b79\u0b7a\5\u0102\u0082\2"+
		"\u0b7a\u0101\3\2\2\2\u0b7b\u0b7c\7\u0113\2\2\u0b7c\u0b7e\5\u0104\u0083"+
		"\2\u0b7d\u0b7b\3\2\2\2\u0b7e\u0b7f\3\2\2\2\u0b7f\u0b7d\3\2\2\2\u0b7f\u0b80"+
		"\3\2\2\2\u0b80\u0b83\3\2\2\2\u0b81\u0b83\3\2\2\2\u0b82\u0b7d\3\2\2\2\u0b82"+
		"\u0b81\3\2\2\2\u0b83\u0103\3\2\2\2\u0b84\u0b88\5\u0106\u0084\2\u0b85\u0b86"+
		"\6\u0083\24\2\u0b86\u0b88\5\u0110\u0089\2\u0b87\u0b84\3\2\2\2\u0b87\u0b85"+
		"\3\2\2\2\u0b88\u0105\3\2\2\2\u0b89\u0b90\7\u0126\2\2\u0b8a\u0b90\5\u0108"+
		"\u0085\2\u0b8b\u0b8c\6\u0084\25\2\u0b8c\u0b90\5\u010e\u0088\2\u0b8d\u0b8e"+
		"\6\u0084\26\2\u0b8e\u0b90\5\u0112\u008a\2\u0b8f\u0b89\3\2\2\2\u0b8f\u0b8a"+
		"\3\2\2\2\u0b8f\u0b8b\3\2\2\2\u0b8f\u0b8d\3\2\2\2\u0b90\u0107\3\2\2\2\u0b91"+
		"\u0b92\7\u0127\2\2\u0b92\u0109\3\2\2\2\u0b93\u0b95\6\u0086\27\2\u0b94"+
		"\u0b96\7\u0113\2\2\u0b95\u0b94\3\2\2\2\u0b95\u0b96\3\2\2\2\u0b96\u0b97"+
		"\3\2\2\2\u0b97\u0bbb\7\u0122\2\2\u0b98\u0b9a\6\u0086\30\2\u0b99\u0b9b"+
		"\7\u0113\2\2\u0b9a\u0b99\3\2\2\2\u0b9a\u0b9b\3\2\2\2\u0b9b\u0b9c\3\2\2"+
		"\2\u0b9c\u0bbb\7\u0123\2\2\u0b9d\u0b9f\6\u0086\31\2\u0b9e\u0ba0\7\u0113"+
		"\2\2\u0b9f\u0b9e\3\2\2\2\u0b9f\u0ba0\3\2\2\2\u0ba0\u0ba1\3\2\2\2\u0ba1"+
		"\u0bbb\t\'\2\2\u0ba2\u0ba4\7\u0113\2\2\u0ba3\u0ba2\3\2\2\2\u0ba3\u0ba4"+
		"\3\2\2\2\u0ba4\u0ba5\3\2\2\2\u0ba5\u0bbb\7\u0121\2\2\u0ba6\u0ba8\7\u0113"+
		"\2\2\u0ba7\u0ba6\3\2\2\2\u0ba7\u0ba8\3\2\2\2\u0ba8\u0ba9\3\2\2\2\u0ba9"+
		"\u0bbb\7\u011e\2\2\u0baa\u0bac\7\u0113\2\2\u0bab\u0baa\3\2\2\2\u0bab\u0bac"+
		"\3\2\2\2\u0bac\u0bad\3\2\2\2\u0bad\u0bbb\7\u011f\2\2\u0bae\u0bb0\7\u0113"+
		"\2\2\u0baf\u0bae\3\2\2\2\u0baf\u0bb0\3\2\2\2\u0bb0\u0bb1\3\2\2\2\u0bb1"+
		"\u0bbb\7\u0120\2\2\u0bb2\u0bb4\7\u0113\2\2\u0bb3\u0bb2\3\2\2\2\u0bb3\u0bb4"+
		"\3\2\2\2\u0bb4\u0bb5\3\2\2\2\u0bb5\u0bbb\7\u0124\2\2\u0bb6\u0bb8\7\u0113"+
		"\2\2\u0bb7\u0bb6\3\2\2\2\u0bb7\u0bb8\3\2\2\2\u0bb8\u0bb9\3\2\2\2\u0bb9"+
		"\u0bbb\7\u0125\2\2\u0bba\u0b93\3\2\2\2\u0bba\u0b98\3\2\2\2\u0bba\u0b9d"+
		"\3\2\2\2\u0bba\u0ba3\3\2\2\2\u0bba\u0ba7\3\2\2\2\u0bba\u0bab\3\2\2\2\u0bba"+
		"\u0baf\3\2\2\2\u0bba\u0bb3\3\2\2\2\u0bba\u0bb7\3\2\2\2\u0bbb\u010b\3\2"+
		"\2\2\u0bbc\u0bbd\7\u00f5\2\2\u0bbd\u0bc4\5\u00e0q\2\u0bbe\u0bc4\5\36\20"+
		"\2\u0bbf\u0bc4\5\u00dep\2\u0bc0\u0bc1\t(\2\2\u0bc1\u0bc2\7\u009b\2\2\u0bc2"+
		"\u0bc4\7\u009c\2\2\u0bc3\u0bbc\3\2\2\2\u0bc3\u0bbe\3\2\2\2\u0bc3\u0bbf"+
		"\3\2\2\2\u0bc3\u0bc0\3\2\2\2\u0bc4\u010d\3\2\2\2\u0bc5\u0bc6\t)\2\2\u0bc6"+
		"\u010f\3\2\2\2\u0bc7\u0bc8\t*\2\2\u0bc8\u0111\3\2\2\2\u0bc9\u0bca\t+\2"+
		"\2\u0bca\u0113\3\2\2\2\u018c\u0118\u0131\u0136\u013e\u0146\u0148\u015c"+
		"\u0160\u0166\u0169\u016c\u0174\u0177\u017b\u017e\u0186\u018b\u018e\u0195"+
		"\u01a1\u01aa\u01ac\u01b0\u01b3\u01ba\u01c5\u01c7\u01cf\u01d4\u01d7\u01dd"+
		"\u01e8\u0228\u0231\u0235\u023b\u023f\u0244\u024a\u0256\u025e\u0264\u0271"+
		"\u0276\u0286\u028d\u0291\u0297\u02a6\u02aa\u02b0\u02b6\u02b9\u02bc\u02c2"+
		"\u02c6\u02ce\u02d0\u02d9\u02dc\u02e5\u02ea\u02f0\u02f7\u02fa\u0300\u030b"+
		"\u030e\u0312\u0317\u031c\u0323\u0326\u0329\u0330\u0335\u033e\u0346\u034c"+
		"\u034f\u0352\u0358\u035c\u0360\u0364\u0366\u036e\u0376\u037c\u0382\u0385"+
		"\u0389\u038c\u0390\u03a9\u03ac\u03b0\u03b6\u03b9\u03bc\u03c2\u03ca\u03cf"+
		"\u03d5\u03db\u03e7\u03ea\u03f1\u03f8\u0400\u0403\u040b\u040f\u0416\u048a"+
		"\u0492\u049a\u04a3\u04ad\u04b1\u04b4\u04ba\u04c0\u04cc\u04d8\u04dd\u04e6"+
		"\u04ee\u04f5\u04f7\u04fc\u0500\u0505\u050a\u050f\u0512\u0517\u051b\u0520"+
		"\u0522\u0526\u052f\u0537\u0540\u0547\u0550\u0555\u0558\u056b\u056d\u0576"+
		"\u057d\u0580\u0587\u058b\u0591\u0599\u05a4\u05af\u05b6\u05bc\u05c9\u05d0"+
		"\u05d7\u05e3\u05eb\u05f1\u05f4\u05fd\u0600\u0609\u060c\u0615\u0618\u0621"+
		"\u0624\u0627\u062c\u062e\u063a\u0641\u0648\u064b\u064d\u0659\u065d\u0661"+
		"\u0667\u066b\u0673\u0677\u067a\u067d\u0680\u0684\u0688\u068b\u068f\u0694"+
		"\u0698\u069b\u069e\u06a1\u06a3\u06af\u06b2\u06b6\u06c0\u06c4\u06c6\u06c9"+
		"\u06cd\u06d3\u06d7\u06e2\u06ec\u06f8\u0707\u070c\u0713\u0723\u0728\u0735"+
		"\u073a\u0742\u0748\u074c\u0755\u0764\u0769\u0775\u077a\u0782\u0785\u0789"+
		"\u0797\u07a4\u07a9\u07ad\u07b0\u07b5\u07be\u07c1\u07c6\u07cd\u07d0\u07d8"+
		"\u07df\u07e6\u07e9\u07ee\u07f1\u07f6\u07fa\u07fd\u0800\u0806\u080b\u0810"+
		"\u0822\u0824\u0827\u0832\u083b\u0842\u084a\u0851\u0855\u085d\u0865\u086b"+
		"\u0873\u087f\u0882\u0888\u088c\u088e\u0897\u08a3\u08a5\u08ac\u08b3\u08b9"+
		"\u08bf\u08c1\u08c8\u08d0\u08d6\u08dd\u08e3\u08e7\u08e9\u08f0\u08f9\u0906"+
		"\u090b\u090f\u091d\u091f\u0927\u0929\u092d\u0935\u093e\u0944\u094c\u0951"+
		"\u095d\u0962\u0965\u096b\u096f\u0974\u0979\u097e\u0984\u0999\u099b\u09a4"+
		"\u09a8\u09b1\u09b5\u09c7\u09ca\u09d2\u09db\u09f2\u09fd\u0a04\u0a07\u0a10"+
		"\u0a14\u0a20\u0a39\u0a40\u0a43\u0a52\u0a56\u0a60\u0a62\u0a6f\u0a71\u0a7e"+
		"\u0a82\u0a89\u0a8e\u0a96\u0a9a\u0aa3\u0aa8\u0ab9\u0abd\u0ac6\u0aca\u0acc"+
		"\u0ad3\u0ada\u0add\u0ae0\u0ae7\u0aee\u0af1\u0af8\u0b00\u0b03\u0b10\u0b24"+
		"\u0b2e\u0b31\u0b3a\u0b3d\u0b3f\u0b42\u0b45\u0b57\u0b60\u0b67\u0b6e\u0b75"+
		"\u0b7f\u0b82\u0b87\u0b8f\u0b95\u0b9a\u0b9f\u0ba3\u0ba7\u0bab\u0baf\u0bb3"+
		"\u0bb7\u0bba\u0bc3";
	public static final String _serializedATN = Utils.join(
		new String[] {
			_serializedATNSegment0,
			_serializedATNSegment1
		},
		""
	);
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}