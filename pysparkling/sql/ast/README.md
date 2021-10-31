# Python Abstract Syntax Tree for Spark SQL

This folder uses ANTLR4 to convert a SQL statement in an Abstract Syntax Tree in Python.

This AST is then transformed in the corresponding pysparkling abtrasaction.

## Example


## Recreate generated files

First, download the ANTLR complete JAR from [the ANTLR site][antlr].

[antlr]:http://www.antlr.org/

Next, install the required dev ANTLR4 Python 3 runtime package:

```
pip install antlr4-python3-runtime
```

Then, run ANTLR to compile the SQL grammar and generate Python code.

```
java -Xmx500M -cp "<path to ANTLR complete JAR>:$CLASSPATH" org.antlr.v4.Tool -Dlanguage=Python3 SqlBase.g4
```
