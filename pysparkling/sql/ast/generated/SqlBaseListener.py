# Generated from ../grammar/SqlBase.g4 by ANTLR 4.7.1
from antlr4 import *
if __name__ is not None and "." in __name__:
    from .SqlBaseParser import SqlBaseParser
else:
    from SqlBaseParser import SqlBaseParser

# This class defines a complete listener for a parse tree produced by SqlBaseParser.
class SqlBaseListener(ParseTreeListener):

    # Enter a parse tree produced by SqlBaseParser#singleStatement.
    def enterSingleStatement(self, ctx:SqlBaseParser.SingleStatementContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#singleStatement.
    def exitSingleStatement(self, ctx:SqlBaseParser.SingleStatementContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#singleExpression.
    def enterSingleExpression(self, ctx:SqlBaseParser.SingleExpressionContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#singleExpression.
    def exitSingleExpression(self, ctx:SqlBaseParser.SingleExpressionContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#singleTableIdentifier.
    def enterSingleTableIdentifier(self, ctx:SqlBaseParser.SingleTableIdentifierContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#singleTableIdentifier.
    def exitSingleTableIdentifier(self, ctx:SqlBaseParser.SingleTableIdentifierContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#singleMultipartIdentifier.
    def enterSingleMultipartIdentifier(self, ctx:SqlBaseParser.SingleMultipartIdentifierContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#singleMultipartIdentifier.
    def exitSingleMultipartIdentifier(self, ctx:SqlBaseParser.SingleMultipartIdentifierContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#singleFunctionIdentifier.
    def enterSingleFunctionIdentifier(self, ctx:SqlBaseParser.SingleFunctionIdentifierContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#singleFunctionIdentifier.
    def exitSingleFunctionIdentifier(self, ctx:SqlBaseParser.SingleFunctionIdentifierContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#singleDataType.
    def enterSingleDataType(self, ctx:SqlBaseParser.SingleDataTypeContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#singleDataType.
    def exitSingleDataType(self, ctx:SqlBaseParser.SingleDataTypeContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#singleTableSchema.
    def enterSingleTableSchema(self, ctx:SqlBaseParser.SingleTableSchemaContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#singleTableSchema.
    def exitSingleTableSchema(self, ctx:SqlBaseParser.SingleTableSchemaContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#statementDefault.
    def enterStatementDefault(self, ctx:SqlBaseParser.StatementDefaultContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#statementDefault.
    def exitStatementDefault(self, ctx:SqlBaseParser.StatementDefaultContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#dmlStatement.
    def enterDmlStatement(self, ctx:SqlBaseParser.DmlStatementContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#dmlStatement.
    def exitDmlStatement(self, ctx:SqlBaseParser.DmlStatementContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#use.
    def enterUse(self, ctx:SqlBaseParser.UseContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#use.
    def exitUse(self, ctx:SqlBaseParser.UseContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#createNamespace.
    def enterCreateNamespace(self, ctx:SqlBaseParser.CreateNamespaceContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#createNamespace.
    def exitCreateNamespace(self, ctx:SqlBaseParser.CreateNamespaceContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#setNamespaceProperties.
    def enterSetNamespaceProperties(self, ctx:SqlBaseParser.SetNamespacePropertiesContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#setNamespaceProperties.
    def exitSetNamespaceProperties(self, ctx:SqlBaseParser.SetNamespacePropertiesContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#setNamespaceLocation.
    def enterSetNamespaceLocation(self, ctx:SqlBaseParser.SetNamespaceLocationContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#setNamespaceLocation.
    def exitSetNamespaceLocation(self, ctx:SqlBaseParser.SetNamespaceLocationContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#dropNamespace.
    def enterDropNamespace(self, ctx:SqlBaseParser.DropNamespaceContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#dropNamespace.
    def exitDropNamespace(self, ctx:SqlBaseParser.DropNamespaceContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#showNamespaces.
    def enterShowNamespaces(self, ctx:SqlBaseParser.ShowNamespacesContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#showNamespaces.
    def exitShowNamespaces(self, ctx:SqlBaseParser.ShowNamespacesContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#createTable.
    def enterCreateTable(self, ctx:SqlBaseParser.CreateTableContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#createTable.
    def exitCreateTable(self, ctx:SqlBaseParser.CreateTableContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#createHiveTable.
    def enterCreateHiveTable(self, ctx:SqlBaseParser.CreateHiveTableContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#createHiveTable.
    def exitCreateHiveTable(self, ctx:SqlBaseParser.CreateHiveTableContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#createTableLike.
    def enterCreateTableLike(self, ctx:SqlBaseParser.CreateTableLikeContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#createTableLike.
    def exitCreateTableLike(self, ctx:SqlBaseParser.CreateTableLikeContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#replaceTable.
    def enterReplaceTable(self, ctx:SqlBaseParser.ReplaceTableContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#replaceTable.
    def exitReplaceTable(self, ctx:SqlBaseParser.ReplaceTableContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#analyze.
    def enterAnalyze(self, ctx:SqlBaseParser.AnalyzeContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#analyze.
    def exitAnalyze(self, ctx:SqlBaseParser.AnalyzeContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#addTableColumns.
    def enterAddTableColumns(self, ctx:SqlBaseParser.AddTableColumnsContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#addTableColumns.
    def exitAddTableColumns(self, ctx:SqlBaseParser.AddTableColumnsContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#renameTableColumn.
    def enterRenameTableColumn(self, ctx:SqlBaseParser.RenameTableColumnContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#renameTableColumn.
    def exitRenameTableColumn(self, ctx:SqlBaseParser.RenameTableColumnContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#dropTableColumns.
    def enterDropTableColumns(self, ctx:SqlBaseParser.DropTableColumnsContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#dropTableColumns.
    def exitDropTableColumns(self, ctx:SqlBaseParser.DropTableColumnsContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#renameTable.
    def enterRenameTable(self, ctx:SqlBaseParser.RenameTableContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#renameTable.
    def exitRenameTable(self, ctx:SqlBaseParser.RenameTableContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#setTableProperties.
    def enterSetTableProperties(self, ctx:SqlBaseParser.SetTablePropertiesContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#setTableProperties.
    def exitSetTableProperties(self, ctx:SqlBaseParser.SetTablePropertiesContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#unsetTableProperties.
    def enterUnsetTableProperties(self, ctx:SqlBaseParser.UnsetTablePropertiesContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#unsetTableProperties.
    def exitUnsetTableProperties(self, ctx:SqlBaseParser.UnsetTablePropertiesContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#alterTableAlterColumn.
    def enterAlterTableAlterColumn(self, ctx:SqlBaseParser.AlterTableAlterColumnContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#alterTableAlterColumn.
    def exitAlterTableAlterColumn(self, ctx:SqlBaseParser.AlterTableAlterColumnContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#hiveChangeColumn.
    def enterHiveChangeColumn(self, ctx:SqlBaseParser.HiveChangeColumnContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#hiveChangeColumn.
    def exitHiveChangeColumn(self, ctx:SqlBaseParser.HiveChangeColumnContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#hiveReplaceColumns.
    def enterHiveReplaceColumns(self, ctx:SqlBaseParser.HiveReplaceColumnsContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#hiveReplaceColumns.
    def exitHiveReplaceColumns(self, ctx:SqlBaseParser.HiveReplaceColumnsContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#setTableSerDe.
    def enterSetTableSerDe(self, ctx:SqlBaseParser.SetTableSerDeContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#setTableSerDe.
    def exitSetTableSerDe(self, ctx:SqlBaseParser.SetTableSerDeContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#addTablePartition.
    def enterAddTablePartition(self, ctx:SqlBaseParser.AddTablePartitionContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#addTablePartition.
    def exitAddTablePartition(self, ctx:SqlBaseParser.AddTablePartitionContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#renameTablePartition.
    def enterRenameTablePartition(self, ctx:SqlBaseParser.RenameTablePartitionContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#renameTablePartition.
    def exitRenameTablePartition(self, ctx:SqlBaseParser.RenameTablePartitionContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#dropTablePartitions.
    def enterDropTablePartitions(self, ctx:SqlBaseParser.DropTablePartitionsContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#dropTablePartitions.
    def exitDropTablePartitions(self, ctx:SqlBaseParser.DropTablePartitionsContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#setTableLocation.
    def enterSetTableLocation(self, ctx:SqlBaseParser.SetTableLocationContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#setTableLocation.
    def exitSetTableLocation(self, ctx:SqlBaseParser.SetTableLocationContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#recoverPartitions.
    def enterRecoverPartitions(self, ctx:SqlBaseParser.RecoverPartitionsContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#recoverPartitions.
    def exitRecoverPartitions(self, ctx:SqlBaseParser.RecoverPartitionsContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#dropTable.
    def enterDropTable(self, ctx:SqlBaseParser.DropTableContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#dropTable.
    def exitDropTable(self, ctx:SqlBaseParser.DropTableContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#dropView.
    def enterDropView(self, ctx:SqlBaseParser.DropViewContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#dropView.
    def exitDropView(self, ctx:SqlBaseParser.DropViewContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#createView.
    def enterCreateView(self, ctx:SqlBaseParser.CreateViewContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#createView.
    def exitCreateView(self, ctx:SqlBaseParser.CreateViewContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#createTempViewUsing.
    def enterCreateTempViewUsing(self, ctx:SqlBaseParser.CreateTempViewUsingContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#createTempViewUsing.
    def exitCreateTempViewUsing(self, ctx:SqlBaseParser.CreateTempViewUsingContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#alterViewQuery.
    def enterAlterViewQuery(self, ctx:SqlBaseParser.AlterViewQueryContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#alterViewQuery.
    def exitAlterViewQuery(self, ctx:SqlBaseParser.AlterViewQueryContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#createFunction.
    def enterCreateFunction(self, ctx:SqlBaseParser.CreateFunctionContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#createFunction.
    def exitCreateFunction(self, ctx:SqlBaseParser.CreateFunctionContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#dropFunction.
    def enterDropFunction(self, ctx:SqlBaseParser.DropFunctionContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#dropFunction.
    def exitDropFunction(self, ctx:SqlBaseParser.DropFunctionContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#explain.
    def enterExplain(self, ctx:SqlBaseParser.ExplainContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#explain.
    def exitExplain(self, ctx:SqlBaseParser.ExplainContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#showTables.
    def enterShowTables(self, ctx:SqlBaseParser.ShowTablesContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#showTables.
    def exitShowTables(self, ctx:SqlBaseParser.ShowTablesContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#showTable.
    def enterShowTable(self, ctx:SqlBaseParser.ShowTableContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#showTable.
    def exitShowTable(self, ctx:SqlBaseParser.ShowTableContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#showTblProperties.
    def enterShowTblProperties(self, ctx:SqlBaseParser.ShowTblPropertiesContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#showTblProperties.
    def exitShowTblProperties(self, ctx:SqlBaseParser.ShowTblPropertiesContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#showColumns.
    def enterShowColumns(self, ctx:SqlBaseParser.ShowColumnsContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#showColumns.
    def exitShowColumns(self, ctx:SqlBaseParser.ShowColumnsContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#showViews.
    def enterShowViews(self, ctx:SqlBaseParser.ShowViewsContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#showViews.
    def exitShowViews(self, ctx:SqlBaseParser.ShowViewsContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#showPartitions.
    def enterShowPartitions(self, ctx:SqlBaseParser.ShowPartitionsContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#showPartitions.
    def exitShowPartitions(self, ctx:SqlBaseParser.ShowPartitionsContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#showFunctions.
    def enterShowFunctions(self, ctx:SqlBaseParser.ShowFunctionsContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#showFunctions.
    def exitShowFunctions(self, ctx:SqlBaseParser.ShowFunctionsContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#showCreateTable.
    def enterShowCreateTable(self, ctx:SqlBaseParser.ShowCreateTableContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#showCreateTable.
    def exitShowCreateTable(self, ctx:SqlBaseParser.ShowCreateTableContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#showCurrentNamespace.
    def enterShowCurrentNamespace(self, ctx:SqlBaseParser.ShowCurrentNamespaceContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#showCurrentNamespace.
    def exitShowCurrentNamespace(self, ctx:SqlBaseParser.ShowCurrentNamespaceContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#describeFunction.
    def enterDescribeFunction(self, ctx:SqlBaseParser.DescribeFunctionContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#describeFunction.
    def exitDescribeFunction(self, ctx:SqlBaseParser.DescribeFunctionContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#describeNamespace.
    def enterDescribeNamespace(self, ctx:SqlBaseParser.DescribeNamespaceContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#describeNamespace.
    def exitDescribeNamespace(self, ctx:SqlBaseParser.DescribeNamespaceContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#describeRelation.
    def enterDescribeRelation(self, ctx:SqlBaseParser.DescribeRelationContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#describeRelation.
    def exitDescribeRelation(self, ctx:SqlBaseParser.DescribeRelationContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#describeQuery.
    def enterDescribeQuery(self, ctx:SqlBaseParser.DescribeQueryContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#describeQuery.
    def exitDescribeQuery(self, ctx:SqlBaseParser.DescribeQueryContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#commentNamespace.
    def enterCommentNamespace(self, ctx:SqlBaseParser.CommentNamespaceContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#commentNamespace.
    def exitCommentNamespace(self, ctx:SqlBaseParser.CommentNamespaceContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#commentTable.
    def enterCommentTable(self, ctx:SqlBaseParser.CommentTableContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#commentTable.
    def exitCommentTable(self, ctx:SqlBaseParser.CommentTableContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#refreshTable.
    def enterRefreshTable(self, ctx:SqlBaseParser.RefreshTableContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#refreshTable.
    def exitRefreshTable(self, ctx:SqlBaseParser.RefreshTableContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#refreshResource.
    def enterRefreshResource(self, ctx:SqlBaseParser.RefreshResourceContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#refreshResource.
    def exitRefreshResource(self, ctx:SqlBaseParser.RefreshResourceContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#cacheTable.
    def enterCacheTable(self, ctx:SqlBaseParser.CacheTableContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#cacheTable.
    def exitCacheTable(self, ctx:SqlBaseParser.CacheTableContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#uncacheTable.
    def enterUncacheTable(self, ctx:SqlBaseParser.UncacheTableContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#uncacheTable.
    def exitUncacheTable(self, ctx:SqlBaseParser.UncacheTableContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#clearCache.
    def enterClearCache(self, ctx:SqlBaseParser.ClearCacheContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#clearCache.
    def exitClearCache(self, ctx:SqlBaseParser.ClearCacheContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#loadData.
    def enterLoadData(self, ctx:SqlBaseParser.LoadDataContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#loadData.
    def exitLoadData(self, ctx:SqlBaseParser.LoadDataContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#truncateTable.
    def enterTruncateTable(self, ctx:SqlBaseParser.TruncateTableContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#truncateTable.
    def exitTruncateTable(self, ctx:SqlBaseParser.TruncateTableContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#repairTable.
    def enterRepairTable(self, ctx:SqlBaseParser.RepairTableContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#repairTable.
    def exitRepairTable(self, ctx:SqlBaseParser.RepairTableContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#manageResource.
    def enterManageResource(self, ctx:SqlBaseParser.ManageResourceContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#manageResource.
    def exitManageResource(self, ctx:SqlBaseParser.ManageResourceContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#failNativeCommand.
    def enterFailNativeCommand(self, ctx:SqlBaseParser.FailNativeCommandContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#failNativeCommand.
    def exitFailNativeCommand(self, ctx:SqlBaseParser.FailNativeCommandContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#setConfiguration.
    def enterSetConfiguration(self, ctx:SqlBaseParser.SetConfigurationContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#setConfiguration.
    def exitSetConfiguration(self, ctx:SqlBaseParser.SetConfigurationContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#resetConfiguration.
    def enterResetConfiguration(self, ctx:SqlBaseParser.ResetConfigurationContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#resetConfiguration.
    def exitResetConfiguration(self, ctx:SqlBaseParser.ResetConfigurationContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#unsupportedHiveNativeCommands.
    def enterUnsupportedHiveNativeCommands(self, ctx:SqlBaseParser.UnsupportedHiveNativeCommandsContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#unsupportedHiveNativeCommands.
    def exitUnsupportedHiveNativeCommands(self, ctx:SqlBaseParser.UnsupportedHiveNativeCommandsContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#createTableHeader.
    def enterCreateTableHeader(self, ctx:SqlBaseParser.CreateTableHeaderContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#createTableHeader.
    def exitCreateTableHeader(self, ctx:SqlBaseParser.CreateTableHeaderContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#replaceTableHeader.
    def enterReplaceTableHeader(self, ctx:SqlBaseParser.ReplaceTableHeaderContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#replaceTableHeader.
    def exitReplaceTableHeader(self, ctx:SqlBaseParser.ReplaceTableHeaderContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#bucketSpec.
    def enterBucketSpec(self, ctx:SqlBaseParser.BucketSpecContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#bucketSpec.
    def exitBucketSpec(self, ctx:SqlBaseParser.BucketSpecContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#skewSpec.
    def enterSkewSpec(self, ctx:SqlBaseParser.SkewSpecContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#skewSpec.
    def exitSkewSpec(self, ctx:SqlBaseParser.SkewSpecContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#locationSpec.
    def enterLocationSpec(self, ctx:SqlBaseParser.LocationSpecContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#locationSpec.
    def exitLocationSpec(self, ctx:SqlBaseParser.LocationSpecContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#commentSpec.
    def enterCommentSpec(self, ctx:SqlBaseParser.CommentSpecContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#commentSpec.
    def exitCommentSpec(self, ctx:SqlBaseParser.CommentSpecContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#query.
    def enterQuery(self, ctx:SqlBaseParser.QueryContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#query.
    def exitQuery(self, ctx:SqlBaseParser.QueryContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#insertOverwriteTable.
    def enterInsertOverwriteTable(self, ctx:SqlBaseParser.InsertOverwriteTableContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#insertOverwriteTable.
    def exitInsertOverwriteTable(self, ctx:SqlBaseParser.InsertOverwriteTableContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#insertIntoTable.
    def enterInsertIntoTable(self, ctx:SqlBaseParser.InsertIntoTableContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#insertIntoTable.
    def exitInsertIntoTable(self, ctx:SqlBaseParser.InsertIntoTableContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#insertOverwriteHiveDir.
    def enterInsertOverwriteHiveDir(self, ctx:SqlBaseParser.InsertOverwriteHiveDirContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#insertOverwriteHiveDir.
    def exitInsertOverwriteHiveDir(self, ctx:SqlBaseParser.InsertOverwriteHiveDirContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#insertOverwriteDir.
    def enterInsertOverwriteDir(self, ctx:SqlBaseParser.InsertOverwriteDirContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#insertOverwriteDir.
    def exitInsertOverwriteDir(self, ctx:SqlBaseParser.InsertOverwriteDirContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#partitionSpecLocation.
    def enterPartitionSpecLocation(self, ctx:SqlBaseParser.PartitionSpecLocationContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#partitionSpecLocation.
    def exitPartitionSpecLocation(self, ctx:SqlBaseParser.PartitionSpecLocationContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#partitionSpec.
    def enterPartitionSpec(self, ctx:SqlBaseParser.PartitionSpecContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#partitionSpec.
    def exitPartitionSpec(self, ctx:SqlBaseParser.PartitionSpecContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#partitionVal.
    def enterPartitionVal(self, ctx:SqlBaseParser.PartitionValContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#partitionVal.
    def exitPartitionVal(self, ctx:SqlBaseParser.PartitionValContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#namespace.
    def enterNamespace(self, ctx:SqlBaseParser.NamespaceContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#namespace.
    def exitNamespace(self, ctx:SqlBaseParser.NamespaceContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#describeFuncName.
    def enterDescribeFuncName(self, ctx:SqlBaseParser.DescribeFuncNameContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#describeFuncName.
    def exitDescribeFuncName(self, ctx:SqlBaseParser.DescribeFuncNameContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#describeColName.
    def enterDescribeColName(self, ctx:SqlBaseParser.DescribeColNameContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#describeColName.
    def exitDescribeColName(self, ctx:SqlBaseParser.DescribeColNameContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#ctes.
    def enterCtes(self, ctx:SqlBaseParser.CtesContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#ctes.
    def exitCtes(self, ctx:SqlBaseParser.CtesContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#namedQuery.
    def enterNamedQuery(self, ctx:SqlBaseParser.NamedQueryContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#namedQuery.
    def exitNamedQuery(self, ctx:SqlBaseParser.NamedQueryContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#tableProvider.
    def enterTableProvider(self, ctx:SqlBaseParser.TableProviderContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#tableProvider.
    def exitTableProvider(self, ctx:SqlBaseParser.TableProviderContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#createTableClauses.
    def enterCreateTableClauses(self, ctx:SqlBaseParser.CreateTableClausesContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#createTableClauses.
    def exitCreateTableClauses(self, ctx:SqlBaseParser.CreateTableClausesContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#tablePropertyList.
    def enterTablePropertyList(self, ctx:SqlBaseParser.TablePropertyListContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#tablePropertyList.
    def exitTablePropertyList(self, ctx:SqlBaseParser.TablePropertyListContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#tableProperty.
    def enterTableProperty(self, ctx:SqlBaseParser.TablePropertyContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#tableProperty.
    def exitTableProperty(self, ctx:SqlBaseParser.TablePropertyContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#tablePropertyKey.
    def enterTablePropertyKey(self, ctx:SqlBaseParser.TablePropertyKeyContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#tablePropertyKey.
    def exitTablePropertyKey(self, ctx:SqlBaseParser.TablePropertyKeyContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#tablePropertyValue.
    def enterTablePropertyValue(self, ctx:SqlBaseParser.TablePropertyValueContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#tablePropertyValue.
    def exitTablePropertyValue(self, ctx:SqlBaseParser.TablePropertyValueContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#constantList.
    def enterConstantList(self, ctx:SqlBaseParser.ConstantListContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#constantList.
    def exitConstantList(self, ctx:SqlBaseParser.ConstantListContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#nestedConstantList.
    def enterNestedConstantList(self, ctx:SqlBaseParser.NestedConstantListContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#nestedConstantList.
    def exitNestedConstantList(self, ctx:SqlBaseParser.NestedConstantListContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#createFileFormat.
    def enterCreateFileFormat(self, ctx:SqlBaseParser.CreateFileFormatContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#createFileFormat.
    def exitCreateFileFormat(self, ctx:SqlBaseParser.CreateFileFormatContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#tableFileFormat.
    def enterTableFileFormat(self, ctx:SqlBaseParser.TableFileFormatContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#tableFileFormat.
    def exitTableFileFormat(self, ctx:SqlBaseParser.TableFileFormatContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#genericFileFormat.
    def enterGenericFileFormat(self, ctx:SqlBaseParser.GenericFileFormatContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#genericFileFormat.
    def exitGenericFileFormat(self, ctx:SqlBaseParser.GenericFileFormatContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#storageHandler.
    def enterStorageHandler(self, ctx:SqlBaseParser.StorageHandlerContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#storageHandler.
    def exitStorageHandler(self, ctx:SqlBaseParser.StorageHandlerContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#resource.
    def enterResource(self, ctx:SqlBaseParser.ResourceContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#resource.
    def exitResource(self, ctx:SqlBaseParser.ResourceContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#singleInsertQuery.
    def enterSingleInsertQuery(self, ctx:SqlBaseParser.SingleInsertQueryContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#singleInsertQuery.
    def exitSingleInsertQuery(self, ctx:SqlBaseParser.SingleInsertQueryContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#multiInsertQuery.
    def enterMultiInsertQuery(self, ctx:SqlBaseParser.MultiInsertQueryContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#multiInsertQuery.
    def exitMultiInsertQuery(self, ctx:SqlBaseParser.MultiInsertQueryContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#deleteFromTable.
    def enterDeleteFromTable(self, ctx:SqlBaseParser.DeleteFromTableContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#deleteFromTable.
    def exitDeleteFromTable(self, ctx:SqlBaseParser.DeleteFromTableContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#updateTable.
    def enterUpdateTable(self, ctx:SqlBaseParser.UpdateTableContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#updateTable.
    def exitUpdateTable(self, ctx:SqlBaseParser.UpdateTableContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#mergeIntoTable.
    def enterMergeIntoTable(self, ctx:SqlBaseParser.MergeIntoTableContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#mergeIntoTable.
    def exitMergeIntoTable(self, ctx:SqlBaseParser.MergeIntoTableContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#queryOrganization.
    def enterQueryOrganization(self, ctx:SqlBaseParser.QueryOrganizationContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#queryOrganization.
    def exitQueryOrganization(self, ctx:SqlBaseParser.QueryOrganizationContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#multiInsertQueryBody.
    def enterMultiInsertQueryBody(self, ctx:SqlBaseParser.MultiInsertQueryBodyContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#multiInsertQueryBody.
    def exitMultiInsertQueryBody(self, ctx:SqlBaseParser.MultiInsertQueryBodyContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#queryTermDefault.
    def enterQueryTermDefault(self, ctx:SqlBaseParser.QueryTermDefaultContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#queryTermDefault.
    def exitQueryTermDefault(self, ctx:SqlBaseParser.QueryTermDefaultContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#setOperation.
    def enterSetOperation(self, ctx:SqlBaseParser.SetOperationContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#setOperation.
    def exitSetOperation(self, ctx:SqlBaseParser.SetOperationContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#queryPrimaryDefault.
    def enterQueryPrimaryDefault(self, ctx:SqlBaseParser.QueryPrimaryDefaultContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#queryPrimaryDefault.
    def exitQueryPrimaryDefault(self, ctx:SqlBaseParser.QueryPrimaryDefaultContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#fromStmt.
    def enterFromStmt(self, ctx:SqlBaseParser.FromStmtContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#fromStmt.
    def exitFromStmt(self, ctx:SqlBaseParser.FromStmtContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#table.
    def enterTable(self, ctx:SqlBaseParser.TableContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#table.
    def exitTable(self, ctx:SqlBaseParser.TableContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#inlineTableDefault1.
    def enterInlineTableDefault1(self, ctx:SqlBaseParser.InlineTableDefault1Context):
        pass

    # Exit a parse tree produced by SqlBaseParser#inlineTableDefault1.
    def exitInlineTableDefault1(self, ctx:SqlBaseParser.InlineTableDefault1Context):
        pass


    # Enter a parse tree produced by SqlBaseParser#subquery.
    def enterSubquery(self, ctx:SqlBaseParser.SubqueryContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#subquery.
    def exitSubquery(self, ctx:SqlBaseParser.SubqueryContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#sortItem.
    def enterSortItem(self, ctx:SqlBaseParser.SortItemContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#sortItem.
    def exitSortItem(self, ctx:SqlBaseParser.SortItemContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#fromStatement.
    def enterFromStatement(self, ctx:SqlBaseParser.FromStatementContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#fromStatement.
    def exitFromStatement(self, ctx:SqlBaseParser.FromStatementContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#fromStatementBody.
    def enterFromStatementBody(self, ctx:SqlBaseParser.FromStatementBodyContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#fromStatementBody.
    def exitFromStatementBody(self, ctx:SqlBaseParser.FromStatementBodyContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#transformQuerySpecification.
    def enterTransformQuerySpecification(self, ctx:SqlBaseParser.TransformQuerySpecificationContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#transformQuerySpecification.
    def exitTransformQuerySpecification(self, ctx:SqlBaseParser.TransformQuerySpecificationContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#regularQuerySpecification.
    def enterRegularQuerySpecification(self, ctx:SqlBaseParser.RegularQuerySpecificationContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#regularQuerySpecification.
    def exitRegularQuerySpecification(self, ctx:SqlBaseParser.RegularQuerySpecificationContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#transformClause.
    def enterTransformClause(self, ctx:SqlBaseParser.TransformClauseContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#transformClause.
    def exitTransformClause(self, ctx:SqlBaseParser.TransformClauseContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#selectClause.
    def enterSelectClause(self, ctx:SqlBaseParser.SelectClauseContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#selectClause.
    def exitSelectClause(self, ctx:SqlBaseParser.SelectClauseContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#setClause.
    def enterSetClause(self, ctx:SqlBaseParser.SetClauseContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#setClause.
    def exitSetClause(self, ctx:SqlBaseParser.SetClauseContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#matchedClause.
    def enterMatchedClause(self, ctx:SqlBaseParser.MatchedClauseContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#matchedClause.
    def exitMatchedClause(self, ctx:SqlBaseParser.MatchedClauseContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#notMatchedClause.
    def enterNotMatchedClause(self, ctx:SqlBaseParser.NotMatchedClauseContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#notMatchedClause.
    def exitNotMatchedClause(self, ctx:SqlBaseParser.NotMatchedClauseContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#matchedAction.
    def enterMatchedAction(self, ctx:SqlBaseParser.MatchedActionContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#matchedAction.
    def exitMatchedAction(self, ctx:SqlBaseParser.MatchedActionContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#notMatchedAction.
    def enterNotMatchedAction(self, ctx:SqlBaseParser.NotMatchedActionContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#notMatchedAction.
    def exitNotMatchedAction(self, ctx:SqlBaseParser.NotMatchedActionContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#assignmentList.
    def enterAssignmentList(self, ctx:SqlBaseParser.AssignmentListContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#assignmentList.
    def exitAssignmentList(self, ctx:SqlBaseParser.AssignmentListContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#assignment.
    def enterAssignment(self, ctx:SqlBaseParser.AssignmentContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#assignment.
    def exitAssignment(self, ctx:SqlBaseParser.AssignmentContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#whereClause.
    def enterWhereClause(self, ctx:SqlBaseParser.WhereClauseContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#whereClause.
    def exitWhereClause(self, ctx:SqlBaseParser.WhereClauseContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#havingClause.
    def enterHavingClause(self, ctx:SqlBaseParser.HavingClauseContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#havingClause.
    def exitHavingClause(self, ctx:SqlBaseParser.HavingClauseContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#hint.
    def enterHint(self, ctx:SqlBaseParser.HintContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#hint.
    def exitHint(self, ctx:SqlBaseParser.HintContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#hintStatement.
    def enterHintStatement(self, ctx:SqlBaseParser.HintStatementContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#hintStatement.
    def exitHintStatement(self, ctx:SqlBaseParser.HintStatementContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#fromClause.
    def enterFromClause(self, ctx:SqlBaseParser.FromClauseContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#fromClause.
    def exitFromClause(self, ctx:SqlBaseParser.FromClauseContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#aggregationClause.
    def enterAggregationClause(self, ctx:SqlBaseParser.AggregationClauseContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#aggregationClause.
    def exitAggregationClause(self, ctx:SqlBaseParser.AggregationClauseContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#groupingSet.
    def enterGroupingSet(self, ctx:SqlBaseParser.GroupingSetContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#groupingSet.
    def exitGroupingSet(self, ctx:SqlBaseParser.GroupingSetContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#pivotClause.
    def enterPivotClause(self, ctx:SqlBaseParser.PivotClauseContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#pivotClause.
    def exitPivotClause(self, ctx:SqlBaseParser.PivotClauseContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#pivotColumn.
    def enterPivotColumn(self, ctx:SqlBaseParser.PivotColumnContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#pivotColumn.
    def exitPivotColumn(self, ctx:SqlBaseParser.PivotColumnContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#pivotValue.
    def enterPivotValue(self, ctx:SqlBaseParser.PivotValueContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#pivotValue.
    def exitPivotValue(self, ctx:SqlBaseParser.PivotValueContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#lateralView.
    def enterLateralView(self, ctx:SqlBaseParser.LateralViewContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#lateralView.
    def exitLateralView(self, ctx:SqlBaseParser.LateralViewContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#setQuantifier.
    def enterSetQuantifier(self, ctx:SqlBaseParser.SetQuantifierContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#setQuantifier.
    def exitSetQuantifier(self, ctx:SqlBaseParser.SetQuantifierContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#relation.
    def enterRelation(self, ctx:SqlBaseParser.RelationContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#relation.
    def exitRelation(self, ctx:SqlBaseParser.RelationContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#joinRelation.
    def enterJoinRelation(self, ctx:SqlBaseParser.JoinRelationContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#joinRelation.
    def exitJoinRelation(self, ctx:SqlBaseParser.JoinRelationContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#joinType.
    def enterJoinType(self, ctx:SqlBaseParser.JoinTypeContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#joinType.
    def exitJoinType(self, ctx:SqlBaseParser.JoinTypeContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#joinCriteria.
    def enterJoinCriteria(self, ctx:SqlBaseParser.JoinCriteriaContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#joinCriteria.
    def exitJoinCriteria(self, ctx:SqlBaseParser.JoinCriteriaContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#sample.
    def enterSample(self, ctx:SqlBaseParser.SampleContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#sample.
    def exitSample(self, ctx:SqlBaseParser.SampleContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#sampleByPercentile.
    def enterSampleByPercentile(self, ctx:SqlBaseParser.SampleByPercentileContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#sampleByPercentile.
    def exitSampleByPercentile(self, ctx:SqlBaseParser.SampleByPercentileContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#sampleByRows.
    def enterSampleByRows(self, ctx:SqlBaseParser.SampleByRowsContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#sampleByRows.
    def exitSampleByRows(self, ctx:SqlBaseParser.SampleByRowsContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#sampleByBucket.
    def enterSampleByBucket(self, ctx:SqlBaseParser.SampleByBucketContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#sampleByBucket.
    def exitSampleByBucket(self, ctx:SqlBaseParser.SampleByBucketContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#sampleByBytes.
    def enterSampleByBytes(self, ctx:SqlBaseParser.SampleByBytesContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#sampleByBytes.
    def exitSampleByBytes(self, ctx:SqlBaseParser.SampleByBytesContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#identifierList.
    def enterIdentifierList(self, ctx:SqlBaseParser.IdentifierListContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#identifierList.
    def exitIdentifierList(self, ctx:SqlBaseParser.IdentifierListContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#identifierSeq.
    def enterIdentifierSeq(self, ctx:SqlBaseParser.IdentifierSeqContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#identifierSeq.
    def exitIdentifierSeq(self, ctx:SqlBaseParser.IdentifierSeqContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#orderedIdentifierList.
    def enterOrderedIdentifierList(self, ctx:SqlBaseParser.OrderedIdentifierListContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#orderedIdentifierList.
    def exitOrderedIdentifierList(self, ctx:SqlBaseParser.OrderedIdentifierListContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#orderedIdentifier.
    def enterOrderedIdentifier(self, ctx:SqlBaseParser.OrderedIdentifierContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#orderedIdentifier.
    def exitOrderedIdentifier(self, ctx:SqlBaseParser.OrderedIdentifierContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#identifierCommentList.
    def enterIdentifierCommentList(self, ctx:SqlBaseParser.IdentifierCommentListContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#identifierCommentList.
    def exitIdentifierCommentList(self, ctx:SqlBaseParser.IdentifierCommentListContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#identifierComment.
    def enterIdentifierComment(self, ctx:SqlBaseParser.IdentifierCommentContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#identifierComment.
    def exitIdentifierComment(self, ctx:SqlBaseParser.IdentifierCommentContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#tableName.
    def enterTableName(self, ctx:SqlBaseParser.TableNameContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#tableName.
    def exitTableName(self, ctx:SqlBaseParser.TableNameContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#aliasedQuery.
    def enterAliasedQuery(self, ctx:SqlBaseParser.AliasedQueryContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#aliasedQuery.
    def exitAliasedQuery(self, ctx:SqlBaseParser.AliasedQueryContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#aliasedRelation.
    def enterAliasedRelation(self, ctx:SqlBaseParser.AliasedRelationContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#aliasedRelation.
    def exitAliasedRelation(self, ctx:SqlBaseParser.AliasedRelationContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#inlineTableDefault2.
    def enterInlineTableDefault2(self, ctx:SqlBaseParser.InlineTableDefault2Context):
        pass

    # Exit a parse tree produced by SqlBaseParser#inlineTableDefault2.
    def exitInlineTableDefault2(self, ctx:SqlBaseParser.InlineTableDefault2Context):
        pass


    # Enter a parse tree produced by SqlBaseParser#tableValuedFunction.
    def enterTableValuedFunction(self, ctx:SqlBaseParser.TableValuedFunctionContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#tableValuedFunction.
    def exitTableValuedFunction(self, ctx:SqlBaseParser.TableValuedFunctionContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#inlineTable.
    def enterInlineTable(self, ctx:SqlBaseParser.InlineTableContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#inlineTable.
    def exitInlineTable(self, ctx:SqlBaseParser.InlineTableContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#functionTable.
    def enterFunctionTable(self, ctx:SqlBaseParser.FunctionTableContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#functionTable.
    def exitFunctionTable(self, ctx:SqlBaseParser.FunctionTableContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#tableAlias.
    def enterTableAlias(self, ctx:SqlBaseParser.TableAliasContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#tableAlias.
    def exitTableAlias(self, ctx:SqlBaseParser.TableAliasContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#rowFormatSerde.
    def enterRowFormatSerde(self, ctx:SqlBaseParser.RowFormatSerdeContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#rowFormatSerde.
    def exitRowFormatSerde(self, ctx:SqlBaseParser.RowFormatSerdeContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#rowFormatDelimited.
    def enterRowFormatDelimited(self, ctx:SqlBaseParser.RowFormatDelimitedContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#rowFormatDelimited.
    def exitRowFormatDelimited(self, ctx:SqlBaseParser.RowFormatDelimitedContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#multipartIdentifierList.
    def enterMultipartIdentifierList(self, ctx:SqlBaseParser.MultipartIdentifierListContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#multipartIdentifierList.
    def exitMultipartIdentifierList(self, ctx:SqlBaseParser.MultipartIdentifierListContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#multipartIdentifier.
    def enterMultipartIdentifier(self, ctx:SqlBaseParser.MultipartIdentifierContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#multipartIdentifier.
    def exitMultipartIdentifier(self, ctx:SqlBaseParser.MultipartIdentifierContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#tableIdentifier.
    def enterTableIdentifier(self, ctx:SqlBaseParser.TableIdentifierContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#tableIdentifier.
    def exitTableIdentifier(self, ctx:SqlBaseParser.TableIdentifierContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#functionIdentifier.
    def enterFunctionIdentifier(self, ctx:SqlBaseParser.FunctionIdentifierContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#functionIdentifier.
    def exitFunctionIdentifier(self, ctx:SqlBaseParser.FunctionIdentifierContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#namedExpression.
    def enterNamedExpression(self, ctx:SqlBaseParser.NamedExpressionContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#namedExpression.
    def exitNamedExpression(self, ctx:SqlBaseParser.NamedExpressionContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#namedExpressionSeq.
    def enterNamedExpressionSeq(self, ctx:SqlBaseParser.NamedExpressionSeqContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#namedExpressionSeq.
    def exitNamedExpressionSeq(self, ctx:SqlBaseParser.NamedExpressionSeqContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#transformList.
    def enterTransformList(self, ctx:SqlBaseParser.TransformListContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#transformList.
    def exitTransformList(self, ctx:SqlBaseParser.TransformListContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#identityTransform.
    def enterIdentityTransform(self, ctx:SqlBaseParser.IdentityTransformContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#identityTransform.
    def exitIdentityTransform(self, ctx:SqlBaseParser.IdentityTransformContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#applyTransform.
    def enterApplyTransform(self, ctx:SqlBaseParser.ApplyTransformContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#applyTransform.
    def exitApplyTransform(self, ctx:SqlBaseParser.ApplyTransformContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#transformArgument.
    def enterTransformArgument(self, ctx:SqlBaseParser.TransformArgumentContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#transformArgument.
    def exitTransformArgument(self, ctx:SqlBaseParser.TransformArgumentContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#expression.
    def enterExpression(self, ctx:SqlBaseParser.ExpressionContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#expression.
    def exitExpression(self, ctx:SqlBaseParser.ExpressionContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#logicalNot.
    def enterLogicalNot(self, ctx:SqlBaseParser.LogicalNotContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#logicalNot.
    def exitLogicalNot(self, ctx:SqlBaseParser.LogicalNotContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#predicated.
    def enterPredicated(self, ctx:SqlBaseParser.PredicatedContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#predicated.
    def exitPredicated(self, ctx:SqlBaseParser.PredicatedContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#exists.
    def enterExists(self, ctx:SqlBaseParser.ExistsContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#exists.
    def exitExists(self, ctx:SqlBaseParser.ExistsContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#logicalBinary.
    def enterLogicalBinary(self, ctx:SqlBaseParser.LogicalBinaryContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#logicalBinary.
    def exitLogicalBinary(self, ctx:SqlBaseParser.LogicalBinaryContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#predicate.
    def enterPredicate(self, ctx:SqlBaseParser.PredicateContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#predicate.
    def exitPredicate(self, ctx:SqlBaseParser.PredicateContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#valueExpressionDefault.
    def enterValueExpressionDefault(self, ctx:SqlBaseParser.ValueExpressionDefaultContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#valueExpressionDefault.
    def exitValueExpressionDefault(self, ctx:SqlBaseParser.ValueExpressionDefaultContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#comparison.
    def enterComparison(self, ctx:SqlBaseParser.ComparisonContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#comparison.
    def exitComparison(self, ctx:SqlBaseParser.ComparisonContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#arithmeticBinary.
    def enterArithmeticBinary(self, ctx:SqlBaseParser.ArithmeticBinaryContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#arithmeticBinary.
    def exitArithmeticBinary(self, ctx:SqlBaseParser.ArithmeticBinaryContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#arithmeticUnary.
    def enterArithmeticUnary(self, ctx:SqlBaseParser.ArithmeticUnaryContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#arithmeticUnary.
    def exitArithmeticUnary(self, ctx:SqlBaseParser.ArithmeticUnaryContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#struct.
    def enterStruct(self, ctx:SqlBaseParser.StructContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#struct.
    def exitStruct(self, ctx:SqlBaseParser.StructContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#dereference.
    def enterDereference(self, ctx:SqlBaseParser.DereferenceContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#dereference.
    def exitDereference(self, ctx:SqlBaseParser.DereferenceContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#simpleCase.
    def enterSimpleCase(self, ctx:SqlBaseParser.SimpleCaseContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#simpleCase.
    def exitSimpleCase(self, ctx:SqlBaseParser.SimpleCaseContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#columnReference.
    def enterColumnReference(self, ctx:SqlBaseParser.ColumnReferenceContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#columnReference.
    def exitColumnReference(self, ctx:SqlBaseParser.ColumnReferenceContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#rowConstructor.
    def enterRowConstructor(self, ctx:SqlBaseParser.RowConstructorContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#rowConstructor.
    def exitRowConstructor(self, ctx:SqlBaseParser.RowConstructorContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#last.
    def enterLast(self, ctx:SqlBaseParser.LastContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#last.
    def exitLast(self, ctx:SqlBaseParser.LastContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#star.
    def enterStar(self, ctx:SqlBaseParser.StarContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#star.
    def exitStar(self, ctx:SqlBaseParser.StarContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#overlay.
    def enterOverlay(self, ctx:SqlBaseParser.OverlayContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#overlay.
    def exitOverlay(self, ctx:SqlBaseParser.OverlayContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#subscript.
    def enterSubscript(self, ctx:SqlBaseParser.SubscriptContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#subscript.
    def exitSubscript(self, ctx:SqlBaseParser.SubscriptContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#subqueryExpression.
    def enterSubqueryExpression(self, ctx:SqlBaseParser.SubqueryExpressionContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#subqueryExpression.
    def exitSubqueryExpression(self, ctx:SqlBaseParser.SubqueryExpressionContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#substring.
    def enterSubstring(self, ctx:SqlBaseParser.SubstringContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#substring.
    def exitSubstring(self, ctx:SqlBaseParser.SubstringContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#currentDatetime.
    def enterCurrentDatetime(self, ctx:SqlBaseParser.CurrentDatetimeContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#currentDatetime.
    def exitCurrentDatetime(self, ctx:SqlBaseParser.CurrentDatetimeContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#cast.
    def enterCast(self, ctx:SqlBaseParser.CastContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#cast.
    def exitCast(self, ctx:SqlBaseParser.CastContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#constantDefault.
    def enterConstantDefault(self, ctx:SqlBaseParser.ConstantDefaultContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#constantDefault.
    def exitConstantDefault(self, ctx:SqlBaseParser.ConstantDefaultContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#lambda.
    def enterLambda(self, ctx:SqlBaseParser.LambdaContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#lambda.
    def exitLambda(self, ctx:SqlBaseParser.LambdaContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#parenthesizedExpression.
    def enterParenthesizedExpression(self, ctx:SqlBaseParser.ParenthesizedExpressionContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#parenthesizedExpression.
    def exitParenthesizedExpression(self, ctx:SqlBaseParser.ParenthesizedExpressionContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#extract.
    def enterExtract(self, ctx:SqlBaseParser.ExtractContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#extract.
    def exitExtract(self, ctx:SqlBaseParser.ExtractContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#trim.
    def enterTrim(self, ctx:SqlBaseParser.TrimContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#trim.
    def exitTrim(self, ctx:SqlBaseParser.TrimContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#functionCall.
    def enterFunctionCall(self, ctx:SqlBaseParser.FunctionCallContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#functionCall.
    def exitFunctionCall(self, ctx:SqlBaseParser.FunctionCallContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#searchedCase.
    def enterSearchedCase(self, ctx:SqlBaseParser.SearchedCaseContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#searchedCase.
    def exitSearchedCase(self, ctx:SqlBaseParser.SearchedCaseContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#position.
    def enterPosition(self, ctx:SqlBaseParser.PositionContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#position.
    def exitPosition(self, ctx:SqlBaseParser.PositionContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#first.
    def enterFirst(self, ctx:SqlBaseParser.FirstContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#first.
    def exitFirst(self, ctx:SqlBaseParser.FirstContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#nullLiteral.
    def enterNullLiteral(self, ctx:SqlBaseParser.NullLiteralContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#nullLiteral.
    def exitNullLiteral(self, ctx:SqlBaseParser.NullLiteralContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#intervalLiteral.
    def enterIntervalLiteral(self, ctx:SqlBaseParser.IntervalLiteralContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#intervalLiteral.
    def exitIntervalLiteral(self, ctx:SqlBaseParser.IntervalLiteralContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#typeConstructor.
    def enterTypeConstructor(self, ctx:SqlBaseParser.TypeConstructorContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#typeConstructor.
    def exitTypeConstructor(self, ctx:SqlBaseParser.TypeConstructorContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#numericLiteral.
    def enterNumericLiteral(self, ctx:SqlBaseParser.NumericLiteralContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#numericLiteral.
    def exitNumericLiteral(self, ctx:SqlBaseParser.NumericLiteralContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#booleanLiteral.
    def enterBooleanLiteral(self, ctx:SqlBaseParser.BooleanLiteralContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#booleanLiteral.
    def exitBooleanLiteral(self, ctx:SqlBaseParser.BooleanLiteralContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#stringLiteral.
    def enterStringLiteral(self, ctx:SqlBaseParser.StringLiteralContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#stringLiteral.
    def exitStringLiteral(self, ctx:SqlBaseParser.StringLiteralContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#comparisonOperator.
    def enterComparisonOperator(self, ctx:SqlBaseParser.ComparisonOperatorContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#comparisonOperator.
    def exitComparisonOperator(self, ctx:SqlBaseParser.ComparisonOperatorContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#arithmeticOperator.
    def enterArithmeticOperator(self, ctx:SqlBaseParser.ArithmeticOperatorContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#arithmeticOperator.
    def exitArithmeticOperator(self, ctx:SqlBaseParser.ArithmeticOperatorContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#predicateOperator.
    def enterPredicateOperator(self, ctx:SqlBaseParser.PredicateOperatorContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#predicateOperator.
    def exitPredicateOperator(self, ctx:SqlBaseParser.PredicateOperatorContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#booleanValue.
    def enterBooleanValue(self, ctx:SqlBaseParser.BooleanValueContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#booleanValue.
    def exitBooleanValue(self, ctx:SqlBaseParser.BooleanValueContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#interval.
    def enterInterval(self, ctx:SqlBaseParser.IntervalContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#interval.
    def exitInterval(self, ctx:SqlBaseParser.IntervalContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#errorCapturingMultiUnitsInterval.
    def enterErrorCapturingMultiUnitsInterval(self, ctx:SqlBaseParser.ErrorCapturingMultiUnitsIntervalContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#errorCapturingMultiUnitsInterval.
    def exitErrorCapturingMultiUnitsInterval(self, ctx:SqlBaseParser.ErrorCapturingMultiUnitsIntervalContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#multiUnitsInterval.
    def enterMultiUnitsInterval(self, ctx:SqlBaseParser.MultiUnitsIntervalContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#multiUnitsInterval.
    def exitMultiUnitsInterval(self, ctx:SqlBaseParser.MultiUnitsIntervalContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#errorCapturingUnitToUnitInterval.
    def enterErrorCapturingUnitToUnitInterval(self, ctx:SqlBaseParser.ErrorCapturingUnitToUnitIntervalContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#errorCapturingUnitToUnitInterval.
    def exitErrorCapturingUnitToUnitInterval(self, ctx:SqlBaseParser.ErrorCapturingUnitToUnitIntervalContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#unitToUnitInterval.
    def enterUnitToUnitInterval(self, ctx:SqlBaseParser.UnitToUnitIntervalContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#unitToUnitInterval.
    def exitUnitToUnitInterval(self, ctx:SqlBaseParser.UnitToUnitIntervalContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#intervalValue.
    def enterIntervalValue(self, ctx:SqlBaseParser.IntervalValueContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#intervalValue.
    def exitIntervalValue(self, ctx:SqlBaseParser.IntervalValueContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#intervalUnit.
    def enterIntervalUnit(self, ctx:SqlBaseParser.IntervalUnitContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#intervalUnit.
    def exitIntervalUnit(self, ctx:SqlBaseParser.IntervalUnitContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#colPosition.
    def enterColPosition(self, ctx:SqlBaseParser.ColPositionContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#colPosition.
    def exitColPosition(self, ctx:SqlBaseParser.ColPositionContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#complexDataType.
    def enterComplexDataType(self, ctx:SqlBaseParser.ComplexDataTypeContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#complexDataType.
    def exitComplexDataType(self, ctx:SqlBaseParser.ComplexDataTypeContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#primitiveDataType.
    def enterPrimitiveDataType(self, ctx:SqlBaseParser.PrimitiveDataTypeContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#primitiveDataType.
    def exitPrimitiveDataType(self, ctx:SqlBaseParser.PrimitiveDataTypeContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#qualifiedColTypeWithPositionList.
    def enterQualifiedColTypeWithPositionList(self, ctx:SqlBaseParser.QualifiedColTypeWithPositionListContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#qualifiedColTypeWithPositionList.
    def exitQualifiedColTypeWithPositionList(self, ctx:SqlBaseParser.QualifiedColTypeWithPositionListContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#qualifiedColTypeWithPosition.
    def enterQualifiedColTypeWithPosition(self, ctx:SqlBaseParser.QualifiedColTypeWithPositionContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#qualifiedColTypeWithPosition.
    def exitQualifiedColTypeWithPosition(self, ctx:SqlBaseParser.QualifiedColTypeWithPositionContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#colTypeList.
    def enterColTypeList(self, ctx:SqlBaseParser.ColTypeListContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#colTypeList.
    def exitColTypeList(self, ctx:SqlBaseParser.ColTypeListContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#colType.
    def enterColType(self, ctx:SqlBaseParser.ColTypeContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#colType.
    def exitColType(self, ctx:SqlBaseParser.ColTypeContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#complexColTypeList.
    def enterComplexColTypeList(self, ctx:SqlBaseParser.ComplexColTypeListContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#complexColTypeList.
    def exitComplexColTypeList(self, ctx:SqlBaseParser.ComplexColTypeListContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#complexColType.
    def enterComplexColType(self, ctx:SqlBaseParser.ComplexColTypeContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#complexColType.
    def exitComplexColType(self, ctx:SqlBaseParser.ComplexColTypeContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#whenClause.
    def enterWhenClause(self, ctx:SqlBaseParser.WhenClauseContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#whenClause.
    def exitWhenClause(self, ctx:SqlBaseParser.WhenClauseContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#windowClause.
    def enterWindowClause(self, ctx:SqlBaseParser.WindowClauseContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#windowClause.
    def exitWindowClause(self, ctx:SqlBaseParser.WindowClauseContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#namedWindow.
    def enterNamedWindow(self, ctx:SqlBaseParser.NamedWindowContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#namedWindow.
    def exitNamedWindow(self, ctx:SqlBaseParser.NamedWindowContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#windowRef.
    def enterWindowRef(self, ctx:SqlBaseParser.WindowRefContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#windowRef.
    def exitWindowRef(self, ctx:SqlBaseParser.WindowRefContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#windowDef.
    def enterWindowDef(self, ctx:SqlBaseParser.WindowDefContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#windowDef.
    def exitWindowDef(self, ctx:SqlBaseParser.WindowDefContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#windowFrame.
    def enterWindowFrame(self, ctx:SqlBaseParser.WindowFrameContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#windowFrame.
    def exitWindowFrame(self, ctx:SqlBaseParser.WindowFrameContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#frameBound.
    def enterFrameBound(self, ctx:SqlBaseParser.FrameBoundContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#frameBound.
    def exitFrameBound(self, ctx:SqlBaseParser.FrameBoundContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#qualifiedNameList.
    def enterQualifiedNameList(self, ctx:SqlBaseParser.QualifiedNameListContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#qualifiedNameList.
    def exitQualifiedNameList(self, ctx:SqlBaseParser.QualifiedNameListContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#functionName.
    def enterFunctionName(self, ctx:SqlBaseParser.FunctionNameContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#functionName.
    def exitFunctionName(self, ctx:SqlBaseParser.FunctionNameContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#qualifiedName.
    def enterQualifiedName(self, ctx:SqlBaseParser.QualifiedNameContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#qualifiedName.
    def exitQualifiedName(self, ctx:SqlBaseParser.QualifiedNameContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#errorCapturingIdentifier.
    def enterErrorCapturingIdentifier(self, ctx:SqlBaseParser.ErrorCapturingIdentifierContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#errorCapturingIdentifier.
    def exitErrorCapturingIdentifier(self, ctx:SqlBaseParser.ErrorCapturingIdentifierContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#errorIdent.
    def enterErrorIdent(self, ctx:SqlBaseParser.ErrorIdentContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#errorIdent.
    def exitErrorIdent(self, ctx:SqlBaseParser.ErrorIdentContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#realIdent.
    def enterRealIdent(self, ctx:SqlBaseParser.RealIdentContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#realIdent.
    def exitRealIdent(self, ctx:SqlBaseParser.RealIdentContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#identifier.
    def enterIdentifier(self, ctx:SqlBaseParser.IdentifierContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#identifier.
    def exitIdentifier(self, ctx:SqlBaseParser.IdentifierContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#unquotedIdentifier.
    def enterUnquotedIdentifier(self, ctx:SqlBaseParser.UnquotedIdentifierContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#unquotedIdentifier.
    def exitUnquotedIdentifier(self, ctx:SqlBaseParser.UnquotedIdentifierContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#quotedIdentifierAlternative.
    def enterQuotedIdentifierAlternative(self, ctx:SqlBaseParser.QuotedIdentifierAlternativeContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#quotedIdentifierAlternative.
    def exitQuotedIdentifierAlternative(self, ctx:SqlBaseParser.QuotedIdentifierAlternativeContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#quotedIdentifier.
    def enterQuotedIdentifier(self, ctx:SqlBaseParser.QuotedIdentifierContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#quotedIdentifier.
    def exitQuotedIdentifier(self, ctx:SqlBaseParser.QuotedIdentifierContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#exponentLiteral.
    def enterExponentLiteral(self, ctx:SqlBaseParser.ExponentLiteralContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#exponentLiteral.
    def exitExponentLiteral(self, ctx:SqlBaseParser.ExponentLiteralContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#decimalLiteral.
    def enterDecimalLiteral(self, ctx:SqlBaseParser.DecimalLiteralContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#decimalLiteral.
    def exitDecimalLiteral(self, ctx:SqlBaseParser.DecimalLiteralContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#legacyDecimalLiteral.
    def enterLegacyDecimalLiteral(self, ctx:SqlBaseParser.LegacyDecimalLiteralContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#legacyDecimalLiteral.
    def exitLegacyDecimalLiteral(self, ctx:SqlBaseParser.LegacyDecimalLiteralContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#integerLiteral.
    def enterIntegerLiteral(self, ctx:SqlBaseParser.IntegerLiteralContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#integerLiteral.
    def exitIntegerLiteral(self, ctx:SqlBaseParser.IntegerLiteralContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#bigIntLiteral.
    def enterBigIntLiteral(self, ctx:SqlBaseParser.BigIntLiteralContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#bigIntLiteral.
    def exitBigIntLiteral(self, ctx:SqlBaseParser.BigIntLiteralContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#smallIntLiteral.
    def enterSmallIntLiteral(self, ctx:SqlBaseParser.SmallIntLiteralContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#smallIntLiteral.
    def exitSmallIntLiteral(self, ctx:SqlBaseParser.SmallIntLiteralContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#tinyIntLiteral.
    def enterTinyIntLiteral(self, ctx:SqlBaseParser.TinyIntLiteralContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#tinyIntLiteral.
    def exitTinyIntLiteral(self, ctx:SqlBaseParser.TinyIntLiteralContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#doubleLiteral.
    def enterDoubleLiteral(self, ctx:SqlBaseParser.DoubleLiteralContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#doubleLiteral.
    def exitDoubleLiteral(self, ctx:SqlBaseParser.DoubleLiteralContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#bigDecimalLiteral.
    def enterBigDecimalLiteral(self, ctx:SqlBaseParser.BigDecimalLiteralContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#bigDecimalLiteral.
    def exitBigDecimalLiteral(self, ctx:SqlBaseParser.BigDecimalLiteralContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#alterColumnAction.
    def enterAlterColumnAction(self, ctx:SqlBaseParser.AlterColumnActionContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#alterColumnAction.
    def exitAlterColumnAction(self, ctx:SqlBaseParser.AlterColumnActionContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#ansiNonReserved.
    def enterAnsiNonReserved(self, ctx:SqlBaseParser.AnsiNonReservedContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#ansiNonReserved.
    def exitAnsiNonReserved(self, ctx:SqlBaseParser.AnsiNonReservedContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#strictNonReserved.
    def enterStrictNonReserved(self, ctx:SqlBaseParser.StrictNonReservedContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#strictNonReserved.
    def exitStrictNonReserved(self, ctx:SqlBaseParser.StrictNonReservedContext):
        pass


    # Enter a parse tree produced by SqlBaseParser#nonReserved.
    def enterNonReserved(self, ctx:SqlBaseParser.NonReservedContext):
        pass

    # Exit a parse tree produced by SqlBaseParser#nonReserved.
    def exitNonReserved(self, ctx:SqlBaseParser.NonReservedContext):
        pass


