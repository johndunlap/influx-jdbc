package org.voidzero.influx.jdbc;

/*-
 * #%L
 * influx-jdbc
 * %%
 * Copyright (C) 2024 John Dunlap
 * %%
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 * #L%
 */

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class InfluxDatabaseMetadata implements DatabaseMetaData {
    private final DatabaseMetaData metadata;

    public InfluxDatabaseMetadata(final DatabaseMetaData metadata) {
        this.metadata = metadata;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return metadata.allProceduresAreCallable();
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return metadata.allTablesAreSelectable();
    }

    @Override
    public String getURL() throws SQLException {
        return metadata.getURL();
    }

    @Override
    public String getUserName() throws SQLException {
        return metadata.getUserName();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return metadata.isReadOnly();
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return metadata.nullsAreSortedHigh();
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return metadata.nullsAreSortedLow();
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return metadata.nullsAreSortedAtStart();
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return metadata.nullsAreSortedAtEnd();
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return metadata.getDatabaseProductName();
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return metadata.getDatabaseProductVersion();
    }

    @Override
    public String getDriverName() throws SQLException {
        return metadata.getDriverName();
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return metadata.getDriverVersion();
    }

    @Override
    public int getDriverMajorVersion() {
        return metadata.getDriverMajorVersion();
    }

    @Override
    public int getDriverMinorVersion() {
        return metadata.getDriverMinorVersion();
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return metadata.usesLocalFiles();
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return metadata.usesLocalFilePerTable();
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return metadata.supportsMixedCaseIdentifiers();
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return metadata.storesUpperCaseIdentifiers();
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return metadata.storesLowerCaseIdentifiers();
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return metadata.storesMixedCaseIdentifiers();
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return metadata.supportsMixedCaseQuotedIdentifiers();
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return metadata.storesUpperCaseQuotedIdentifiers();
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return metadata.storesLowerCaseQuotedIdentifiers();
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return metadata.storesMixedCaseQuotedIdentifiers();
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return metadata.getIdentifierQuoteString();
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return metadata.getSQLKeywords();
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return metadata.getNumericFunctions();
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return metadata.getStringFunctions();
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return metadata.getSystemFunctions();
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return metadata.getTimeDateFunctions();
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return metadata.getSearchStringEscape();
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return metadata.getExtraNameCharacters();
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return metadata.supportsAlterTableWithAddColumn();
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return metadata.supportsAlterTableWithDropColumn();
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return metadata.supportsColumnAliasing();
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return metadata.nullPlusNonNullIsNull();
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return metadata.supportsConvert();
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return metadata.supportsConvert(fromType, toType);
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return metadata.supportsTableCorrelationNames();
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return metadata.supportsDifferentTableCorrelationNames();
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return metadata.supportsExpressionsInOrderBy();
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return metadata.supportsOrderByUnrelated();
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return metadata.supportsGroupBy();
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return metadata.supportsGroupByUnrelated();
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return metadata.supportsGroupByBeyondSelect();
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return metadata.supportsLikeEscapeClause();
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return metadata.supportsMultipleResultSets();
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return metadata.supportsMultipleTransactions();
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return metadata.supportsNonNullableColumns();
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return metadata.supportsMinimumSQLGrammar();
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return metadata.supportsCoreSQLGrammar();
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return metadata.supportsExtendedSQLGrammar();
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return metadata.supportsANSI92EntryLevelSQL();
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return metadata.supportsANSI92IntermediateSQL();
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return metadata.supportsANSI92FullSQL();
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return metadata.supportsIntegrityEnhancementFacility();
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return metadata.supportsOuterJoins();
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return metadata.supportsFullOuterJoins();
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return metadata.supportsLimitedOuterJoins();
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return metadata.getSchemaTerm();
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return metadata.getProcedureTerm();
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return metadata.getCatalogTerm();
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return metadata.isCatalogAtStart();
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return metadata.getCatalogSeparator();
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return metadata.supportsSchemasInDataManipulation();
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return metadata.supportsSchemasInProcedureCalls();
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return metadata.supportsSchemasInTableDefinitions();
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return metadata.supportsSchemasInIndexDefinitions();
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return metadata.supportsSchemasInPrivilegeDefinitions();
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return metadata.supportsCatalogsInDataManipulation();
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return metadata.supportsCatalogsInProcedureCalls();
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return metadata.supportsCatalogsInTableDefinitions();
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return metadata.supportsCatalogsInIndexDefinitions();
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return metadata.supportsCatalogsInPrivilegeDefinitions();
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return metadata.supportsPositionedDelete();
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return metadata.supportsPositionedUpdate();
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return metadata.supportsSelectForUpdate();
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return metadata.supportsStoredProcedures();
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return metadata.supportsSubqueriesInComparisons();
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return metadata.supportsSubqueriesInExists();
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return metadata.supportsSubqueriesInIns();
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return metadata.supportsSubqueriesInQuantifieds();
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return metadata.supportsCorrelatedSubqueries();
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return metadata.supportsUnion();
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return metadata.supportsUnionAll();
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return metadata.supportsOpenCursorsAcrossCommit();
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return metadata.supportsOpenCursorsAcrossRollback();
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return metadata.supportsOpenStatementsAcrossCommit();
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return metadata.supportsOpenStatementsAcrossRollback();
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return metadata.getMaxBinaryLiteralLength();
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return metadata.getMaxCharLiteralLength();
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return metadata.getMaxColumnNameLength();
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return metadata.getMaxColumnsInGroupBy();
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return metadata.getMaxColumnsInIndex();
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return metadata.getMaxColumnsInOrderBy();
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return metadata.getMaxColumnsInSelect();
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return metadata.getMaxColumnsInTable();
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return metadata.getMaxConnections();
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return metadata.getMaxCursorNameLength();
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return metadata.getMaxIndexLength();
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return metadata.getMaxSchemaNameLength();
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return metadata.getMaxProcedureNameLength();
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return metadata.getMaxCatalogNameLength();
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return metadata.getMaxRowSize();
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return metadata.doesMaxRowSizeIncludeBlobs();
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return metadata.getMaxStatementLength();
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return metadata.getMaxStatements();
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return metadata.getMaxTableNameLength();
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return metadata.getMaxTablesInSelect();
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return metadata.getMaxUserNameLength();
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return metadata.getDefaultTransactionIsolation();
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return metadata.supportsTransactions();
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return metadata.supportsTransactionIsolationLevel(level);
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return metadata.supportsDataDefinitionAndDataManipulationTransactions();
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return metadata.supportsDataManipulationTransactionsOnly();
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return metadata.dataDefinitionCausesTransactionCommit();
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return metadata.dataDefinitionIgnoredInTransactions();
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return metadata.getProcedures(catalog, schemaPattern, procedureNamePattern);
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return metadata.getProcedureColumns(catalog, schemaPattern, procedureNamePattern, columnNamePattern);
    }

    /**
     * Get all tables from all schemas.
     *
     * @return Return a list of all tables from all schemas.
     * @throws SQLException thrown when something goes wrong
     */
    public List<TableMetadata> getTables() throws SQLException {
        return getTables(null);
    }

    /**
     * Get all tables from the specified schema, or from all schemas if schema is null.
     * @param schema The schema from which table metadata should be loaded
     * @return A list of table metadata objects
     * @throws SQLException throw when something goes wrong
     */
    public List<TableMetadata> getTables(final String schema) throws SQLException {
        ResultSet tableResultSet = getTables(
                null,
                schema,
                null,
                null
        );

        List<TableMetadata> tables = new ArrayList<>();

        while (tableResultSet.next()) {
            TableMetadata tableMetaData = new TableMetadata(tableResultSet);
            List<ColumnMetadata> columns = getColumns(schema, tableMetaData.getTableName());
            tableMetaData.setColumns(columns);
            tables.add(tableMetaData);
        }

        return tables;
    }

    /**
     * Returns table metadata for the specified table in the specified schema.
     *
     * @param schema the schema from which table metadata should be loaded
     * @param tableName the table for which metadata should be loaded.
     * @return table metadata for the specified table
     * @throws SQLException thrown when something goes wrong
     */
    public TableMetadata getTable(String schema, String tableName) throws SQLException {
        // HSQLDB needs this
        if (storesUpperCaseIdentifiers()) {
            schema = schema.toUpperCase();
            tableName = tableName.toUpperCase();
        }

        ResultSet tableResultSet = getTables(
                null,
                schema,
                tableName,
                null
        );

        if (!tableResultSet.next()) {
            String message;

            if (schema != null) {
                message = "Table " + schema + "." + tableName + " does not exist";
            } else {
                message = "Table " + tableName + " does not exist";
            }

            throw new SQLException(message);
        }

        TableMetadata tableMetaData = new TableMetadata(tableResultSet);
        List<ColumnMetadata> columns = getColumns(schema, tableMetaData.getTableName());
        tableMetaData.setColumns(columns);

        return tableMetaData;
    }
    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        return metadata.getTables(catalog, schemaPattern, tableNamePattern, types);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return metadata.getSchemas();
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return metadata.getCatalogs();
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        return metadata.getTableTypes();
    }

    public List<ColumnMetadata> getColumns(final String schema, final String table) throws SQLException {
        ResultSet columnResultSet = getColumns(null, schema, table, null);

        List<ColumnMetadata> columns = new ArrayList<>();

        while (columnResultSet.next()) {
            columns.add(new ColumnMetadata(columnResultSet));
        }

        return columns;
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return metadata.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        return metadata.getColumnPrivileges(catalog, schema, table, columnNamePattern);
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return metadata.getTablePrivileges(catalog, schemaPattern, tableNamePattern);
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        return metadata.getBestRowIdentifier(catalog, schema, table, scope, nullable);
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return metadata.getVersionColumns(catalog, schema, table);
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return metadata.getPrimaryKeys(catalog, schema, table);
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return metadata.getImportedKeys(catalog, schema, table);
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return metadata.getExportedKeys(catalog, schema, table);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return metadata.getCrossReference(parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable);
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return metadata.getTypeInfo();
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        return metadata.getIndexInfo(catalog, schema, table, unique, approximate);
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return metadata.supportsResultSetType(type);
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return metadata.supportsResultSetConcurrency(type, concurrency);
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return metadata.ownUpdatesAreVisible(type);
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return metadata.ownDeletesAreVisible(type);
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return metadata.ownInsertsAreVisible(type);
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return metadata.othersUpdatesAreVisible(type);
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return metadata.othersDeletesAreVisible(type);
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return metadata.othersInsertsAreVisible(type);
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return metadata.updatesAreDetected(type);
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return metadata.deletesAreDetected(type);
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return metadata.insertsAreDetected(type);
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return metadata.supportsBatchUpdates();
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return metadata.getUDTs(catalog, schemaPattern, typeNamePattern, types);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return metadata.getConnection();
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return metadata.supportsSavepoints();
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return metadata.supportsNamedParameters();
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return metadata.supportsMultipleOpenResults();
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return metadata.supportsGetGeneratedKeys();
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return metadata.getSuperTypes(catalog, schemaPattern, typeNamePattern);
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return metadata.getSuperTables(catalog, schemaPattern, tableNamePattern);
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return metadata.getAttributes(catalog, schemaPattern, typeNamePattern, attributeNamePattern);
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return metadata.supportsResultSetHoldability(holdability);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return metadata.getResultSetHoldability();
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return metadata.getDatabaseMajorVersion();
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return metadata.getDatabaseMinorVersion();
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return metadata.getJDBCMajorVersion();
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return metadata.getJDBCMinorVersion();
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return metadata.getSQLStateType();
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return metadata.locatorsUpdateCopy();
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return metadata.supportsStatementPooling();
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return metadata.getRowIdLifetime();
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return metadata.getSchemas(catalog, schemaPattern);
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return metadata.supportsStoredFunctionsUsingCallSyntax();
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return metadata.autoCommitFailureClosesAllResultSets();
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return metadata.getClientInfoProperties();
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return metadata.getFunctions(catalog, schemaPattern, functionNamePattern);
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return metadata.getFunctionColumns(catalog, schemaPattern, functionNamePattern, columnNamePattern);
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return metadata.getPseudoColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return metadata.generatedKeyAlwaysReturned();
    }

    @Override
    public long getMaxLogicalLobSize() throws SQLException {
        return metadata.getMaxLogicalLobSize();
    }

    @Override
    public boolean supportsRefCursors() throws SQLException {
        return metadata.supportsRefCursors();
    }

    @Override
    public boolean supportsSharding() throws SQLException {
        return metadata.supportsSharding();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return metadata.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return metadata.isWrapperFor(iface);
    }
}
