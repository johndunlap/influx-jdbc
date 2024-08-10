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

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * This class contains metadata which describes a single database column.
 */
public class ColumnMetadata {

    private String catalog;

    private String schema;

    private String tableName;

    private String columnName;

    private int dataType;

    private String typeName;

    private int columnSize;

    private int decimalDigits;

    private int numPrecRadix;

    private int nullable;

    private String remarks;

    private String columnDef;

    private int sqlDataType;

    private int sqlDatetimeSub;

    private int charOctetLength;

    private int ordinalPosition;

    private String isNullable;

    private String scopeCatalog;

    private String scopeSchema;

    private String scopeTable;

    private Short sourceDataType;

    private String isAutoincrement;

    private String isGeneratedColumn;

    /**
     * Create a new instance of this class from the specified result set.
     *
     * @param resultSet The result set from which data should be taken.
     * @throws SQLException Thrown if something goes wrong.
     */
    public ColumnMetadata(final ResultSet resultSet) throws SQLException {
        this.catalog = resultSet.getString("TABLE_CAT");
        this.schema = resultSet.getString("TABLE_SCHEM");
        this.tableName = resultSet.getString("TABLE_NAME");
        this.columnName = resultSet.getString("COLUMN_NAME");
        this.dataType = resultSet.getInt("DATA_TYPE");
        this.typeName = resultSet.getString("TYPE_NAME");
        this.columnSize = resultSet.getInt("COLUMN_SIZE");
        this.decimalDigits = resultSet.getInt("DECIMAL_DIGITS");
        this.numPrecRadix = resultSet.getInt("NUM_PREC_RADIX");
        this.nullable = resultSet.getInt("NULLABLE");
        this.remarks = resultSet.getString("REMARKS");
        this.columnDef = resultSet.getString("COLUMN_DEF");
        this.sqlDataType = resultSet.getInt("SQL_DATA_TYPE");
        this.sqlDatetimeSub = resultSet.getInt("SQL_DATETIME_SUB");
        this.charOctetLength = resultSet.getInt("CHAR_OCTET_LENGTH");
        this.ordinalPosition = resultSet.getInt("ORDINAL_POSITION");
        this.isNullable = resultSet.getString("IS_NULLABLE");
        this.scopeCatalog = resultSet.getString("SCOPE_CATALOG");
        this.scopeSchema = resultSet.getString("SCOPE_SCHEMA");
        this.scopeTable = resultSet.getString("SCOPE_TABLE");
        this.sourceDataType = resultSet.getShort("SOURCE_DATA_TYPE");
        this.isAutoincrement = resultSet.getString("IS_AUTOINCREMENT");
        this.isGeneratedColumn = resultSet.getString("IS_GENERATEDCOLUMN");
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public int getDataType() {
        return dataType;
    }

    public void setDataType(int dataType) {
        this.dataType = dataType;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public int getColumnSize() {
        return columnSize;
    }

    public void setColumnSize(int columnSize) {
        this.columnSize = columnSize;
    }

    public int getDecimalDigits() {
        return decimalDigits;
    }

    public void setDecimalDigits(int decimalDigits) {
        this.decimalDigits = decimalDigits;
    }

    public int getNumPrecRadix() {
        return numPrecRadix;
    }

    public void setNumPrecRadix(int numPrecRadix) {
        this.numPrecRadix = numPrecRadix;
    }

    public int getNullable() {
        return nullable;
    }

    public void setNullable(int nullable) {
        this.nullable = nullable;
    }

    public String getRemarks() {
        return remarks;
    }

    public void setRemarks(String remarks) {
        this.remarks = remarks;
    }

    public String getColumnDef() {
        return columnDef;
    }

    public void setColumnDef(String columnDef) {
        this.columnDef = columnDef;
    }

    public int getSqlDataType() {
        return sqlDataType;
    }

    public void setSqlDataType(int sqlDataType) {
        this.sqlDataType = sqlDataType;
    }

    public int getSqlDatetimeSub() {
        return sqlDatetimeSub;
    }

    public void setSqlDatetimeSub(int sqlDatetimeSub) {
        this.sqlDatetimeSub = sqlDatetimeSub;
    }

    public int getCharOctetLength() {
        return charOctetLength;
    }

    public void setCharOctetLength(int charOctetLength) {
        this.charOctetLength = charOctetLength;
    }

    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    public void setOrdinalPosition(int ordinalPosition) {
        this.ordinalPosition = ordinalPosition;
    }

    public String getIsNullable() {
        return isNullable;
    }

    public void setIsNullable(String isNullable) {
        this.isNullable = isNullable;
    }

    public String getScopeCatalog() {
        return scopeCatalog;
    }

    public void setScopeCatalog(String scopeCatalog) {
        this.scopeCatalog = scopeCatalog;
    }

    public String getScopeSchema() {
        return scopeSchema;
    }

    public void setScopeSchema(String scopeSchema) {
        this.scopeSchema = scopeSchema;
    }

    public String getScopeTable() {
        return scopeTable;
    }

    public void setScopeTable(String scopeTable) {
        this.scopeTable = scopeTable;
    }

    public Short getSourceDataType() {
        return sourceDataType;
    }

    public void setSourceDataType(Short sourceDataType) {
        this.sourceDataType = sourceDataType;
    }

    public String getIsAutoincrement() {
        return isAutoincrement;
    }

    public void setIsAutoincrement(String isAutoincrement) {
        this.isAutoincrement = isAutoincrement;
    }

    public String getIsGeneratedColumn() {
        return isGeneratedColumn;
    }

    public void setIsGeneratedColumn(String isGeneratedColumn) {
        this.isGeneratedColumn = isGeneratedColumn;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{"
                + "catalog='" + catalog + '\''
                + ", schema='" + schema + '\''
                + ", tableName='" + tableName + '\''
                + ", columnName='" + columnName + '\''
                + ", dataType=" + dataType
                + ", typeName='" + typeName + '\''
                + ", columnSize=" + columnSize
                + ", decimalDigits=" + decimalDigits
                + ", numPrecRadix=" + numPrecRadix
                + ", nullable=" + nullable
                + ", remarks='" + remarks + '\''
                + ", columnDef='" + columnDef + '\''
                + ", sqlDataType=" + sqlDataType
                + ", sqlDatetimeSub=" + sqlDatetimeSub
                + ", charOctetLength=" + charOctetLength
                + ", ordinalPosition=" + ordinalPosition
                + ", isNullable='" + isNullable + '\''
                + ", scopeCatalog='" + scopeCatalog + '\''
                + ", scopeSchema='" + scopeSchema + '\''
                + ", scopeTable='" + scopeTable + '\''
                + ", sourceDataType=" + sourceDataType
                + ", isAutoincrement='" + isAutoincrement + '\''
                + ", isGeneratedColumn='" + isGeneratedColumn + '\''
                + '}';
    }
}
