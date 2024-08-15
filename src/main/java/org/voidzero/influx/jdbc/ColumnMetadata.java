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

    /**
     * Get the catalog of this column.
     *
     * @return The catalog of this column
     */
    public String getCatalog() {
        return catalog;
    }

    /**
     * Get the schema of this column.
     *
     * @return the schema of this column
     */
    public String getSchema() {
        return schema;
    }

    /**
     * Get the table name to which this column belongs.
     *
     * @return The table name to which this column belongs
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Get column name.
     *
     * @return column name
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * Get column data type.
     *
     * @return column data type
     */
    public int getDataType() {
        return dataType;
    }

    /**
     * Get the type name of this column.
     *
     * @return Type name for this column
     */
    public String getTypeName() {
        return typeName;
    }

    /**
     * Get the column size.
     *
     * @return The column size
     */
    public int getColumnSize() {
        return columnSize;
    }

    /**
     * Get the number of decimal digits supported by this column.
     *
     * @return The number of decimal digits supported by this column
     */
    public int getDecimalDigits() {
        return decimalDigits;
    }

    /**
     * Get numPrecRadix.
     *
     * @return numPredRadix
     */
    public int getNumPrecRadix() {
        return numPrecRadix;
    }

    /**
     * Return non-zero if this column is nullable and 0 otherwise.
     *
     * @return Non-zero if this column is nullable and 0 otherwise
     */
    public int getNullable() {
        return nullable;
    }

    /**
     * Get the comments which have been associated with this column.
     *
     * @return The comments which have been associated with this column
     */
    public String getRemarks() {
        return remarks;
    }

    /**
     * Get the column definition.
     *
     * @return column definition
     */
    public String getColumnDef() {
        return columnDef;
    }

    /**
     * Get the SQL data type of this column.
     *
     * @return the SQL data type of this column
     */
    public int getSqlDataType() {
        return sqlDataType;
    }

    /**
     * Get sqlDatetimeSub.
     *
     * @return sqlDatetimeSub
     */
    public int getSqlDatetimeSub() {
        return sqlDatetimeSub;
    }

    /**
     * Get the character octet length of this column.
     *
     * @return The character octet length of this column
     */
    public int getCharOctetLength() {
        return charOctetLength;
    }

    /**
     * Get the ordinal position of this column within its table.
     *
     * @return The ordinal position of this column within its table
     */
    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    /**
     * TODO: Why is the type string here?
     * Returns true if this column is nullable and false otherwise.
     *
     * @return True if this column is nullable and false otherwise
     */
    public String getIsNullable() {
        return isNullable;
    }

    /**
     * Get scope catalog.
     *
     * @return scope catalog
     */
    public String getScopeCatalog() {
        return scopeCatalog;
    }

    /**
     * Get scope schema.
     *
     * @return scope schema
     */
    public String getScopeSchema() {
        return scopeSchema;
    }

    /**
     * Get scope table.
     *
     * @return scope table
     */
    public String getScopeTable() {
        return scopeTable;
    }

    /**
     * Get source data type.
     *
     * @return source data type
     */
    public Short getSourceDataType() {
        return sourceDataType;
    }

    /**
     * TODO: Why isn't this boolean?
     * Get isAutoincrement.
     *
     * @return isAutoincrement
     */
    public String getIsAutoincrement() {
        return isAutoincrement;
    }

    /**
     * TODO: Why isn't this boolean?
     * Get isGeneratedColumn.
     *
     * @return isGeneratedColumn
     */
    public String getIsGeneratedColumn() {
        return isGeneratedColumn;
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
