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
import java.util.ArrayList;
import java.util.List;

/**
 * This is a wrapper for a result set which contains table metadata.
 *
 * @author <a href="mailto:john.david.dunlap@gmail.com">John Dunlap</a>
 */
public class TableMetadata {
    private String catalog;

    private String schema;

    private String tableName;

    private String tableType;

    private String remarks;

    private String typeCatalog;

    private String typeSchema;

    private String typeName;

    private String selfReferencingColName;

    private String refGeneration;

    /**
     * This list will contain one metadata object for each column in the result set.
     */
    private List<ColumnMetadata> columns = new ArrayList<>();

    /**
     * Create a new instance and populate it with the specified {@link ResultSet}.
     *
     * @param resultSet The result set from which table metadata should be extracted.
     * @throws SQLException Thrown if something goes wrong.
     */
    public TableMetadata(final ResultSet resultSet) throws SQLException {
        this.catalog = resultSet.getString("TABLE_CAT");
        this.schema = resultSet.getString("TABLE_SCHEM");
        this.tableName = resultSet.getString("TABLE_NAME");
        this.tableType = resultSet.getString("TABLE_TYPE");
        this.remarks = resultSet.getString("REMARKS");
        this.typeCatalog = resultSet.getString("TYPE_CAT");
        this.typeSchema = resultSet.getString("TYPE_SCHEM");
        this.typeName = resultSet.getString("TYPE_NAME");
        this.selfReferencingColName = resultSet.getString("SELF_REFERENCING_COL_NAME");
        this.refGeneration = resultSet.getString("REF_GENERATION");
    }

    /**
     * Get a list of {@link ColumnMetadata} objects which describe the columns in the table.
     *
     * @return List of {@link ColumnMetadata} objects which describe the columns in the table.
     */
    public List<ColumnMetadata> getColumns() {
        return columns;
    }

    /**
     * Sets the list of {@link ColumnMetadata} objects which describe the columns in the table.
     *
     * @param columns List of {@link ColumnMetadata} objects which describe the columns in the table.
     */
    public void setColumns(List<ColumnMetadata> columns) {
        this.columns = columns;
    }

    /**
     * Gets the catalog name of this table.
     *
     * @return The catalog name of this table.
     */
    public String getCatalog() {
        return catalog;
    }

    /**
     * Gets the schema of this table.
     *
     * @return The schema of this table.
     */
    public String getSchema() {
        return schema;
    }

    /**
     * Gets the name of this table.
     *
     * @return The name of this table.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Gets the table type of this table.
     *
     * @return the table type of this table.
     */
    public String getTableType() {
        return tableType;
    }

    /**
     * Gets the remarks or comments which describe this table.
     *
     * @return The remarks or comments which describe this table.
     */
    public String getRemarks() {
        return remarks;
    }

    /**
     * Gets the type catalog name of this table.
     *
     * @return The type catalog name of this table.
     */
    public String getTypeCatalog() {
        return typeCatalog;
    }

    /**
     * Gets the type schema name of this table.
     *
     * @return The type schema name of this table.
     */
    public String getTypeSchema() {
        return typeSchema;
    }

    /**
     * Gets the type name of this table.
     *
     * @return The type name of this table.
     */
    public String getTypeName() {
        return typeName;
    }

    /**
     * Get the self referencing column name of this table.
     *
     * @return Get the self referencing column name of this table.
     */
    public String getSelfReferencingColName() {
        return selfReferencingColName;
    }

    /**
     * Get the ref generation for this table.
     *
     * @return The ref generation for this table.
     */
    public String getRefGeneration() {
        return refGeneration;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{"
                + "catalog='" + catalog + '\''
                + ", schema='" + schema + '\''
                + ", tableName='" + tableName + '\''
                + ", tableType='" + tableType + '\''
                + ", remarks='" + remarks + '\''
                + ", typeCatalog='" + typeCatalog + '\''
                + ", typeSchema='" + typeSchema + '\''
                + ", typeName='" + typeName + '\''
                + ", selfReferencingColName='" + selfReferencingColName + '\''
                + ", refGeneration='" + refGeneration + '\''
                + '}';
    }
}
