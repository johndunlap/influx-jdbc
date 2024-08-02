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

    public TableMetadata(final ResultSet tableResultSet) throws SQLException {
        this.catalog = tableResultSet.getString("TABLE_CAT");
        this.schema = tableResultSet.getString("TABLE_SCHEM");
        this.tableName = tableResultSet.getString("TABLE_NAME");
        this.tableType = tableResultSet.getString("TABLE_TYPE");
        this.remarks = tableResultSet.getString("REMARKS");
        this.typeCatalog = tableResultSet.getString("TYPE_CAT");
        this.typeSchema = tableResultSet.getString("TYPE_SCHEM");
        this.typeName = tableResultSet.getString("TYPE_NAME");
        this.selfReferencingColName = tableResultSet.getString("SELF_REFERENCING_COL_NAME");
        this.refGeneration = tableResultSet.getString("REF_GENERATION");
    }

    public List<ColumnMetadata> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnMetadata> columns) {
        this.columns = columns;
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

    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    public String getRemarks() {
        return remarks;
    }

    public void setRemarks(String remarks) {
        this.remarks = remarks;
    }

    public String getTypeCatalog() {
        return typeCatalog;
    }

    public void setTypeCatalog(String typeCatalog) {
        this.typeCatalog = typeCatalog;
    }

    public String getTypeSchema() {
        return typeSchema;
    }

    public void setTypeSchema(String typeSchema) {
        this.typeSchema = typeSchema;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public String getSelfReferencingColName() {
        return selfReferencingColName;
    }

    public void setSelfReferencingColName(String selfReferencingColName) {
        this.selfReferencingColName = selfReferencingColName;
    }

    public String getRefGeneration() {
        return refGeneration;
    }

    public void setRefGeneration(String refGeneration) {
        this.refGeneration = refGeneration;
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
