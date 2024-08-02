package org.voidzero.influx.jdbc;

/*-
 * #%L
 * influx-jdbc
 * %%
 * Copyright (C) 2024 John Dunlap<john.david.dunlap@gmail.com>
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

import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

/**
 * @author <a href="mailto:john.david.dunlap@gmail.com">John D. Dunlap</a>
 */
public class InfluxResultSet implements ResultSet {
    private final ResultSet resultSet;

    public InfluxResultSet(final ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    public int getColumnCount() throws SQLException {
        return resultSet.getMetaData().getColumnCount();
    }

    public String getColumnName(final int position) throws SQLException {
        return resultSet.getMetaData().getColumnName(position);
    }

    public String getColumnClassName(final int position) throws SQLException {
        return resultSet.getMetaData().getColumnClassName(position);
    }

    public Object getValue(final int position) throws SQLException {
        return resultSet.getObject(position);
    }

    public String getStringByName(final String columnLabel) throws SQLException {
        return resultSet.getString(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public boolean next() throws SQLException {
        return resultSet.next();
    }

    /**
     * {@inheritDoc}
     */
    public void close() throws SQLException {
        resultSet.close();
    }

    /**
     * {@inheritDoc}
     */
    public boolean wasNull() throws SQLException {
        return resultSet.wasNull();
    }

    /**
     * {@inheritDoc}
     */
    public String getString(int columnIndex) throws SQLException {
        return resultSet.getString(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    public boolean getBoolean(int columnIndex) throws SQLException {
        return resultSet.getBoolean(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    public byte getByte(int columnIndex) throws SQLException {
        return resultSet.getByte(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    public short getShort(int columnIndex) throws SQLException {
        return resultSet.getShort(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    public int getInt(int columnIndex) throws SQLException {
        return resultSet.getInt(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    public long getLong(int columnIndex) throws SQLException {
        return resultSet.getLong(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    public float getFloat(int columnIndex) throws SQLException {
        return resultSet.getFloat(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    public double getDouble(int columnIndex) throws SQLException {
        return resultSet.getDouble(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return resultSet.getBigDecimal(columnIndex, scale);
    }

    /**
     * {@inheritDoc}
     */
    public byte[] getBytes(int columnIndex) throws SQLException {
        return resultSet.getBytes(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    public Date getDate(int columnIndex) throws SQLException {
        return resultSet.getDate(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    public Time getTime(int columnIndex) throws SQLException {
        return resultSet.getTime(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return resultSet.getTimestamp(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return resultSet.getAsciiStream(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return resultSet.getBinaryStream(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    public String getString(String columnLabel) throws SQLException {
        return resultSet.getString(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public boolean getBoolean(String columnLabel) throws SQLException {
        return resultSet.getBoolean(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public byte getByte(String columnLabel) throws SQLException {
        return resultSet.getByte(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public short getShort(String columnLabel) throws SQLException {
        return resultSet.getShort(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public int getInt(String columnLabel) throws SQLException {
        return resultSet.getInt(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public long getLong(String columnLabel) throws SQLException {
        return resultSet.getLong(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public float getFloat(String columnLabel) throws SQLException {
        return resultSet.getFloat(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public double getDouble(String columnLabel) throws SQLException {
        return resultSet.getDouble(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return resultSet.getBigDecimal(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public byte[] getBytes(String columnLabel) throws SQLException {
        return resultSet.getBytes(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public Date getDate(String columnLabel) throws SQLException {
        return resultSet.getDate(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public Time getTime(String columnLabel) throws SQLException {
        return resultSet.getTime(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return resultSet.getTimestamp(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return resultSet.getAsciiStream(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return resultSet.getBinaryStream(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public SQLWarning getWarnings() throws SQLException {
        return resultSet.getWarnings();
    }

    /**
     * {@inheritDoc}
     */
    public void clearWarnings() throws SQLException {
        resultSet.clearWarnings();
    }

    /**
     * {@inheritDoc}
     */
    public String getCursorName() throws SQLException {
        return resultSet.getCursorName();
    }

    /**
     * {@inheritDoc}
     */
    public ResultSetMetaData getMetaData() throws SQLException {
        return resultSet.getMetaData();
    }

    /**
     * {@inheritDoc}
     */
    public Object getObject(int columnIndex) throws SQLException {
        return resultSet.getObject(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    public Object getObject(String columnLabel) throws SQLException {
        return resultSet.getObject(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public int findColumn(String columnLabel) throws SQLException {
        return resultSet.findColumn(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return resultSet.getCharacterStream(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return resultSet.getCharacterStream(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return resultSet.getBigDecimal(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return resultSet.getBigDecimal(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public boolean isBeforeFirst() throws SQLException {
        return resultSet.isBeforeFirst();
    }

    /**
     * {@inheritDoc}
     */
    public boolean isAfterLast() throws SQLException {
        return resultSet.isAfterLast();
    }

    /**
     * {@inheritDoc}
     */
    public boolean isFirst() throws SQLException {
        return resultSet.isFirst();
    }

    /**
     * {@inheritDoc}
     */
    public boolean isLast() throws SQLException {
        return resultSet.isLast();
    }

    /**
     * {@inheritDoc}
     */
    public void beforeFirst() throws SQLException {
        resultSet.beforeFirst();
    }

    /**
     * {@inheritDoc}
     */
    public void afterLast() throws SQLException {
        resultSet.afterLast();
    }

    /**
     * {@inheritDoc}
     */
    public boolean first() throws SQLException {
        return resultSet.first();
    }

    /**
     * {@inheritDoc}
     */
    public boolean last() throws SQLException {
        return resultSet.last();
    }

    /**
     * {@inheritDoc}
     */
    public int getRow() throws SQLException {
        return resultSet.getRow();
    }

    /**
     * {@inheritDoc}
     */
    public boolean absolute(int row) throws SQLException {
        return resultSet.absolute(row);
    }

    /**
     * {@inheritDoc}
     */
    public boolean relative(int rows) throws SQLException {
        return resultSet.relative(rows);
    }

    /**
     * {@inheritDoc}
     */
    public boolean previous() throws SQLException {
        return resultSet.previous();
    }

    /**
     * {@inheritDoc}
     */
    public void setFetchDirection(int direction) throws SQLException {
        resultSet.setFetchDirection(direction);
    }

    /**
     * {@inheritDoc}
     */
    public int getFetchDirection() throws SQLException {
        return resultSet.getFetchDirection();
    }

    /**
     * {@inheritDoc}
     */
    public void setFetchSize(int rows) throws SQLException {
        resultSet.setFetchSize(rows);
    }

    /**
     * {@inheritDoc}
     */
    public int getFetchSize() throws SQLException {
        return resultSet.getFetchSize();
    }

    /**
     * {@inheritDoc}
     */
    public int getType() throws SQLException {
        return resultSet.getType();
    }

    /**
     * {@inheritDoc}
     */
    public int getConcurrency() throws SQLException {
        return resultSet.getConcurrency();
    }

    /**
     * {@inheritDoc}
     */
    public boolean rowUpdated() throws SQLException {
        return resultSet.rowUpdated();
    }

    /**
     * {@inheritDoc}
     */
    public boolean rowInserted() throws SQLException {
        return resultSet.rowInserted();
    }

    /**
     * {@inheritDoc}
     */
    public boolean rowDeleted() throws SQLException {
        return resultSet.rowDeleted();
    }

    /**
     * {@inheritDoc}
     */
    public void updateNull(int columnIndex) throws SQLException {
        resultSet.updateNull(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        resultSet.updateBoolean(columnIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateByte(int columnIndex, byte x) throws SQLException {
        resultSet.updateByte(columnIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateShort(int columnIndex, short x) throws SQLException {
        resultSet.updateShort(columnIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateInt(int columnIndex, int x) throws SQLException {
        resultSet.updateInt(columnIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateLong(int columnIndex, long x) throws SQLException {
        resultSet.updateLong(columnIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateFloat(int columnIndex, float x) throws SQLException {
        resultSet.updateFloat(columnIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateDouble(int columnIndex, double x) throws SQLException {
        resultSet.updateDouble(columnIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        resultSet.updateBigDecimal(columnIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateString(int columnIndex, String x) throws SQLException {
        resultSet.updateString(columnIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        resultSet.updateBytes(columnIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateDate(int columnIndex, Date x) throws SQLException {
        resultSet.updateDate(columnIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateTime(int columnIndex, Time x) throws SQLException {
        resultSet.updateTime(columnIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        resultSet.updateTimestamp(columnIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        resultSet.updateAsciiStream(columnIndex, x, length);
    }

    /**
     * {@inheritDoc}
     */
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        resultSet.updateBinaryStream(columnIndex, x, length);
    }

    /**
     * {@inheritDoc}
     */
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        resultSet.updateCharacterStream(columnIndex, x, length);
    }

    /**
     * {@inheritDoc}
     */
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        resultSet.updateObject(columnIndex, x, scaleOrLength);
    }

    /**
     * {@inheritDoc}
     */
    public void updateObject(int columnIndex, Object x) throws SQLException {
        resultSet.updateObject(columnIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateNull(String columnLabel) throws SQLException {
        resultSet.updateNull(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        resultSet.updateBoolean(columnLabel, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateByte(String columnLabel, byte x) throws SQLException {
        resultSet.updateByte(columnLabel, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateShort(String columnLabel, short x) throws SQLException {
        resultSet.updateShort(columnLabel, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateInt(String columnLabel, int x) throws SQLException {
        resultSet.updateInt(columnLabel, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateLong(String columnLabel, long x) throws SQLException {
        resultSet.updateLong(columnLabel, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateFloat(String columnLabel, float x) throws SQLException {
        resultSet.updateFloat(columnLabel, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateDouble(String columnLabel, double x) throws SQLException {
        resultSet.updateDouble(columnLabel, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        resultSet.updateBigDecimal(columnLabel, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateString(String columnLabel, String x) throws SQLException {
        resultSet.updateString(columnLabel, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        resultSet.updateBytes(columnLabel, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateDate(String columnLabel, Date x) throws SQLException {
        resultSet.updateDate(columnLabel, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateTime(String columnLabel, Time x) throws SQLException {
        resultSet.updateTime(columnLabel, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        resultSet.updateTimestamp(columnLabel, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        resultSet.updateAsciiStream(columnLabel, x, length);
    }

    /**
     * {@inheritDoc}
     */
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        resultSet.updateBinaryStream(columnLabel, x, length);
    }

    /**
     * {@inheritDoc}
     */
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        resultSet.updateCharacterStream(columnLabel, reader, length);
    }

    /**
     * {@inheritDoc}
     */
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        resultSet.updateObject(columnLabel, x, scaleOrLength);
    }

    /**
     * {@inheritDoc}
     */
    public void updateObject(String columnLabel, Object x) throws SQLException {
        resultSet.updateObject(columnLabel, x);
    }

    /**
     * {@inheritDoc}
     */
    public void insertRow() throws SQLException {
        resultSet.insertRow();
    }

    /**
     * {@inheritDoc}
     */
    public void updateRow() throws SQLException {
        resultSet.updateRow();
    }

    /**
     * {@inheritDoc}
     */
    public void deleteRow() throws SQLException {
        resultSet.deleteRow();
    }

    /**
     * {@inheritDoc}
     */
    public void refreshRow() throws SQLException {
        resultSet.refreshRow();
    }

    /**
     * {@inheritDoc}
     */
    public void cancelRowUpdates() throws SQLException {
        resultSet.cancelRowUpdates();
    }

    /**
     * {@inheritDoc}
     */
    public void moveToInsertRow() throws SQLException {
        resultSet.moveToInsertRow();
    }

    /**
     * {@inheritDoc}
     */
    public void moveToCurrentRow() throws SQLException {
        resultSet.moveToCurrentRow();
    }

    /**
     * {@inheritDoc}
     */
    public Statement getStatement() throws SQLException {
        return resultSet.getStatement();
    }

    /**
     * {@inheritDoc}
     */
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return resultSet.getObject(columnIndex, map);
    }

    /**
     * {@inheritDoc}
     */
    public Ref getRef(int columnIndex) throws SQLException {
        return resultSet.getRef(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    public Blob getBlob(int columnIndex) throws SQLException {
        return resultSet.getBlob(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    public Clob getClob(int columnIndex) throws SQLException {
        return resultSet.getClob(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    public Array getArray(int columnIndex) throws SQLException {
        return resultSet.getArray(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return resultSet.getObject(columnLabel, map);
    }

    /**
     * {@inheritDoc}
     */
    public Ref getRef(String columnLabel) throws SQLException {
        return resultSet.getRef(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public Blob getBlob(String columnLabel) throws SQLException {
        return resultSet.getBlob(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public Clob getClob(String columnLabel) throws SQLException {
        return resultSet.getClob(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public Array getArray(String columnLabel) throws SQLException {
        return resultSet.getArray(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return resultSet.getDate(columnIndex, cal);
    }

    /**
     * {@inheritDoc}
     */
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return resultSet.getDate(columnLabel, cal);
    }

    /**
     * {@inheritDoc}
     */
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return resultSet.getTime(columnIndex, cal);
    }

    /**
     * {@inheritDoc}
     */
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return resultSet.getTime(columnLabel, cal);
    }

    /**
     * {@inheritDoc}
     */
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return resultSet.getTimestamp(columnIndex, cal);
    }

    /**
     * {@inheritDoc}
     */
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return resultSet.getTimestamp(columnLabel, cal);
    }

    /**
     * {@inheritDoc}
     */
    public URL getURL(int columnIndex) throws SQLException {
        return resultSet.getURL(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    public URL getURL(String columnLabel) throws SQLException {
        return resultSet.getURL(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        resultSet.updateRef(columnIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        resultSet.updateRef(columnLabel, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        resultSet.updateBlob(columnIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        resultSet.updateBlob(columnLabel, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        resultSet.updateClob(columnIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        resultSet.updateClob(columnLabel, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateArray(int columnIndex, Array x) throws SQLException {
        resultSet.updateArray(columnIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateArray(String columnLabel, Array x) throws SQLException {
        resultSet.updateArray(columnLabel, x);
    }

    /**
     * {@inheritDoc}
     */
    public RowId getRowId(int columnIndex) throws SQLException {
        return resultSet.getRowId(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    public RowId getRowId(String columnLabel) throws SQLException {
        return resultSet.getRowId(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        resultSet.updateRowId(columnIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        resultSet.updateRowId(columnLabel, x);
    }

    /**
     * {@inheritDoc}
     */
    public int getHoldability() throws SQLException {
        return resultSet.getHoldability();
    }

    /**
     * {@inheritDoc}
     */
    public boolean isClosed() throws SQLException {
        return resultSet.isClosed();
    }

    /**
     * {@inheritDoc}
     */
    public void updateNString(int columnIndex, String nString) throws SQLException {
        resultSet.updateNString(columnIndex, nString);
    }

    /**
     * {@inheritDoc}
     */
    public void updateNString(String columnLabel, String nString) throws SQLException {
        resultSet.updateNString(columnLabel, nString);
    }

    /**
     * {@inheritDoc}
     */
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        resultSet.updateNClob(columnIndex, nClob);
    }

    /**
     * {@inheritDoc}
     */
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        resultSet.updateNClob(columnLabel, nClob);
    }

    /**
     * {@inheritDoc}
     */
    public NClob getNClob(int columnIndex) throws SQLException {
        return resultSet.getNClob(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    public NClob getNClob(String columnLabel) throws SQLException {
        return resultSet.getNClob(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return resultSet.getSQLXML(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return resultSet.getSQLXML(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        resultSet.updateSQLXML(columnIndex, xmlObject);
    }

    /**
     * {@inheritDoc}
     */
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        resultSet.updateSQLXML(columnLabel, xmlObject);
    }

    /**
     * {@inheritDoc}
     */
    public String getNString(int columnIndex) throws SQLException {
        return resultSet.getNString(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    public String getNString(String columnLabel) throws SQLException {
        return resultSet.getNString(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return resultSet.getNCharacterStream(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return resultSet.getNCharacterStream(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        resultSet.updateNCharacterStream(columnIndex, x, length);
    }

    /**
     * {@inheritDoc}
     */
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        resultSet.updateNCharacterStream(columnLabel, reader, length);
    }

    /**
     * {@inheritDoc}
     */
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        resultSet.updateAsciiStream(columnIndex, x, length);
    }

    /**
     * {@inheritDoc}
     */
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        resultSet.updateBinaryStream(columnIndex, x, length);
    }

    /**
     * {@inheritDoc}
     */
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        resultSet.updateCharacterStream(columnIndex, x, length);
    }

    /**
     * {@inheritDoc}
     */
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        resultSet.updateAsciiStream(columnLabel, x, length);
    }

    /**
     * {@inheritDoc}
     */
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        resultSet.updateBinaryStream(columnLabel, x, length);
    }

    /**
     * {@inheritDoc}
     */
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        resultSet.updateCharacterStream(columnLabel, reader, length);
    }

    /**
     * {@inheritDoc}
     */
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        resultSet.updateBlob(columnIndex, inputStream, length);
    }

    /**
     * {@inheritDoc}
     */
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        resultSet.updateBlob(columnLabel, inputStream, length);
    }

    /**
     * {@inheritDoc}
     */
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        resultSet.updateClob(columnIndex, reader, length);
    }

    /**
     * {@inheritDoc}
     */
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        resultSet.updateClob(columnLabel, reader, length);
    }

    /**
     * {@inheritDoc}
     */
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        resultSet.updateNClob(columnIndex, reader, length);
    }

    /**
     * {@inheritDoc}
     */
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        resultSet.updateNClob(columnLabel, reader, length);
    }

    /**
     * {@inheritDoc}
     */
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        resultSet.updateNCharacterStream(columnIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        resultSet.updateNClob(columnLabel, reader);
    }

    /**
     * {@inheritDoc}
     */
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        resultSet.updateAsciiStream(columnIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        resultSet.updateBinaryStream(columnIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        resultSet.updateCharacterStream(columnIndex, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        resultSet.updateAsciiStream(columnLabel, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        resultSet.updateBinaryStream(columnLabel, x);
    }

    /**
     * {@inheritDoc}
     */
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        resultSet.updateCharacterStream(columnLabel, reader);
    }

    /**
     * {@inheritDoc}
     */
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        resultSet.updateBlob(columnIndex, inputStream);
    }

    /**
     * {@inheritDoc}
     */
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        resultSet.updateBlob(columnLabel, inputStream);
    }

    /**
     * {@inheritDoc}
     */
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        resultSet.updateClob(columnIndex, reader);
    }

    /**
     * {@inheritDoc}
     */
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        resultSet.updateClob(columnLabel, reader);
    }

    /**
     * {@inheritDoc}
     */
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        resultSet.updateNClob(columnIndex, reader);
    }

    /**
     * {@inheritDoc}
     */
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        resultSet.updateNClob(columnLabel, reader);
    }

    /**
     * {@inheritDoc}
     */
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return resultSet.getObject(columnIndex, type);
    }

    /**
     * {@inheritDoc}
     */
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return resultSet.getObject(columnLabel, type);
    }


    /**
     * {@inheritDoc}
     *
     * This is provided only because {@link java.sql.ResultSet} requires that it exist.
     */
    @Deprecated
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return resultSet.getUnicodeStream(columnIndex);
    }

    /**
     * {@inheritDoc}
     *
     * This is provided only because {@link java.sql.ResultSet} requires that it exist.
     */
    @Deprecated
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return resultSet.getUnicodeStream(columnLabel);
    }

    public <T> T fetchEntity(final T entity) throws SQLException {
        try {
            int columnCount = getColumnCount();

            for (int index = 1; index <= columnCount; index++) {
                String columnName = InfluxConnection.toCamelCase(getColumnName(index));
                String columnClassName = getColumnClassName(index);
                Object value = getValue(index);

                // Attempt to find the appropriate setter method
                Method setterMethod = findSetter(entity, columnName, columnClassName);
                Class<?> argumentType = setterMethod.getParameterTypes()[0];

                // Perform automatic type conversions, where possible
                if (argumentType.equals(Long.class) && value instanceof Integer) {
                    value = Long.valueOf((Integer)value);
                }
                if (argumentType.equals(java.util.Date.class) && value instanceof java.sql.Timestamp) {
                    value = new java.util.Date(((java.sql.Timestamp) value).getTime());
                }

                // Invoke the setter
                setterMethod.invoke(entity, value);
            }

            return entity;
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | ClassNotFoundException e) {
            throw new SQLException("Cannot invoke setter: ", e);
        }
    }

    /**
     * This method attempts to locate the appropriate setter method within the specified object based
     * on the column name and data type which was returned by the database.
     * @param entity The entity in which we are looking for a setter method
     * @param columnName the column name of the data for which we are trying to find a setter method
     * @param columnTypeName the name of the data type which was returned by the database
     * @return An instance of @{link org.voidzero.influx.jdbc.SetterMethod()} or null if a setter cannot be found
     * @throws ClassNotFoundException thrown when something exceptional happens
     * @throws NoSuchMethodException thrown when something exceptional happens
     * @throws SQLException thrown when something exceptional happens
     */
    protected static Method findSetter(final Object entity, final String columnName, final String columnTypeName) throws ClassNotFoundException, NoSuchMethodException, SQLException {
        Class<?> columnClass = Class.forName(columnTypeName);
        Class<?> entityClass = entity.getClass();

        // Attempt to find a setter name for the property
        String setterName = "set"
                + String.valueOf(columnName.charAt(0)).toUpperCase()
                + columnName.substring(1);

        Method setterMethod;

        try {
            setterMethod = entityClass.getMethod(setterName, columnClass);
        } catch (NoSuchMethodException e) {
            // Attempt some type conversions, where possible
            if (columnTypeName.equals("java.lang.Integer")) {
                return findSetter(entity, columnName, "java.lang.Long");
            } else if (columnTypeName.equals("java.sql.Timestamp")) {
                return findSetter(entity, columnName, "java.util.Date");
            } else {
                throw e;
            }
        }

        Type[] parameterTypes = setterMethod.getParameterTypes();

        if (parameterTypes.length != 1) {
            throw new SQLException("Setter methods should only accept a single parameter");
        }

        Type setterArgumentType = parameterTypes[0];

        // TODO: This probably isn't very portable
        String setterArgumentTypeName = setterArgumentType.toString().replaceAll("class ", "");

        if (!setterArgumentTypeName.equals(columnTypeName)) {
            throw new SQLException("Setter argument type("
                    + setterArgumentTypeName
                    + ") does not match the column type name("
                    + columnTypeName
                    + ")"
            );
        }

        // Return the setter
        return setterMethod;
    }

    public <T> T fetchEntity(final Class<T> clazz) throws SQLException {
        try {

            T entity = clazz.getDeclaredConstructor().newInstance();
            return fetchEntity(entity);
        } catch (InstantiationException e) {
            throw new SQLException("Cannot instantiate entity: " + clazz.getCanonicalName(), e);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new SQLException("Failed to invoke no-argument constructor of entity: " + clazz.getCanonicalName(), e);
        } catch (NoSuchMethodException e) {
            throw new SQLException("Entity does not have a no-argument constructor: " + clazz.getCanonicalName(), e);
        }
    }

    public StringBuilder toCsv() throws SQLException {
        StringBuilder csv = new StringBuilder();
        ResultSetMetaData metaData = getMetaData();
        int columnCount = metaData.getColumnCount();

        // Append the header row
        for (int i = 1; i <= columnCount; i++) {
            if (i > 1) csv.append(",");
            csv.append(escapeCsv(metaData.getColumnName(i)));
        }
        csv.append("\n");

        // Append the data rows
        while (next()) {
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) csv.append(",");
                csv.append(escapeCsv(getString(i)));
            }
            csv.append("\n");
        }

        return csv;
    }

    /**
     * Escapes special characters for CSV format.
     *
     * @param value The value to escape.
     * @return The escaped value.
     */
    protected String escapeCsv(String value) {
        if (value == null) return "";
        // Escape double quotes by doubling them
        value = value.replace("\"", "\"\"");
        // Enclose in quotes if it contains a comma, newline, or quote
        if (value.contains(",") || value.contains("\n") || value.contains("\"")) {
            value = "\"" + value + "\"";
        }
        return value;
    }
}
