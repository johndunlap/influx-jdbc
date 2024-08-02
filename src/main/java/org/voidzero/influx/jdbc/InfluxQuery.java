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

import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

import static org.voidzero.influx.jdbc.InfluxConnection.bindObject;

public class InfluxQuery {
    private final PreparedStatement preparedStatement;
    private int position = 1;

    public InfluxQuery(final InfluxConnection connection, final String sql) throws SQLException {
        this.preparedStatement = connection.prepareStatement(sql);
    }

    public InfluxQuery addParam(Object value) throws SQLException {
        bindObject(preparedStatement, position++, value);
        return this;
    }

    public InfluxResultSet fetch() throws SQLException {
        return new InfluxResultSet(preparedStatement.executeQuery());
    }

    public <T> T fetchEntity(Class<T> clazz) throws SQLException {
        try (InfluxResultSet resultSet = fetch()) {
            if (!resultSet.next()) {
                return null;
            }
            return resultSet.fetchEntity(clazz);
        }
    }

    // TODO
    public <T> List<T> fetchList(Class<T> clazz) {
        throw new RuntimeException("IMPLEMENT ME");
    }

    public String fetchString() throws SQLException {
        try (InfluxResultSet resultSet = fetch()) {
            return resultSet.getString(1);
        }
    }

    public Integer fetchInteger() throws SQLException {
        try (InfluxResultSet resultSet = fetch()) {
            return resultSet.getInt(1);
        }
    }

    public Short fetchShort() throws SQLException {
        try (InfluxResultSet resultSet = fetch()) {
            return resultSet.getShort(1);
        }
    }

    public Byte fetchByte() throws SQLException {
        try (InfluxResultSet resultSet = fetch()) {
            return resultSet.getByte(1);
        }
    }

    public Timestamp fetchTimestamp() throws SQLException {
        try (InfluxResultSet resultSet = fetch()) {
            return resultSet.getTimestamp(1);
        }
    }

    public Time fetchTime() throws SQLException {
        try (InfluxResultSet resultSet = fetch()) {
            return resultSet.getTime(1);
        }
    }

    public URL fetchURL() throws SQLException {
        try (InfluxResultSet resultSet = fetch()) {
            return resultSet.getURL(1);
        }
    }
}
/*
        SimpleResultSet fetch(final PreparedStatement statement, final Object... arguments) throws SQLException;

        <T> T fetch(final ResultSetHandler<T> handler, final String sql, final Object... arguments) throws SQLException;

        <T> T fetch(final ResultSetHandler<T> handler, final PreparedStatement statement, final Object... arguments) throws SQLException;

        List<Integer> fetchListInteger(final String sql, final Object... arguments) throws SQLException;

        List<Integer> fetchListInteger(final PreparedStatement statement, final Object... arguments) throws SQLException;

        List<Long> fetchListLong(final String sql, final Object... arguments) throws SQLException;

        List<Long> fetchListLong(final PreparedStatement statement, final Object... arguments) throws SQLException;

        <T> List<T> fetchAllEntity(final Class<T> clazz, final String sql, final Object... arguments) throws SQLException;

        <T> List<T> fetchAllEntity(final Class<T> clazz, final PreparedStatement statement, final Object... arguments) throws SQLException;

        <T> Map<String, T> fetchAllEntityMap(final Class<T> clazz, final String columnLabel, final String sql, final Object... arguments) throws SQLException;

        <T> Map<String, T> fetchAllEntityMap(final Class<T> clazz, final String columnLabel, final PreparedStatement statement, final Object... arguments) throws SQLException;

        List<Map<String, Object>> fetchAllMap(final String sql, final Object... arguments) throws SQLException;

        List<Map<String, Object>> fetchAllMap(final PreparedStatement statement, final Object... arguments) throws SQLException;

        <T> T fetchEntity(final T entity, final String sql, final Object... arguments) throws SQLException;

        <T> T fetchEntity(final T entity, final PreparedStatement statement, final Object... arguments) throws SQLException;

        <T> T fetchEntity(final Class<T> clazz, final String sql, final Object... arguments) throws SQLException;

        Map<String, Object> fetchMap(final String sql, final Object... arguments) throws SQLException;

        Map<String, Object> fetchMap(final PreparedStatement statement, final Object... arguments) throws SQLException;

        String fetchString(final String sql, final Object... args) throws SQLException;

        Integer fetchInt(final String sql, final Object... args) throws SQLException;

        Short fetchShort(final String sql, final Object... args) throws SQLException;

        Byte fetchByte(final String sql, final Object... args) throws SQLException;

        Timestamp fetchTimestamp(final String sql, final Object... args) throws SQLException;

        Time fetchTime(final String sql, final Object... args) throws SQLException;

        URL fetchURL(final String sql, final Object... args) throws SQLException;

        Array fetchArray(final String sql, final Object... args) throws SQLException;

        InputStream fetchAsciiStream(final String sql, final Object... args) throws SQLException;

        InputStream fetchBinaryStream(final String sql, final Object... args) throws SQLException;

        Blob fetchBlob(final String sql, final Object... args) throws SQLException;

        Reader fetchCharacterStream(final String sql, final Object... args) throws SQLException;

        Reader fetchNCharacterStream(final String sql, final Object... args) throws SQLException;

        Clob fetchClob(final String sql, final Object... args) throws SQLException;

        NClob fetchNClob(final String sql, final Object... args) throws SQLException;

        Ref fetchRef(final String sql, final Object... args) throws SQLException;

        String fetchNString(final String sql, final Object... args) throws SQLException;

        SQLXML fetchSQLXML(final String sql, final Object... args) throws SQLException;

        byte[] fetchBytes(final String sql, final Object... args) throws SQLException;

        Long fetchLong(final String sql, final Object... args) throws SQLException;

        Float fetchFloat(final String sql, final Object... args) throws SQLException;

        Double fetchDouble(final String sql, final Object... args) throws SQLException;

        BigDecimal fetchBigDecimal(final String sql, final Object... args) throws SQLException;

        Date fetchDate(final String sql, final Object... args) throws SQLException;

        Boolean fetchBoolean(final String sql, final Object... args) throws SQLException;

        Object fetchObject(final String sql, final Object... args) throws SQLException;
*/
