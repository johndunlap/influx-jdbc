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

import static org.voidzero.influx.jdbc.InfluxConnection.bindObject;

import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;


/**
 * This class provider builder-like semantics for constructing queries using named parameters, which are not
 * directly supported by JDBC.
 *
 * @author <a href="mailto:john.david.dunlap@gmail.com">John Dunlap</a>
 */
public class InfluxQuery {
    /**
     * The prepared statement which should be used to interact with the database.
     */
    private final PreparedStatement preparedStatement;

    /**
     * The current parameter number.
     */
    private int position = 1;

    /**
     * Create a new instance of this class.
     *
     * @param connection The connection which should be used to construct a prepared statement.
     * @param sql The query for which a prepared statement should be created.
     * @throws SQLException Thrown if something goes wrong.
     */
    public InfluxQuery(final InfluxConnection connection, final String sql) throws SQLException {
        // TODO: Extract named parameters from the query, replace them with ordered parameters, and create a prepared
        //  statement
        this.preparedStatement = connection.prepareStatement(sql);
    }

    /**
     * Add a named parameter.
     *
     * @param value The value of the parameter.
     *
     * @return A reference to this class to support method chaining.
     * @throws SQLException Thrown if something goes wrong.
     */
    public InfluxQuery addParam(Object value) throws SQLException {
        bindObject(preparedStatement, position++, value);
        return this;
    }

    /**
     * Run the query and return a result set.
     *
     * @return A result set
     * @throws SQLException thrown if something goes wrong
     */
    public InfluxResultSet fetch() throws SQLException {
        return new InfluxResultSet(preparedStatement.executeQuery());
    }

    /**
     * Fetch a record from the database and bind the results to a class of the specified type.
     *
     * @param clazz The object type which should be instantiated and bound to the results of the query
     * @param <T> The generic type of the object type being bound
     *
     * @return An instance of the specified object type
     *
     * @throws SQLException Thrown if something goes wrong.
     */
    public <T> T fetchEntity(Class<T> clazz) throws SQLException {
        try (InfluxResultSet resultSet = fetch()) {
            if (!resultSet.next()) {
                return null;
            }
            return resultSet.getEntity(clazz);
        }
    }

    /**
     * Fetch a list of entities.
     *
     * @param clazz The object type to instantiate and bind to.
     * @param <T> The generic type of the object being bound to
     *
     * @return An instance of the specified object type
     * @throws SQLException Thrown if something goes wrong.
     */
    public <T> List<T> fetchList(Class<T> clazz) throws SQLException {
        throw new RuntimeException("IMPLEMENT ME");
    }

    /**
     * Fetch a string from the database.
     *
     * @return A string or null.
     * @throws SQLException Thrown if something goes wrong.
     */
    public String fetchString() throws SQLException {
        try (InfluxResultSet resultSet = fetch()) {
            return resultSet.getString(1);
        }
    }

    /**
     * Fetch an integer value from the database.
     *
     * @return An integer value or null.
     * @throws SQLException Thrown if something goes wrong.
     */
    public Integer fetchInteger() throws SQLException {
        try (InfluxResultSet resultSet = fetch()) {
            return resultSet.getInt(1);
        }
    }

    /**
     * Fetch a short from the database.
     *
     * @return A short value or null.
     * @throws SQLException Thrown if something goes wrong.
     */
    public Short fetchShort() throws SQLException {
        try (InfluxResultSet resultSet = fetch()) {
            return resultSet.getShort(1);
        }
    }

    /**
     * Fetch a single byte from the database.
     *
     * @return A byte value or null.
     * @throws SQLException Thrown if something goes wrong.
     */
    public Byte fetchByte() throws SQLException {
        try (InfluxResultSet resultSet = fetch()) {
            return resultSet.getByte(1);
        }
    }

    /**
     * Fetch a single timestamp from the database.
     *
     * @return A timestamp value or null.
     * @throws SQLException Thrown if something goes wrong.
     */
    public Timestamp fetchTimestamp() throws SQLException {
        try (InfluxResultSet resultSet = fetch()) {
            return resultSet.getTimestamp(1);
        }
    }

    /**
     * Fetch a single time value from the database.
     *
     * @return A time value or null.
     * @throws SQLException Thrown if something goes wrong.
     */
    public Time fetchTime() throws SQLException {
        try (InfluxResultSet resultSet = fetch()) {
            return resultSet.getTime(1);
        }
    }

    /**
     * Fetch a url from the database.
     *
     * @return A url or null.
     * @throws SQLException Thrown if something goes wrong.
     */
    public URL fetchUrl() throws SQLException {
        try (InfluxResultSet resultSet = fetch()) {
            return resultSet.getURL(1);
        }
    }
}
/*
        SimpleResultSet fetch(final PreparedStatement statement, final Object... arguments) throws SQLException;

        <T> T fetch(final ResultSetHandler<T> handler, final String sql, final Object... arguments) throws SQLException;

        <T> T fetch(final ResultSetHandler<T> handler, final PreparedStatement statement, final Object... arguments)
         throws SQLException;

        List<Integer> fetchListInteger(final String sql, final Object... arguments) throws SQLException;

        List<Integer> fetchListInteger(final PreparedStatement statement, final Object... arguments) throws
         SQLException;

        List<Long> fetchListLong(final String sql, final Object... arguments) throws SQLException;

        List<Long> fetchListLong(final PreparedStatement statement, final Object... arguments) throws SQLException;

        <T> List<T> fetchAllEntity(final Class<T> clazz, final String sql, final Object... arguments) throws
         SQLException;

        <T> List<T> fetchAllEntity(final Class<T> clazz, final PreparedStatement statement, final Object... arguments)
         throws SQLException;

        <T> Map<String, T> fetchAllEntityMap(final Class<T> clazz, final String columnLabel, final String sql,
         final Object... arguments) throws SQLException;

        <T> Map<String, T> fetchAllEntityMap(final Class<T> clazz, final String columnLabel,
         final PreparedStatement statement, final Object... arguments) throws SQLException;

        List<Map<String, Object>> fetchAllMap(final String sql, final Object... arguments) throws SQLException;

        List<Map<String, Object>> fetchAllMap(final PreparedStatement statement, final Object... arguments)
         throws SQLException;

        <T> T fetchEntity(final T entity, final String sql, final Object... arguments) throws SQLException;

        <T> T fetchEntity(final T entity, final PreparedStatement statement, final Object... arguments) throws
         SQLException;

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
