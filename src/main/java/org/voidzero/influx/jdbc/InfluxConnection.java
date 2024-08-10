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


import javax.sql.DataSource;
import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Ref;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.Executor;

/**
 * This object implements {@link java.sql.Connection} by accepting a reference to an instance which implements
 * {@link java.sql.Connection}, during construction, and proxying all methods to their counterparts in that instance.
 * This is necessary because we can't can't extend the object which implements {@link java.sql.Connection} interface
 * because it is provided by the JDBC driver and, as such, will be different between databases. Additional methods which
 * simplify database interaction have been added to this object. Further, it is intended that this object be extended to
 * support vendor specific features, which are not available in all databases.
 *
 * @author <a href="mailto:john.david.dunlap@gmail.com">John D. Dunlap</a>
 */
public class InfluxConnection implements Connection {
    /**
     * The connection which should be used to interact with the database
     */
    private final Connection connection;

    /**
     * The value of this variable should be true between calls to being and
     * commit/rollback and false otherwise. It is not exposed outside of this
     * class because, if you are writing your code correctly, then you should
     * ALWAYS know if you are within a transaction or not. If you know that,
     * then you do not need access to this value. If you don't know
     * that, then there is a serious problem with your application which needs
     * to be corrected. In either case,
     */
    private boolean transactionActive = false;

    public static InfluxConnection connect(String url, String user, String password) throws SQLException {
        return new InfluxConnection(DriverManager.getConnection(url, user, password));
    }

    public static InfluxConnection connect(String url) throws SQLException {
        return new InfluxConnection(DriverManager.getConnection(url));
    }

    public static InfluxConnection connect(String url, Properties info) throws SQLException {
        return new InfluxConnection(DriverManager.getConnection(url, info));
    }

    /**
     * Construct an instance of this class and use the provided {@link javax.sql.DataSource} to obtain
     * a {@link java.sql.Connection}
     * @param dataSource the datasource which should be used to obtain a database connection
     * @throws SQLException throw when something exceptional happens
     */
    public InfluxConnection(final DataSource dataSource) throws SQLException {
        this.connection = dataSource.getConnection();
    }

    /**
     * Construct an instance of this class with the provided {@link java.sql.Connection} object
     * @param connection the connection object which should be used to access the database
     */
    public InfluxConnection(final Connection connection) {
        this.connection = connection;
    }

    /**
     * Prepares the specified sql statement for execution and bind the provided arguments to it
     * @param sql the sql which should be prepared
     * @param arguments the arguments which should be bound to the statement
     * @return a reference to the prepared statement after the arguments have been bound to it
     * @throws SQLException thrown when something exceptional happens
     */
    public PreparedStatement prepareStatement(final String sql, final Object... arguments) throws SQLException {
        return bindArguments(connection.prepareStatement(sql), arguments);
    }

    /**
     * This must be implemented by a child class so that the child class can control which child class of
     * {@link InfluxResultSet} is returned
     * @param statement the statement which should be used to obtain the resultset
     * @return reference to the resultset
     * @throws SQLException thrown when something exceptional happens
     */
    protected InfluxResultSet fetch(final PreparedStatement statement) throws SQLException {
        return new InfluxResultSet(statement.executeQuery());
    }

    /**
     * Fetches a {@link InfluxResultSet} from the provided sql and arguments.
     * @param sql the sql query which should be executed
     * @param arguments the arguments which should be bound to the query
     * @return a reference to an instance of {@link InfluxResultSet} which contains the results of the query
     * @throws SQLException thrown when something exceptional happens
     */
    public InfluxResultSet fetch(final String sql, final Object... arguments) throws SQLException {
        try(PreparedStatement statement = prepareStatement(sql)) {
            return fetch(statement, arguments);
        }
    }

    /**
     * Bind the provided arguments to the provided statement, execute the statement, and return an instance of
     * {@link InfluxResultSet}
     * @param statement the statement which should be executed
     * @param arguments the arguments which should be bound to the query
     * @return an instance of {@link InfluxResultSet} which contains the results of the query
     * @throws SQLException thrown when something exceptional happens
     */
    public InfluxResultSet fetch(final PreparedStatement statement, final Object... arguments) throws SQLException {
        // Attempt to bind the arguments to the query
        bindArguments(statement, arguments);

        // Run the query
        return fetch(statement);
    }

    public <T> T fetch(final InfluxResultSetHandler<T> handler, final String sql, final Object ... arguments)
            throws SQLException {
        try(PreparedStatement statement = prepareCall(sql)) {
            return fetch(handler, statement, arguments);
        }
    }

    public <T> T fetch(final InfluxResultSetHandler<T> handler, final PreparedStatement statement,
                       final Object ... arguments) throws SQLException {
        try (InfluxResultSet resultSet = fetch(statement, arguments)) {
            // Return null if there is no data
            if (!resultSet.next()) {
                return null;
            }

            return handler.handle(resultSet);
        }
    }

    public List<Integer> fetchListInteger(final String sql, final Object... arguments) throws SQLException {
        try (PreparedStatement statement = prepareStatement(sql)) {
            return fetchListInteger(statement, arguments);
        }
    }

    public List<Integer> fetchListInteger(final PreparedStatement statement, final Object... arguments)
            throws SQLException {
        List<Integer> list = fetch(resultSet -> {
            List<Integer> integerList = new ArrayList<>();

            do {
                integerList.add(resultSet.getInt(1));
            } while (resultSet.next());

            return integerList;
        }, statement, arguments);

        // We need to return an empty list, in this case, because we want to be able to use the return value of this
        // method in a for each loop even if there are no rows in the result set
        if (list == null) {
            return new ArrayList<>();
        }

        return list;
    }

    public List<Long> fetchListLong(final String sql, final Object... arguments) throws SQLException {
        try (PreparedStatement statement = prepareStatement(sql)) {
            return fetchListLong(statement, arguments);
        }
    }

    public List<Long> fetchListLong(final PreparedStatement statement, final Object... arguments) throws SQLException {
        List<Long> list = fetch(resultSet -> {
            List<Long> integerList = new ArrayList<>();

            do {
                integerList.add(resultSet.getLong(1));
            } while (resultSet.next());

            return integerList;
        }, statement, arguments);

        // We need to return an empty list, in this case, because we want to be able to use the return value of this
        // method in a for each loop even if there are no rows in the result set
        if (list == null) {
            return new ArrayList<>();
        }

        return list;
    }

    /**
     * Exceutes the procided statement
     * @param statement the statement which should be executed
     * @return true if the statement contains a resultset and false otherwise
     * @throws SQLException thrown if something exceptional happens
     */
    public boolean execute(final PreparedStatement statement) throws SQLException {
        return statement.execute();
    }

    /**
     * Binds the provided arguments to the provided sql query, executes the query
     * @param sql the sql which should be executed
     * @param arguments the arguments which should be bound to the query
     * @return true if the query returns a resultset and false otherwise. If true is returned and you need access to the
     * resultset, use the <i>execute(PreparedStatement, Object...)</i> or <i>fetch(PreparedStatement, Object...)</i>
     * methods instead
     * @throws SQLException thrown when something exceptional happens
     */
    public boolean execute(final String sql, final Object... arguments) throws SQLException {
        try (PreparedStatement statement = prepareStatement(sql)) {
            return execute(statement, arguments);
        }
    }

    /**
     * Binds the provided arguments to the provided statement and executes the statement.
     * @param statement the statement which should be executed
     * @param arguments the arguments which should be bound to the statement
     * @return true if a resultset is available and false otherwise
     * @throws SQLException thrown when something exceptional happens
     */
    public boolean execute(final PreparedStatement statement, final Object... arguments) throws SQLException {
        return execute(bindArguments(statement, arguments));
    }

    /**
     * Returns a list of entities which are instances of the specified class and which have been populated by the
     * provided sql and arguments.
     * @param clazz the class type of the entities which should be returned
     * @param sql the sql which should be used to obtain data from the database
     * @param arguments the arguments which should be bound to the query
     * @param <T> the generic type of the entities which should be returned
     * @return a list of entities which have the results of the query injected into them
     * @throws SQLException thrown when something exceptional happens
     */
    public <T> List<T> fetchAllEntity(final Class<T> clazz, final String sql, final Object... arguments)
            throws SQLException {
        try (PreparedStatement statement = prepareStatement(sql)) {
            return fetchAllEntity(clazz, statement, arguments);
        }
    }

    public <T> List<T> fetchAllEntity(final Class<T> clazz, final PreparedStatement statement,
                                      final Object... arguments) throws SQLException {
        // Attempt to bind the arguments to the query
        bindArguments(statement, arguments);

        // Run the query
        try (InfluxResultSet resultSet = fetch(statement)) {
            List<T> entities = new ArrayList<>();

            // Iterate over the results
            while (resultSet.next()) {
                entities.add(resultSet.fetchEntity(clazz));
            }

            return entities;
        }
    }

    public <T> Map<String, T> fetchAllEntityMap(final Class<T> clazz, final String columnLabel, final String sql,
                                                final Object... arguments) throws SQLException {
        try (PreparedStatement statement = prepareStatement(sql)) {
            return fetchAllEntityMap(clazz, columnLabel, statement, arguments);
        }
    }

    public <T> Map<String, T> fetchAllEntityMap(final Class<T> clazz, final String columnLabel,
                                                final PreparedStatement statement, final Object... arguments) throws SQLException {
        // Attempt to bind the arguments to the query
        bindArguments(statement, arguments);

        // Run the query
        try (InfluxResultSet resultSet = fetch(statement)) {
            Map<String, T> entities = new HashMap<>();

            // Iterate over the results
            while (resultSet.next()) {
                entities.put(resultSet.getStringByName(columnLabel), resultSet.fetchEntity(clazz));
            }

            return entities;
        }
    }

    public List<Map<String, Object>> fetchAllMap(final String sql, final Object ... arguments) throws SQLException {
        try (PreparedStatement statement = prepareStatement(sql)) {
            return fetchAllMap(statement, arguments);
        }
    }

    public List<Map<String, Object>> fetchAllMap(final PreparedStatement statement, final Object ... arguments)
            throws SQLException {
        // Attempt to bind the arguments to the query
        bindArguments(statement, arguments);

        // Run the query
        try (InfluxResultSet resultSet = fetch(statement)) {
            List<Map<String, Object>> entities = new ArrayList<>();

            // Iterate over the results
            while(resultSet.next()) {
                entities.add(fetchMap(resultSet));
            }

            // Not technically type safe but necessary to create the illusion of it
            return entities;
        }
    }

    public <T> T fetchEntity(final T entity, final String sql, final Object ... arguments) throws SQLException {
        try (PreparedStatement statement = prepareStatement(sql)) {
            return fetchEntity(entity, statement, arguments);
        }
    }

    public <T> T fetchEntity(final T entity, final PreparedStatement statement, final Object ... arguments)
            throws SQLException {
        // Attempt to bind the arguments to the query
        bindArguments(statement, arguments);

        // Run the query
        try (InfluxResultSet resultSet = fetch(statement)) {
            int count = 0;

            // Iterate over the results
            while(resultSet.next()) {
                if (count > 0) {
                    throw new SQLException("Encountered a second record where a single record was expected");
                }

                // Populate the entity
                resultSet.fetchEntity(entity);
                count++;
            }

            return entity;
        }
    }

    public <T> T fetchEntity(final Class<T> clazz, final String sql, final Object ... arguments) throws SQLException {
        try {
            T entity = clazz.getDeclaredConstructor().newInstance();
            return fetchEntity(entity, sql, arguments);
        } catch (InstantiationException e) {
            throw new SQLException("Cannot instantiate entity: " + clazz.getCanonicalName(), e);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new SQLException("Cannot access no-argument constructor of entity: " + clazz.getCanonicalName(), e);
        } catch (NoSuchMethodException e) {
            throw new SQLException("No argument constructor does not exist: " + clazz.getCanonicalName(), e);
        }
    }

    public Map<String, Object> fetchMap(final String sql, final Object ... arguments) throws SQLException {
        try (PreparedStatement statement = prepareStatement(sql)) {
            return fetchMap(statement, arguments);
        }
    }

    public Map<String, Object> fetchMap(final PreparedStatement statement, final Object ... arguments)
            throws SQLException {
        // Attempt to bind the arguments to the query
        bindArguments(statement, arguments);

        // Run the query
        try (InfluxResultSet resultSet = fetch(statement)) {
            int count = 0;

            Map<String, Object> entity = null;

            // Iterate over the results
            while (resultSet.next()) {
                if (count > 0) {
                    throw new SQLException("Encountered a second record where a single record was expected");
                }

                entity = fetchMap(resultSet);
                count++;
            }

            return entity;
        }
    }

    protected Map<String, Object> fetchMap(final InfluxResultSet resultSet) throws SQLException {
        Map<String, Object> entity = new HashMap<>();

        int columnCount = resultSet.getColumnCount();

        for (int index = 1; index <= columnCount; index++) {
            String columnName = resultSet.getColumnName(index).toLowerCase();
            Object value = resultSet.getValue(index);
            entity.put(columnName, value);
        }

        return entity;
    }

    /**
     * Fetches a single string from the database
     * @param sql the sql which should be sent to the database
     * @param args the arguments which should be sent to the database
     * @return the requested string or null if nothing was returned by the database
     * @throws SQLException thrown when something exceptional happens
     */
    public String fetchString(final String sql, final Object ... args) throws SQLException {
        return fetch(resultSet -> resultSet.getString(1), sql, args);
    }

    /**
     * Fetches a single integer from the database
     * @param sql the sql which should be sent to the database
     * @param args the arguments which should be sent to the database
     * @return the requested integer or null if nothing was returned by the database
     * @throws SQLException thrown when something exceptional happens
     */
    public Integer fetchInt(final String sql, final Object ... args) throws SQLException {
        return fetch(resultSet -> resultSet.getInt(1), sql, args);
    }

    /**
     * Fetches a single short from the database
     * @param sql the sql which should be sent to the database
     * @param args the arguments which should be sent to the database
     * @return the requested short or null if nothing was returned by the database
     * @throws SQLException thrown when something exceptional happens
     */
    public Short fetchShort(final String sql, final Object ... args) throws SQLException {
        return fetch(resultSet -> resultSet.getShort(1), sql, args);
    }

    /**
     * Fetches a single byte from the database
     * @param sql the sql which should be sent to the database
     * @param args the arguments which should be sent to the database
     * @return the requested byte or null if nothing was returned by the database
     * @throws SQLException thrown when something exceptional happens
     */
    public Byte fetchByte(final String sql, final Object ... args) throws SQLException {
        return fetch(resultSet -> resultSet.getByte(1), sql, args);
    }

    /**
     * Fetches a single timestamp from the database
     * @param sql the sql which should be sent to the database
     * @param args the arguments which should be sent to the database
     * @return the requested timestamp or null if nothing was returned by the database
     * @throws SQLException thrown when something exceptional happens
     */
    public Timestamp fetchTimestamp(final String sql, final Object ... args) throws SQLException {
        return fetch(resultSet -> resultSet.getTimestamp(1), sql, args);
    }

    /**
     * Fetches a single time from the database
     * @param sql the sql which should be sent to the database
     * @param args the arguments which should be sent to the database
     * @return the requested time or null if nothing was returned by the database
     * @throws SQLException thrown when something exceptional happens
     */
    public Time fetchTime(final String sql, final Object ... args) throws SQLException {
        return fetch(resultSet -> resultSet.getTime(1), sql, args);
    }

    /**
     * Fetches a single url from the database
     * @param sql the sql which should be sent to the database
     * @param args the arguments which should be sent to the database
     * @return the requested url or null if nothing was returned by the database
     * @throws SQLException thrown when something exceptional happens
     */
    public URL fetchURL(final String sql, final Object ... args) throws SQLException {
        return fetch(resultSet -> resultSet.getURL(1), sql, args);
    }

    /**
     * Fetches an array from the database
     * @param sql the sql which should be sent to the database
     * @param args the arguments which should be sent to the database
     * @return the requested array or null if nothing was returned by the database
     * @throws SQLException thrown when something exceptional happens
     */
    public Array fetchArray(final String sql, final Object ... args) throws SQLException {
        return fetch(resultSet -> resultSet.getArray(1), sql, args);
    }

    /**
     * Fetches an ASCII stream from the database
     * @param sql the sql which should be sent to the database
     * @param args the arguments which should be sent to the database
     * @return the requested ASCII stream or null if nothing was returned by the database
     * @throws SQLException thrown when something exceptional happens
     */
    public InputStream fetchAsciiStream(final String sql, final Object ... args) throws SQLException {
        return fetch(resultSet -> resultSet.getAsciiStream(1), sql, args);
    }

    /**
     * Fetches an binary stream from the database
     * @param sql the sql which should be sent to the database
     * @param args the arguments which should be sent to the database
     * @return the requested binary stream or null if nothing was returned by the database
     * @throws SQLException thrown when something exceptional happens
     */
    public InputStream fetchBinaryStream(final String sql, final Object ... args) throws SQLException {
        return fetch(resultSet -> resultSet.getBinaryStream(1), sql, args);
    }

    /**
     * Fetches a blob from the database
     * @param sql the sql which should be sent to the database
     * @param args the arguments which should be sent to the database
     * @return the requested blob stream or null if nothing was returned by the database
     * @throws SQLException thrown when something exceptional happens
     */
    public Blob fetchBlob(final String sql, final Object ... args) throws SQLException {
        return fetch(resultSet -> resultSet.getBlob(1), sql, args);
    }

    /**
     * Fetches a character stream from the database
     * @param sql the sql which should be sent to the database
     * @param args the arguments which should be sent to the database
     * @return the requested character stream or null if nothing was returned by the database
     * @throws SQLException thrown when something exceptional happens
     */
    public Reader fetchCharacterStream(final String sql, final Object ... args) throws SQLException {
        return fetch(resultSet -> resultSet.getCharacterStream(1), sql, args);
    }

    /**
     * Fetches an NCharacter stream from the database
     * @param sql the sql which should be sent to the database
     * @param args the arguments which should be sent to the database
     * @return the requested NCharacter stream or null if nothing was returned by the database
     * @throws SQLException thrown when something exceptional happens
     */
    public Reader fetchNCharacterStream(final String sql, final Object ... args) throws SQLException {
        return fetch(resultSet -> resultSet.getNCharacterStream(1), sql, args);
    }

    /**
     * Fetches a clob from the database
     * @param sql the sql which should be sent to the database
     * @param args the arguments which should be sent to the database
     * @return the requested clob or null if nothing was returned by the database
     * @throws SQLException thrown when something exceptional happens
     */
    public Clob fetchClob(final String sql, final Object ... args) throws SQLException {
        return fetch(resultSet -> resultSet.getClob(1), sql, args);
    }

    /**
     * Fetches a NClob from the database
     * @param sql the sql which should be sent to the database
     * @param args the arguments which should be sent to the database
     * @return the requested NClob or null if nothing was returned by the database
     * @throws SQLException thrown when something exceptional happens
     */
    public NClob fetchNClob(final String sql, final Object ... args) throws SQLException {
        return fetch(resultSet -> resultSet.getNClob(1), sql, args);
    }

    /**
     * Fetches a ref from the database
     * @param sql the sql which should be sent to the database
     * @param args the arguments which should be sent to the database
     * @return the requested ref or null if nothing was returned by the database
     * @throws SQLException thrown when something exceptional happens
     */
    public Ref fetchRef(final String sql, final Object ... args) throws SQLException {
        return fetch(resultSet -> resultSet.getRef(1), sql, args);
    }

    /**
     * Fetches an NString from the database
     * @param sql the sql which should be sent to the database
     * @param args the arguments which should be sent to the database
     * @return the requested NString or null if nothing was returned by the database
     * @throws SQLException thrown when something exceptional happens
     */
    public String fetchNString(final String sql, final Object ... args) throws SQLException {
        return fetch(resultSet -> resultSet.getNString(1), sql, args);
    }

    /**
     * Fetches an SQLXML from the database
     * @param sql the sql which should be sent to the database
     * @param args the arguments which should be sent to the database
     * @return the requested SQLXML or null if nothing was returned by the database
     * @throws SQLException thrown when something exceptional happens
     */
    public SQLXML fetchSQLXML(final String sql, final Object ... args) throws SQLException {
        return fetch(resultSet -> resultSet.getSQLXML(1), sql, args);
    }

    /**
     * Fetches an array of bytes from the database
     * @param sql the sql which should be sent to the database
     * @param args the arguments which should be sent to the database
     * @return the requested array of bytes
     * @throws SQLException thrown when something exceptional happens
     */
    public byte[] fetchBytes(final String sql, final Object ... args) throws SQLException {
        return fetch(resultSet -> resultSet.getBytes(1), sql, args);
    }

    /**
     * Fetches a single long from the database
     * @param sql the sql which should be sent to the database
     * @param args the arguments which should be sent to the database
     * @return the requested long or null if nothing was returned by the database
     * @throws SQLException thrown when something exceptional happens
     */
    public Long fetchLong(final String sql, final Object ... args) throws SQLException {
        return fetch(resultSet -> resultSet.getLong(1), sql, args);
    }

    /**
     * Fetches a single float from the database
     * @param sql the sql which should be sent to the database
     * @param args the arguments which should be sent to the database
     * @return the requested float or null if nothing was returned by the database
     * @throws SQLException thrown when something exceptional happens
     */
    public Float fetchFloat(final String sql, final Object ... args) throws SQLException {
        return fetch(resultSet -> resultSet.getFloat(1), sql, args);
    }

    /**
     * Fetches a single double from the database
     * @param sql the sql which should be sent to the database
     * @param args the arguments which should be sent to the database
     * @return the requested double or null if nothing was returned by the database
     * @throws SQLException thrown when something exceptional happens
     */
    public Double fetchDouble(final String sql, final Object ... args) throws SQLException {
        return fetch(resultSet -> resultSet.getDouble(1), sql, args);
    }

    /**
     * Fetches a single big decimal from the database
     * @param sql the sql which should be sent to the database
     * @param args the arguments which should be sent to the database
     * @return the requested big decimal or null if nothing was returned by the database
     * @throws SQLException thrown when something exceptional happens
     */
    public BigDecimal fetchBigDecimal(final String sql, final Object ... args) throws SQLException {
        return fetch(resultSet -> resultSet.getBigDecimal(1), sql, args);
    }

    /**
     * Fetches a single date from the database
     * @param sql the sql which should be sent to the database
     * @param args the arguments which should be sent to the database
     * @return the requested date or null if nothing was returned by the database
     * @throws SQLException thrown when something exceptional happens
     */
    public Date fetchDate(final String sql, final Object ... args) throws SQLException {
        return fetch((InfluxResultSetHandler<Date>) resultSet -> resultSet.getDate(1), sql, args);
    }

    /**
     * Fetches a single boolean from the database
     * @param sql the sql which should be sent to the database
     * @param args the arguments which should be sent to the database
     * @return the requested boolean or null if nothing was returned by the database
     * @throws SQLException thrown when something exceptional happens
     */
    public Boolean fetchBoolean(final String sql, final Object ... args) throws SQLException {
        return fetch(resultSet -> resultSet.getBoolean(1), sql, args);
    }

    /**
     * Fetches a single Object from the database. Reflection must be used to infer the actual
     * data type which was returned.
     * @param sql the sql which should be sent to the database
     * @param args the arguments which should be sent to the database
     * @return the requested object or null if nothing was returned by the database
     * @throws SQLException thrown when something exceptional happens
     */
    public Object fetchObject(final String sql, final Object ... args) throws SQLException {
        return fetch(resultSet -> resultSet.getObject(1), sql, args);
    }

    /**
     * Attempts to bind the specified arguments to the specified statement
     * @param statement the statement to which the arguments should be bound
     * @param args the arguments which should be bound to the statement
     * @return the statement after the arguments have been bound to it
     * @throws SQLException thrown when something exceptional happens
     */
    public PreparedStatement bindArguments(final PreparedStatement statement, final Object ... args)
            throws SQLException {
        int index = 1;

        // Iterate through the arguments
        for (Object argument : args) {
            // Attempt to bind the argument to the query
            bindObject(statement, index, argument);
            index++;
        }

        return statement;
    }

    /**
     * Binds the specified object to the specified statement in the specified position. If a null object is specified,
     * then null will be bound to the statement instead
     * @param statement the statement to which the object should be bound
     * @param position the position in which the object should be bound
     * @param value the object which should be bound to the statement
     * @throws SQLException thrown when something exceptional happens
     */
    protected static void bindObject(final PreparedStatement statement, final int position, final Object value)
            throws SQLException {
        if (value == null) {
            statement.setNull(position, Types.NULL);
        } else if (value instanceof String) {
            statement.setString(position, (String) value);
        } else if (value instanceof Integer) {
            statement.setInt(position, (Integer) value);
        } else if (value instanceof Long) {
            statement.setLong(position, (Long) value);
        } else if (value instanceof Float) {
            statement.setFloat(position, (Float) value);
        } else if (value instanceof Double) {
            statement.setDouble(position, (Double) value);
        } else if (value instanceof Boolean) {
            statement.setBoolean(position, (Boolean) value);
        } else if (value instanceof Time) {
            statement.setTime(position, (Time) value);
        } else if (value instanceof Timestamp) {
            statement.setTimestamp(position, (Timestamp) value);
        } else if (value instanceof Date) {
            statement.setDate(position, new java.sql.Date(((Date)value).getTime()));
        } else if (value instanceof BigDecimal) {
            statement.setBigDecimal(position, (BigDecimal) value);
        } else if (value instanceof Short) {
            statement.setShort(position, (Short) value);
        } else if (value instanceof Byte) {
            statement.setByte(position, (Byte) value);
        } else if (value instanceof Array) {
            statement.setArray(position, (Array) value);
        } else if (value instanceof Blob) {
            statement.setBlob(position, (Blob) value);
        } else if (value instanceof NClob) {
            statement.setNClob(position, (NClob) value);
        } else if (value instanceof Clob) {
            statement.setClob(position, (Clob) value);
        } else if (value instanceof InputStream) {
            statement.setBinaryStream(position, (InputStream) value);
        } else if (value instanceof SQLXML) {
            statement.setSQLXML(position, (SQLXML) value);
        } else if (value instanceof Ref) {
            statement.setRef(position, (Ref) value);
        } else if (value instanceof byte[]) {
            statement.setBytes(position, (byte[]) value);
        } else {
            // Try this as a last resort, if we don't have an explicit way of
            // handling the type
            statement.setObject(position, value);
        }
    }

    /**
     * This method converts underscore delimited column names to camel case so that they can
     * be used to locate the appropriate setter method in the target entity. For example, the
     * column name my_column_name would become myColumnName.
     * @param columnName underscore separated column name
     * @return camel case equivalent of the underscore separated input value
     * @throws SQLException thrown then something exceptional happens
     */
    public static String toCamelCase(final String columnName) throws SQLException {
        String c = columnName.toLowerCase();

        // Leading underscores are not supported
        if (c.getBytes()[0] == '_') {
            throw new SQLException("Column names cannot begin with underscores");
        }

        // Return the unmodified column name, if the column name does not contain any underscores
        if (c.indexOf('_') == -1) {
            return c;
        }

        StringTokenizer st = new StringTokenizer(c.toLowerCase(), "_");
        StringBuilder result = new StringBuilder();
        boolean first = true;

        // Otherwise, iterate through the tokens doing our thing
        while (st.hasMoreTokens()) {
            String tmp = st.nextToken();

            if (first) {
                // Don't capitalize the first token
                result.append(tmp.toLowerCase());
                first = false;
            } else {
                result.append(String.valueOf(tmp.charAt(0)).toUpperCase())
                    .append(tmp.substring(1));
            }
        }

        // Return the camel case property name
        return result.toString();
    }

    /**
     *
     * @throws SQLException throw when something exceptional happens
     */
    public void begin() throws SQLException {
        // Throw an error if we attempt to being a transaction without committing or
        // rolling back the previous transaction
        if (transactionActive) {
            throw new SQLException("Cannot begin a new transaction because one is already active");
        }

        // Enable auto-commit. There is no "begin" method in the JDBC API for starting new transactions. Once you
        // set this boolean, then a new transaction will being with the execution of your next statement
        connection.setAutoCommit(false);

        // This only happens if the previous method call succeeds
        transactionActive = true;
    }

    /**
     * {@inheritDoc}
     */
    public void commit() throws SQLException {
        // Throw an error if we attempt to commit back a transaction without calling begin first
        if (!transactionActive) {
            throw new SQLException("Attempted to roll back a transaction when no transaction was active.");
        }

        // Attempt to commit the transaction
        connection.commit();
        connection.setAutoCommit(true);

        // This only happens if the previous method calls succeed
        transactionActive = false;
    }

    /**
     * {@inheritDoc}
     */
    public void rollback() throws SQLException {
        // Throw an error if we attempt to roll back a transaction without calling begin first
        if (!transactionActive) {
            throw new SQLException("Attempted to roll back a transaction when no transaction was active.");
        }

        // Attempt to roll back the transaction
        connection.rollback();
        connection.setAutoCommit(true);

        // This only happens if the previous method calls succeed
        transactionActive = false;
    }

    /**
     * {@inheritDoc}
     */
    public void close() throws SQLException {
        this.connection.close();
    }

    /**
     * {@inheritDoc}
     */
    public boolean isClosed() throws SQLException {
        return this.connection.isClosed();
    }

    /**
     * {@inheritDoc}
     */
    public InfluxDatabaseMetadata getMetaData() throws SQLException {
        return new InfluxDatabaseMetadata(this.connection.getMetaData());
    }

    /**
     * {@inheritDoc}
     */
    public void setReadOnly(boolean readOnly) throws SQLException {
        this.connection.setReadOnly(readOnly);
    }

    /**
     * {@inheritDoc}
     */
    public boolean isReadOnly() throws SQLException {
        return this.connection.isReadOnly();
    }

    /**
     * {@inheritDoc}
     */
    public void setCatalog(String catalog) throws SQLException {
        this.connection.setCatalog(catalog);
    }

    /**
     * {@inheritDoc}
     */
    public String getCatalog() throws SQLException {
        return this.connection.getCatalog();
    }

    /**
     * {@inheritDoc}
     */
    public void setTransactionIsolation(int level) throws SQLException {
        this.connection.setTransactionIsolation(level);
    }

    /**
     * {@inheritDoc}
     */
    public int getTransactionIsolation() throws SQLException {
        return this.connection.getTransactionIsolation();
    }

    /**
     * {@inheritDoc}
     */
    public SQLWarning getWarnings() throws SQLException {
        return this.connection.getWarnings();
    }

    /**
     * {@inheritDoc}
     */
    public void clearWarnings() throws SQLException {
        this.connection.clearWarnings();
    }

    /**
     * {@inheritDoc}
     */
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return this.connection.createStatement();
    }

    /**
     * {@inheritDoc}
     */
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return this.connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    /**
     * {@inheritDoc}
     */
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return this.connection.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    /**
     * {@inheritDoc}
     */
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return this.connection.getTypeMap();
    }

    /**
     * {@inheritDoc}
     */
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        this.connection.setTypeMap(map);
    }

    /**
     * {@inheritDoc}
     */
    public void setHoldability(int holdability) throws SQLException {
        this.connection.setHoldability(holdability);
    }

    /**
     * {@inheritDoc}
     */
    public int getHoldability() throws SQLException {
        return this.connection.getHoldability();
    }

    /**
     * {@inheritDoc}
     */
    public Savepoint setSavepoint() throws SQLException {
        return this.connection.setSavepoint();
    }

    /**
     * {@inheritDoc}
     */
    public Savepoint setSavepoint(String name) throws SQLException {
        return this.connection.setSavepoint(name);
    }

    /**
     * {@inheritDoc}
     */
    public void rollback(final Savepoint savepoint) throws SQLException {
        this.connection.rollback(savepoint);
    }

    /**
     * {@inheritDoc}
     */
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        this.connection.releaseSavepoint(savepoint);
    }

    /**
     * {@inheritDoc}
     */
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return this.connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    /**
     * {@inheritDoc}
     */
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        return this.connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    /**
     * {@inheritDoc}
     */
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        return this.connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    /**
     * {@inheritDoc}
     */
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return this.connection.prepareStatement(sql, autoGeneratedKeys);
    }

    /**
     * {@inheritDoc}
     */
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return this.connection.prepareStatement(sql, columnIndexes);
    }

    /**
     * {@inheritDoc}
     */
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return this.connection.prepareStatement(sql, columnNames);
    }

    /**
     * {@inheritDoc}
     */
    public Clob createClob() throws SQLException {
        return this.connection.createClob();
    }

    /**
     * {@inheritDoc}
     */
    public Blob createBlob() throws SQLException {
        return this.connection.createBlob();
    }

    /**
     * {@inheritDoc}
     */
    public NClob createNClob() throws SQLException {
        return this.connection.createNClob();
    }

    /**
     * {@inheritDoc}
     */
    public SQLXML createSQLXML() throws SQLException {
        return this.connection.createSQLXML();
    }

    /**
     * {@inheritDoc}
     */
    public boolean isValid(int timeout) throws SQLException {
        return this.connection.isValid(timeout);
    }

    /**
     * {@inheritDoc}
     */
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        this.connection.setClientInfo(name, value);
    }

    /**
     * {@inheritDoc}
     */
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
       this.connection.setClientInfo(properties);
    }

    /**
     * {@inheritDoc}
     */
    public String getClientInfo(String name) throws SQLException {
        return this.connection.getClientInfo(name);
    }

    /**
     * {@inheritDoc}
     */
    public Properties getClientInfo() throws SQLException {
        return this.connection.getClientInfo();
    }

    /**
     * {@inheritDoc}
     */
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return this.connection.createArrayOf(typeName, elements);
    }

    /**
     * {@inheritDoc}
     */
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return this.connection.createStruct(typeName, attributes);
    }

    /**
     * {@inheritDoc}
     */
    public void setSchema(String schema) throws SQLException {
        this.connection.setSchema(schema);
    }

    /**
     * {@inheritDoc}
     */
    public String getSchema() throws SQLException {
        return this.connection.getSchema();
    }

    /**
     * {@inheritDoc}
     */
    public void abort(Executor executor) throws SQLException {
        this.connection.abort(executor);
    }

    /**
     * {@inheritDoc}
     */
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        this.connection.setNetworkTimeout(executor, milliseconds);
    }

    /**
     * {@inheritDoc}
     */
    public int getNetworkTimeout() throws SQLException {
        return this.connection.getNetworkTimeout();
    }

    /**
     * {@inheritDoc}
     */
    public boolean getAutoCommit() throws SQLException {
        return this.connection.getAutoCommit();
    }

    /**
     * {@inheritDoc}
     */
    public Statement createStatement() throws SQLException {
        return this.connection.createStatement();
    }

    /**
     * {@inheritDoc}
     */
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return this.connection.prepareStatement(sql);
    }

    /**
     * {@inheritDoc}
     */
    public CallableStatement prepareCall(String sql) throws SQLException {
        return this.connection.prepareCall(sql);
    }

    /**
     * {@inheritDoc}
     */
    public String nativeSQL(String sql) throws SQLException {
        return this.connection.nativeSQL(sql);
    }

    /**
     * {@inheritDoc}
     */
    public void  setAutoCommit(final boolean autoCommit) throws SQLException {
        this.connection.setAutoCommit(autoCommit);
    }

    /**
     * {@inheritDoc}
     */
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return this.connection.unwrap(iface);
    }

    /**
     * {@inheritDoc}
     */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return this.connection.isWrapperFor(iface);
    }

    /**
     * Create a new query builder for the specified query.
     * @param sql The query which should be executed
     * @return An instance of {@link QueryBuilder}
     * @throws SQLException Thrown when something goes wrong.
     */
    public QueryBuilder query(final String sql) throws SQLException {
        return new QueryBuilder(this, sql);
    }

    /**
     * A simple query builder class which handles ordinal (not named) query parameters.
     */
    public static class QueryBuilder {
        private final PreparedStatement preparedStatement;
        private int position = 1;

        private QueryBuilder(final InfluxConnection connection, final String sql) throws SQLException {
            this.preparedStatement = connection.prepareStatement(sql);
        }

        public QueryBuilder addParam(Object value) throws SQLException {
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

        public Integer toInteger() throws SQLException {
            try (InfluxResultSet resultSet = fetch()) {
                return resultSet.getInt(1);
            }
        }

        public Short toShort() throws SQLException {
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

        <T> T fetch(final ResultSetHandler<T> handler, final String sql, final Object... arguments)
        throws SQLException;

        <T> T fetch(final ResultSetHandler<T> handler, final PreparedStatement statement, final Object... arguments)
        throws SQLException;

        List<Integer> fetchListInteger(final String sql, final Object... arguments) throws SQLException;

        List<Integer> fetchListInteger(final PreparedStatement statement, final Object... arguments)
        throws SQLException;

        List<Long> fetchListLong(final String sql, final Object... arguments) throws SQLException;

        List<Long> fetchListLong(final PreparedStatement statement, final Object... arguments) throws SQLException;

        <T> List<T> fetchAllEntity(final Class<T> clazz, final String sql, final Object... arguments)
        throws SQLException;

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

        <T> T fetchEntity(final T entity, final PreparedStatement statement, final Object... arguments)
        throws SQLException;

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
}
