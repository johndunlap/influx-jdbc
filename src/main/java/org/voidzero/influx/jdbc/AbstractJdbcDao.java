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

import java.sql.SQLException;

/**
 * @author <a href="mailto:john.david.dunlap@gmail.com">John D. Dunlap</a>
 */
public abstract class AbstractJdbcDao {

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

    /**
     * Connection to the database.
     */
    private final SimpleConnection connection;

    /**
     * Constructor for this object which accepts a reference to the database
     * connection which should be used to interact with the database.
     * @param connection reference to the database
     * connection which should be used to interact with the database
     */
    public AbstractJdbcDao(final SimpleConnection connection) {
        this.connection = connection;
    }

    /**
     * Returns a reference to the database connection which should be used
     * to interact with the database. This method is only exposed to child
     * classes because directly accessing it, from outside of a DAO, defeats
     * the purpose of having a DAO in the first place.
     * @return reference to the database
     * connection which should be used to interact with the database
     */
    protected SimpleConnection getConnection() {
        return connection;
    }

    /**
     *
     * @throws SQLException throw when something exceptional happens
     */
    public AbstractJdbcDao begin() throws SQLException {
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

        // Allow for method chaining
        return this;
    }

    public AbstractJdbcDao commit() throws SQLException {
        // Throw an error if we attempt to commit back a transaction without calling begin first
        if (!transactionActive) {
            throw new SQLException("Attempted to roll back a transaction when no transaction was active.");
        }

        // Attempt to commit the transaction
        connection.commit();
        connection.setAutoCommit(true);

        // This only happens if the previous method calls succeed
        transactionActive = false;

        // Allow for method chaining
        return this;
    }

    public AbstractJdbcDao rollback() throws SQLException {
        // Throw an error if we attempt to roll back a transaction without calling begin first
        if (!transactionActive) {
            throw new SQLException("Attempted to roll back a transaction when no transaction was active.");
        }

        // Attempt to roll back the transaction
        connection.rollback();
        connection.setAutoCommit(true);

        // This only happens if the previous method calls succeed
        transactionActive = false;

        // Allow for method chaining
        return this;
    }
}
