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

import org.junit.Test;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.sql.SQLException;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * @author <a href="mailto:john.david.dunlap@gmail.com">John D. Dunlap</a>
 */
public class AbstractJdbcDaoTest extends AbstractUnitTest {
    protected static SimpleUserJdbcDao jdbcDao = new SimpleUserJdbcDao(connection);

    @Test
    public void testTransactionRollbackOnException() throws SQLException {
        String oldPassword = "";
        String newPassword = "";
        String tmp = "";

        // Repeat this if necessary
        while (oldPassword.equals(newPassword)) {
            oldPassword = new BigInteger(130, new SecureRandom()).toString(32);
            newPassword = new BigInteger(130, new SecureRandom()).toString(32);
        }

        try {
            jdbcDao.begin();

            // Make sure that we start in a known state
            jdbcDao.updatePasswordByUsername("admin", oldPassword);

            // Re-Load the old password
            tmp = jdbcDao.getPasswordByUsername("admin");

            // Make sure that the password was actually set
            assertEquals(oldPassword, tmp);

            // Attempt to change the password again
            jdbcDao.updatePasswordByUsername("admin", newPassword);

            if (new Random().nextInt() - 1 != Integer.MAX_VALUE) {
                throw new SQLException("this should always be thrown");
            }

            jdbcDao.commit();
        } catch(SQLException e) {
            jdbcDao.rollback();
        }

        String currentPassword = jdbcDao.getPasswordByUsername("admin");

        // Make sure that the new password was never persisted
        assertNotEquals(newPassword, currentPassword);
    }

    @Test
    public void testTransactionCommitWithoutException() throws SQLException {
        String oldPassword = "";
        String newPassword = "";

        // Repeat this if necessary
        while (oldPassword.equals(newPassword)) {
            oldPassword = new BigInteger(130, new SecureRandom()).toString(32);
            newPassword = new BigInteger(130, new SecureRandom()).toString(32);
        }

        try {
            jdbcDao.begin();

            // Make sure that we start in a known state
            jdbcDao.updatePasswordByUsername("admin", oldPassword);

            // Re-Load the old password
            String tmp = jdbcDao.getPasswordByUsername("admin");

            // Make sure that the password was actually set
            assertEquals(oldPassword, tmp);

            // Attempt to change the password again
            jdbcDao.updatePasswordByUsername("admin", newPassword);

            jdbcDao.commit();
        } catch(SQLException e) {
            jdbcDao.rollback();
            throw e;
        }

        // Make sure that the new password was persisted
        assertEquals(
            newPassword,
            jdbcDao.getPasswordByUsername("admin")
        );
    }

    public static class SimpleUserJdbcDao extends AbstractJdbcDao {
        /**
         * Constructor for this object which accepts a reference to the database
         * connection which should be used to interact with the database.
         *
         * @param connection reference to the database
         *                   connection which should be used to interact with the database
         */
        public SimpleUserJdbcDao(final SimpleConnection connection) {
            super(connection);
        }

        public String getPasswordByUsername(final String username) throws SQLException {
            return connection.fetchString(
                "select password from users where username = ?",
                username
            );
        }

        public String updatePasswordByUsername(final String username, final String password) throws SQLException {
            connection.execute(
                "update users set password = ? where username = ?",
                password,
                username
            );
            return connection.fetchString(
                "select password from users where username = ?",
                username
            );
        }
    }
}
