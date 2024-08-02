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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.sql.SQLException;
import java.util.Random;
import org.junit.Test;

/**
 * This verifies the transactional behavior of the begin, commit, and rollback methods.
 *
 * @author <a href="mailto:john.david.dunlap@gmail.com">John D. Dunlap</a>
 */
public class TransactionTest extends AbstractUnitTest {
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
            connection.begin();

            // Make sure that we start in a known state
            updatePasswordByUsername("admin", oldPassword, connection);

            // Re-Load the old password
            tmp = getPasswordByUsername("admin", connection);

            // Make sure that the password was actually set
            assertEquals(oldPassword, tmp);

            // Attempt to change the password again
            updatePasswordByUsername("admin", newPassword, connection);

            if (new Random().nextInt() - 1 != Integer.MAX_VALUE) {
                throw new SQLException("this should always be thrown");
            }

            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
        }

        String currentPassword = getPasswordByUsername("admin", connection);

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
            connection.begin();

            // Make sure that we start in a known state
            updatePasswordByUsername("admin", oldPassword, connection);

            // Re-Load the old password
            String tmp = getPasswordByUsername("admin", connection);

            // Make sure that the password was actually set
            assertEquals(oldPassword, tmp);

            // Attempt to change the password again
            updatePasswordByUsername("admin", newPassword, connection);

            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        }

        // Make sure that the new password was persisted
        assertEquals(
            newPassword,
            getPasswordByUsername("admin", connection)
        );
    }

    /**
     * Retreieve a password from the database by username.
     *
     * @param username The username which should be used to locate the password
     * @param connection The connection which should be used to talk to the database
     * @return The password of the specified user
     * @throws SQLException Thrown if something goes wrong
     */
    public static String getPasswordByUsername(final String username,
                                               final InfluxConnection connection) throws SQLException {
        return connection.fetchString(
                "select password from users where username = ?",
                username
        );
    }

    /**
     * Updates the password of the specified username.
     *
     * @param username The username of user whose password is being updated.
     * @param password The new password
     * @param connection The database connection which should be used to update the password
     * @return The new password
     * @throws SQLException Thrown if something goes wrong
     */
    public static String updatePasswordByUsername(final String username,
                                                  final String password,
                                                  final InfluxConnection connection) throws SQLException {
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
