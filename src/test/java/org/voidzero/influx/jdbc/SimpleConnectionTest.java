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

import org.voidzero.influx.jdbc.entity.User;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author <a href="mailto:john.david.dunlap@gmail.com">John D. Dunlap</a>
 */
public class SimpleConnectionTest extends AbstractUnitTest {
    @Test
    public void testExecuteMethod() throws SQLException {
        String oldPassword = "password";
        String newPassword = "new34password";

        // Change the password
        connection.execute("update users set password = ? where username = ?", newPassword, "admin");
        assertEquals(newPassword, connection.fetchString("select password from users where username = ?", "admin"));

        // Change the password back
        connection.execute("update users set password = ? where username = ?", oldPassword, "admin");
        assertEquals(oldPassword, connection.fetchString("select password from users where username = ?", "admin"));
    }

    @Test
    public void testFetchStringMethod() throws SQLException {
        String username = connection.fetchString("select username from users where id = ?", 1);
        assertEquals(username, "admin");
    }

    @Test
    public void testFetchObjectMethod() throws SQLException {
        Object object = connection.fetchObject("select username from users where id = ?", 1);
        assertEquals(String.class, object.getClass());

        object = connection.fetchObject("select id from users where username = ?", "admin");
        assertEquals(Integer.class, object.getClass());
    }

    @Test
    public void testFetchIntegerMethod() throws SQLException {
        Integer id = connection.fetchInt("select id from users where username = ?", "admin");
        assertEquals(id, Integer.valueOf(1));
    }

    @Test
    public void testFetchLongMethod() throws SQLException {
        Long id = connection.fetchLong("select id from users where username = ?", "admin");
        assertEquals(id, Long.valueOf(1));
    }

    @Test
    public void testFetchBooleanMethod() throws SQLException {
        Boolean active = connection.fetchBoolean("select active from users where username = ?", "admin");
        assertEquals(active, true);
    }

    @Test
    public void testFetchFloatMethod() throws SQLException {
        Float balance = connection.fetchFloat("select balance from users where username = ?", "admin");
        assertEquals(balance, Float.valueOf(1345.23f));
    }

    @Test
    public void testFetchDoubleMethod() throws SQLException {
        Double balance = connection.fetchDouble("select balance from users where username = ?", "admin");
        assertEquals(balance, Double.valueOf(1345.23));
    }

    @Test
    public void testFetchBigDecimalMethod() throws SQLException {
        BigDecimal balance = connection.fetchBigDecimal("select balance from users where username = ?", "admin");
        assertEquals(Double.valueOf(balance.doubleValue()), Double.valueOf(new BigDecimal("1345.23").doubleValue()));
    }

    @Test
    public void testFetchDateMethod() throws ParseException, SQLException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date lastActive = connection.fetchDate("select last_active from users where username = ?", "admin");
        assertEquals(lastActive, formatter.parse("1970-01-01 00:00:00"));
    }

    @Test
    public void testFetchEntityMethod() throws ParseException, SQLException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        User user = connection.fetchEntity(
            User.class,
            "select id, username, password, active, last_active, balance from users where id = ?",
            1
        );

        assertEquals(user.getId(), Long.valueOf(1));
        assertEquals(user.getUsername(), "admin");
        assertEquals(user.getPassword(), "password");
        assertEquals(user.getActive(), true);
        assertEquals(user.getLastActive(), formatter.parse("1970-01-01 00:00:00"));
        assertEquals(Double.valueOf(user.getBalance().doubleValue()), Double.valueOf(new BigDecimal("1345.23").doubleValue()));

        // Now attempt to override a value in the entity
        connection.fetchEntity(
            user,
            "select password from users where id = ?",
            2
        );

        // Verify that the password attribute has been overridden and that all other attributes have
        // remained the same
        assertEquals(user.getId(), Long.valueOf(1));
        assertEquals(user.getUsername(), "admin");
        assertEquals(user.getPassword(), "password2");
        assertEquals(user.getActive(), true);
        assertEquals(user.getLastActive(), formatter.parse("1970-01-01 00:00:00"));
        assertEquals(Double.valueOf(user.getBalance().doubleValue()), Double.valueOf(new BigDecimal("1345.23").doubleValue()));
    }

    @Test
    public void testFetchMapMethod() throws ParseException, SQLException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Map<String, Object> user = connection.fetchMap(
            "select id, username, password, active, last_active, balance from users where id = ?",
            1
        );

        assertNotNull(user);

        assertEquals(Long.valueOf((Integer) user.get("id")), Long.valueOf(1));
        assertEquals(user.get("username"), "admin");
        assertEquals(user.get("password"), "password");
        assertEquals(user.get("active"), true);
        assertEquals(new Date(((java.sql.Timestamp) user.get("last_active")).getTime()), formatter.parse("1970-01-01 00:00:00"));
        assertEquals(Double.valueOf(((BigDecimal) user.get("balance")).doubleValue()), Double.valueOf(new BigDecimal("1345.23").doubleValue()));
    }

    @Test
    public void testFetchListIntegerMethod() throws SQLException {
        List<Integer> list = connection.fetchListInteger(
            "select id from users order by id asc"
        );

        assertEquals(2, list.size());
        assertEquals(Integer.valueOf(1), list.get(0));
        assertEquals(Integer.valueOf(2), list.get(1));
    }

    @Test
    public void testFetchLongMethodWithQueryWhichReturnsNothing() throws SQLException {
        Long l = connection.fetchLong(
            "select id from users where username = ?",
            "999999"
        );

        assertNull(l);
    }

    @Test
    public void testFetchListLongMethod() throws SQLException {
        List<Long> list = connection.fetchListLong(
            "select id from users order by id asc"
        );

        assertEquals(2, list.size());
        assertEquals(Long.valueOf(1), list.get(0));
        assertEquals(Long.valueOf(2), list.get(1));
    }

    @Test
    public void testFetchListLongMethodWithQueryWhichReturnsNothing() throws SQLException {
        List<Long> list = connection.fetchListLong(
            "select id from users where id < 0 order by id asc"
        );

        assertNotNull(list);
        assertEquals(0, list.size());
    }

    @Test
    public void testFetchListIntegerMethodWithQueryWhichReturnsNothing() throws SQLException {
        List<Integer> list = connection.fetchListInteger(
            "select id from users where id < 0 order by id asc"
        );

        assertNotNull(list);
        assertEquals(0, list.size());
    }

    @Test
    public void testFetchAllMapMethod() throws ParseException, SQLException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        List<Map<String, Object>> users = connection.fetchAllMap(
            "select id, username, password, active, last_active, balance from users order by id asc"
        );

        assertEquals(Integer.valueOf(2), (Integer) users.size());

        Map<String, Object> user = users.get(0);
        assertEquals(Long.valueOf((Integer) user.get("id")), Long.valueOf(1));
        assertEquals(user.get("username"), "admin");
        assertEquals(user.get("password"), "password");
        assertEquals(user.get("active"), true);
        assertEquals(new Date(((java.sql.Timestamp) user.get("last_active")).getTime()), formatter.parse("1970-01-01 00:00:00"));
        assertEquals(Double.valueOf(((BigDecimal) user.get("balance")).doubleValue()), Double.valueOf(new BigDecimal("1345.23").doubleValue()));

        user = users.get(1);
        assertEquals(Long.valueOf((Integer) user.get("id")), Long.valueOf(2));
        assertEquals(user.get("username"), "bob.wiley");
        assertEquals(user.get("password"), "password2");
        assertEquals(user.get("active"), true);
        assertEquals(new Date(((java.sql.Timestamp) user.get("last_active")).getTime()), formatter.parse("1973-02-02 00:00:00"));
        assertEquals(Double.valueOf(((BigDecimal) user.get("balance")).doubleValue()), Double.valueOf(new BigDecimal("564.77").doubleValue()));
    }

    @Test
    public void testFetchAllEntityMethod() throws ParseException, SQLException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        List<User> users = connection.fetchAllEntity(
            User.class,
            "select id, username, password, active, last_active, balance from users order by id asc"
        );

        assertEquals(Integer.valueOf(2), (Integer) users.size());

        User user = users.get(0);
        assertEquals(user.getId(), Long.valueOf(1));
        assertEquals(user.getUsername(), "admin");
        assertEquals(user.getPassword(), "password");
        assertEquals(user.getActive(), true);
        assertEquals(user.getLastActive(), formatter.parse("1970-01-01 00:00:00"));
        assertEquals(Double.valueOf(user.getBalance().doubleValue()), Double.valueOf(new BigDecimal("1345.23").doubleValue()));

        user = users.get(1);
        assertEquals(user.getId(), Long.valueOf(2));
        assertEquals(user.getUsername(), "bob.wiley");
        assertEquals(user.getPassword(), "password2");
        assertEquals(user.getActive(), true);
        assertEquals(user.getLastActive(), formatter.parse("1973-02-02 00:00:00"));
        assertEquals(Double.valueOf(user.getBalance().doubleValue()), Double.valueOf(new BigDecimal("564.77").doubleValue()));
    }

    @Test
    public void testFetchAllEntityMap() throws ParseException, SQLException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Map<String, User> users = connection.fetchAllEntityMap(
            User.class,
            "username",
            "select id, username, password, active, last_active, balance from users order by id asc"
        );

        assertEquals(Integer.valueOf(2), (Integer) users.size());

        User user = users.get("admin");
        assertEquals(user.getId(), Long.valueOf(1));
        assertEquals(user.getUsername(), "admin");
        assertEquals(user.getPassword(), "password");
        assertEquals(user.getActive(), true);
        assertEquals(user.getLastActive(), formatter.parse("1970-01-01 00:00:00"));
        assertEquals(Double.valueOf(user.getBalance().doubleValue()), Double.valueOf(new BigDecimal("1345.23").doubleValue()));

        user = users.get("bob.wiley");
        assertEquals(user.getId(), Long.valueOf(2));
        assertEquals(user.getUsername(), "bob.wiley");
        assertEquals(user.getPassword(), "password2");
        assertEquals(user.getActive(), true);
        assertEquals(user.getLastActive(), formatter.parse("1973-02-02 00:00:00"));
        assertEquals(Double.valueOf(user.getBalance().doubleValue()), Double.valueOf(new BigDecimal("564.77").doubleValue()));
    }

    @Test
    public void testToCamelCaseMethod() throws SQLException {
        assertEquals("myColumnName", connection.toCamelCase("my_column_name"));
        assertEquals("thisIsATest", connection.toCamelCase("this_is_a_test"));
        assertEquals("test", connection.toCamelCase("test"));
        assertEquals("test", connection.toCamelCase("tEst"));

        // Some databases are case-insensitive, so we can't rely on the case
        // of the column name
        assertEquals("myColumnName", connection.toCamelCase("MY_COLUMN_NAME"));
        assertEquals("myColumnName", connection.toCamelCase("My_CoLuMn_NaMe"));
        assertEquals("thisIsATest", connection.toCamelCase("THIS_IS_A_TEST"));
    }
}
