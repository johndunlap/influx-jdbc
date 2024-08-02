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

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.voidzero.influx.jdbc.InfluxConnection.toCamelCase;

/**
 * @author <a href="mailto:john.david.dunlap@gmail.com">John D. Dunlap</a>
 */
public class InfluxConnectionTest extends AbstractUnitTest {
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

        String sql = "select id, username, password, active, last_active, balance from users where id = ?";

        User user = connection.query(sql)
                .addParam(1)
                .fetchEntity(User.class);

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
        assertEquals("myColumnName", toCamelCase("my_column_name"));
        assertEquals("thisIsATest", toCamelCase("this_is_a_test"));
        assertEquals("test", toCamelCase("test"));
        assertEquals("test", toCamelCase("tEst"));

        // Some databases are case-insensitive, so we can't rely on the case
        // of the column name
        assertEquals("myColumnName", toCamelCase("MY_COLUMN_NAME"));
        assertEquals("myColumnName", toCamelCase("My_CoLuMn_NaMe"));
        assertEquals("thisIsATest", toCamelCase("THIS_IS_A_TEST"));
    }

    @Test
    public void testMetaData() throws SQLException {
        InfluxDatabaseMetadata databaseMetadata = connection.getMetaData();
        TableMetadata tableMetadata = databaseMetadata.getTable("public", "users");
        assertNotNull(tableMetadata);
        assertEquals("PUBLIC", tableMetadata.getSchema());
        assertEquals("USERS", tableMetadata.getTableName());
        assertEquals(6, tableMetadata.getColumns().size());
        assertEquals("ID", tableMetadata.getColumns().get(0).getColumnName());
        assertEquals("INTEGER", tableMetadata.getColumns().get(0).getTypeName());
        assertEquals("USERNAME", tableMetadata.getColumns().get(1).getColumnName());
        assertEquals("VARCHAR", tableMetadata.getColumns().get(1).getTypeName());
        assertEquals("PASSWORD", tableMetadata.getColumns().get(2).getColumnName());
        assertEquals("VARCHAR", tableMetadata.getColumns().get(2).getTypeName());
        assertEquals("ACTIVE", tableMetadata.getColumns().get(3).getColumnName());
        assertEquals("BOOLEAN", tableMetadata.getColumns().get(3).getTypeName());
        assertEquals("LAST_ACTIVE", tableMetadata.getColumns().get(4).getColumnName());
        assertEquals("TIMESTAMP", tableMetadata.getColumns().get(4).getTypeName());
        assertEquals("BALANCE", tableMetadata.getColumns().get(5).getColumnName());
        assertEquals("NUMERIC", tableMetadata.getColumns().get(5).getTypeName());
    }

    @Test
    public void testResultSetToCsvMethod() throws SQLException {
        String sql = "select id, username, password, active, last_active, balance from users order by id asc";
        String expected = "ID,USERNAME,PASSWORD,ACTIVE,LAST_ACTIVE,BALANCE\n" +
                "1,admin,password,TRUE,1970-01-01 00:00:00.000000,1345.23\n" +
                "2,bob.wiley,password2,TRUE,1973-02-02 00:00:00.000000,564.77\n";
        InfluxResultSet resultSet = connection.fetch(sql);
        assertEquals(expected, resultSet.toCsv().toString());
    }

    /**
     * @author <a href="mailto:john.david.dunlap@gmail.com">John D. Dunlap</a>
     */
    private static class User {
        private Long id;
        private String username;
        private String password;
        private Boolean active;
        private Date lastActive;
        private BigDecimal balance;

        public User() {
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public Boolean getActive() {
            return active;
        }

        public void setActive(Boolean active) {
            this.active = active;
        }

        public Date getLastActive() {
            return lastActive;
        }

        public void setLastActive(Date lastActive) {
            this.lastActive = lastActive;
        }

        public BigDecimal getBalance() {
            return balance;
        }

        public void setBalance(BigDecimal balance) {
            this.balance = balance;
        }

        @Override
        public String toString() {
            return "User{" +
                    "id=" + id +
                    ", username='" + username + '\'' +
                    ", password='" + password + '\'' +
                    ", active=" + active +
                    ", lastActive=" + lastActive +
                    ", balance=" + balance +
                    '}';
        }
    }
}
