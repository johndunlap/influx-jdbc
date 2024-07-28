# influx-jdbc

The simplest way to run SQL queries with Java.

Generally speaking, the vast majority of SQL database interactions fall into one of the following categories:
* Query returns nothing
* Query returns a single row with a single column
* Query returns a single row with multiple columns
* Query returns multiple rows with one or more columns

These are the core use cases which are handled by influx-jdbc.

# Features
* Small and very lightweight
* Query methods which eliminate JDBC boiler plate without getting in your way
* Query results are injected into java objects
* Compatible with existing JDBC applications

# Documentation
* [Javadoc](https://johndunlap.github.io/influx-jdbc/)

# Examples
For more working examples, look at the unit tests [here](https://github.com/johndunlap/influx-jdbc/tree/master/src/test/java/org.voidzero.influx.jdbc/test).

_You will need to have the JDBC HSQLDB driver(only necessary for demonstration purposes) on your classpath for the following examples to run. You can add this driver to your project by adding the following XML snippet to your Maven POM:_
```xml
<dependency>
    <groupId>hsqldb</groupId>
    <artifactId>hsqldb</artifactId>
    <version>1.8.0.10</version>
</dependency>
```
_Dependency coordinates for other build systems can be found [here](http://search.maven.org/#artifactdetails%7Chsqldb%7Chsqldb%7C1.8.0.10%7Cjar)_

```java

import org.voidzero.influx.jdbc.InfluxConnection;
import org.voidzero.influx.jdbc.SimpleConnection;

import java.sql.SQLException;
import java.util.Date;
import java.util.List;

public class Main {
    public static void main(String[] args) throws SQLException {
        InfluxConnection connection = InfluxJdbc.getConnection("jdbc:hsqldb:mem:test", "sa", "");
        connection.execute("create table users(\n" +
                "    id INTEGER not null,\n" +
                "    username char(25),\n" +
                "    password char(25),\n" +
                "    active BOOLEAN,\n" +
                "    last_active TIMESTAMP,\n" +
                "    PRIMARY KEY (id)\n" +
                ");"
        );

        // Add some data
        connection.execute(
                "insert into users(id, username, password, active, last_active) values(?,?,?,?,?)",
                1,
                "admin",
                "password",
                true,
                "1970-01-01 00:00:00"
        );
        connection.execute(
                "insert into users(id, username, password, active, last_active) values(?,?,?,?,?)",
                2,
                "bob.wiley",
                "password2",
                true,
                "1973-02-02 00:00:00"
        );

        String username = "bob.wiley";

        System.out.println("==== FETCH A SINGLE ROW WITH A SINGLE COLUMN ====");

        Long userId = connection.fetchLong(
                "select id from users where username = ?",
                username
        );

        System.out.println("User id for username " + username + " is " + userId);

        System.out.println("\n==== FETCH A SINGLE ROW WITH MULTIPLE COLUMNS ====");

        User user = connection.fetchEntity(
                User.class,
                "select * from users where id = ?",
                2
        );

        System.out.println("User before update: " + user);

        // ==== RUN A QUERY WHICH RETURNS NOTHING ====

        connection.execute(
                "update users set password = ? where id = ?",
                "password3",
                2
        );

        System.out.println("\n==== FETCH A SINGLE ROW INTO AN EXISTING ENTITY ====");

        connection.fetchEntity(
                user,
                "select password from users where id = ?",
                2
        );

        System.out.println("User after update: " + user);

        System.out.println("\n==== FETCH MULTIPLE ROWS WITH MULTIPLE COLUMNS ====");

        List<User> users = connection.fetchAllEntity(
                User.class,
                "select id, username, password, active, last_active from users"
        );

        for (User u : users) {
            System.out.println(u);
        }
    }

    public static class User {
        private Long id;
        private String username;
        private String password;
        private Boolean active;
        private Date lastActive;

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

        @Override
        public String toString() {
            return "User{" +
                    "id=" + id +
                    ", username='" + username + '\'' +
                    ", password='" + password + '\'' +
                    ", active=" + active +
                    ", lastActive=" + lastActive +
                    '}';
        }
    }
}
```
