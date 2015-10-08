package cyeagy.dorm;

import org.h2.jdbcx.JdbcDataSource;
import org.h2.tools.Server;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class SqlSupportTest {
    private SqlSupport SQL_SUPPORT = SqlSupport.fromDefaults();
    private static Server server;
    private static Connection connection;

    @BeforeClass
    public static void setUpClass() throws Exception {
        server = Server.createTcpServer().start();
        Class.forName("org.h2.Driver");
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setUrl("jdbc:h2:mem:");
        connection = dataSource.getConnection();
        String create = new Scanner(DormTest.class.getResourceAsStream("/sql/test_create.sql"), "UTF-8").useDelimiter("\\A").next();
        Statement statement = connection.createStatement();
        statement.execute(create);
        statement.close();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        connection.close();
        server.shutdown();
    }

    private void truncateAndInsert() throws SQLException {
        Statement statement = connection.createStatement();
        String insert = new Scanner(DormTest.class.getResourceAsStream("/sql/test_insert.sql"), "UTF-8").useDelimiter("\\A").next();
        statement.execute(insert);
        statement.close();
    }

    @Test
    public void testSelectList() throws Exception {
        truncateAndInsert();
        String select = "SELECT test_key, some_long, some_int, some_string, some_dtm FROM test_bean WHERE test_key > :test_key";
        final List<TestBean> testBeans = SQL_SUPPORT.queryList(connection, select, ps -> ps.setLong("test_key", 1),
                (rs, i) -> new TestBean(rs.getLong("test_key"), rs.getLong("some_long"), rs.getInt("some_int"), rs.getString("some_string"), rs.getTimestamp("some_dtm")));
        assertNotNull(testBeans);
        assertThat(testBeans, not(empty()));
        testBeans.forEach(bean -> assertThat(bean.getTestKey(), greaterThan(1l)));
    }

    @Test
    public void testSelectMap() throws Exception {
        truncateAndInsert();
        String select = "SELECT test_key, some_long, some_int, some_string, some_dtm FROM test_bean WHERE test_key > :test_key";
        final Map<Long, TestBean> testBeans = SQL_SUPPORT.queryMapped(connection, select, ps -> ps.setLong("test_key", 1),
                (rs, i) -> new TestBean(rs.getLong("test_key"), rs.getLong("some_long"), rs.getInt("some_int"), rs.getString("some_string"), rs.getTimestamp("some_dtm")),
                (rs1, idx) -> rs1.getLong("test_key"));
        assertNotNull(testBeans);
        assertThat(testBeans.entrySet(), not(empty()));
        testBeans.keySet().forEach(key -> assertThat(key, greaterThan(1l)));
    }

    @Test
    public void testFull() throws Exception {
        final Timestamp now = Timestamp.from(Instant.now());

        String insert = "INSERT INTO test_bean (some_long, some_int, some_string, some_dtm) VALUES (:some_long, :some_int, :some_string, :some_dtm)";
        final Long key = SQL_SUPPORT.insert(connection, insert, ps -> {
            ps.setLong("some_long", Long.MAX_VALUE);
            ps.setInt("some_int", Integer.MAX_VALUE);
            ps.setString("some_string", "test string");
            ps.setTimestamp("some_dtm", now);
        });
        assertNotNull(key);
        assertThat(key, not(equalTo(0)));

        String select = "SELECT * FROM test_bean WHERE test_key = :test_key";
        final TestBean bean = SQL_SUPPORT.query(connection, select, ps -> ps.setLong("test_key", key),
                (rs, i) -> new TestBean(rs.getLong("test_key"), rs.getLong("some_long"), rs.getInt("some_int"), rs.getString("some_string"), rs.getTimestamp("some_dtm")));
        assertNotNull(bean);
        assertThat(bean.getSomeLong(), equalTo(Long.MAX_VALUE));
        assertThat(bean.getSomeInt(), equalTo(Integer.MAX_VALUE));
        assertThat(bean.getSomeString(), equalTo("test string"));
        assertThat(bean.getSomeDtm(), equalTo(now));

        String update = "UPDATE test_bean SET some_long = :some_long, some_int = :some_int, some_string = :some_string, some_dtm = :some_dtm WHERE test_key = :test_key";
        final int rowsUpdated = SQL_SUPPORT.update(connection, update, ps -> {
            ps.setLong("some_long", bean.getSomeLong());
            ps.setInt("some_int", bean.getSomeInt());
            ps.setString("some_string", "changed string");
            ps.setTimestamp("some_dtm", bean.getSomeDtm());
            ps.setLong("test_key", key);
        });
        assertThat(rowsUpdated, equalTo(1));

        final TestBean updateBean = SQL_SUPPORT.query(connection, select, ps -> ps.setLong("test_key", key),
                (rs, i) -> new TestBean(rs.getLong("test_key"), rs.getLong("some_long"), rs.getInt("some_int"), rs.getString("some_string"), rs.getTimestamp("some_dtm")));
        assertNotNull(updateBean);
        assertThat(updateBean.getSomeString(), equalTo("changed string"));

        String delete = "DELETE FROM test_bean WHERE test_key = :test_key";
        final int rowsDeleted = SQL_SUPPORT.update(connection, delete, ps -> ps.setLong("test_key", key));
        assertThat(rowsDeleted, equalTo(1));

        final TestBean deleteBean = SQL_SUPPORT.query(connection, select, ps -> ps.setLong("test_key", key),
                (rs, i) -> new TestBean(rs.getLong("test_key"), rs.getLong("some_long"), rs.getInt("some_int"), rs.getString("some_string"), rs.getTimestamp("some_dtm")));
        assertNull(deleteBean);
    }

    @Test
    public void testFullBuilder() throws Exception {
        final Timestamp now = Timestamp.from(Instant.now());

        String insert = "INSERT INTO test_bean (some_long, some_int, some_string, some_dtm) VALUES (:some_long, :some_int, :some_string, :some_dtm)";
        final Long key = SQL_SUPPORT.builder(insert)
                .queryBinding(ps -> {
                    ps.setLong("some_long", Long.MAX_VALUE);
                    ps.setInt("some_int", Integer.MAX_VALUE);
                    ps.setString("some_string", "test string");
                    ps.setTimestamp("some_dtm", now);
                })
                .executeInsert(connection);
        assertNotNull(key);
        assertThat(key, not(equalTo(0)));

        String select = "SELECT * FROM test_bean WHERE test_key = :test_key";
        final TestBean bean = SQL_SUPPORT.builder(select)
                .queryBinding(ps -> ps.setLong("test_key", key))
                .resultMapping((rs, i) -> new TestBean(rs.getLong("test_key"), rs.getLong("some_long"), rs.getInt("some_int"), rs.getString("some_string"), rs.getTimestamp("some_dtm")))
                .executeQuery(connection);
        assertNotNull(bean);
        assertThat(bean.getSomeLong(), equalTo(Long.MAX_VALUE));
        assertThat(bean.getSomeInt(), equalTo(Integer.MAX_VALUE));
        assertThat(bean.getSomeString(), equalTo("test string"));
        assertThat(bean.getSomeDtm(), equalTo(now));

        String update = "UPDATE test_bean SET some_long = :some_long, some_int = :some_int, some_string = :some_string, some_dtm = :some_dtm WHERE test_key = :test_key";
        final int rowsUpdated = SQL_SUPPORT.builder(update)
                .queryBinding(ps -> {
                    ps.setLong("some_long", bean.getSomeLong());
                    ps.setInt("some_int", bean.getSomeInt());
                    ps.setString("some_string", "changed string");
                    ps.setTimestamp("some_dtm", bean.getSomeDtm());
                    ps.setLong("test_key", key);
                })
                .executeUpdate(connection);
        assertThat(rowsUpdated, equalTo(1));

        final TestBean updateBean = SQL_SUPPORT.builder(select)
                .queryBinding(ps -> ps.setLong("test_key", key))
                .resultMapping(rs -> new TestBean(rs.getLong("test_key"), rs.getLong("some_long"), rs.getInt("some_int"), rs.getString("some_string"), rs.getTimestamp("some_dtm")))
                .executeQuery(connection);
        assertNotNull(updateBean);
        assertThat(updateBean.getSomeString(), equalTo("changed string"));

        String delete = "DELETE FROM test_bean WHERE test_key = :test_key";
        final int rowsDeleted = SQL_SUPPORT.builder(delete)
                .queryBinding(ps -> ps.setLong("test_key", key))
                .executeUpdate(connection);
        assertThat(rowsDeleted, equalTo(1));

        final TestBean deleteBean = SQL_SUPPORT.builder(select)
                .queryBinding(ps -> ps.setLong("test_key", key))
                .resultMapping(rs -> new TestBean(rs.getLong("test_key"), rs.getLong("some_long"), rs.getInt("some_int"), rs.getString("some_string"), rs.getTimestamp("some_dtm")))
                .executeQuery(connection);
        assertNull(deleteBean);
    }
}