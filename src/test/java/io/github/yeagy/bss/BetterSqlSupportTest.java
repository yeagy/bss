package io.github.yeagy.bss;

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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class BetterSqlSupportTest {
    private static BetterSqlSupport SQL_SUPPORT = BetterSqlSupport.fromDefaults();
    private static Server server;
    private static Connection connection;

    private static final ResultMapping<TestBean> TEST_BEAN_RESULT_MAPPING = rs -> new TestBean(rs.getLong("test_key"), rs.getLong("some_long"), rs.getInt("some_int"), rs.getString("some_string"), rs.getTimestamp("some_dtm"), 0.0, TestBean.Status.valueOf(rs.getString("some_enum")));

    @BeforeClass
    public static void setUpClass() throws Exception {
        server = Server.createTcpServer().start();
        Class.forName("org.h2.Driver");
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setUrl("jdbc:h2:mem:");
        connection = dataSource.getConnection();
        String create = new Scanner(BetterSqlMapperTest.class.getResourceAsStream("/sql/test_create.sql"), "UTF-8").useDelimiter("\\A").next();
        Statement statement = connection.createStatement();
        statement.execute(create);
        statement.close();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        connection.close();
        server.shutdown();
    }

    static void truncateAndInsert() throws SQLException {
        Statement statement = connection.createStatement();
        String insert = new Scanner(BetterSqlMapperTest.class.getResourceAsStream("/sql/test_insert.sql"), "UTF-8").useDelimiter("\\A").next();
        statement.execute(insert);
        statement.close();
    }

    @Test
    public void testSelectList() throws Exception {
        truncateAndInsert();
        String select = "SELECT test_key, some_long, some_int, some_string, some_dtm, some_enum FROM test_bean WHERE test_key > :test_key";
        final List<TestBean> testBeans = SQL_SUPPORT.queryList(connection, select, ps -> ps.setLong("test_key", 1), TEST_BEAN_RESULT_MAPPING);
        assertNotNull(testBeans);
        assertThat(testBeans, not(empty()));
        testBeans.forEach(bean -> assertThat(bean.getTestKey(), greaterThan(1L)));
    }

    @Test
    public void testSelectListIn() throws Exception {
        truncateAndInsert();
        String select = "SELECT test_key, some_long, some_int, some_string, some_dtm, some_enum FROM test_bean WHERE test_key IN (:test_keys)";
        final List<TestBean> testBeans = SQL_SUPPORT.queryList(connection, select, ps -> ps.setArray("test_keys", Arrays.asList(2L, 3L, 4L, 5L)), TEST_BEAN_RESULT_MAPPING);
        assertNotNull(testBeans);
        assertThat(testBeans, not(empty()));
        testBeans.forEach(bean -> assertThat(bean.getTestKey(), greaterThan(1L)));
    }

    @Test
    public void testSelectMap() throws Exception {
        truncateAndInsert();
        String select = "SELECT test_key, some_long, some_int, some_string, some_dtm, some_enum FROM test_bean WHERE test_key > :test_key";
        final Map<Long, TestBean> testBeans = SQL_SUPPORT.queryMap(connection, select, ps -> ps.setLong("test_key", 1), TEST_BEAN_RESULT_MAPPING, rs -> rs.getLong("test_key"));
        assertNotNull(testBeans);
        assertThat(testBeans.keySet().size(), equalTo(4));
        testBeans.keySet().forEach(key -> assertThat(key, greaterThan(1L)));
    }

    @Test
    public void testSelectMultiMap() throws Exception {
        truncateAndInsert();
        String select = "SELECT test_key, some_long, some_int, some_string, some_dtm, some_enum FROM test_bean WHERE test_key > :test_key";
        final Map<Integer, List<TestBean>> testBeans = SQL_SUPPORT.queryMultiMap(connection, select, ps -> ps.setLong("test_key", 1), TEST_BEAN_RESULT_MAPPING, rs -> rs.getInt("some_int"));
        assertNotNull(testBeans);
        assertThat(testBeans.keySet().size(), equalTo(3));
        testBeans.keySet().forEach(key -> assertThat(key, greaterThan(1)));
        assertThat(testBeans.get(124).size(), equalTo(2));
    }

    @Test
    public void testFull() {
        final Timestamp now = Timestamp.from(Instant.now());

        String insert = "INSERT INTO test_bean (some_long, some_int, some_string, some_dtm, some_enum) VALUES (:some_long, :some_int, :some_string, :some_dtm, :some_enum)";
        final Long key = SQL_SUPPORT.insert(connection, insert, ps -> {
            ps.setLong("some_long", Long.MAX_VALUE);
            ps.setInt("some_int", Integer.MAX_VALUE);
            ps.setString("some_string", "test string");
            ps.setTimestamp("some_dtm", now);
            ps.setString("some_enum", TestBean.Status.OFF.name());
        });
        assertNotNull(key);
        assertThat(key, not(equalTo(0)));

        String select = "SELECT * FROM test_bean WHERE test_key = :test_key";
        final TestBean bean = SQL_SUPPORT.query(connection, select, ps -> ps.setLong("test_key", key), TEST_BEAN_RESULT_MAPPING);
        assertNotNull(bean);
        assertThat(bean.getSomeLong(), equalTo(Long.MAX_VALUE));
        assertThat(bean.getSomeInt(), equalTo(Integer.MAX_VALUE));
        assertThat(bean.getSomeString(), equalTo("test string"));
        assertThat(bean.getSomeDtm(), equalTo(now));

        String update = "UPDATE test_bean SET some_long = :some_long, some_int = :some_int, some_string = :some_string, some_dtm = :some_dtm, some_enum = :some_enum WHERE test_key = :test_key";
        final int rowsUpdated = SQL_SUPPORT.update(connection, update, ps -> {
            ps.setLong("some_long", bean.getSomeLong());
            ps.setInt("some_int", bean.getSomeInt());
            ps.setString("some_string", "changed string");
            ps.setTimestamp("some_dtm", bean.getSomeDtm());
            ps.setLong("test_key", key);
            ps.setString("some_enum", bean.getSomeEnum().name());
        });
        assertThat(rowsUpdated, equalTo(1));

        final TestBean updateBean = SQL_SUPPORT.query(connection, select, ps -> ps.setLong("test_key", key), TEST_BEAN_RESULT_MAPPING);
        assertNotNull(updateBean);
        assertThat(updateBean.getSomeString(), equalTo("changed string"));

        String delete = "DELETE FROM test_bean WHERE test_key = :test_key";
        final int rowsDeleted = SQL_SUPPORT.update(connection, delete, ps -> ps.setLong("test_key", key));
        assertThat(rowsDeleted, equalTo(1));

        final TestBean deleteBean = SQL_SUPPORT.query(connection, select, ps -> ps.setLong("test_key", key), TEST_BEAN_RESULT_MAPPING);
        assertNull(deleteBean);
    }

    @Test
    public void testFullBuilder() {
        final Timestamp now = Timestamp.from(Instant.now());

        String insert = "INSERT INTO test_bean (some_long, some_int, some_string, some_dtm, some_enum) VALUES (:some_long, :some_int, :some_string, :some_dtm, :some_enum)";
        final Long key = SQL_SUPPORT.builder(insert)
                .bind(ps -> {
                    ps.setLong("some_long", Long.MAX_VALUE);
                    ps.setInt("some_int", Integer.MAX_VALUE);
                    ps.setString("some_string", "test string");
                    ps.setTimestamp("some_dtm", now);
                    ps.setString("some_enum", TestBean.Status.ON.name());
                })
                .insert(connection);
        assertNotNull(key);
        assertThat(key, not(equalTo(0)));

        String select = "SELECT * FROM test_bean WHERE test_key = :test_key";
        final TestBean bean = SQL_SUPPORT.builder(select)
                .bind(ps -> ps.setLong("test_key", key))
                .mapResult(TEST_BEAN_RESULT_MAPPING)
                .query(connection);
        assertNotNull(bean);
        assertThat(bean.getSomeLong(), equalTo(Long.MAX_VALUE));
        assertThat(bean.getSomeInt(), equalTo(Integer.MAX_VALUE));
        assertThat(bean.getSomeString(), equalTo("test string"));
        assertThat(bean.getSomeDtm(), equalTo(now));

        String update = "UPDATE test_bean SET some_long = :some_long, some_int = :some_int, some_string = :some_string, some_dtm = :some_dtm, some_enum = :some_enum WHERE test_key = :test_key";
        final int rowsUpdated = SQL_SUPPORT.builder(update)
                .bind(ps -> {
                    ps.setLong("some_long", bean.getSomeLong());
                    ps.setInt("some_int", bean.getSomeInt());
                    ps.setString("some_string", "changed string");
                    ps.setTimestamp("some_dtm", bean.getSomeDtm());
                    ps.setLong("test_key", key);
                    ps.setString("some_enum", bean.getSomeEnum().name());
                })
                .update(connection);
        assertThat(rowsUpdated, equalTo(1));

        final TestBean updateBean = SQL_SUPPORT.builder(select)
                .bind(ps -> ps.setLong("test_key", key))
                .mapResult(TEST_BEAN_RESULT_MAPPING)
                .query(connection);
        assertNotNull(updateBean);
        assertThat(updateBean.getSomeString(), equalTo("changed string"));

        String delete = "DELETE FROM test_bean WHERE test_key = :test_key";
        final int rowsDeleted = SQL_SUPPORT.builder(delete)
                .bind(ps -> ps.setLong("test_key", key))
                .update(connection);
        assertThat(rowsDeleted, equalTo(1));

        final TestBean deleteBean = SQL_SUPPORT.builder(select)
                .bind(ps -> ps.setLong("test_key", key))
                .mapResult(TEST_BEAN_RESULT_MAPPING)
                .query(connection);
        assertNull(deleteBean);
    }
}
