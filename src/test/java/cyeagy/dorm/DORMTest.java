package cyeagy.dorm;

import org.h2.jdbcx.JdbcDataSource;
import org.h2.tools.Server;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class DORMTest {
    private static DORM DORM = new DORM();
    private static Server server;
    private static Connection connection;

    @BeforeClass
    public static void setUp() throws Exception {
        server = Server.createTcpServer().start();
        Class.forName("org.h2.Driver");
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setUrl("jdbc:h2:mem:");
        connection = dataSource.getConnection();
        String create = new Scanner(DORMTest.class.getResourceAsStream("/sql/test_create.sql"), "UTF-8").useDelimiter("\\A").next();
        Statement statement = connection.createStatement();
        statement.execute(create);
        statement.close();
        String insert = new Scanner(DORMTest.class.getResourceAsStream("/sql/test_insert.sql"), "UTF-8").useDelimiter("\\A").next();
        statement = connection.createStatement();
        statement.execute(insert);
        statement.close();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        connection.close();
        server.shutdown();
    }

    @Test
    public void testFull() throws Exception {
        TestBean bean = new TestBean(null, Long.MAX_VALUE, Integer.MAX_VALUE, "test string", Timestamp.from(Instant.now()));
        TestBean result = DORM.insert(connection, bean);
        assertNotNull(result.getTestKey());

        result = DORM.select(connection, result.getTestKey(), TestBean.class);
        assertThat(result.getSomeString(), equalTo(bean.getSomeString()));

        DORM.update(connection, new TestBean(result.getTestKey(), bean.getSomeLong(), bean.getSomeInt(), "changed string", bean.getSomeDtm()));

        result = DORM.select(connection, result.getTestKey(), TestBean.class);
        assertThat(bean.getSomeString(), not(equalTo(result.getSomeString())));

        DORM.delete(connection, result.getTestKey(), TestBean.class);

        result = DORM.select(connection, result.getTestKey(), TestBean.class);
        assertNull(result);
    }

    @Test
    public void testInsert() throws Exception {
        Timestamp now = Timestamp.from(Instant.now());
        {
            TestBean testBean = new TestBean(null, Long.MAX_VALUE, Integer.MAX_VALUE, "test string", now);
            TestBean result = DORM.insert(connection, testBean);
            assertNotNull(result);
            assertThat(result, is(not(testBean)));
            assertNotNull(result.getTestKey());
            assertThat(result.getTestKey(), is(not(0l)));
            assertThat(result.getSomeLong(), equalTo(testBean.getSomeLong()));
            assertThat(result.getSomeInt(), equalTo(testBean.getSomeInt()));
            assertThat(result.getSomeString(), equalTo(testBean.getSomeString()));
            assertThat(result.getSomeDtm(), equalTo(testBean.getSomeDtm()));
        }
        {
            final AnnotatedTestBean testbean = new AnnotatedTestBean(null, Long.MAX_VALUE, Integer.MAX_VALUE, "test string", now);
            final AnnotatedTestBean result = DORM.insert(connection, testbean);
            assertNotNull(result);
            assertThat(result, is(not(testbean)));
            assertNotNull(result.getLegacyKey());
            assertThat(result.getLegacyKey(), is(not(0l)));
            assertThat(result.getLegacyLong(), equalTo(testbean.getLegacyLong()));
            assertThat(result.getLegacyInt(), equalTo(testbean.getLegacyInt()));
            assertThat(result.getLegacyString(), equalTo(testbean.getLegacyString()));
            assertThat(result.getLegacyTimestamp(), equalTo(testbean.getLegacyTimestamp()));
        }
    }

    @Test
    public void testUpdate() throws Exception {
        Timestamp now = Timestamp.from(Instant.now().plus(1, ChronoUnit.DAYS));
        {
            final long key = 4;
            TestBean testBean = new TestBean(4l, Long.MAX_VALUE, Integer.MAX_VALUE, "FOURTH", now);

            final TestBean preSelect = DORM.select(connection, key, TestBean.class);
            assertNotNull(preSelect);
            assertThat(preSelect.getTestKey(), equalTo(testBean.getTestKey()));
            assertThat(preSelect.getSomeLong(), not(equalTo(testBean.getSomeLong())));
            assertThat(preSelect.getSomeInt(), not(testBean.getSomeInt()));
            assertThat(preSelect.getSomeString(), not(testBean.getSomeString()));
            assertThat(preSelect.getSomeDtm(), not(testBean.getSomeDtm()));

            DORM.update(connection, testBean);

            final TestBean select = DORM.select(connection, key, TestBean.class);
            assertNotNull(select);
            assertThat(select.getTestKey(), equalTo(testBean.getTestKey()));
            assertThat(select.getSomeLong(), equalTo(testBean.getSomeLong()));
            assertThat(select.getSomeInt(), equalTo(testBean.getSomeInt()));
            assertThat(select.getSomeString(), equalTo(testBean.getSomeString()));
            assertThat(select.getSomeDtm(), equalTo(testBean.getSomeDtm()));
        }
        {
            final long key = 5;
            AnnotatedTestBean testBean = new AnnotatedTestBean(key, Long.MAX_VALUE, Integer.MAX_VALUE, "FOURTH", now);

            final AnnotatedTestBean preSelect = DORM.select(connection, key, AnnotatedTestBean.class);
            assertNotNull(preSelect);
            assertThat(preSelect.getLegacyKey(), equalTo(testBean.getLegacyKey()));
            assertThat(preSelect.getLegacyLong(), not(equalTo(testBean.getLegacyLong())));
            assertThat(preSelect.getLegacyInt(), not(testBean.getLegacyInt()));
            assertThat(preSelect.getLegacyString(), not(testBean.getLegacyString()));
            assertThat(preSelect.getLegacyTimestamp(), not(testBean.getLegacyTimestamp()));

            DORM.update(connection, testBean);

            final AnnotatedTestBean select = DORM.select(connection, key, AnnotatedTestBean.class);
            assertNotNull(select);
            assertThat(select.getLegacyKey(), equalTo(testBean.getLegacyKey()));
            assertThat(select.getLegacyLong(), equalTo(testBean.getLegacyLong()));
            assertThat(select.getLegacyInt(), equalTo(testBean.getLegacyInt()));
            assertThat(select.getLegacyString(), equalTo(testBean.getLegacyString()));
            assertThat(select.getLegacyTimestamp(), equalTo(testBean.getLegacyTimestamp()));
        }
    }

    @Test
    public void testSelect() throws Exception {
        final long key = 3;
        {
            final TestBean result = DORM.select(connection, key, TestBean.class);
            assertNotNull(result);
            assertThat(result.getTestKey(), equalTo(key));
        }
        {
            final AnnotatedTestBean result = DORM.select(connection, key, AnnotatedTestBean.class);
            assertNotNull(result);
            assertThat(result.getLegacyKey(), equalTo(key));
        }
    }

    @Ignore//H2 does not support connection.createArray
    @Test
    public void testBulkSelect() throws Exception {
        Set<Long> keys = new HashSet<>(Arrays.asList(3l, 4l, 5l));
        final Set<TestBean> results = DORM.select(connection, keys, TestBean.class);
        assertNotNull(results);
        assertThat(results.size(), equalTo(keys.size()));
        for (TestBean result : results) {
            assertThat(keys, contains(result.getTestKey()));
        }
    }

    @Test
    public void testDelete() throws Exception {
        {
            final long key = 1;
            final TestBean preSelect = DORM.select(connection, key, TestBean.class);
            assertNotNull(preSelect);
            assertThat(preSelect.getTestKey(), equalTo(key));

            DORM.delete(connection, key, TestBean.class);

            final TestBean select = DORM.select(connection, key, TestBean.class);
            assertNull(select);
        }
        {
            final long key = 2;
            final AnnotatedTestBean preSelect = DORM.select(connection, key, AnnotatedTestBean.class);
            assertNotNull(preSelect);
            assertThat(preSelect.getLegacyKey(), equalTo(key));

            DORM.delete(connection, key, AnnotatedTestBean.class);

            final AnnotatedTestBean select = DORM.select(connection, key, AnnotatedTestBean.class);
            assertNull(select);
        }
    }
}