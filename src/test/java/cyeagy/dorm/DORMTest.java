package cyeagy.dorm;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import org.h2.jdbcx.JdbcDataSource;
import org.h2.tools.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Set;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class DORMTest {
    private static DORM DORM = new DORM();

    private Server server;
    private Connection connection;

    @Before
    public void setUp() throws Exception {
        server = Server.createTcpServer().start();
        Class.forName("org.h2.Driver");
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setUrl("jdbc:h2:mem:");
        connection = dataSource.getConnection();
        String create = Resources.toString(Resources.getResource("sql/test_create.sql"), Charsets.UTF_8);
        Statement statement = connection.createStatement();
        statement.execute(create);
        statement.close();
    }

    @Test
    public void testInsert() throws Exception {
        Timestamp now = Timestamp.from(Instant.now());
        TestBean testBean = new TestBean(null, Long.MAX_VALUE, Integer.MAX_VALUE, "test string", now);

        TestBean result = DORM.insert(connection, testBean);
        assertNotNull(result);
        assertThat(result, is(not(testBean)));
        assertNotNull(result.getTest_key());
        assertThat(result.getTest_key(), is(not(0l)));
        assertThat(result.getSome_long(), equalTo(testBean.getSome_long()));
        assertThat(result.getSome_int(), equalTo(testBean.getSome_int()));
        assertThat(result.getSome_string(), equalTo(testBean.getSome_string()));
        assertThat(result.getSome_dtm(), equalTo(testBean.getSome_dtm()));
    }

    @Test
    public void testUpdate() throws Exception {
        Timestamp now = Timestamp.from(Instant.now().plus(1, ChronoUnit.DAYS));
        TestBean testBean = new TestBean(4l, Long.MAX_VALUE, Integer.MAX_VALUE, "FOURTH", now);

        final TestBean preSelect = DORM.select(connection, 4l, TestBean.class);
        assertNotNull(preSelect);
        assertThat(preSelect.getTest_key(), equalTo(testBean.getTest_key()));
        assertThat(preSelect.getSome_long(), not(equalTo(testBean.getSome_long())));
        assertThat(preSelect.getSome_int(), not(testBean.getSome_int()));
        assertThat(preSelect.getSome_string(), not(testBean.getSome_string()));
        assertThat(preSelect.getSome_dtm(), not(testBean.getSome_dtm()));

        DORM.update(connection, testBean);

        final TestBean select = DORM.select(connection, 4l, TestBean.class);
        assertNotNull(select);
        assertThat(select.getTest_key(), equalTo(testBean.getTest_key()));
        assertThat(select.getSome_long(), equalTo(testBean.getSome_long()));
        assertThat(select.getSome_int(), equalTo(testBean.getSome_int()));
        assertThat(select.getSome_string(), equalTo(testBean.getSome_string()));
        assertThat(select.getSome_dtm(), equalTo(testBean.getSome_dtm()));
    }

    @Test
    public void testSelect() throws Exception {
        final TestBean result = DORM.select(connection, 1l, TestBean.class);
        assertNotNull(result);
        assertThat(result.getTest_key(), equalTo(1l));
    }

    @Ignore//H2 does not support connection.createArray
    @Test
    public void testBulkSelect() throws Exception {
        Set<Long> keys = Sets.newHashSet(1l, 2l, 3l, 5l);
        final Set<TestBean> results = DORM.select(connection, keys, TestBean.class);
        assertNotNull(results);
        assertThat(results.size(), equalTo(keys.size()));
        for (TestBean result : results) {
            assertThat(keys, contains(result.getTest_key()));
        }
    }

    @Test
    public void testDelete() throws Exception {
        final long key = 1;
        final TestBean preSelect = DORM.select(connection, key, TestBean.class);
        assertNotNull(preSelect);
        assertThat(preSelect.getTest_key(), equalTo(key));

        DORM.delete(connection, key, TestBean.class);

        final TestBean select = DORM.select(connection, key, TestBean.class);
        assertNull(select);
    }

    @After
    public void tearDown() throws Exception {
        connection.close();
        server.shutdown();
    }
}