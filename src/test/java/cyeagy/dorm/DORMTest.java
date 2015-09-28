package cyeagy.dorm;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.h2.jdbcx.JdbcDataSource;
import org.h2.tools.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class DORMTest {

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
        assertThat(result.getSome_long(), equalTo(testBean.getSome_long()));
        assertThat(result.getSome_int(), equalTo(testBean.getSome_int()));
        assertThat(result.getSome_string(), equalTo(testBean.getSome_string()));
        assertThat(result.getSome_dtm(), equalTo(testBean.getSome_dtm()));
    }

    @After
    public void tearDown() throws Exception {
        connection.close();
        server.shutdown();
    }
}