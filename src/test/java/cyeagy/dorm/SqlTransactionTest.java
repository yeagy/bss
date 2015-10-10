package cyeagy.dorm;

import org.h2.jdbcx.JdbcDataSource;
import org.h2.tools.Server;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Scanner;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class SqlTransactionTest {
    private static Dorm DORM = Dorm.fromDefaults();
    private static Server server;
    private static Connection connectionH2;

    @BeforeClass
    public static void setUpClass() throws Exception {
        server = Server.createTcpServer().start();
        Class.forName("org.h2.Driver");
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setUrl("jdbc:h2:mem:");
        connectionH2 = dataSource.getConnection();
        String create = new Scanner(DormTest.class.getResourceAsStream("/sql/test_create.sql"), "UTF-8").useDelimiter("\\A").next();
        Statement statement = connectionH2.createStatement();
        statement.execute(create);
        statement.close();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        connectionH2.close();
        server.shutdown();
    }

    @Test
    public void testTransaction() throws Exception {
        SqlTransaction.with(connection -> {
            TestBean bean = new TestBean(null, Long.MAX_VALUE, Integer.MAX_VALUE, "test string", Timestamp.from(Instant.now()));
            TestBean result = DORM.insert(connection, bean);
            assertNotNull(result.getTestKey());

            result = DORM.select(connection, result.getTestKey(), TestBean.class);
            assertThat(result.getSomeString(), equalTo(bean.getSomeString()));

            DORM.delete(connection, result.getTestKey(), TestBean.class);

            result = DORM.select(connection, result.getTestKey(), TestBean.class);
            assertNull(result);
        }).execute(connectionH2);
    }

    @Test
    public void testReturning() throws Exception {
        TestBean bean = new TestBean(null, Long.MAX_VALUE, Integer.MAX_VALUE, "test string", Timestamp.from(Instant.now()));
        final TestBean result = SqlTransaction.returning(connection -> DORM.insert(connection, bean)).execute(connectionH2);
        assertNotNull(result);
        assertNotNull(result.getTestKey());
    }
}