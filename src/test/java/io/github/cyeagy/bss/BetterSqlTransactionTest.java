package io.github.cyeagy.bss;

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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;

public class BetterSqlTransactionTest {
    private static BetterSqlMapper BDSM = BetterSqlMapper.fromDefaults();
    private static Server server;
    private static Connection connectionH2;

    @BeforeClass
    public static void setUpClass() throws Exception {
        server = Server.createTcpServer().start();
        Class.forName("org.h2.Driver");
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setUrl("jdbc:h2:mem:");
        connectionH2 = dataSource.getConnection();
        String create = new Scanner(BetterSqlMapperTest.class.getResourceAsStream("/sql/test_create.sql"), "UTF-8").useDelimiter("\\A").next();
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
        BetterSqlTransaction.with(connection -> {
            TestBean bean = new TestBean(null, Long.MAX_VALUE, Integer.MAX_VALUE, "test string", Timestamp.from(Instant.now()), 0.0);
            TestBean result = BDSM.insert(connection, bean);
            assertNotNull(result.getTestKey());

            result = BDSM.find(connection, result.getTestKey(), TestBean.class);
            assertThat(result.getSomeString(), equalTo(bean.getSomeString()));

            BDSM.delete(connection, result.getTestKey(), TestBean.class);

            result = BDSM.find(connection, result.getTestKey(), TestBean.class);
            assertNull(result);
        }).execute(connectionH2);
    }

    @Test
    public void testReturning() throws Exception {
        TestBean bean = new TestBean(null, Long.MAX_VALUE, Integer.MAX_VALUE, "test string", Timestamp.from(Instant.now()), 0.0);
        final TestBean result = BetterSqlTransaction.returning(connection -> BDSM.insert(connection, bean)).execute(connectionH2);
        assertNotNull(result);
        assertNotNull(result.getTestKey());
    }
}