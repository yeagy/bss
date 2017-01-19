package io.github.yeagy.bss;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;

public class IntegrationTest {
    private static Connection PG_CONNECTION;
    private static List<IntegrationTestBean> PG_BEANS;
    private static BetterSqlMapper PG_MAPPER;
    private static BetterSqlSupport PG_SUPPORT;

    private static Connection MY_CONNECTION;
    private static List<IntegrationTestBean> MY_BEANS;
    private static BetterSqlMapper MY_MAPPER;

    @BeforeClass
    public static void setUpClass() throws Exception {
        PG_CONNECTION = DriverManager.getConnection("jdbc:postgresql://localhost/postgres");
        String pgCreate = new Scanner(IntegrationTest.class.getResourceAsStream("/sql/postgres_create.sql"), "UTF-8").useDelimiter("\\A").next();
        executeStatement(PG_CONNECTION, pgCreate);
        BetterOptions pgOptions = BetterOptions.from(BetterOptions.Option.ARRAY_SUPPORT);
        PG_MAPPER = BetterSqlMapper.from(pgOptions);
        PG_SUPPORT = BetterSqlSupport.from(pgOptions);
        PG_BEANS = createTestBeans(PG_CONNECTION, PG_MAPPER);

        MY_CONNECTION = DriverManager.getConnection("jdbc:mysql://localhost:3306/?allowMultiQueries=true&user=root");
        String create = new Scanner(IntegrationTest.class.getResourceAsStream("/sql/mysql_create.sql"), "UTF-8").useDelimiter("\\A").next();
        executeStatement(MY_CONNECTION, create);
        MY_MAPPER = BetterSqlMapper.from(BetterOptions.fromDefaults());
        MY_BEANS = createTestBeans(MY_CONNECTION, MY_MAPPER);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        String pgDrop = "DROP SCHEMA bss_test CASCADE";
        executeStatement(PG_CONNECTION, pgDrop);
        PG_CONNECTION.close();

        String mysqlDrop = "DROP DATABASE bss_test";
        executeStatement(MY_CONNECTION, mysqlDrop);
        MY_CONNECTION.close();
    }

    @Test
    public void testDormSelectWithArrayPostgres() {
        testSelectWithArray(PG_CONNECTION, PG_MAPPER, PG_BEANS);
    }

    @Test
    public void testDormSelectWithArrayMysql() {
        testSelectWithArray(MY_CONNECTION, MY_MAPPER, MY_BEANS);
    }

    @Test
    public void testCompositeKeyPostgres() {
        testCompositeKey(PG_CONNECTION, PG_MAPPER);
    }

    private void testCompositeKey(Connection connection, BetterSqlMapper mapper) {
        long keyA = 1;
        long keyB = 100;
        mapper.insert(connection, new CompositeKeyBean(keyA, keyB, -12L, 15, "test"));

        String select = "SELECT * FROM bss_test.composite_key_test WHERE key_a = :a AND key_b = :b";
        StatementBinding binding = ps -> {
            ps.setLong("a", keyA);
            ps.setLong("b", keyB);
        };
        CompositeKeyBean bean = mapper.select(select, CompositeKeyBean.class).bind(binding).one(connection);

        assertNotNull(bean);
        assertThat(bean.getSomeString(), equalTo("test"));

        mapper.update(connection, new CompositeKeyBean(keyA, keyB, -99, 99, "update"));

        bean = mapper.select(select, CompositeKeyBean.class).bind(binding).one(connection);

        assertNotNull(bean);
        assertThat(bean.getSomeString(), equalTo("update"));

        mapper.delete(connection, bean);

        bean = mapper.select(select, CompositeKeyBean.class).bind(binding).one(connection);
        assertNull(bean);
    }

    @Test
    public void testStringArray() {
        Set<String> keys = PG_BEANS.stream().map(IntegrationTestBean::getSomeString).collect(toSet());
        String select = "SELECT * FROM bss_test.integration_test WHERE some_string IN (SELECT unnest(?))";
        List<String> dbKeys = PG_SUPPORT.builder(select)
                .statementBinding(ps -> ps.setArray(1, keys))
                .resultMapping(rs -> rs.getString("some_string"))
                .executeQueryList(PG_CONNECTION);
        assertThat(makeSet(dbKeys).size(), equalTo(keys.size()));
        assertTrue(dbKeys.containsAll(keys));
    }

    @Test
    public void testDoubleArray() {
        Set<Double> keys = PG_BEANS.stream().map(IntegrationTestBean::getSomeDouble).collect(toSet());
        String select = "SELECT * FROM bss_test.integration_test WHERE some_double IN (SELECT unnest(?))";
        List<Double> dbKeys = PG_SUPPORT.builder(select)
                .statementBinding(ps -> ps.setArray(1, keys))
                .resultMapping(rs -> rs.getDouble("some_double"))
                .executeQueryList(PG_CONNECTION);
        assertThat(makeSet(dbKeys).size(), equalTo(keys.size()));
        assertTrue(dbKeys.containsAll(keys));
    }

    @Test
    public void testLongArray() {
        Set<Long> keys = PG_BEANS.stream().map(IntegrationTestBean::getSomeLong).collect(toSet());
        String select = "SELECT * FROM bss_test.integration_test WHERE some_long IN (SELECT unnest(?))";
        List<Long> dbKeys = PG_SUPPORT.builder(select)
                .statementBinding(ps -> ps.setArray(1, keys))
                .resultMapping(rs -> rs.getLong("some_long"))
                .executeQueryList(PG_CONNECTION);
        assertThat(makeSet(dbKeys).size(), equalTo(keys.size()));
        assertTrue(dbKeys.containsAll(keys));
    }

    @Test
    public void testBigDecimalArray() {
        Set<BigDecimal> keys = PG_BEANS.stream().map(IntegrationTestBean::getSomeBd).collect(toSet());
        String select = "SELECT * FROM bss_test.integration_test WHERE some_bd IN (SELECT unnest(?))";
        List<BigDecimal> dbKeys = PG_SUPPORT.builder(select)
                .statementBinding(ps -> ps.setArray(1, keys))
                .resultMapping(rs -> rs.getBigDecimal("some_bd"))
                .executeQueryList(PG_CONNECTION);
        assertThat(makeSet(dbKeys).size(), equalTo(keys.size()));
        assertTrue(dbKeys.containsAll(keys));
    }

    @Test
    public void testIntArray() {
        Set<Integer> keys = PG_BEANS.stream().map(IntegrationTestBean::getSomeInt).collect(toSet());
        String select = "SELECT * FROM bss_test.integration_test WHERE some_int IN (SELECT unnest(?))";
        List<Integer> dbKeys = PG_SUPPORT.builder(select)
                .statementBinding(ps -> ps.setArray(1, keys))
                .resultMapping(rs -> rs.getInt("some_int"))
                .executeQueryList(PG_CONNECTION);
        assertThat(makeSet(dbKeys).size(), equalTo(keys.size()));
        assertTrue(dbKeys.containsAll(keys));
    }

    @Test
    public void testShortArray() {
        Set<Short> keys = PG_BEANS.stream().map(IntegrationTestBean::getSomeShort).collect(toSet());
        String select = "SELECT * FROM bss_test.integration_test WHERE some_short IN (SELECT unnest(?))";
        List<Short> dbKeys = PG_SUPPORT.builder(select)
                .statementBinding(ps -> ps.setArray(1, keys))
                .resultMapping(rs -> rs.getShort("some_short"))
                .executeQueryList(PG_CONNECTION);
        assertThat(makeSet(dbKeys).size(), equalTo(keys.size()));
        assertTrue(dbKeys.containsAll(keys));
    }

    @Test
    public void testFloatArray() {
        Set<Float> keys = PG_BEANS.stream().map(IntegrationTestBean::getSomeFloat).collect(toSet());
        String select = "SELECT * FROM bss_test.integration_test WHERE some_float IN (SELECT unnest(?))";
        List<Float> dbKeys = PG_SUPPORT.builder(select)
                .statementBinding(ps -> ps.setArray(1, keys))
                .resultMapping(rs -> rs.getFloat("some_float"))
                .executeQueryList(PG_CONNECTION);
        assertThat(makeSet(dbKeys).size(), equalTo(keys.size()));
        assertTrue(dbKeys.containsAll(keys));
    }

    @Test
    public void testBooleanArray() {
        Set<Boolean> keys = PG_BEANS.stream().map(IntegrationTestBean::isSomeBool).collect(toSet());
        String select = "SELECT * FROM bss_test.integration_test WHERE some_bool IN (SELECT unnest(?))";
        List<Boolean> dbKeys = PG_SUPPORT.builder(select)
                .statementBinding(ps -> ps.setArray(1, keys))
                .resultMapping(rs -> rs.getBoolean("some_bool"))
                .executeQueryList(PG_CONNECTION);
        assertThat(makeSet(dbKeys).size(), equalTo(keys.size()));
        assertTrue(dbKeys.containsAll(keys));
    }

    @Test
    public void testTimeArray() {
        Set<Time> keys = PG_BEANS.stream().map(IntegrationTestBean::getSomeTime).collect(toSet());
        String select = "SELECT * FROM bss_test.integration_test WHERE some_time IN (SELECT unnest(?))";
        List<Time> dbKeys = PG_SUPPORT.builder(select)
                .statementBinding(ps -> ps.setArray(1, keys))
                .resultMapping(rs -> rs.getTime("some_time"))
                .executeQueryList(PG_CONNECTION);
        assertThat(makeSet(dbKeys).size(), equalTo(keys.size()));
        assertTrue(dbKeys.containsAll(keys));
    }

    @Test
    public void testDateArray() {
        Set<Date> keys = PG_BEANS.stream().map(IntegrationTestBean::getSomeDate).collect(toSet());
        String select = "SELECT * FROM bss_test.integration_test WHERE some_date IN (SELECT unnest(?))";
        List<Date> dbKeys = PG_SUPPORT.builder(select)
                .statementBinding(ps -> ps.setArray(1, keys))
                .resultMapping(rs -> rs.getDate("some_date"))
                .executeQueryList(PG_CONNECTION);
        assertThat(makeSet(dbKeys).size(), equalTo(keys.size()));
        assertTrue(dbKeys.containsAll(keys));
    }

    @Test
    public void testTimestampArray() {
        Set<Timestamp> keys = PG_BEANS.stream().map(IntegrationTestBean::getSomeDtm).collect(toSet());
        String select = "SELECT * FROM bss_test.integration_test WHERE some_dtm IN (SELECT unnest(?))";
        List<Timestamp> dbKeys = PG_SUPPORT.builder(select)
                .statementBinding(ps -> ps.setArray(1, keys))
                .resultMapping(rs -> rs.getTimestamp("some_dtm"))
                .executeQueryList(PG_CONNECTION);
        assertThat(makeSet(dbKeys).size(), equalTo(keys.size()));
        assertTrue(dbKeys.containsAll(keys));
    }

    private void testSelectWithArray(Connection connection, BetterSqlMapper mapper, List<IntegrationTestBean> beans) {
        Set<Long> keys = beans.stream().map(IntegrationTestBean::getTestKey).collect(toSet());
        List<IntegrationTestBean> fromDB = mapper.find(connection, keys, IntegrationTestBean.class);
        Set<Long> dbKeys = fromDB.stream().map(IntegrationTestBean::getTestKey).collect(toSet());
        assertThat(dbKeys.size(), equalTo(keys.size()));
        assertTrue(dbKeys.containsAll(keys));
    }

    private <T> Set<T> makeSet(List<T> list){
        return new HashSet<>(list);
    }

    private static List<IntegrationTestBean> createTestBeans(Connection connection, BetterSqlMapper mapper) {
        List<IntegrationTestBean> beans = new ArrayList<>();
        LocalDateTime dateTime = LocalDateTime.now();
        beans.add(mapper.insert(connection, new IntegrationTestBean(null, 3331L, 123, (short) 91, 888.88, 12.341f, true, "one", BigDecimal.ONE,
                Time.valueOf(dateTime.toLocalTime()), Date.valueOf(dateTime.toLocalDate()), Timestamp.valueOf(dateTime))));

        dateTime = dateTime.plusDays(1);
        beans.add(mapper.insert(connection, new IntegrationTestBean(null, 3332L, 123, (short) 92, 888.88, 12.342f, true, "two", BigDecimal.ONE,
                Time.valueOf(dateTime.toLocalTime()), Date.valueOf(dateTime.toLocalDate()), Timestamp.valueOf(dateTime))));

        dateTime = dateTime.plusDays(1);
        beans.add(mapper.insert(connection, new IntegrationTestBean(null, 3333L, 123, (short) 93, 888.88, 12.343f, true, "three", BigDecimal.ZERO,
                Time.valueOf(dateTime.toLocalTime()), Date.valueOf(dateTime.toLocalDate()), Timestamp.valueOf(dateTime))));

        dateTime = dateTime.plusDays(1);
        beans.add(mapper.insert(connection, new IntegrationTestBean(null, 3334L, 123, (short) 94, 888.88, 12.344f, true, "four", BigDecimal.TEN,
                Time.valueOf(dateTime.toLocalTime()), Date.valueOf(dateTime.toLocalDate()), Timestamp.valueOf(dateTime))));

        dateTime = dateTime.plusDays(1);
        beans.add(mapper.insert(connection, new IntegrationTestBean(null, 3335L, 123, (short) 95, 888.88, 12.345f, true, "five", BigDecimal.TEN,
                Time.valueOf(dateTime.toLocalTime()), Date.valueOf(dateTime.toLocalDate()), Timestamp.valueOf(dateTime))));
        return beans;
    }

    private static void executeStatement(Connection connection, String statement) throws SQLException {
        Statement s = connection.createStatement();
        s.execute(statement);
        s.close();
    }
}
