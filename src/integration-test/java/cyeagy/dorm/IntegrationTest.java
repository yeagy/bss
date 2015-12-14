package cyeagy.dorm;

import org.junit.Test;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class IntegrationTest {
    private static Dorm DORM = Dorm.fromDefaults();
    private static SqlSupport SQL_SUPPORT = SqlSupport.fromDefaults();

    @Test
    public void testDormSelectWithArray() throws Exception {
        String url = "jdbc:postgresql://localhost/postgres";
        try(Connection connection = DriverManager.getConnection(url)) {

            createTestTable(connection);

            List<IntegrationTestBean> beans = new ArrayList<>();

            LocalDateTime dateTime = LocalDateTime.now();
            beans.add(DORM.insert(connection, new IntegrationTestBean(null, 3331L, 123, (short) 91, 888.88, 12.341f, true, "one", BigDecimal.ONE,
                    Time.valueOf(dateTime.toLocalTime()), Date.valueOf(dateTime.toLocalDate()), Timestamp.valueOf(dateTime))));

            dateTime = dateTime.plusDays(1);
            beans.add(DORM.insert(connection, new IntegrationTestBean(null, 3332L, 123, (short) 92, 888.88, 12.342f, true, "two", BigDecimal.ONE,
                    Time.valueOf(dateTime.toLocalTime()), Date.valueOf(dateTime.toLocalDate()), Timestamp.valueOf(dateTime))));

            dateTime = dateTime.plusDays(1);
            beans.add(DORM.insert(connection, new IntegrationTestBean(null, 3333L, 123, (short) 93, 888.88, 12.343f, true, "three", BigDecimal.ZERO,
                    Time.valueOf(dateTime.toLocalTime()), Date.valueOf(dateTime.toLocalDate()), Timestamp.valueOf(dateTime))));

            dateTime = dateTime.plusDays(1);
            beans.add(DORM.insert(connection, new IntegrationTestBean(null, 3334L, 123, (short) 94, 888.88, 12.344f, true, "four", BigDecimal.TEN,
                    Time.valueOf(dateTime.toLocalTime()), Date.valueOf(dateTime.toLocalDate()), Timestamp.valueOf(dateTime))));

            dateTime = dateTime.plusDays(1);
            beans.add(DORM.insert(connection, new IntegrationTestBean(null, 3335L, 123, (short) 95, 888.88, 12.345f, true, "five", BigDecimal.TEN,
                    Time.valueOf(dateTime.toLocalTime()), Date.valueOf(dateTime.toLocalDate()), Timestamp.valueOf(dateTime))));

            Set<Long> keys = beans.stream().map(IntegrationTestBean::getTestKey).collect(Collectors.toSet());
            List<IntegrationTestBean> fromDB = DORM.select(connection, keys, IntegrationTestBean.class);
            Set<Long> dbKeys = fromDB.stream().map(IntegrationTestBean::getTestKey).collect(Collectors.toSet());
            assertThat(dbKeys.size(), equalTo(keys.size()));
            assertTrue(dbKeys.containsAll(keys));

            Set<String> strings = beans.stream().map(IntegrationTestBean::getSomeString).collect(Collectors.toSet());
            String selectByString = "SELECT * FROM public.dorm_integration_test WHERE some_string IN (SELECT unnest(?))";
            List<String> dbStrings = SQL_SUPPORT.builder(selectByString)
                    .queryBinding(ps -> ps.setArray(1, strings))
                    .resultMapping(rs -> rs.getString("some_string"))
                    .executeQueryList(connection);
            assertThat(dbStrings.size(), equalTo(strings.size()));
            assertTrue(dbStrings.containsAll(strings));

            Set<Double> doubles = beans.stream().map(IntegrationTestBean::getSomeDouble).collect(Collectors.toSet());
            String selectBydouble = "SELECT * FROM public.dorm_integration_test WHERE some_double IN (SELECT unnest(?))";
            List<Double> dbDoubles = SQL_SUPPORT.builder(selectBydouble)
                    .queryBinding(ps -> ps.setArray(1, doubles))
                    .resultMapping(rs -> rs.getDouble("some_double"))
                    .executeQueryList(connection);
            assertTrue(dbDoubles.containsAll(doubles));

            deleteTestTable(connection);
        }
    }

    private void deleteTestTable(Connection connection) throws SQLException {
        String delete = "DROP TABLE public.dorm_integration_test";
        Statement statement = connection.createStatement();
        statement.execute(delete);
        statement.close();
    }

    private void createTestTable(Connection connection) throws SQLException {
        String create = new Scanner(DormTest.class.getResourceAsStream("/sql/itest_create.sql"), "UTF-8").useDelimiter("\\A").next();
        Statement statement = connection.createStatement();
        statement.execute(create);
        statement.close();
    }
}
