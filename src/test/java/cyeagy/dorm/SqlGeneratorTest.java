package cyeagy.dorm;

import org.junit.Test;

import java.sql.Timestamp;
import java.time.Instant;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class SqlGeneratorTest {

    final static SqlGenerator GENERATOR = new SqlGenerator();

    @Test
    public void testGenerateSelectSql() throws Exception {
        TestBean testBean = new TestBean(999l, Long.MAX_VALUE, Integer.MAX_VALUE, "test string", Timestamp.from(Instant.now()));
        String control = "SELECT test_key, some_long, some_int, some_string, some_dtm FROM test_bean WHERE test_key = %s";
        control = String.format(control, testBean.getTest_key());
        String select = GENERATOR.generateSelectSql(testBean);
        assertThat(select, equalTo(control));
    }

    @Test
    public void testGenerateSelectSqlTemplate() throws Exception {
        String control = "SELECT test_key, some_long, some_int, some_string, some_dtm FROM test_bean WHERE test_key = ?";
        String select = GENERATOR.generateSelectSqlTemplate(TestBean.class);
        assertThat(select, equalTo(control));
    }

    @Test
    public void testGenerateSelectSqlTemplateNamed() throws Exception {
        String control = "SELECT test_key, some_long, some_int, some_string, some_dtm FROM test_bean WHERE test_key = :test_key";
        String select = GENERATOR.generateSelectSqlTemplateNamed(TestBean.class);
        assertThat(select, equalTo(control));
    }

    @Test
    public void testGenerateInsertSql() throws Exception {
        Timestamp now = Timestamp.from(Instant.now());
        TestBean testBean = new TestBean(null, Long.MAX_VALUE, Integer.MAX_VALUE, "test string", now);
        String control = "INSERT INTO test_bean (some_long, some_int, some_string, some_dtm) VALUES (%s, %s, '%s', '%s')";
        control = String.format(control, testBean.getSome_long(), testBean.getSome_int(), testBean.getSome_string(), testBean.getSome_dtm());
        String insert = GENERATOR.generateInsertSql(testBean);
        assertThat(insert, equalTo(control));
    }

    @Test
    public void testGenerateInsertSqlTemplate() throws Exception {
        String control = "INSERT INTO test_bean (some_long, some_int, some_string, some_dtm) VALUES (?, ?, ?, ?)";
        String insert = GENERATOR.generateInsertSqlTemplate(TestBean.class);
        assertThat(insert, equalTo(control));
    }

    @Test
    public void testGenerateInsertSqlTemplateNamed() throws Exception {
        String control = "INSERT INTO test_bean (some_long, some_int, some_string, some_dtm) VALUES (:some_long, :some_int, :some_string, :some_dtm)";
        String insert = GENERATOR.generateInsertSqlTemplateNamed(TestBean.class);
        assertThat(insert, equalTo(control));
    }

    @Test
    public void testGenerateUpdateSql() throws Exception {
        Timestamp now = Timestamp.from(Instant.now());
        TestBean testBean = new TestBean(999l, Long.MAX_VALUE, Integer.MAX_VALUE, "test string", now);
        String control = "UPDATE test_bean SET some_long = %s, some_int = %s, some_string = '%s', some_dtm = '%s' WHERE test_key = %s";
        control = String.format(control, testBean.getSome_long(), testBean.getSome_int(), testBean.getSome_string(), testBean.getSome_dtm(), testBean.getTest_key());
        String update = GENERATOR.generateUpdateSql(testBean);
        assertThat(update, equalTo(control));
    }

    @Test
    public void testGenerateUpdateSqlTemplate() throws Exception {
        String control = "UPDATE test_bean SET some_long = ?, some_int = ?, some_string = ?, some_dtm = ? WHERE test_key = ?";
        String update = GENERATOR.generateUpdateSqlTemplate(TestBean.class);
        assertThat(update, equalTo(control));
    }

    @Test
    public void testGenerateUpdateSqlTemplateNamed() throws Exception {
        String control = "UPDATE test_bean SET some_long = :some_long, some_int = :some_int, some_string = :some_string, some_dtm = :some_dtm WHERE test_key = :test_key";
        String update = GENERATOR.generateUpdateSqlTemplateNamed(TestBean.class);
        assertThat(update, equalTo(control));
    }

    @Test
    public void testGenerateDeleteSql() throws Exception {
        TestBean testBean = new TestBean(999l, Long.MAX_VALUE, Integer.MAX_VALUE, "test string", Timestamp.from(Instant.now()));
        String control = "DELETE FROM test_bean WHERE test_key = %s";
        control = String.format(control, testBean.getTest_key());
        String delete = GENERATOR.generateDeleteSql(testBean);
        assertThat(delete, equalTo(control));
    }

    @Test
    public void testGenerateDeleteSqlTemplate() throws Exception {
        String control = "DELETE FROM test_bean WHERE test_key = ?";
        String delete = GENERATOR.generateDeleteSqlTemplate(TestBean.class);
        assertThat(delete, equalTo(control));
    }

    @Test
    public void testGenerateDeleteSqlTemplateNamed() throws Exception {
        String control = "DELETE FROM test_bean WHERE test_key = :test_key";
        String delete = GENERATOR.generateDeleteSqlTemplateNamed(TestBean.class);
        assertThat(delete, equalTo(control));
    }

    public static class TestBean{
        @Id
        private final Long test_key;
        private final long some_long;
        private final int some_int;
        private final String some_string;
        private final Timestamp some_dtm;

        public TestBean(Long test_key, long some_long, int some_int, String some_string, Timestamp some_dtm) {
            this.test_key = test_key;
            this.some_long = some_long;
            this.some_int = some_int;
            this.some_string = some_string;
            this.some_dtm = some_dtm;
        }

        public Long getTest_key() {
            return test_key;
        }

        public long getSome_long() {
            return some_long;
        }

        public int getSome_int() {
            return some_int;
        }

        public String getSome_string() {
            return some_string;
        }

        public Timestamp getSome_dtm() {
            return some_dtm;
        }
    }
}