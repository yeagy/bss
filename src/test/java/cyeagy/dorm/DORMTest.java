package cyeagy.dorm;

import org.junit.Test;

import java.sql.Timestamp;
import java.time.Instant;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class DORMTest {

    @Test
    public void testSelect() throws Exception {
        TestBean testBean = new TestBean(999l, Long.MAX_VALUE, Integer.MAX_VALUE, "test string", Timestamp.from(Instant.now()));
        String control = "SELECT test_key, some_long, some_int, some_string, some_dtm FROM test_bean WHERE test_key = %s";
        control = String.format(control, testBean.getTest_key());
        String select = DORM.generateSelectSql(testBean);
        assertThat(select, equalTo(control));
    }

    @Test
    public void testInsert() throws Exception {
        Timestamp now = Timestamp.from(Instant.now());
        TestBean testBean = new TestBean(null, Long.MAX_VALUE, Integer.MAX_VALUE, "test string", now);
        String control = "INSERT INTO test_bean (some_long, some_int, some_string, some_dtm) VALUES (%s, %s, '%s', '%s')";
        control = String.format(control, testBean.getSome_long(), testBean.getSome_int(), testBean.getSome_string(), testBean.getSome_dtm());
        String insert = DORM.generateInsertSql(testBean);
        assertThat(insert, equalTo(control));
    }

    @Test
    public void testUpdate() throws Exception {
        Timestamp now = Timestamp.from(Instant.now());
        TestBean testBean = new TestBean(999l, Long.MAX_VALUE, Integer.MAX_VALUE, "test string", now);
        String control = "UPDATE test_bean SET some_long = %s, some_int = %s, some_string = '%s', some_dtm = '%s' WHERE test_key = %s";
        control = String.format(control, testBean.getSome_long(), testBean.getSome_int(), testBean.getSome_string(), testBean.getSome_dtm(), testBean.getTest_key());
        String update = DORM.generateUpdateSql(testBean);
        assertThat(update, equalTo(control));
    }

    @Test
    public void testDelete() throws Exception {
        TestBean testBean = new TestBean(999l, Long.MAX_VALUE, Integer.MAX_VALUE, "test string", Timestamp.from(Instant.now()));
        String control = "DELETE FROM test_bean WHERE test_key = %s";
        control = String.format(control, testBean.getTest_key());
        String delete = DORM.generateDeleteSql(testBean);
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