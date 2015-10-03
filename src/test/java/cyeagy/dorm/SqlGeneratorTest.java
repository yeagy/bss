package cyeagy.dorm;

import org.junit.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Scanner;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class SqlGeneratorTest {
    private static final SqlGenerator GENERATOR = new SqlGenerator();
    private static final SqlGenerator.Extras EXTRAS = GENERATOR.new Extras();

    @Test
    public void testGenerateSelectSqlTemplate() throws Exception {
        String control = "SELECT test_key, some_long, some_int, some_string, some_dtm FROM test_bean WHERE test_key = ?";
        TableData tableData = TableData.analyze(TestBean.class);
        String select = GENERATOR.generateSelectSqlTemplate(tableData);
        assertThat(select, equalTo(control));
    }

    @Test
    public void testGenerateSelectSqlTemplateNamed() throws Exception {
        String control = "SELECT test_key, some_long, some_int, some_string, some_dtm FROM test_bean WHERE test_key = :test_key";
        TableData tableData = TableData.analyze(TestBean.class);
        String select = GENERATOR.generateSelectSqlTemplateNamed(tableData);
        assertThat(select, equalTo(control));
    }

    @Test
    public void testGenerateBulkSelectSqlTemplate() throws Exception {
        String control = "SELECT test_key, some_long, some_int, some_string, some_dtm FROM test_bean WHERE test_key IN (SELECT unnest(?))";
        TableData tableData = TableData.analyze(TestBean.class);
        String select = GENERATOR.generateBulkSelectSqlTemplate(tableData);
        assertThat(select, equalTo(control));
    }

    @Test
    public void testGenerateBulkSelectSqlTemplateNamed() throws Exception {
        String control = "SELECT test_key, some_long, some_int, some_string, some_dtm FROM test_bean WHERE test_key IN (SELECT unnest(:test_key))";
        TableData tableData = TableData.analyze(TestBean.class);
        String select = GENERATOR.generateBulkSelectSqlTemplateNamed(tableData);
        assertThat(select, equalTo(control));
    }

    @Test
    public void testGenerateInsertSqlTemplate() throws Exception {
        String control = "INSERT INTO test_bean (some_long, some_int, some_string, some_dtm) VALUES (?, ?, ?, ?)";
        TableData tableData = TableData.analyze(TestBean.class);
        String insert = GENERATOR.generateInsertSqlTemplate(tableData);
        assertThat(insert, equalTo(control));
    }

    @Test
    public void testGenerateInsertSqlTemplateNamed() throws Exception {
        String control = "INSERT INTO test_bean (some_long, some_int, some_string, some_dtm) VALUES (:some_long, :some_int, :some_string, :some_dtm)";
        TableData tableData = TableData.analyze(TestBean.class);
        String insert = GENERATOR.generateInsertSqlTemplateNamed(tableData);
        assertThat(insert, equalTo(control));
    }

    @Test
    public void testGenerateUpdateSqlTemplate() throws Exception {
        String control = "UPDATE test_bean SET some_long = ?, some_int = ?, some_string = ?, some_dtm = ? WHERE test_key = ?";
        TableData tableData = TableData.analyze(TestBean.class);
        String update = GENERATOR.generateUpdateSqlTemplate(tableData);
        assertThat(update, equalTo(control));
    }

    @Test
    public void testGenerateUpdateSqlTemplateNamed() throws Exception {
        String control = "UPDATE test_bean SET some_long = :some_long, some_int = :some_int, some_string = :some_string, some_dtm = :some_dtm WHERE test_key = :test_key";
        TableData tableData = TableData.analyze(TestBean.class);
        String update = GENERATOR.generateUpdateSqlTemplateNamed(tableData);
        assertThat(update, equalTo(control));
    }

    @Test
    public void testGenerateDeleteSqlTemplate() throws Exception {
        String control = "DELETE FROM test_bean WHERE test_key = ?";
        TableData tableData = TableData.analyze(TestBean.class);
        String delete = GENERATOR.generateDeleteSqlTemplate(tableData);
        assertThat(delete, equalTo(control));
    }

    @Test
    public void testGenerateDeleteSqlTemplateNamed() throws Exception {
        String control = "DELETE FROM test_bean WHERE test_key = :test_key";
        TableData tableData = TableData.analyze(TestBean.class);
        String delete = GENERATOR.generateDeleteSqlTemplateNamed(tableData);
        assertThat(delete, equalTo(control));
    }

    @Test
    public void testGenerateCreateStatement() throws Exception {
        String control = new Scanner(DORMTest.class.getResourceAsStream("/sql/test_create.sql"), "UTF-8").useDelimiter("\\A").next();
        control = control.replaceAll("\\n", "").replaceAll("  ", " ").replaceAll("\\( ", "\\(").replaceAll(" AUTO_INCREMENT", "");
        TableData tableData = TableData.analyze(TestBean.class);
        String create = GENERATOR.generateCreateStatement(tableData);
        assertThat(create, equalTo(control));
    }

    @Test
    public void testGenerateSelectSql() throws Exception {
        TestBean testBean = new TestBean(999l, Long.MAX_VALUE, Integer.MAX_VALUE, "test string", Timestamp.from(Instant.now()));
        String control = "SELECT test_key, some_long, some_int, some_string, some_dtm FROM test_bean WHERE test_key = %s";
        control = String.format(control, testBean.getTestKey());
        TableData tableData = TableData.analyze(TestBean.class);
        String select = EXTRAS.generateSelectSql(tableData, testBean);
        assertThat(select, equalTo(control));
    }

    @Test
    public void testGenerateInsertSql() throws Exception {
        Timestamp now = Timestamp.from(Instant.now());
        TestBean testBean = new TestBean(null, Long.MAX_VALUE, Integer.MAX_VALUE, "test string", now);
        String control = "INSERT INTO test_bean (some_long, some_int, some_string, some_dtm) VALUES (%s, %s, '%s', '%s')";
        control = String.format(control, testBean.getSomeLong(), testBean.getSomeInt(), testBean.getSomeString(), testBean.getSomeDtm());
        TableData tableData = TableData.analyze(TestBean.class);
        String insert = EXTRAS.generateInsertSql(tableData, testBean);
        assertThat(insert, equalTo(control));
    }

    @Test
    public void testGenerateUpdateSql() throws Exception {
        Timestamp now = Timestamp.from(Instant.now());
        TestBean testBean = new TestBean(999l, Long.MAX_VALUE, Integer.MAX_VALUE, "test string", now);
        String control = "UPDATE test_bean SET some_long = %s, some_int = %s, some_string = '%s', some_dtm = '%s' WHERE test_key = %s";
        control = String.format(control, testBean.getSomeLong(), testBean.getSomeInt(), testBean.getSomeString(), testBean.getSomeDtm(), testBean.getTestKey());
        TableData tableData = TableData.analyze(TestBean.class);
        String update = EXTRAS.generateUpdateSql(tableData, testBean);
        assertThat(update, equalTo(control));
    }

    @Test
    public void testGenerateDeleteSql() throws Exception {
        TestBean testBean = new TestBean(999l, Long.MAX_VALUE, Integer.MAX_VALUE, "test string", Timestamp.from(Instant.now()));
        String control = "DELETE FROM test_bean WHERE test_key = %s";
        control = String.format(control, testBean.getTestKey());
        TableData tableData = TableData.analyze(TestBean.class);
        String delete = EXTRAS.generateDeleteSql(tableData, testBean);
        assertThat(delete, equalTo(control));
    }

}
