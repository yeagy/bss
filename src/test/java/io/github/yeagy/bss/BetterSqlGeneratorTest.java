package io.github.yeagy.bss;

import org.junit.Ignore;
import org.junit.Test;

import java.util.Scanner;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class BetterSqlGeneratorTest {
    private static final BetterSqlGenerator GENERATOR = BetterSqlGenerator.fromDefaults();
    private static final BetterSqlGenerator GENERATOR_ARRAY = BetterSqlGenerator.from(BetterOptions.from(BetterOptions.Option.ARRAY_SUPPORT));

    @Test
    public void testGenerateSelectSqlTemplate() throws Exception {
        String control = "SELECT test_key, some_long, some_int, some_string, some_dtm FROM test_bean WHERE test_key = ?";
        TableData tableData = TableData.from(TestBean.class);
        String select = GENERATOR.generateSelectSqlTemplate(tableData);
        assertThat(select, equalTo(control));
    }

    @Test
    public void testGenerateSelectSqlTemplateNamed() throws Exception {
        String control = "SELECT test_key, some_long, some_int, some_string, some_dtm FROM test_bean WHERE test_key = :test_key";
        TableData tableData = TableData.from(TestBean.class);
        String select = GENERATOR.generateSelectSqlTemplateNamed(tableData);
        assertThat(select, equalTo(control));
    }

    @Test
    public void testGenerateBulkSelectSqlTemplate() throws Exception {
        String control = "SELECT test_key, some_long, some_int, some_string, some_dtm FROM test_bean WHERE test_key IN (?)";
        TableData tableData = TableData.from(TestBean.class);
        String select = GENERATOR.generateBulkSelectSqlTemplate(tableData);
        assertThat(select, equalTo(control));

        control = "SELECT test_key, some_long, some_int, some_string, some_dtm FROM test_bean WHERE test_key IN (SELECT unnest(?))";
        select = GENERATOR_ARRAY.generateBulkSelectSqlTemplate(tableData);
        assertThat(select, equalTo(control));
    }

    @Test
    public void testGenerateBulkSelectSqlTemplateNamed() throws Exception {
        String control = "SELECT test_key, some_long, some_int, some_string, some_dtm FROM test_bean WHERE test_key IN (:test_key)";
        TableData tableData = TableData.from(TestBean.class);
        String select = GENERATOR.generateBulkSelectSqlTemplateNamed(tableData);
        assertThat(select, equalTo(control));

        control = "SELECT test_key, some_long, some_int, some_string, some_dtm FROM test_bean WHERE test_key IN (SELECT unnest(:test_key))";
        select = GENERATOR_ARRAY.generateBulkSelectSqlTemplateNamed(tableData);
        assertThat(select, equalTo(control));
    }

    @Test
    public void testGenerateInsertSqlTemplate() throws Exception {
        String control = "INSERT INTO test_bean (some_long, some_int, some_string, some_dtm) VALUES (?, ?, ?, ?)";
        TableData tableData = TableData.from(TestBean.class);
        String insert = GENERATOR.generateInsertSqlTemplate(tableData);
        assertThat(insert, equalTo(control));
    }

    @Test
    public void testGenerateInsertSqlTemplateNamed() throws Exception {
        String control = "INSERT INTO test_bean (some_long, some_int, some_string, some_dtm) VALUES (:some_long, :some_int, :some_string, :some_dtm)";
        TableData tableData = TableData.from(TestBean.class);
        String insert = GENERATOR.generateInsertSqlTemplateNamed(tableData);
        assertThat(insert, equalTo(control));
    }

    @Test
    public void testGenerateUpdateSqlTemplate() throws Exception {
        String control = "UPDATE test_bean SET some_long = ?, some_int = ?, some_string = ?, some_dtm = ? WHERE test_key = ?";
        TableData tableData = TableData.from(TestBean.class);
        String update = GENERATOR.generateUpdateSqlTemplate(tableData);
        assertThat(update, equalTo(control));
    }

    @Test
    public void testGenerateUpdateSqlTemplateNamed() throws Exception {
        String control = "UPDATE test_bean SET some_long = :some_long, some_int = :some_int, some_string = :some_string, some_dtm = :some_dtm WHERE test_key = :test_key";
        TableData tableData = TableData.from(TestBean.class);
        String update = GENERATOR.generateUpdateSqlTemplateNamed(tableData);
        assertThat(update, equalTo(control));
    }

    @Test
    public void testGenerateDeleteSqlTemplate() throws Exception {
        String control = "DELETE FROM test_bean WHERE test_key = ?";
        TableData tableData = TableData.from(TestBean.class);
        String delete = GENERATOR.generateDeleteSqlTemplate(tableData);
        assertThat(delete, equalTo(control));
    }

    @Test
    public void testGenerateDeleteSqlTemplateNamed() throws Exception {
        String control = "DELETE FROM test_bean WHERE test_key = :test_key";
        TableData tableData = TableData.from(TestBean.class);
        String delete = GENERATOR.generateDeleteSqlTemplateNamed(tableData);
        assertThat(delete, equalTo(control));
    }

    @Test
    public void testGenerateBulkDeleteSqlTemplate() throws Exception {
        String control = "DELETE FROM test_bean WHERE test_key IN (?)";
        TableData tableData = TableData.from(TestBean.class);
        String delete = GENERATOR.generateBulkDeleteSqlTemplate(tableData);
        assertThat(delete, equalTo(control));

        control = "DELETE FROM test_bean WHERE test_key IN (SELECT unnest(?))";
        delete = GENERATOR_ARRAY.generateBulkDeleteSqlTemplate(tableData);
        assertThat(delete, equalTo(control));
    }

    @Test
    public void testGenerateBulkDeleteSqlTemplateNamed() throws Exception {
        String control = "DELETE FROM test_bean WHERE test_key IN (:test_key)";
        TableData tableData = TableData.from(TestBean.class);
        String delete = GENERATOR.generateBulkDeleteSqlTemplateNamed(tableData);
        assertThat(delete, equalTo(control));

        control = "DELETE FROM test_bean WHERE test_key IN (SELECT unnest(:test_key))";
        delete = GENERATOR_ARRAY.generateBulkDeleteSqlTemplateNamed(tableData);
        assertThat(delete, equalTo(control));
    }

    @Ignore//for now now using postgres data types
    @Test
    public void testGenerateCreateStatement() throws Exception {
        String control = new Scanner(BetterSqlMapperTest.class.getResourceAsStream("/sql/test_create.sql"), "UTF-8").useDelimiter("\\A").next();
        control = control.replaceAll("\\n", "").replaceAll("  ", " ").replaceAll("\\( ", "\\(").replaceAll(" AUTO_INCREMENT", "");
        TableData tableData = TableData.from(TestBean.class);
        String create = GENERATOR.generateCreateStatement(tableData);
        assertThat(create, equalTo(control));
    }
}
