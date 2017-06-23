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
    public void testGenerateSelectSqlTemplate() {
        String control = "SELECT test_key, some_long, some_int, some_string, some_dtm, some_enum FROM test_bean WHERE test_key = ?";
        TableData tableData = TableData.from(TestBean.class);
        String select = GENERATOR.generateSelectSqlTemplate(tableData);
        assertThat(select, equalTo(control));
    }

    @Test
    public void testGenerateSelectSqlTemplateNamed() {
        String control = "SELECT test_key, some_long, some_int, some_string, some_dtm, some_enum FROM test_bean WHERE test_key = :test_key";
        TableData tableData = TableData.from(TestBean.class);
        String select = GENERATOR.generateSelectSqlTemplateNamed(tableData);
        assertThat(select, equalTo(control));
    }

    @Test
    public void testGenerateBulkSelectSqlTemplate() {
        String control = "SELECT test_key, some_long, some_int, some_string, some_dtm, some_enum FROM test_bean WHERE test_key IN (?)";
        TableData tableData = TableData.from(TestBean.class);
        String select = GENERATOR.generateBulkSelectSqlTemplate(tableData);
        assertThat(select, equalTo(control));

        control = "SELECT test_key, some_long, some_int, some_string, some_dtm, some_enum FROM test_bean WHERE test_key IN (SELECT unnest(?))";
        select = GENERATOR_ARRAY.generateBulkSelectSqlTemplate(tableData);
        assertThat(select, equalTo(control));
    }

    @Test
    public void testGenerateBulkSelectSqlTemplateNamed() {
        String control = "SELECT test_key, some_long, some_int, some_string, some_dtm, some_enum FROM test_bean WHERE test_key IN (:test_key)";
        TableData tableData = TableData.from(TestBean.class);
        String select = GENERATOR.generateBulkSelectSqlTemplateNamed(tableData);
        assertThat(select, equalTo(control));

        control = "SELECT test_key, some_long, some_int, some_string, some_dtm, some_enum FROM test_bean WHERE test_key IN (SELECT unnest(:test_key))";
        select = GENERATOR_ARRAY.generateBulkSelectSqlTemplateNamed(tableData);
        assertThat(select, equalTo(control));
    }

    @Test
    public void testGenerateInsertSqlTemplate() {
        String control = "INSERT INTO test_bean (some_long, some_int, some_string, some_dtm, some_enum) VALUES (?, ?, ?, ?, ?)";
        TableData tableData = TableData.from(TestBean.class);
        String insert = GENERATOR.generateInsertSqlTemplate(tableData);
        assertThat(insert, equalTo(control));
    }

    @Test
    public void testGenerateInsertSqlTemplateNamed() {
        String control = "INSERT INTO test_bean (some_long, some_int, some_string, some_dtm, some_enum) VALUES (:some_long, :some_int, :some_string, :some_dtm, :some_enum)";
        TableData tableData = TableData.from(TestBean.class);
        String insert = GENERATOR.generateInsertSqlTemplateNamed(tableData);
        assertThat(insert, equalTo(control));
    }

    @Test
    public void testGenerateUpdateSqlTemplate() {
        String control = "UPDATE test_bean SET some_long = ?, some_int = ?, some_string = ?, some_dtm = ?, some_enum = ? WHERE test_key = ?";
        TableData tableData = TableData.from(TestBean.class);
        String update = GENERATOR.generateUpdateSqlTemplate(tableData);
        assertThat(update, equalTo(control));
    }

    @Test
    public void testGenerateUpdateSqlTemplateNamed() {
        String control = "UPDATE test_bean SET some_long = :some_long, some_int = :some_int, some_string = :some_string, some_dtm = :some_dtm, some_enum = :some_enum WHERE test_key = :test_key";
        TableData tableData = TableData.from(TestBean.class);
        String update = GENERATOR.generateUpdateSqlTemplateNamed(tableData);
        assertThat(update, equalTo(control));
    }

    @Test
    public void testGenerateDeleteSqlTemplate() {
        String control = "DELETE FROM test_bean WHERE test_key = ?";
        TableData tableData = TableData.from(TestBean.class);
        String delete = GENERATOR.generateDeleteSqlTemplate(tableData);
        assertThat(delete, equalTo(control));
    }

    @Test
    public void testGenerateDeleteSqlTemplateNamed() {
        String control = "DELETE FROM test_bean WHERE test_key = :test_key";
        TableData tableData = TableData.from(TestBean.class);
        String delete = GENERATOR.generateDeleteSqlTemplateNamed(tableData);
        assertThat(delete, equalTo(control));
    }

    @Test
    public void testGenerateBulkDeleteSqlTemplate() {
        String control = "DELETE FROM test_bean WHERE test_key IN (?)";
        TableData tableData = TableData.from(TestBean.class);
        String delete = GENERATOR.generateBulkDeleteSqlTemplate(tableData);
        assertThat(delete, equalTo(control));

        control = "DELETE FROM test_bean WHERE test_key IN (SELECT unnest(?))";
        delete = GENERATOR_ARRAY.generateBulkDeleteSqlTemplate(tableData);
        assertThat(delete, equalTo(control));
    }

    @Test
    public void testGenerateBulkDeleteSqlTemplateNamed() {
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
    public void testGenerateCreateStatement() {
        String control = new Scanner(BetterSqlMapperTest.class.getResourceAsStream("/sql/test_create.sql"), "UTF-8").useDelimiter("\\A").next();
        control = control.replaceAll("\\n", "").replaceAll("  ", " ").replaceAll("\\( ", "\\(").replaceAll(" AUTO_INCREMENT", "");
        TableData tableData = TableData.from(TestBean.class);
        String create = GENERATOR.generateCreateStatement(tableData);
        assertThat(create, equalTo(control));
    }
}
