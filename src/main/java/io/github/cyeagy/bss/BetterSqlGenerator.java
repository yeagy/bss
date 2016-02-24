package io.github.cyeagy.bss;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collector;

import static io.github.cyeagy.bss.TableData.getColumnName;
import static java.util.stream.Collectors.joining;

/**
 * Use reflection to generate SQL prepared statements from POJOs
 */
public class BetterSqlGenerator {
    private static final Collector<CharSequence, ?, String> COMMA_JOIN = joining(", ");

    private final BetterOptions options;

    private BetterSqlGenerator(BetterOptions options) {
        this.options = options;
    }

    public static BetterSqlGenerator fromDefaults() {
        return from(BetterOptions.fromDefaults());
    }

    public static BetterSqlGenerator from(BetterOptions options){
        return new BetterSqlGenerator(options);
    }

    public String generateSelectSqlTemplate(TableData table) {
        return formatSelect(columnsWithPrimaryKey(table), table.getTableName(), getColumnName(table.getPrimaryKey()), "?");
    }

    public String generateSelectSqlTemplateNamed(TableData table) {
        final String pk = getColumnName(table.getPrimaryKey());
        return formatSelect(columnsWithPrimaryKey(table), table.getTableName(), pk, ":" + pk);
    }

    private String formatSelect(String columns, String tableName, String primaryKey, String primaryKeyValue) {
        return String.format("SELECT %s FROM %s WHERE %s = %s", columns, tableName, primaryKey, primaryKeyValue);
    }

    public String generateBulkSelectSqlTemplate(TableData table) {
        if(options.arraySupport()){
            return formatBulkSelectArrayUnnest(columnsWithPrimaryKey(table), table.getTableName(), getColumnName(table.getPrimaryKey()), "?");
        }
        return formatBulkSelect(columnsWithPrimaryKey(table), table.getTableName(), getColumnName(table.getPrimaryKey()), "?");
    }

    public String generateBulkSelectSqlTemplateNamed(TableData table) {
        final String pk = getColumnName(table.getPrimaryKey());
        if(options.arraySupport()){
            return formatBulkSelectArrayUnnest(columnsWithPrimaryKey(table), table.getTableName(), pk, ":" + pk);
        }
        return formatBulkSelect(columnsWithPrimaryKey(table), table.getTableName(), pk, ":" + pk);
    }

    private String formatBulkSelect(String columns, String tableName, String primaryKey, String primaryKeyValue) {
        return String.format("SELECT %s FROM %s WHERE %s IN (%s)", columns, tableName, primaryKey, primaryKeyValue);
    }

    private String formatBulkSelectArrayUnnest(String columns, String tableName, String primaryKey, String primaryKeyValue) {
        return String.format("SELECT %s FROM %s WHERE %s IN (SELECT unnest(%s))", columns, tableName, primaryKey, primaryKeyValue);
    }

    public String generateInsertSqlTemplate(TableData table) {
        return generateInsertSqlTemplate(table, false);
    }

    public String generateInsertSqlTemplateNamed(TableData table) {
        return generateInsertSqlTemplateNamed(table, false);
    }

    public String generateInsertSqlTemplate(TableData table, boolean includePrimaryKey) {
        String columns = columns(table);
        int numCols = table.getColumns().size();
        if (includePrimaryKey) {
            columns = getColumnName(table.getPrimaryKey()) + ", " + columns;
            numCols++;
        }
        return formatInsert(table.getTableName(), columns, columnsIndexParams(numCols));
    }

    public String generateInsertSqlTemplateNamed(TableData table, boolean includePrimaryKey) {
        String columns = columns(table);
        String namedParams = columnsNamedParams(table);
        if (includePrimaryKey) {
            columns = getColumnName(table.getPrimaryKey()) + ", " + columns;
            namedParams = ":" + getColumnName(table.getPrimaryKey()) + ", " + columns;
        }
        return formatInsert(table.getTableName(), columns, namedParams);
    }

    private String formatInsert(String tableName, String columns, String values) {
        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, values);
    }

    public String generateUpdateSqlTemplate(TableData table) {
        return formatUpdate(table.getTableName(), columnsWithIndexParams(table), getColumnName(table.getPrimaryKey()), "?");
    }

    public String generateUpdateSqlTemplateNamed(TableData table) {
        final String pk = getColumnName(table.getPrimaryKey());
        return formatUpdate(table.getTableName(), columnsWithNamedParams(table), pk, ":" + pk);
    }

    private String formatUpdate(String tableName, String columnsAndValues, String primaryKey, String primaryKeyValue) {
        return String.format("UPDATE %s SET %s WHERE %s = %s", tableName, columnsAndValues, primaryKey, primaryKeyValue);
    }

    public String generateDeleteSqlTemplate(TableData table) {
        return formatDelete(table.getTableName(), getColumnName(table.getPrimaryKey()), "?");
    }

    public String generateDeleteSqlTemplateNamed(TableData table) {
        final String pk = getColumnName(table.getPrimaryKey());
        return formatDelete(table.getTableName(), pk, ":" + pk);
    }

    private String formatDelete(String tableName, String primaryKey, String primaryKeyValue) {
        return String.format("DELETE FROM %s WHERE %s = %s", tableName, primaryKey, primaryKeyValue);
    }

    public String generateBulkDeleteSqlTemplate(TableData table) {
        if(options.arraySupport()){
            return formatBulkDeleteArrayUnnest(table.getTableName(), getColumnName(table.getPrimaryKey()), "?");
        }
        return formatBulkDelete(table.getTableName(), getColumnName(table.getPrimaryKey()), "?");
    }

    public String generateBulkDeleteSqlTemplateNamed(TableData table) {
        final String pk = getColumnName(table.getPrimaryKey());
        if(options.arraySupport()){
            return formatBulkDeleteArrayUnnest(table.getTableName(), pk, ":" + pk);
        }
        return formatBulkDelete(table.getTableName(), pk, ":" + pk);
    }

    private String formatBulkDelete(String tableName, String primaryKey, String primaryKeyValue) {
        return String.format("DELETE FROM %s WHERE %s IN (%s)", tableName, primaryKey, primaryKeyValue);
    }

    private String formatBulkDeleteArrayUnnest(String tableName, String primaryKey, String primaryKeyValue) {
        return String.format("DELETE FROM %s WHERE %s IN (SELECT unnest(%s))", tableName, primaryKey, primaryKeyValue);
    }

    public String generateCreateStatement(TableData table) {
        final List<String> columns = new ArrayList<>(table.getColumns().size() + 1);
        columns.add(getColumnName(table.getPrimaryKey()) + " " + TypeMappers.getSqlType(table.getPrimaryKey().getType()).toUpperCase() + " PRIMARY KEY");
        for (Field field : table.getColumns()) {
            columns.add(getColumnName(field) + " " + TypeMappers.getSqlType(field.getType()).toUpperCase() + (field.getType().isPrimitive() ? " NOT NULL" : ""));
        }
        return formatCreate(table.getTableName(), columns);
    }

    private String formatCreate(String tableName, List<String> columns) {
        return String.format("CREATE TABLE %s (%s)", tableName, columns.stream().collect(COMMA_JOIN));
    }

    private String columns(TableData table) {
        return table.getColumns().stream().map(TableData::getColumnName).collect(COMMA_JOIN);
    }

    private String columnsWithPrimaryKey(TableData table) {
        return getColumnName(table.getPrimaryKey()) + ", " + columns(table);
    }

    private String columnsNamedParams(TableData table) {
        return table.getColumns().stream().map(f -> ":" + getColumnName(f)).collect(COMMA_JOIN);
    }

    private String columnsWithIndexParams(TableData table) {
        return table.getColumns().stream().map(f -> getColumnName(f) + " = ?").collect(COMMA_JOIN);
    }

    private String columnsWithNamedParams(TableData table) {
        return table.getColumns().stream().map(f -> getColumnName(f) + " = :" + getColumnName(f)).collect(COMMA_JOIN);
    }

    private String columnsIndexParams(int size) {
        return String.join(", ", Collections.nCopies(size, "?"));
    }
}
