package io.github.yeagy.bss;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collector;

import static java.util.stream.Collectors.joining;

/**
 * Use reflection to generate SQL prepared statements from POJOs
 * <p>
 * Bulk select currently unsupported for compound keys. could do this with a disjunction of conjunctions, but the performance would be abysmal on anything large.
 */
public final class BetterSqlGenerator {
    private static final Collector<CharSequence, ?, String> COMMA_JOIN = joining(", ");
    private static final Collector<CharSequence, ?, String> AND_JOIN = joining(" AND ");

    private final BetterOptions options;

    private BetterSqlGenerator(BetterOptions options) {
        this.options = options;
    }

    public static BetterSqlGenerator fromDefaults() {
        return from(BetterOptions.fromDefaults());
    }

    public static BetterSqlGenerator from(BetterOptions options) {
        return new BetterSqlGenerator(options);
    }

    public String generateSelectSqlTemplate(TableData table) {
        return formatSelect(columns(table, true), table.getTableName(), primaryKeysWithIndexParams(table));
    }

    public String generateSelectSqlTemplateNamed(TableData table) {
        return formatSelect(columns(table, true), table.getTableName(), primaryKeysWithNamedParams(table));
    }

    private String formatSelect(String columns, String tableName, String conditions) {
        return String.format("SELECT %s FROM %s WHERE %s", columns, tableName, conditions);
    }

    public String generateBulkSelectSqlTemplate(TableData table) {
        if (table.getPrimaryKeys().size() > 1) {
            throw new UnsupportedOperationException("bulk select sql generation not supported for compound keys");
        }
        final String pk = TableData.getColumnName(table.getPrimaryKeys().get(0));
        if (options.arraySupport()) {
            return formatBulkSelectArrayUnnest(columns(table, true), table.getTableName(), pk, "?");
        }
        return formatBulkSelect(columns(table, true), table.getTableName(), pk, "?");
    }

    public String generateBulkSelectSqlTemplateNamed(TableData table) {
        if (table.getPrimaryKeys().size() > 1) {
            throw new UnsupportedOperationException("bulk select sql generation not supported for compound keys");
        }
        final String pk = TableData.getColumnName(table.getPrimaryKeys().get(0));
        if (options.arraySupport()) {
            return formatBulkSelectArrayUnnest(columns(table, true), table.getTableName(), pk, ":" + pk);
        }
        return formatBulkSelect(columns(table, true), table.getTableName(), pk, ":" + pk);
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
        final String columns = columns(table, includePrimaryKey);
        final int numCols = includePrimaryKey ? table.getPrimaryKeys().size() + table.getColumns().size() : table.getColumns().size();
        return formatInsert(table.getTableName(), columns, columnsIndexParams(numCols));
    }

    public String generateInsertSqlTemplateNamed(TableData table, boolean includePrimaryKey) {
        final String columns = columns(table, includePrimaryKey);
        final String namedParams = columnsAsNamedParams(table, includePrimaryKey);
        return formatInsert(table.getTableName(), columns, namedParams);
    }

    private String formatInsert(String tableName, String columns, String values) {
        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, values);
    }

    public String generateUpdateSqlTemplate(TableData table) {
        return formatUpdate(table.getTableName(), columnsWithIndexParams(table), primaryKeysWithIndexParams(table));
    }

    public String generateUpdateSqlTemplateNamed(TableData table) {
        return formatUpdate(table.getTableName(), columnsWithNamedParams(table), primaryKeysWithNamedParams(table));
    }

    private String formatUpdate(String tableName, String columnsAndValues, String conditions) {
        return String.format("UPDATE %s SET %s WHERE %s", tableName, columnsAndValues, conditions);
    }

    public String generateDeleteSqlTemplate(TableData table) {
        return formatDelete(table.getTableName(), primaryKeysWithIndexParams(table));
    }

    public String generateDeleteSqlTemplateNamed(TableData table) {
        return formatDelete(table.getTableName(), primaryKeysWithNamedParams(table));
    }

    private String formatDelete(String tableName, String conditions) {
        return String.format("DELETE FROM %s WHERE %s", tableName, conditions);
    }

    public String generateBulkDeleteSqlTemplate(TableData table) {
        if (table.getPrimaryKeys().size() > 1) {
            throw new UnsupportedOperationException("bulk delete sql generation not supported for compound keys");
        }
        final String pk = TableData.getColumnName(table.getPrimaryKeys().get(0));
        if (options.arraySupport()) {
            return formatBulkDeleteArrayUnnest(table.getTableName(), pk, "?");
        }
        return formatBulkDelete(table.getTableName(), pk, "?");
    }

    public String generateBulkDeleteSqlTemplateNamed(TableData table) {
        if (table.getPrimaryKeys().size() > 1) {
            throw new UnsupportedOperationException("bulk delete sql generation not supported for compound keys");
        }
        final String pk = TableData.getColumnName(table.getPrimaryKeys().get(0));
        if (options.arraySupport()) {
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

    //todo fix compound keys
    public String generateCreateStatement(TableData table) {
        final List<String> columns = new ArrayList<>(table.getColumns().size() + table.getPrimaryKeys().size());
        for (Field field : table.getPrimaryKeys()) {
            columns.add(TableData.getColumnName(field) + " " + TypeMappers.getSqlType(field.getType()).toUpperCase() + " PRIMARY KEY");
        }
        for (Field field : table.getColumns()) {
            columns.add(TableData.getColumnName(field) + " " + TypeMappers.getSqlType(field.getType()).toUpperCase() + (field.getType().isPrimitive() ? " NOT NULL" : ""));
        }
        return formatCreate(table.getTableName(), columns);
    }

    private String formatCreate(String tableName, List<String> columns) {
        return String.format("CREATE TABLE %s (%s)", tableName, columns.stream().collect(COMMA_JOIN));
    }

    private String primaryKeysWithIndexParams(TableData table) {
        return table.getPrimaryKeys().stream().map(k -> TableData.getColumnName(k) + " = ?").collect(AND_JOIN);
    }

    private String primaryKeysWithNamedParams(TableData table) {
        return table.getPrimaryKeys().stream().map(k -> TableData.getColumnName(k) + " = :" + TableData.getColumnName(k)).collect(AND_JOIN);
    }

    private String columns(TableData table, boolean includePrimaryKeys) {
        final String columns = table.getColumns().stream().map(TableData::getColumnName).collect(COMMA_JOIN);
        if (includePrimaryKeys) {
            return table.getPrimaryKeys().stream().map(TableData::getColumnName).collect(COMMA_JOIN) + ", " + columns;
        }
        return columns;
    }

    private String columnsAsNamedParams(TableData table, boolean includePrimaryKeys) {
        final String columns = table.getColumns().stream().map(f -> ":" + TableData.getColumnName(f)).collect(COMMA_JOIN);
        if (includePrimaryKeys) {
            return table.getPrimaryKeys().stream().map(f -> ":" + TableData.getColumnName(f)).collect(COMMA_JOIN) + ", " + columns;
        }
        return columns;
    }

    private String columnsWithIndexParams(TableData table) {
        return table.getColumns().stream().map(f -> TableData.getColumnName(f) + " = ?").collect(COMMA_JOIN);
    }

    private String columnsWithNamedParams(TableData table) {
        return table.getColumns().stream().map(f -> TableData.getColumnName(f) + " = :" + TableData.getColumnName(f)).collect(COMMA_JOIN);
    }

    private String columnsIndexParams(int size) {
        return String.join(", ", Collections.nCopies(size, "?"));
    }
}
