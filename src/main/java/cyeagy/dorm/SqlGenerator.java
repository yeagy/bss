package cyeagy.dorm;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.lang.reflect.Field;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.joining;

public class SqlGenerator {
    private static final List<Class<?>> QUOTED_CLASSES = Lists.newArrayList(String.class, Timestamp.class, Date.class, Time.class);
    private static final Collector<CharSequence, ?, String> COMMA_JOIN = joining(", ");

    public String generateSelectSql(TableData table, Object bean) throws IllegalAccessException {
        return formatSelect(columnsWithPrimaryKey(table), table.getTableName(), table.getPrimaryKey().getName(), readFieldValue(bean, table.getPrimaryKey()));
    }

    public String generateSelectSqlTemplate(TableData table) {
        return formatSelect(columnsWithPrimaryKey(table), table.getTableName(), table.getPrimaryKey().getName(), "?");
    }

    public String generateSelectSqlTemplateNamed(TableData table) {
        final String pk = table.getPrimaryKey().getName();
        return formatSelect(columnsWithPrimaryKey(table), table.getTableName(), pk, ":" + pk);
    }

    private String formatSelect(String columns, String tableName, String primaryKey, String primaryKeyValue){
        return String.format("SELECT %s FROM %s WHERE %s = %s", columns, tableName, primaryKey, primaryKeyValue);
    }

    public String generateInsertSql(TableData table, Object bean) throws IllegalAccessException {
        return formatInsert(table.getTableName(), columns(table), columnValues(table, bean));
    }

    public String generateInsertSqlTemplate(TableData table) {
        return formatInsert(table.getTableName(), columns(table), columnsIndexParams(table.getColumns().size()));
    }

    public String generateInsertSqlTemplateNamed(TableData table) {
        return formatInsert(table.getTableName(), columns(table), columnsNamedParams(table));
    }

    private String formatInsert(String tableName, String columns, String values){
        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, values);
    }

    public String generateUpdateSql(TableData table, Object bean) throws IllegalAccessException {
        return formatUpdate(table.getTableName(), columnsAndValues(table, bean), table.getPrimaryKey().getName(), readFieldValue(bean, table.getPrimaryKey()));
    }

    public String generateUpdateSqlTemplate(TableData table) {
        return formatUpdate(table.getTableName(), columnsWithIndexParams(table), table.getPrimaryKey().getName(), "?");
    }

    public String generateUpdateSqlTemplateNamed(TableData table) {
        final String pk = table.getPrimaryKey().getName();
        return formatUpdate(table.getTableName(), columnsWithNamedParams(table), pk, ":" + pk);
    }

    private String formatUpdate(String tableName, String columnsAndValues, String primaryKey, String primaryKeyValue){
        return String.format("UPDATE %s SET %s WHERE %s = %s", tableName, columnsAndValues, primaryKey, primaryKeyValue);
    }

    public String generateDeleteSql(TableData table, Object bean) throws IllegalAccessException {
        return formatDelete(table.getTableName(), table.getPrimaryKey().getName(), readFieldValue(bean, table.getPrimaryKey()));
    }

    public String generateDeleteSqlTemplate(TableData table) {
        return formatDelete(table.getTableName(), table.getPrimaryKey().getName(), "?");
    }

    public String generateDeleteSqlTemplateNamed(TableData table) {
        final String pk = table.getPrimaryKey().getName();
        return formatDelete(table.getTableName(), pk, ":" + pk);
    }

    private String formatDelete(String tableName, String primaryKey, String primaryKeyValue){
        return String.format("DELETE FROM %s WHERE %s = %s", tableName, primaryKey, primaryKeyValue);
    }

    private String columns(TableData table) {
        return table.getColumns().stream().map(Field::getName).collect(COMMA_JOIN);
    }

    private String columnsWithPrimaryKey(TableData table) {
        return table.getPrimaryKey().getName() + ", " + columns(table);
    }

    private String columnValues(TableData table, Object bean) throws IllegalAccessException {
        final List<String> vals = new ArrayList<>(table.getColumns().size());
        for (Field column : table.getColumns()) {
            vals.add(readFieldValue(bean, column));
        }
        return vals.stream().collect(COMMA_JOIN);
    }

    private String columnsAndValues(TableData table, Object bean) throws IllegalAccessException {
        final List<String> vals = new ArrayList<>(table.getColumns().size());
        for (Field column : table.getColumns()) {
            vals.add(column.getName() + " = " + readFieldValue(bean, column));
        }
        return vals.stream().collect(COMMA_JOIN);
    }

    private String columnsNamedParams(TableData table) {
        return table.getColumns().stream().map(f -> ":" + f.getName()).collect(COMMA_JOIN);
    }

    private String columnsWithIndexParams(TableData table) {
        return table.getColumns().stream().map(f -> f.getName() + " = ?").collect(COMMA_JOIN);
    }

    private String columnsWithNamedParams(TableData table) {
        return table.getColumns().stream().map(f -> f.getName() + " = :" + f.getName()).collect(COMMA_JOIN);
    }

    private String columnsIndexParams(int size) {
        return IntStream.range(0, size).mapToObj(i -> "?").collect(COMMA_JOIN);
    }

    private String readFieldValue(Object bean, Field field) throws IllegalAccessException {
        Object o = FieldUtils.readField(field, bean, true);
        if(o == null){
            return null;
        }
        String val = o.toString();
        if(QUOTED_CLASSES.contains(field.getType())){
            val = "'" + val + "'";
        }
        return val;
    }
}
