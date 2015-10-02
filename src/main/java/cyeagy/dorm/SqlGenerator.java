package cyeagy.dorm;

import java.lang.reflect.Field;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.IntStream;

import static cyeagy.dorm.TableData.getColumnName;
import static java.util.stream.Collectors.joining;

public class SqlGenerator {
    public static final Map<Class<?>, String> CLASS_SQL_TYPE_MAP = initClassTypeMap();//just using this for primary key array casting so far...
    private static final List<Class<?>> QUOTED_CLASSES = Arrays.asList(String.class, Timestamp.class, Date.class, Time.class);
    private static final Collector<CharSequence, ?, String> COMMA_JOIN = joining(", ");

    private static Map<Class<?>, String> initClassTypeMap(){
        final Map<Class<?>, String> map = new HashMap<>();
        map.put(Long.class, "BIGINT");
        map.put(Long.TYPE, "BIGINT");
        map.put(Integer.class, "INTEGER");
        map.put(Integer.TYPE, "INTEGER");
        map.put(String.class, "VARCHAR");
        return Collections.unmodifiableMap(map);
    }

    public String generateSelectSqlTemplate(TableData table) {
        return formatSelect(columnsWithPrimaryKey(table), table.getTableName(), getColumnName(table.getPrimaryKey()), "?");
    }

    public String generateSelectSqlTemplateNamed(TableData table) {
        final String pk = getColumnName(table.getPrimaryKey());
        return formatSelect(columnsWithPrimaryKey(table), table.getTableName(), pk, ":" + pk);
    }

    private String formatSelect(String columns, String tableName, String primaryKey, String primaryKeyValue){
        return String.format("SELECT %s FROM %s WHERE %s = %s", columns, tableName, primaryKey, primaryKeyValue);
    }

    public String generateBulkSelectSqlTemplate(TableData table) {
        final String type = CLASS_SQL_TYPE_MAP.get(table.getPrimaryKey().getType());
        return formatBulkSelect(columnsWithPrimaryKey(table), table.getTableName(), getColumnName(table.getPrimaryKey()), "?", type);
    }

    public String generateBulkSelectSqlTemplateNamed(TableData table) {
        final String pk = getColumnName(table.getPrimaryKey());
        final String type = CLASS_SQL_TYPE_MAP.get(table.getPrimaryKey().getType());
        return formatBulkSelect(columnsWithPrimaryKey(table), table.getTableName(), pk, ":" + pk, type);
    }

    private String formatBulkSelect(String columns, String tableName, String primaryKey, String primaryKeyValue, String arrayType){
        return String.format("SELECT %s FROM %s WHERE %s = ANY (%s :: %s[])", columns, tableName, primaryKey, primaryKeyValue, arrayType);
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

    public String generateUpdateSqlTemplate(TableData table) {
        return formatUpdate(table.getTableName(), columnsWithIndexParams(table), getColumnName(table.getPrimaryKey()), "?");
    }

    public String generateUpdateSqlTemplateNamed(TableData table) {
        final String pk = getColumnName(table.getPrimaryKey());
        return formatUpdate(table.getTableName(), columnsWithNamedParams(table), pk, ":" + pk);
    }

    private String formatUpdate(String tableName, String columnsAndValues, String primaryKey, String primaryKeyValue){
        return String.format("UPDATE %s SET %s WHERE %s = %s", tableName, columnsAndValues, primaryKey, primaryKeyValue);
    }

    public String generateDeleteSqlTemplate(TableData table) {
        return formatDelete(table.getTableName(), getColumnName(table.getPrimaryKey()), "?");
    }

    public String generateDeleteSqlTemplateNamed(TableData table) {
        final String pk = getColumnName(table.getPrimaryKey());
        return formatDelete(table.getTableName(), pk, ":" + pk);
    }

    private String formatDelete(String tableName, String primaryKey, String primaryKeyValue){
        return String.format("DELETE FROM %s WHERE %s = %s", tableName, primaryKey, primaryKeyValue);
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
        return IntStream.range(0, size).mapToObj(i -> "?").collect(COMMA_JOIN);
    }

    public class Extras{

        public String generateSelectSql(TableData table, Object bean) throws IllegalAccessException {
            return formatSelect(columnsWithPrimaryKey(table), table.getTableName(), getColumnName(table.getPrimaryKey()), readFieldValue(table.getPrimaryKey(), bean));
        }

        public String generateInsertSql(TableData table, Object bean) throws IllegalAccessException {
            return formatInsert(table.getTableName(), columns(table), columnValues(table, bean));
        }

        public String generateUpdateSql(TableData table, Object bean) throws IllegalAccessException {
            return formatUpdate(table.getTableName(), columnsAndValues(table, bean), getColumnName(table.getPrimaryKey()), readFieldValue(table.getPrimaryKey(), bean));
        }

        public String generateDeleteSql(TableData table, Object bean) throws IllegalAccessException {
            return formatDelete(table.getTableName(), getColumnName(table.getPrimaryKey()), readFieldValue(table.getPrimaryKey(), bean));
        }

        private String columnValues(TableData table, Object bean) throws IllegalAccessException {
            final List<String> vals = new ArrayList<>(table.getColumns().size());
            for (Field column : table.getColumns()) {
                vals.add(readFieldValue(column, bean));
            }
            return vals.stream().collect(COMMA_JOIN);
        }

        private String columnsAndValues(TableData table, Object bean) throws IllegalAccessException {
            final List<String> vals = new ArrayList<>(table.getColumns().size());
            for (Field column : table.getColumns()) {
                vals.add(getColumnName(column) + " = " + readFieldValue(column, bean));
            }
            return vals.stream().collect(COMMA_JOIN);
        }

        private String readFieldValue(Field field, Object bean) throws IllegalAccessException {
            ReflectUtil.setAccessible(field);
            Object o = ReflectUtil.readField(field, bean);
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
}
