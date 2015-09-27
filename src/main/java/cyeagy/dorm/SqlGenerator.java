package cyeagy.dorm;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.lang.reflect.Field;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.IntStream;

import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import static java.util.stream.Collectors.joining;

public class SqlGenerator {
    public String generateSelectSql(Object bean) throws IllegalAccessException {
        TableData table = TableData.analyze(bean);
        return formatSelect(table.columnsWithPrimaryKey(), table.tableName, table.primaryKey, table.primaryKeyValue);
    }

    public String generateSelectSqlTemplate(Class<?> clazz) throws IllegalAccessException {
        TableData table = TableData.analyze(clazz);
        return formatSelect(table.columnsWithPrimaryKey(), table.tableName, table.primaryKey, "?");
    }

    public String generateSelectSqlTemplateNamed(Class<?> clazz) throws IllegalAccessException {
        TableData table = TableData.analyze(clazz);
        return formatSelect(table.columnsWithPrimaryKey(), table.tableName, table.primaryKey, ":" + table.primaryKey);
    }

    private String formatSelect(String columns, String tableName, String primaryKey, String primaryKeyValue){
        return String.format("SELECT %s FROM %s WHERE %s = %s", columns, tableName, primaryKey, primaryKeyValue);
    }

    public String generateInsertSql(Object bean) throws IllegalAccessException {
        TableData table = TableData.analyze(bean);
        return formatInsert(table.tableName, table.columns(), table.columnValues());
    }

    public String generateInsertSqlTemplate(Class<?> clazz) throws IllegalAccessException {
        TableData table = TableData.analyze(clazz);
        return formatInsert(table.tableName, table.columns(), table.columnsIndexParams());
    }

    public String generateInsertSqlTemplateNamed(Class<?> clazz) throws IllegalAccessException {
        TableData table = TableData.analyze(clazz);
        return formatInsert(table.tableName, table.columns(), table.columnsNamedParams());
    }

    private String formatInsert(String tableName, String columns, String values){
        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, values);
    }

    public String generateUpdateSql(Object bean) throws IllegalAccessException {
        TableData table = TableData.analyze(bean);
        return formatUpdate(table.tableName, table.columnsAndValues(), table.primaryKey, table.primaryKeyValue);
    }

    public String generateUpdateSqlTemplate(Class<?> clazz) throws IllegalAccessException {
        TableData table = TableData.analyze(clazz);
        return formatUpdate(table.tableName, table.columnsWithIndexParams(), table.primaryKey, "?");
    }

    public String generateUpdateSqlTemplateNamed(Class<?> clazz) throws IllegalAccessException {
        TableData table = TableData.analyze(clazz);
        return formatUpdate(table.tableName, table.columnsWithNamedParams(), table.primaryKey, ":" + table.primaryKey);
    }

    private String formatUpdate(String tableName, String columnsAndValues, String primaryKey, String primaryKeyValue){
        return String.format("UPDATE %s SET %s WHERE %s = %s", tableName, columnsAndValues, primaryKey, primaryKeyValue);
    }

    public String generateDeleteSql(Object bean) throws IllegalAccessException {
        TableData table = TableData.analyze(bean);
        return formatDelete(table.tableName, table.primaryKey, table.primaryKeyValue);
    }

    public String generateDeleteSqlTemplate(Class<?> clazz) throws IllegalAccessException {
        TableData table = TableData.analyze(clazz);
        return formatDelete(table.tableName, table.primaryKey, "?");
    }

    public String generateDeleteSqlTemplateNamed(Class<?> clazz) throws IllegalAccessException {
        TableData table = TableData.analyze(clazz);
        return formatDelete(table.tableName, table.primaryKey, ":" + table.primaryKey);
    }

    private String formatDelete(String tableName, String primaryKey, String primaryKeyValue){
        return String.format("DELETE FROM %s WHERE %s = %s", tableName, primaryKey, primaryKeyValue);
    }

    private static class TableData{
        private static final List<Class<?>> QUOTED_CLASSES = Lists.newArrayList(String.class, Timestamp.class, Date.class, Time.class);
        private static final Collector<CharSequence, ?, String> COMMA_JOIN = joining(", ");

        private final String tableName;
        private final String primaryKey;
        private final String primaryKeyValue;
        private final List<Pair<String, String>> columnData;//excluding PK

        private TableData(String tableName, String primaryKey, String primaryKeyValue, List<Pair<String, String>> columnData) {
            this.tableName = tableName;
            this.primaryKey = primaryKey;
            this.primaryKeyValue = primaryKeyValue;
            this.columnData = columnData;
        }

        private static TableData analyze(Object bean) throws IllegalAccessException {
            final Class<?> clazz = bean.getClass();
            final String tableName = getTableName(clazz);
            final Field[] fields = clazz.getDeclaredFields();
            final List<Pair<String, String>> columns = new ArrayList<>(fields.length);
            String primaryKey = null;
            String primaryKeyVal = null;
            for (Field field : fields) {
                if(field.isAnnotationPresent(Id.class)){
                    primaryKey = field.getName();
                    primaryKeyVal = readFieldValue(bean, field);
                } else {
                    columns.add(new ImmutablePair<>(field.getName(), readFieldValue(bean, field)));
                }
            }
            return new TableData(tableName, primaryKey, primaryKeyVal, columns);
        }

        private static TableData analyze(Class<?> clazz){
            final String tableName = getTableName(clazz);
            final Field[] fields = clazz.getDeclaredFields();
            final List<Pair<String, String>> columns = new ArrayList<>(fields.length);
            String primaryKey = null;
            for (Field field : fields) {
                if(field.isAnnotationPresent(Id.class)){
                    primaryKey = field.getName();
                } else {
                    columns.add(new ImmutablePair<>(field.getName(), null));
                }
            }
            return new TableData(tableName, primaryKey, null, columns);
        }

        private static String readFieldValue(Object bean, Field field) throws IllegalAccessException {
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

        private static String getTableName(Class<?> beanClass){
            return UPPER_CAMEL.to(LOWER_UNDERSCORE, beanClass.getSimpleName());
        }

        public String columns() {
            return columnData.stream().map(Pair::getLeft).collect(COMMA_JOIN);
        }

        public String columnsWithPrimaryKey() {
            return primaryKey + ", " + columnData.stream().map(Pair::getLeft).collect(COMMA_JOIN);
        }

        public String columnValues() {
            return columnData.stream().map(Pair::getRight).collect(COMMA_JOIN);
        }

        public String columnsAndValues() {
            return columnData.stream().map(p -> p.getLeft() + " = " + p.getRight()).collect(COMMA_JOIN);
        }

        public String columnsNamedParams() {
            return columnData.stream().map(p -> ":" + p.getLeft()).collect(COMMA_JOIN);
        }

        public String columnsWithIndexParams() {
            return columnData.stream().map(p -> p.getLeft() + " = ?").collect(COMMA_JOIN);
        }

        public String columnsWithNamedParams() {
            return columnData.stream().map(p -> p.getLeft() + " = :" + p.getLeft()).collect(COMMA_JOIN);
        }

        public String columnsIndexParams() {
            return IntStream.range(0, columnData.size()).mapToObj(i -> "?").collect(COMMA_JOIN);
        }
    }
}
