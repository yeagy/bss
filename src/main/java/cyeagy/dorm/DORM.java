package cyeagy.dorm;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.lang.reflect.Field;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.CaseFormat.*;
import static java.util.stream.Collectors.*;

public class DORM {
    private static final List<Class<?>> QUOTED_CLASSES = Lists.newArrayList(String.class, Timestamp.class, Date.class, Time.class);

    public static String generateSelectSql(Object bean) throws IllegalAccessException {
        final Class<?> beanClass = bean.getClass();
        final String tableName = getTableName(beanClass);
        final Field[] fields = beanClass.getDeclaredFields();
        String primaryKey = null;
        String primaryKeyVal = null;
        for (Field field : fields) {
            if(field.isAnnotationPresent(Id.class)){
                primaryKey = field.getName();
                primaryKeyVal = readFieldValue(bean, field);
                break;
            }
        }
        final String colString = Arrays.stream(fields).map(Field::getName).collect(joining(", "));
        final String where = primaryKey == null ? "" : String.format(" WHERE %s = %s", primaryKey, primaryKeyVal);
        return String.format("SELECT %s FROM %s%s", colString, tableName, where);
    }

    public static String generateInsertSql(Object bean) throws IllegalAccessException {
        final Class<?> beanClass = bean.getClass();
        final String tableName = getTableName(beanClass);
        final Field[] fields = beanClass.getDeclaredFields();
        final List<String> cols = new ArrayList<>(fields.length);
        final List<String> vals = new ArrayList<>(fields.length);
        for (Field field : fields) {
            if(field.isAnnotationPresent(Id.class)){

            } else {
                cols.add(field.getName());
                String val = readFieldValue(bean, field);
                vals.add(val);
            }
        }
        String colString = cols.stream().collect(joining(", "));
        String valString = vals.stream().collect(joining(", "));
        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, colString, valString);
    }

    public static String generateUpdateSql(Object bean) throws IllegalAccessException {
        final Class<?> beanClass = bean.getClass();
        final String tableName = getTableName(beanClass);
        final Field[] fields = beanClass.getDeclaredFields();
        final List<String> colVals = new ArrayList<>(fields.length);
        String primaryKey = null;
        String primaryKeyVal = null;
        for (Field field : fields) {
            if (field.isAnnotationPresent(Id.class)) {
                primaryKey = field.getName();
                primaryKeyVal = readFieldValue(bean, field);
            } else{
                String val = readFieldValue(bean, field);
                colVals.add(field.getName() + " = " + val);
            }
        }
        final String colValString = colVals.stream().collect(joining(", "));
        return String.format("UPDATE %s SET %s WHERE %s = %s", tableName, colValString, primaryKey, primaryKeyVal);
    }

    private static String readFieldValue(Object bean, Field field) throws IllegalAccessException {
        String val = FieldUtils.readField(field, bean, true).toString();
        if(QUOTED_CLASSES.contains(field.getType())){
            val = "'" + val + "'";
        }
        return val;
    }

    private static String getTableName(Class<?> beanClass){
        return UPPER_CAMEL.to(LOWER_UNDERSCORE, beanClass.getSimpleName());
    }

    public static String generateDeleteSql(Object bean) throws IllegalAccessException {
        final Class<?> beanClass = bean.getClass();
        final String tableName = getTableName(beanClass);
        final Field[] fields = beanClass.getDeclaredFields();
        String primaryKey = null;
        String primaryKeyVal = null;
        for (Field field : fields) {
            if(field.isAnnotationPresent(Id.class)){
                primaryKey = field.getName();
                primaryKeyVal = readFieldValue(bean, field);
                break;
            }
        }
        return String.format("DELETE FROM %s WHERE %s = %s", tableName, primaryKey, primaryKeyVal);
    }
}
