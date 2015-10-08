package cyeagy.dorm;

import java.lang.reflect.Field;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This class analyzes a POJO via reflection to identify table data
 * this can be created via the SqlGenerator class.
 */
public class TableData {
    private final String tableName;
    private final Field primaryKey;
    private final List<Field> columns;//excluding PK

    private TableData(String tableName, Field primaryKey, List<Field> columns) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.columns = columns;
    }

    public String getTableName() {
        return tableName;
    }

    public Field getPrimaryKey() {
        return primaryKey;
    }

    public List<Field> getColumns() {
        return columns;
    }

    public static TableData analyze(Class<?> clazz){
        return analyze(clazz, true);
    }

    public static TableData analyze(Class<?> clazz, boolean forceAccessible){
        final String tableName = getTableName(clazz);
        final Field[] fields = clazz.getDeclaredFields();
        final List<Field> columns = new ArrayList<>(fields.length);
        Field primaryKey = null;
        for (Field field : fields) {
            if(field.isAnnotationPresent(Id.class)){
                primaryKey = field;
            } else {
                columns.add(field);
            }
            if(forceAccessible){
                ReflectUtil.setAccessible(field);
            }
        }
        if(primaryKey == null){
            primaryKey = fields[0];
            columns.remove(0);//remove the pk from the column list
        }
        return new TableData(tableName, primaryKey, Collections.unmodifiableList(columns));
    }

    public static String getColumnName(Field field) {
        final Column annotation = field.getDeclaredAnnotation(Column.class);
        return camelToSnake(annotation == null ? field.getName() : annotation.name());
    }

    public static String getTableName(Class<?> clazz){
        final Table annotation = clazz.getDeclaredAnnotation(Table.class);
        if(annotation != null){
            if(annotation.schema() == null || annotation.schema().isEmpty()){
                return annotation.name();
            }
            return annotation.schema() + "." + annotation.name();
        }
        return camelToSnake(clazz.getSimpleName());
    }

    private static String camelToSnake(String camel){
        final StringBuilder sb = new StringBuilder();
        final StringCharacterIterator iter = new StringCharacterIterator(camel);
        boolean lastLower = false;
        for(char c = iter.first(); c != CharacterIterator.DONE; c = iter.next()){
            if(Character.isUpperCase(c)){
                if(lastLower){
                    sb.append('_');
                }
                sb.append(Character.toLowerCase(c));
                lastLower = false;
            } else {
                sb.append(c);
                lastLower = true;
            }
        }
        return sb.toString();
    }
}
