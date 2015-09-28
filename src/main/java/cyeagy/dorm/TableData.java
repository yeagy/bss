package cyeagy.dorm;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;

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
        final String tableName = determineTableName(clazz);
        final Field[] fields = clazz.getDeclaredFields();
        final List<Field> columns = new ArrayList<>(fields.length);
        Field primaryKey = null;
        for (Field field : fields) {
            if(field.isAnnotationPresent(Id.class)){
                primaryKey = field;
            } else {
                columns.add(field);
            }
        }
        if(primaryKey == null){
            primaryKey = fields[0];
            columns.remove(0);//remove the pk from the column list
        }
        return new TableData(tableName, primaryKey, columns);
    }

    private static String determineTableName(Class<?> beanClass){
        return UPPER_CAMEL.to(LOWER_UNDERSCORE, beanClass.getSimpleName());
    }
}
