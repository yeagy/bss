package cyeagy.dorm;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;

public class DORM {
    private static final SqlGenerator GENERATOR = new SqlGenerator();

    public static <T> T insert(Connection connection, T bean) throws Exception {
        TableData tableData = TableData.analyze(bean.getClass());
        String insert = GENERATOR.generateInsertSqlTemplate(tableData);
        try(PreparedStatement ps = connection.prepareStatement(insert, Statement.RETURN_GENERATED_KEYS)){
            for (int i = 0; i < tableData.getColumns().size(); i++) {
                final Field field = tableData.getColumns().get(i);
                final Class<?> type = field.getType();
                final int idx = i + 1;
                if(type == Long.TYPE){
                    final Long v = (Long) FieldUtils.readField(field, bean, true);
                    ps.setLong(idx, v);
                } else if(type == Integer.TYPE){
                    final Integer v = (Integer) FieldUtils.readField(field, bean, true);
                    ps.setInt(idx, v);
                } else if(type == String.class){
                    final String v = (String) FieldUtils.readField(field, bean, true);
                    ps.setString(idx, v);
                } else if(type == Timestamp.class){
                    final Timestamp v = (Timestamp) FieldUtils.readField(field, bean, true);
                    ps.setTimestamp(idx, v);
                }
            }
            ps.execute();
            try(final ResultSet rs = ps.getGeneratedKeys()){
                if(rs.next()){
                    final Class<?> type = tableData.getPrimaryKey().getType();
                    if(type == Long.class){
                        final long pk = rs.getLong(1);
                        final T result = ConstructorUtils.invokeConstructor((Class<T>) bean.getClass());
                        FieldUtils.writeField(tableData.getPrimaryKey(), result, pk, true);
                        for (Field field : tableData.getColumns()) {
                            FieldUtils.writeField(field, result, FieldUtils.readField(field, bean, true), true);
                        }
                        return result;
                    }
                }
            }
        }
        return null;
    }
}
