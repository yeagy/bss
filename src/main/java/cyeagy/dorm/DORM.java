package cyeagy.dorm;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static cyeagy.dorm.ReflectUtil.*;

public class DORM {
    private static final SqlGenerator GENERATOR = new SqlGenerator();

    public <T> T select(Connection connection, Object key, Class<T> clazz) throws Exception{
        final TableData tableData = TableData.analyze(clazz);
        final String select = GENERATOR.generateSelectSqlTemplate(tableData);
        try(final PreparedStatement ps = connection.prepareStatement(select)){
            setParameter(ps, key, 1);
            try(final ResultSet rs = ps.executeQuery()){
                if (rs.next()){
                    final T result = constructNewInstance(clazz);
                    setField(rs, tableData.getPrimaryKey(), result, null);
                    for (Field field : tableData.getColumns()) {
                        setField(rs, field, result, null);
                    }
                    return result;
                }
            }
        }
        return null;
    }

    public <T> Set<T> select(Connection connection, Collection<?> keys, Class<T> clazz) throws Exception{
        final TableData tableData = TableData.analyze(clazz);
        final String select = GENERATOR.generateSelectSqlTemplate(tableData);
        final Set<T> results = new HashSet<>(keys.size());
        try(final PreparedStatement ps = connection.prepareStatement(select)){
            final Array array = connection.createArrayOf(GENERATOR.CLASS_TYPE_STRING.get(tableData.getPrimaryKey().getType()), keys.toArray());
            ps.setArray(1, array);
            try(final ResultSet rs = ps.executeQuery()){
                while (rs.next()){
                    final T result = constructNewInstance(clazz);
                    setField(rs, tableData.getPrimaryKey(), result, null);
                    for (Field field : tableData.getColumns()) {
                        setField(rs, field, result, null);
                    }
                    results.add(result);
                }
            }
        }
        return results;
    }

    public <T> T insert(Connection connection, T bean) throws Exception {
        final TableData tableData = TableData.analyze(bean.getClass());
        final String insert = GENERATOR.generateInsertSqlTemplate(tableData);
        try(final PreparedStatement ps = connection.prepareStatement(insert, Statement.RETURN_GENERATED_KEYS)){
            for (int i = 0; i < tableData.getColumns().size(); i++) {
                final Field field = tableData.getColumns().get(i);
                final int idx = i + 1;
                setParameter(ps, field, bean, idx);
            }
            ps.execute();
            try(final ResultSet rs = ps.getGeneratedKeys()){
                if(rs.next()){
                    final Class<?> type = tableData.getPrimaryKey().getType();
                    if(type == Long.class){
                        final T result = constructNewInstance((Class<T>) bean.getClass());
                        setField(rs, tableData.getPrimaryKey(), result, 1);
                        for (Field field : tableData.getColumns()) {
                            copyField(field, result, bean);
                        }
                        return result;
                    }
                }
            }
        }
        return null;
    }

    public void update(Connection connection, Object bean) throws Exception{
        final TableData tableData = TableData.analyze(bean.getClass());
        final String update = GENERATOR.generateUpdateSqlTemplate(tableData);
        try(final PreparedStatement ps = connection.prepareStatement(update)){
            int idx = 0;
            for (Field field : tableData.getColumns()) {
                setParameter(ps, field, bean, ++idx);
            }
            setParameter(ps, tableData.getPrimaryKey(), bean, ++idx);
            ps.execute();
        }
    }

    public void delete(Connection connection, Object key, Class<?> clazz) throws Exception{
        final TableData tableData = TableData.analyze(clazz);
        final String delete = GENERATOR.generateDeleteSqlTemplate(tableData);
        try(final PreparedStatement ps = connection.prepareStatement(delete)){
            setParameter(ps, key, 1);
            ps.execute();
        }
    }

    private void copyField(Field field, Object bean, Object origin) throws IllegalAccessException {
        final Class<?> type = field.getType();
        if(type == Long.TYPE){
            writeLong(field, bean, readLong(field, origin));
        } else if(type == Integer.TYPE){
            writeInt(field, bean, readInt(field, origin));
        } else if(type == Boolean.TYPE){
            writeBoolean(field, bean, readBoolean(field, origin));
        } else if(type == Double.TYPE){
            writeDouble(field, bean, readDouble(field, origin));
        } else if(type == Float.TYPE){
            writeFloat(field, bean, readFloat(field, origin));
        } else if(type == Short.TYPE){
            writeShort(field, bean, readShort(field, origin));
        } else if(type == Byte.TYPE){
            writeByte(field, bean, readByte(field, origin));
        } else if(type == Character.TYPE){
            writeChar(field, bean, readChar(field, origin));
        } else{
            writeField(field, bean, readField(field, origin));
        }
    }

    private void setField(ResultSet rs, Field field, Object bean, Integer idx) throws SQLException, IllegalAccessException {
        final Class<?> type = field.getType();
        if(type == Long.TYPE){
            final long v = idx == null ? rs.getLong(field.getName()) : rs.getLong(idx);
            writeLong(field, bean, v);
        } else if(type == Integer.TYPE){
            final int v = idx == null ? rs.getInt(field.getName()) : rs.getInt(idx);
            writeInt(field, bean, v);
        } else if(type == Boolean.TYPE){
            final boolean v = idx == null ? rs.getBoolean(field.getName()) : rs.getBoolean(idx);
            writeBoolean(field, bean, v);
        } else if(type == Double.TYPE){
            final double v = idx == null ? rs.getDouble(field.getName()) : rs.getDouble(idx);
            writeDouble(field, bean, v);
        } else if(type == Float.TYPE){
            final float v = idx == null ? rs.getFloat(field.getName()) : rs.getFloat(idx);
            writeFloat(field, bean, v);
        } else if(type == Short.TYPE){
            final short v = idx == null ? rs.getShort(field.getName()) : rs.getShort(idx);
            writeShort(field, bean, v);
        } else if(type == Byte.TYPE){
            final byte v = idx == null ? rs.getByte(field.getName()) : rs.getByte(idx);
            writeByte(field, bean, v);
        } else if(type == Character.TYPE){
            final String s = idx == null ? rs.getString(field.getName()) : rs.getString(idx);
            if(s.length() > 1){
                throw new IllegalStateException("result set data for character type was longer than length 1");
            }
            writeChar(field, bean, s.charAt(0));
        } else{
            final Object v = idx == null ? rs.getObject(field.getName()) : rs.getObject(idx);
            writeField(field, bean, v);
        }
    }

    private void setParameter(PreparedStatement ps, Object value, int idx) throws SQLException {
        final Class<?> type = value.getClass();
        if(type == Long.class){
            ps.setLong(idx, (Long) value);
        } else if(type == Integer.class){
            ps.setInt(idx, (Integer) value);
        } else if(type == Double.class){
            ps.setDouble(idx, (Double) value);
        } else if(type == Boolean.class){
            ps.setBoolean(idx, (Boolean) value);
        } else if(type == Short.class){
            ps.setShort(idx, (Short) value);
        } else if(type == Float.class){
            ps.setFloat(idx, (Float) value);
        } else if(type == Byte.class){
            ps.setByte(idx, (Byte) value);
        } else if(type == Character.class){
            ps.setString(idx, value.toString());
        } else if(type == Byte.class){
            ps.setByte(idx, (Byte) value);
        } else if(type == String.class){
            ps.setString(idx, (String) value);
        } else if(type == BigDecimal.class){
            ps.setBigDecimal(idx, (BigDecimal) value);
        } else if(type == Timestamp.class){
            ps.setTimestamp(idx, (Timestamp) value);
        } else if(type == Date.class){
            ps.setDate(idx, (Date) value);
        } else if(type == Time.class){
            ps.setTime(idx, (Time) value);
        } else{
            throw new IllegalArgumentException("prepared select parameter setting does not support class " + type.getName());
        }
    }

    private void setParameter(PreparedStatement ps, Field field, Object bean, int idx) throws IllegalAccessException, SQLException {
        final Class<?> type = field.getType();
        if(type == Long.TYPE){
            ps.setLong(idx, readLong(field, bean));
        } else if(type == Long.class){
            ps.setLong(idx, (Long) readField(field, bean));
        } else if(type == Integer.TYPE){
            ps.setInt(idx, readInt(field, bean));
        } else if(type == Integer.class){
            ps.setInt(idx, (Integer) readField(field, bean));
        } else if(type == Double.TYPE){
            ps.setDouble(idx, readDouble(field, bean));
        } else if(type == Double.class){
            ps.setDouble(idx, (Double) readField(field, bean));
        } else if(type == Boolean.TYPE){
            ps.setBoolean(idx, readBoolean(field, bean));
        } else if(type == Boolean.class){
            ps.setBoolean(idx, (Boolean) readField(field, bean));
        } else if(type == Short.TYPE){
            ps.setShort(idx, readShort(field, bean));
        } else if(type == Short.class){
            ps.setShort(idx, (Short) readField(field, bean));
        } else if(type == Float.TYPE){
            ps.setFloat(idx, readFloat(field, bean));
        } else if(type == Float.class){
            ps.setFloat(idx, (Float) readField(field, bean));
        } else if(type == Byte.TYPE){
            ps.setByte(idx, readByte(field, bean));
        } else if(type == Byte.class){
            ps.setByte(idx, (Byte) readField(field, bean));
        } else if(type == Character.TYPE){
            ps.setString(idx, String.valueOf(readChar(field, bean)));
        } else if(type == Character.class){
            ps.setString(idx, readField(field, bean).toString());//todo null check
        } else if(type == String.class){
            ps.setString(idx, (String) readField(field, bean));
        } else if(type == Timestamp.class){
            ps.setTimestamp(idx, (Timestamp) readField(field, bean));
        } else if(type == Date.class){
            ps.setDate(idx, (Date) readField(field, bean));
        } else if(type == Time.class){
            ps.setTime(idx, (Time) readField(field, bean));
        } else if(type == BigDecimal.class){
            ps.setBigDecimal(idx, (BigDecimal) readField(field, bean));
        } else{
            throw new IllegalArgumentException("prepared select parameter setting does not support class " + type.getName());
        }
    }
}
