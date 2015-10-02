package cyeagy.dorm;

import com.google.common.collect.ImmutableMap;

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
import java.util.Map;
import java.util.Set;

import static cyeagy.dorm.ReflectUtil.*;

public class DORM {
    private static final SqlGenerator GENERATOR = new SqlGenerator();

    public <T> T select(Connection connection, Object key, Class<T> clazz) throws Exception{
        final TableData tableData = TableData.analyze(clazz, true);
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
        final TableData tableData = TableData.analyze(clazz, true);
        final String select = GENERATOR.generateSelectSqlTemplate(tableData);
        final Set<T> results = new HashSet<>(keys.size());
        try(final PreparedStatement ps = connection.prepareStatement(select)){
            final Array array = connection.createArrayOf(GENERATOR.CLASS_SQL_TYPE_MAP.get(tableData.getPrimaryKey().getType()), keys.toArray());
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
        final TableData tableData = TableData.analyze(bean.getClass(), true);
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
        final TableData tableData = TableData.analyze(bean.getClass(), true);
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
        final TableData tableData = TableData.analyze(clazz, true);
        final String delete = GENERATOR.generateDeleteSqlTemplate(tableData);
        try(final PreparedStatement ps = connection.prepareStatement(delete)){
            setParameter(ps, key, 1);
            ps.execute();
        }
    }

    private void copyField(Field field, Object bean, Object origin) throws IllegalAccessException {
        final Class<?> type = field.getType();
        final TypeMappers.FieldCopier copier = TypeMappers.FIELD_COPIER_MAP.get(type);
        if(copier != null){
            copier.copy(field, bean, origin);
        } else{
            writeField(field, bean, readField(field, origin));
        }
    }

    private void setField(ResultSet rs, Field field, Object bean, Integer idx) throws SQLException, IllegalAccessException {
        final TypeMappers.FieldResultWriter writer = TypeMappers.FIELD_RESULT_WRITER_MAP.get(field.getType());
        if(writer != null){
            writer.write(rs, field, bean, idx);
        } else{
            final Object v = idx == null ? rs.getObject(TableData.getColumnName(field)) : rs.getObject(idx);
            writeField(field, bean, v);
        }
    }

    private void setParameter(PreparedStatement ps, Object value, int idx) throws SQLException {
        final Class<?> type = value.getClass();
        final TypeMappers.ObjectParamSetter setter = TypeMappers.OBJECT_PARAM_SETTER_MAP.get(type);
        if(setter != null){
            setter.set(ps, value, idx);
        } else{
            throw new IllegalArgumentException("prepared select parameter setting does not support class " + type.getName());
        }
    }

    private void setParameter(PreparedStatement ps, Field field, Object bean, int idx) throws IllegalAccessException, SQLException {
        final Class<?> type = field.getType();
        final TypeMappers.FieldParamSetter setter = TypeMappers.FIELD_PARAM_SETTER_MAP.get(field.getType());
        if(setter != null){
            setter.set(ps, field, bean, idx);
        } else{
            throw new IllegalArgumentException("prepared select parameter setting does not support class " + type.getName());
        }
    }

    private interface TypeMappers{
        Map<Class<?>, FieldCopier> FIELD_COPIER_MAP = initFieldCopierMap();
        Map<Class<?>, FieldResultWriter> FIELD_RESULT_WRITER_MAP = initFieldResultWriterMap();
        Map<Class<?>, ObjectParamSetter> OBJECT_PARAM_SETTER_MAP = initObjectParamSetterMap();
        Map<Class<?>, FieldParamSetter> FIELD_PARAM_SETTER_MAP = initFieldParamSetterMap();
        interface FieldCopier{
            void copy(Field field, Object bean, Object origin) throws IllegalAccessException;
        }

        interface FieldResultWriter{
            void write(ResultSet rs, Field field, Object bean, Integer idx) throws SQLException, IllegalAccessException;
        }

        interface ObjectParamSetter{
            void set(PreparedStatement ps, Object value, int idx) throws SQLException;
        }

        interface FieldParamSetter{
            void set(PreparedStatement ps, Field field, Object bean, int idx) throws IllegalAccessException, SQLException;
        }

        static Map<Class<?>, FieldCopier> initFieldCopierMap() {
            return ImmutableMap.<Class<?>, FieldCopier>builder()
                    .put(Long.TYPE, (field, bean, origin) -> writeLong(field, bean, readLong(field, origin)))
                    .put(Integer.TYPE, (field, bean, origin) -> writeInt(field, bean, readInt(field, origin)))
                    .put(Boolean.TYPE, (field, bean, origin) -> writeBoolean(field, bean, readBoolean(field, origin)))
                    .put(Double.TYPE, (field, bean, origin) -> writeDouble(field, bean, readDouble(field, origin)))
                    .put(Float.TYPE, (field, bean, origin) -> writeFloat(field, bean, readFloat(field, origin)))
                    .put(Short.TYPE, (field, bean, origin) -> writeShort(field, bean, readShort(field, origin)))
                    .put(Byte.TYPE, (field, bean, origin) -> writeByte(field, bean, readByte(field, origin)))
                    .put(Character.TYPE, (field, bean, origin) -> writeChar(field, bean, readChar(field, origin)))
                    .build();
        }

        static Map<Class<?>, FieldResultWriter> initFieldResultWriterMap() {
            return ImmutableMap.<Class<?>, FieldResultWriter>builder()
                    .put(Long.TYPE, (rs, field, bean, idx) -> writeLong(field, bean, idx == null ? rs.getLong(TableData.getColumnName(field)) : rs.getLong(idx)))
                    .put(Integer.TYPE, (rs, field, bean, idx) -> writeInt(field, bean, idx == null ? rs.getInt(TableData.getColumnName(field)) : rs.getInt(idx)))
                    .put(Boolean.TYPE, (rs, field, bean, idx) -> writeBoolean(field, bean, idx == null ? rs.getBoolean(TableData.getColumnName(field)) : rs.getBoolean(idx)))
                    .put(Double.TYPE, (rs, field, bean, idx) -> writeDouble(field, bean, idx == null ? rs.getDouble(TableData.getColumnName(field)) : rs.getDouble(idx)))
                    .put(Float.TYPE, (rs, field, bean, idx) -> writeFloat(field, bean, idx == null ? rs.getFloat(TableData.getColumnName(field)) : rs.getFloat(idx)))
                    .put(Short.TYPE, (rs, field, bean, idx) -> writeShort(field, bean, idx == null ? rs.getShort(TableData.getColumnName(field)) : rs.getShort(idx)))
                    .put(Byte.TYPE, (rs, field, bean, idx) -> writeByte(field, bean, idx == null ? rs.getByte(TableData.getColumnName(field)) : rs.getByte(idx)))
                    .put(Character.TYPE, (rs, field, bean, idx) -> {
                        final String s = idx == null ? rs.getString(TableData.getColumnName(field)) : rs.getString(idx);
                        if(s.length() > 1){
                            throw new IllegalStateException("result set data for character type was longer than length 1");
                        }
                        writeChar(field, bean, s.charAt(0));
                    })
                    .build();
        }

        static Map<Class<?>, ObjectParamSetter> initObjectParamSetterMap() {
            return ImmutableMap.<Class<?>, ObjectParamSetter>builder()
                    .put(Long.class, (ps, value, idx) -> ps.setLong(idx, (Long) value))
                    .put(Integer.class, (ps, value, idx) -> ps.setInt(idx, (Integer) value))
                    .put(Double.class, (ps, value, idx) -> ps.setDouble(idx, (Double) value))
                    .put(Boolean.class, (ps, value, idx) -> ps.setBoolean(idx, (Boolean) value))
                    .put(Short.class, (ps, value, idx) -> ps.setShort(idx, (Short) value))
                    .put(Float.class, (ps, value, idx) -> ps.setFloat(idx, (Float) value))
                    .put(Byte.class, (ps, value, idx) -> ps.setByte(idx, (Byte) value))
                    .put(Character.class, (ps, value, idx) ->  ps.setString(idx, value.toString()))
                    .put(String.class, (ps, value, idx) ->  ps.setString(idx, (String) value))
                    .put(BigDecimal.class, (ps, value, idx) -> ps.setBigDecimal(idx, (BigDecimal) value))
                    .put(Timestamp.class, (ps, value, idx) -> ps.setTimestamp(idx, (Timestamp) value))
                    .put(Date.class, (ps, value, idx) -> ps.setDate(idx, (Date) value))
                    .put(Time.class, (ps, value, idx) -> ps.setTime(idx, (Time) value))
                    .build();
        }

        static ImmutableMap<Class<?>, FieldParamSetter> initFieldParamSetterMap(){
            return ImmutableMap.<Class<?>, FieldParamSetter>builder()
                    .put(Long.TYPE, (ps, field, bean, idx) -> ps.setLong(idx, readLong(field, bean)))
                    .put(Long.class, (ps, field, bean, idx) -> ps.setLong(idx, (Long) readField(field, bean)))
                    .put(Integer.TYPE, (ps, field, bean, idx) -> ps.setInt(idx, readInt(field, bean)))
                    .put(Integer.class, (ps, field, bean, idx) -> ps.setInt(idx, (Integer) readField(field, bean)))
                    .put(Double.TYPE, (ps, field, bean, idx) -> ps.setDouble(idx, readDouble(field, bean)))
                    .put(Double.class, (ps, field, bean, idx) -> ps.setDouble(idx, (Double) readField(field, bean)))
                    .put(Boolean.TYPE, (ps, field, bean, idx) -> ps.setBoolean(idx, readBoolean(field, bean)))
                    .put(Boolean.class, (ps, field, bean, idx) -> ps.setBoolean(idx, (Boolean) readField(field, bean)))
                    .put(Short.TYPE, (ps, field, bean, idx) -> ps.setBoolean(idx, (Boolean) readField(field, bean)))
                    .put(Short.class, (ps, field, bean, idx) -> ps.setShort(idx, (Short) readField(field, bean)))
                    .put(Float.TYPE, (ps, field, bean, idx) -> ps.setFloat(idx, readFloat(field, bean)))
                    .put(Float.class, (ps, field, bean, idx) -> ps.setFloat(idx, (Float) readField(field, bean)))
                    .put(Byte.TYPE, (ps, field, bean, idx) -> ps.setByte(idx, readByte(field, bean)))
                    .put(Byte.class, (ps, field, bean, idx) -> ps.setByte(idx, (Byte) readField(field, bean)))
                    .put(Character.TYPE, (ps, field, bean, idx) -> ps.setString(idx, String.valueOf(readChar(field, bean))))
                    .put(Character.class, (ps, field, bean, idx) -> ps.setString(idx, readField(field, bean).toString()))//todo null check
                    .put(String.class, (ps, field, bean, idx) -> ps.setString(idx, (String) readField(field, bean)))
                    .put(Timestamp.class, (ps, field, bean, idx) -> ps.setTimestamp(idx, (Timestamp) readField(field, bean)))
                    .put(Date.class, (ps, field, bean, idx) -> ps.setDate(idx, (Date) readField(field, bean)))
                    .put(Time.class, (ps, field, bean, idx) -> ps.setTime(idx, (Time) readField(field, bean)))
                    .put(BigDecimal.class, (ps, field, bean, idx) -> ps.setBigDecimal(idx, (BigDecimal) readField(field, bean)))
                    .build();
        }
    }
}
