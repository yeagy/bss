package io.github.cyeagy.bss;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.github.cyeagy.bss.ReflectUtil.*;
import static io.github.cyeagy.bss.TableData.getColumnName;

class TypeMappers {
    private TypeMappers() { }
    private static final Map<Class<?>, String> CLASS_SQL_TYPE_MAP_POSTGRES = initClassTypeMapPostgres();
    private static final Map<Class<?>, FieldCopier> FIELD_COPIER_MAP = initFieldCopierMap();
    private static final Map<Class<?>, FieldResultWriter> FIELD_RESULT_WRITER_MAP = initFieldResultWriterMap();
    private static final Map<Class<?>, ObjectParamSetter> OBJECT_PARAM_SETTER_MAP = initObjectParamSetterMap();
    private static final Map<Class<?>, FieldParamSetter> FIELD_PARAM_SETTER_MAP = initFieldParamSetterMap();

    static String getSqlType(Class<?> clazz) {
        return CLASS_SQL_TYPE_MAP_POSTGRES.get(clazz);
    }

    static FieldCopier getFieldCopier(Class<?> clazz) {
        return FIELD_COPIER_MAP.get(clazz);
    }

    static FieldResultWriter getFieldResultWriter(Class<?> clazz) {
        return FIELD_RESULT_WRITER_MAP.get(clazz);
    }

    static ObjectParamSetter getObjectParamSetter(Class<?> clazz) {
        return OBJECT_PARAM_SETTER_MAP.get(clazz);
    }

    static FieldParamSetter getFieldParamSetter(Class<?> clazz) {
        return FIELD_PARAM_SETTER_MAP.get(clazz);
    }

    @FunctionalInterface
    interface FieldCopier {
        void copy(Field field, Object target, Object origin) throws IllegalAccessException;
    }

    @FunctionalInterface
    interface FieldResultWriter {
        void write(BetterResultSet rs, Field field, Object target, Integer idx) throws SQLException, IllegalAccessException;
    }

    @FunctionalInterface
    interface ObjectParamSetter {
        void set(BetterPreparedStatement ps, Object value, int idx) throws SQLException;
    }

    @FunctionalInterface
    interface FieldParamSetter {
        void set(BetterPreparedStatement ps, Field field, Object target, int idx) throws IllegalAccessException, SQLException;
    }

    //mainly used to infer data types from a java class when creating an array in JDBC
    //postgres specific: org.postgresql.jdbc2.TypeInfoCache
    private static Map<Class<?>, String> initClassTypeMapPostgres() {
        final Map<Class<?>, String> map = new HashMap<>();
        map.put(Long.class, "int8");
        map.put(Long.TYPE, "int8");
        map.put(Integer.class, "int4");
        map.put(Integer.TYPE, "int4");
        map.put(Boolean.class, "bool");
        map.put(Boolean.TYPE, "bool");
        map.put(Short.class, "int2");
        map.put(Short.TYPE, "int2");
        map.put(Byte.class, "int2");//notably no byte type in postgres
        map.put(Byte.TYPE, "int2");//notably no byte type in postgres
        map.put(Double.class, "float8");
        map.put(Double.TYPE, "float8");
        map.put(Float.class, "float4");
        map.put(Float.TYPE, "float4");
        map.put(Character.class, "varchar");
        map.put(Character.TYPE, "varchar");
        map.put(String.class, "varchar");
        map.put(BigDecimal.class, "numeric");
        map.put(Time.class, "time");
        map.put(Date.class, "date");
        map.put(Timestamp.class, "timestamp");
        return Collections.unmodifiableMap(map);
    }

    private static Map<Class<?>, FieldCopier> initFieldCopierMap() {
        final Map<Class<?>, FieldCopier> map = new HashMap<>();
        map.put(Long.TYPE, (field, target, origin) -> writeLong(field, target, readLong(field, origin)));
        map.put(Integer.TYPE, (field, target, origin) -> writeInt(field, target, readInt(field, origin)));
        map.put(Boolean.TYPE, (field, target, origin) -> writeBoolean(field, target, readBoolean(field, origin)));
        map.put(Double.TYPE, (field, target, origin) -> writeDouble(field, target, readDouble(field, origin)));
        map.put(Float.TYPE, (field, target, origin) -> writeFloat(field, target, readFloat(field, origin)));
        map.put(Short.TYPE, (field, target, origin) -> writeShort(field, target, readShort(field, origin)));
        map.put(Byte.TYPE, (field, target, origin) -> writeByte(field, target, readByte(field, origin)));
        map.put(Character.TYPE, (field, target, origin) -> writeChar(field, target, readChar(field, origin)));
        return Collections.unmodifiableMap(map);
    }

    private static Map<Class<?>, FieldResultWriter> initFieldResultWriterMap() {
        final Map<Class<?>, FieldResultWriter> map = new HashMap<>();
        map.put(Long.TYPE, (rs, field, target, idx) -> writeLong(field, target, idx == null ? rs.getLong(getColumnName(field)) : rs.getLong(idx)));
        map.put(Integer.TYPE, (rs, field, target, idx) -> writeInt(field, target, idx == null ? rs.getInt(getColumnName(field)) : rs.getInt(idx)));
        map.put(Boolean.TYPE, (rs, field, target, idx) -> writeBoolean(field, target, idx == null ? rs.getBoolean(getColumnName(field)) : rs.getBoolean(idx)));
        map.put(Double.TYPE, (rs, field, target, idx) -> writeDouble(field, target, idx == null ? rs.getDouble(getColumnName(field)) : rs.getDouble(idx)));
        map.put(Float.TYPE, (rs, field, target, idx) -> writeFloat(field, target, idx == null ? rs.getFloat(getColumnName(field)) : rs.getFloat(idx)));
        map.put(Short.TYPE, (rs, field, target, idx) -> writeShort(field, target, idx == null ? rs.getShort(getColumnName(field)) : rs.getShort(idx)));
        map.put(Byte.TYPE, (rs, field, target, idx) -> writeByte(field, target, idx == null ? rs.getByte(getColumnName(field)) : rs.getByte(idx)));
        map.put(Character.TYPE, (rs, field, target, idx) -> {
            final String s = idx == null ? rs.getString(getColumnName(field)) : rs.getString(idx);
            if (s != null) {
                if (s.length() != 1) {
                    throw new IllegalStateException("result set data for character type was longer than length 1. column: " + getColumnName(field));
                }
                writeChar(field, target, s.charAt(0));
            }
        });
        return Collections.unmodifiableMap(map);
    }

    private static Map<Class<?>, ObjectParamSetter> initObjectParamSetterMap() {
        final Map<Class<?>, ObjectParamSetter> map = new HashMap<>();
        map.put(Long.class, (ps, value, idx) -> ps.setLongNullable(idx, (Long) value));
        map.put(Integer.class, (ps, value, idx) -> ps.setIntNullable(idx, (Integer) value));
        map.put(Double.class, (ps, value, idx) -> ps.setDoubleNullable(idx, (Double) value));
        map.put(Boolean.class, (ps, value, idx) -> ps.setBooleanNullable(idx, (Boolean) value));
        map.put(Short.class, (ps, value, idx) -> ps.setShortNullable(idx, (Short) value));
        map.put(Float.class, (ps, value, idx) -> ps.setFloatNullable(idx, (Float) value));
        map.put(Byte.class, (ps, value, idx) -> ps.setByteNullable(idx, (Byte) value));
        map.put(Character.class, (ps, value, idx) -> ps.setString(idx, Objects.toString(value)));
        map.put(String.class, (ps, value, idx) -> ps.setString(idx, (String) value));
        map.put(BigDecimal.class, (ps, value, idx) -> ps.setBigDecimal(idx, (BigDecimal) value));
        map.put(Timestamp.class, (ps, value, idx) -> ps.setTimestamp(idx, (Timestamp) value));
        map.put(Date.class, (ps, value, idx) -> ps.setDate(idx, (Date) value));
        map.put(Time.class, (ps, value, idx) -> ps.setTime(idx, (Time) value));
        map.put(Blob.class, (ps, value, idx) -> ps.setBlob(idx, (Blob) value));
        map.put(Clob.class, (ps, value, idx) -> ps.setClob(idx, (Clob) value));
        return Collections.unmodifiableMap(map);
    }

    private static Map<Class<?>, FieldParamSetter> initFieldParamSetterMap() {
        final Map<Class<?>, FieldParamSetter> map = new HashMap<>();
        map.put(Long.TYPE, (ps, field, target, idx) -> ps.setLong(idx, readLong(field, target)));
        map.put(Long.class, (ps, field, target, idx) -> ps.setLongNullable(idx, (Long) readField(field, target)));
        map.put(Integer.TYPE, (ps, field, target, idx) -> ps.setInt(idx, readInt(field, target)));
        map.put(Integer.class, (ps, field, target, idx) -> ps.setIntNullable(idx, (Integer) readField(field, target)));
        map.put(Double.TYPE, (ps, field, target, idx) -> ps.setDouble(idx, readDouble(field, target)));
        map.put(Double.class, (ps, field, target, idx) -> ps.setDoubleNullable(idx, (Double) readField(field, target)));
        map.put(Boolean.TYPE, (ps, field, target, idx) -> ps.setBoolean(idx, readBoolean(field, target)));
        map.put(Boolean.class, (ps, field, target, idx) -> ps.setBooleanNullable(idx, (Boolean) readField(field, target)));
        map.put(Short.TYPE, (ps, field, target, idx) -> ps.setShort(idx, readShort(field, target)));
        map.put(Short.class, (ps, field, target, idx) -> ps.setShortNullable(idx, (Short) readField(field, target)));
        map.put(Float.TYPE, (ps, field, target, idx) -> ps.setFloat(idx, readFloat(field, target)));
        map.put(Float.class, (ps, field, target, idx) -> ps.setFloatNullable(idx, (Float) readField(field, target)));
        map.put(Byte.TYPE, (ps, field, target, idx) -> ps.setByte(idx, readByte(field, target)));
        map.put(Byte.class, (ps, field, target, idx) -> ps.setByteNullable(idx, (Byte) readField(field, target)));
        map.put(Character.TYPE, (ps, field, target, idx) -> ps.setString(idx, String.valueOf(readChar(field, target))));
        map.put(Character.class, (ps, field, target, idx) -> ps.setString(idx, Objects.toString(readField(field, target))));
        map.put(String.class, (ps, field, target, idx) -> ps.setString(idx, (String) readField(field, target)));
        map.put(Timestamp.class, (ps, field, target, idx) -> ps.setTimestamp(idx, (Timestamp) readField(field, target)));
        map.put(Date.class, (ps, field, target, idx) -> ps.setDate(idx, (Date) readField(field, target)));
        map.put(Time.class, (ps, field, target, idx) -> ps.setTime(idx, (Time) readField(field, target)));
        map.put(BigDecimal.class, (ps, field, target, idx) -> ps.setBigDecimal(idx, (BigDecimal) readField(field, target)));
        map.put(Blob.class, (ps, field, target, idx) -> ps.setBlob(idx, (Blob) readField(field, target)));
        map.put(Clob.class, (ps, field, target, idx) -> ps.setClob(idx, (Clob) readField(field, target)));
        return Collections.unmodifiableMap(map);
    }
}
