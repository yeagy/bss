package io.github.yeagy.bss;

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

final class TypeMappers {
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
        map.put(Long.class, "bigint");
        map.put(long.class, "bigint");
        map.put(Integer.class, "integer");
        map.put(int.class, "integer");
        map.put(Boolean.class, "boolean");
        map.put(boolean.class, "boolean");
        map.put(Short.class, "smallint");
        map.put(short.class, "smallint");
        map.put(Byte.class, "smallint");//notably no byte type in postgres
        map.put(byte.class, "smallint");//notably no byte type in postgres
        map.put(Double.class, "float");
        map.put(double.class, "float");
        map.put(Float.class, "float4");//no alias for real in postgres driver
        map.put(float.class, "float4");//no alias for real in postgres driver
        map.put(Character.class, "varchar");
        map.put(char.class, "varchar");
        map.put(String.class, "varchar");
        map.put(BigDecimal.class, "numeric");
        map.put(Time.class, "time");
        map.put(Date.class, "date");
        map.put(Timestamp.class, "timestamp");
        return Collections.unmodifiableMap(map);
    }

    private static Map<Class<?>, FieldCopier> initFieldCopierMap() {
        final Map<Class<?>, FieldCopier> map = new HashMap<>();
        map.put(long.class, (field, target, origin) -> field.setLong(target, field.getLong(origin)));
        map.put(int.class, (field, target, origin) -> field.setInt(target, field.getInt(origin)));
        map.put(boolean.class, (field, target, origin) -> field.setBoolean(target, field.getBoolean(origin)));
        map.put(double.class, (field, target, origin) -> field.setDouble(target, field.getDouble(origin)));
        map.put(float.class, (field, target, origin) -> field.setFloat(target, field.getFloat(origin)));
        map.put(short.class, (field, target, origin) -> field.setShort(target, field.getShort(origin)));
        map.put(byte.class, (field, target, origin) -> field.setByte(target, field.getByte(origin)));
        map.put(char.class, (field, target, origin) -> field.setChar(target, field.getChar(origin)));
        return Collections.unmodifiableMap(map);
    }

    private static Map<Class<?>, FieldResultWriter> initFieldResultWriterMap() {
        final Map<Class<?>, FieldResultWriter> map = new HashMap<>();
        map.put(long.class, (rs, field, target, idx) -> field.setLong(target, idx == null ? rs.getLong(TableData.getColumnName(field)) : rs.getLong(idx)));
        map.put(int.class, (rs, field, target, idx) -> field.setInt(target, idx == null ? rs.getInt(TableData.getColumnName(field)) : rs.getInt(idx)));
        map.put(boolean.class, (rs, field, target, idx) -> field.setBoolean(target, idx == null ? rs.getBoolean(TableData.getColumnName(field)) : rs.getBoolean(idx)));
        map.put(double.class, (rs, field, target, idx) -> field.setDouble(target, idx == null ? rs.getDouble(TableData.getColumnName(field)) : rs.getDouble(idx)));
        map.put(float.class, (rs, field, target, idx) -> field.setFloat(target, idx == null ? rs.getFloat(TableData.getColumnName(field)) : rs.getFloat(idx)));
        map.put(short.class, (rs, field, target, idx) -> field.setShort(target, idx == null ? rs.getShort(TableData.getColumnName(field)) : rs.getShort(idx)));
        map.put(byte.class, (rs, field, target, idx) -> field.setByte(target, idx == null ? rs.getByte(TableData.getColumnName(field)) : rs.getByte(idx)));
        map.put(char.class, (rs, field, target, idx) -> {
            final String s = idx == null ? rs.getString(TableData.getColumnName(field)) : rs.getString(idx);
            if (s != null) {
                if (s.length() != 1) {
                    throw new IllegalStateException("result set data for character type was longer than length 1. column: " + TableData.getColumnName(field));
                }
                field.setChar(target, s.charAt(0));
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
        map.put(long.class, (ps, field, target, idx) -> ps.setLong(idx, field.getLong(target)));
        map.put(Long.class, (ps, field, target, idx) -> ps.setLongNullable(idx, (Long) field.get(target)));
        map.put(int.class, (ps, field, target, idx) -> ps.setInt(idx, field.getInt(target)));
        map.put(Integer.class, (ps, field, target, idx) -> ps.setIntNullable(idx, (Integer) field.get(target)));
        map.put(double.class, (ps, field, target, idx) -> ps.setDouble(idx, field.getDouble(target)));
        map.put(Double.class, (ps, field, target, idx) -> ps.setDoubleNullable(idx, (Double) field.get(target)));
        map.put(boolean.class, (ps, field, target, idx) -> ps.setBoolean(idx, field.getBoolean(target)));
        map.put(Boolean.class, (ps, field, target, idx) -> ps.setBooleanNullable(idx, (Boolean) field.get(target)));
        map.put(short.class, (ps, field, target, idx) -> ps.setShort(idx, field.getShort(target)));
        map.put(Short.class, (ps, field, target, idx) -> ps.setShortNullable(idx, (Short) field.get(target)));
        map.put(float.class, (ps, field, target, idx) -> ps.setFloat(idx, field.getFloat(target)));
        map.put(Float.class, (ps, field, target, idx) -> ps.setFloatNullable(idx, (Float) field.get(target)));
        map.put(byte.class, (ps, field, target, idx) -> ps.setByte(idx, field.getByte(target)));
        map.put(Byte.class, (ps, field, target, idx) -> ps.setByteNullable(idx, (Byte) field.get(target)));
        map.put(char.class, (ps, field, target, idx) -> ps.setString(idx, String.valueOf(field.getChar(target))));
        map.put(Character.class, (ps, field, target, idx) -> ps.setString(idx, Objects.toString(field.get(target))));
        map.put(String.class, (ps, field, target, idx) -> ps.setString(idx, (String) field.get(target)));
        map.put(Timestamp.class, (ps, field, target, idx) -> ps.setTimestamp(idx, (Timestamp) field.get(target)));
        map.put(Date.class, (ps, field, target, idx) -> ps.setDate(idx, (Date) field.get(target)));
        map.put(Time.class, (ps, field, target, idx) -> ps.setTime(idx, (Time) field.get(target)));
        map.put(BigDecimal.class, (ps, field, target, idx) -> ps.setBigDecimal(idx, (BigDecimal) field.get(target)));
        map.put(Blob.class, (ps, field, target, idx) -> ps.setBlob(idx, (Blob) field.get(target)));
        map.put(Clob.class, (ps, field, target, idx) -> ps.setClob(idx, (Clob) field.get(target)));
        return Collections.unmodifiableMap(map);
    }
}
