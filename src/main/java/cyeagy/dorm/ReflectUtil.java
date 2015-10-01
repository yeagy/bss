package cyeagy.dorm;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

public class ReflectUtil {
    static Object readField(Field field, Object bean) throws IllegalAccessException {
        setAccessible(field);
        return field.get(bean);
    }

    static long readLong(Field field, Object bean) throws IllegalAccessException {
        setAccessible(field);
        return field.getLong(bean);
    }

    static int readInt(Field field, Object bean) throws IllegalAccessException {
        setAccessible(field);
        return field.getInt(bean);
    }

    static short readShort(Field field, Object bean) throws IllegalAccessException {
        setAccessible(field);
        return field.getShort(bean);
    }

    static double readDouble(Field field, Object bean) throws IllegalAccessException {
        setAccessible(field);
        return field.getDouble(bean);
    }

    static float readFloat(Field field, Object bean) throws IllegalAccessException {
        setAccessible(field);
        return field.getFloat(bean);
    }

    static boolean readBoolean(Field field, Object bean) throws IllegalAccessException {
        setAccessible(field);
        return field.getBoolean(bean);
    }

    static byte readByte(Field field, Object bean) throws IllegalAccessException {
        setAccessible(field);
        return field.getByte(bean);
    }

    static char readChar(Field field, Object bean) throws IllegalAccessException {
        setAccessible(field);
        return field.getChar(bean);
    }

    static void writeField(Field field, Object bean, Object value) throws IllegalAccessException {
        setAccessible(field);
        field.set(bean, value);
    }

    static void writeLong(Field field, Object bean, long value) throws IllegalAccessException {
        setAccessible(field);
        field.setLong(bean, value);
    }

    static void writeInt(Field field, Object bean, int value) throws IllegalAccessException {
        setAccessible(field);
        field.setInt(bean, value);
    }

    static void writeShort(Field field, Object bean, short value) throws IllegalAccessException {
        setAccessible(field);
        field.setShort(bean, value);
    }

    static void writeDouble(Field field, Object bean, double value) throws IllegalAccessException {
        setAccessible(field);
        field.setDouble(bean, value);
    }

    static void writeFloat(Field field, Object bean, float value) throws IllegalAccessException {
        setAccessible(field);
        field.setFloat(bean, value);
    }

    static void writeBoolean(Field field, Object bean, boolean value) throws IllegalAccessException {
        setAccessible(field);
        field.setBoolean(bean, value);
    }

    static void writeByte(Field field, Object bean, byte value) throws IllegalAccessException {
        setAccessible(field);
        field.setByte(bean, value);
    }

    static void writeChar(Field field, Object bean, char value) throws IllegalAccessException {
        setAccessible(field);
        field.setChar(bean, value);
    }

    static <T> T constructNewInstance(Class<T> clazz) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        final Constructor<T> constructor = clazz.getDeclaredConstructor();
        setAccessible(constructor);
        return constructor.newInstance();
    }

    static void setAccessible(AccessibleObject o){
        if(!o.isAccessible()){
            o.setAccessible(true);
        }
    }
}
