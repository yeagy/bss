package cyeagy.dorm;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

class ReflectUtil {
    static Object readField(Field field, Object bean) throws IllegalAccessException {
        return field.get(bean);
    }

    static long readLong(Field field, Object bean) throws IllegalAccessException {
        return field.getLong(bean);
    }

    static int readInt(Field field, Object bean) throws IllegalAccessException {
        return field.getInt(bean);
    }

    static short readShort(Field field, Object bean) throws IllegalAccessException {
        return field.getShort(bean);
    }

    static double readDouble(Field field, Object bean) throws IllegalAccessException {
        return field.getDouble(bean);
    }

    static float readFloat(Field field, Object bean) throws IllegalAccessException {
        return field.getFloat(bean);
    }

    static boolean readBoolean(Field field, Object bean) throws IllegalAccessException {
        return field.getBoolean(bean);
    }

    static byte readByte(Field field, Object bean) throws IllegalAccessException {
        return field.getByte(bean);
    }

    static char readChar(Field field, Object bean) throws IllegalAccessException {
        return field.getChar(bean);
    }

    static void writeField(Field field, Object bean, Object value) throws IllegalAccessException {
        field.set(bean, value);
    }

    static void writeLong(Field field, Object bean, long value) throws IllegalAccessException {
        field.setLong(bean, value);
    }

    static void writeInt(Field field, Object bean, int value) throws IllegalAccessException {
        field.setInt(bean, value);
    }

    static void writeShort(Field field, Object bean, short value) throws IllegalAccessException {
        field.setShort(bean, value);
    }

    static void writeDouble(Field field, Object bean, double value) throws IllegalAccessException {
        field.setDouble(bean, value);
    }

    static void writeFloat(Field field, Object bean, float value) throws IllegalAccessException {
        field.setFloat(bean, value);
    }

    static void writeBoolean(Field field, Object bean, boolean value) throws IllegalAccessException {
        field.setBoolean(bean, value);
    }

    static void writeByte(Field field, Object bean, byte value) throws IllegalAccessException {
        field.setByte(bean, value);
    }

    static void writeChar(Field field, Object bean, char value) throws IllegalAccessException {
        field.setChar(bean, value);
    }

    static <T> T constructNewInstance(Class<T> clazz) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        final Constructor<T> constructor = clazz.getDeclaredConstructor();
        setAccessible(constructor);
        return constructor.newInstance();
    }

    static void setAccessible(AccessibleObject o) {
        if (!o.isAccessible()) {
            o.setAccessible(true);
        }
    }
}
