package cyeagy.dorm;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static cyeagy.dorm.ReflectUtil.*;

/**
 * Joinless ORM. No setup required.
 */
public class Dorm {
    private static final SqlGenerator GENERATOR = SqlGenerator.fromDefaults();
    private static final SqlSupport SUPPORT = SqlSupport.fromDefaults();

    public static Dorm fromDefaults() {
        return new Dorm();
    }

    private Dorm() { }

    /**
     * execute a select query filtering on the primary key.
     *
     * @param connection db connection. close it yourself
     * @param key        primary key to filter on
     * @param clazz      entity type class
     * @param <T>        entity type
     * @return matching entity or null
     * @throws SQLException
     * @throws DormException
     */
    public <T> T select(Connection connection, Object key, Class<T> clazz) throws SQLException, DormException {
        Objects.requireNonNull(connection);
        Objects.requireNonNull(key);
        Objects.requireNonNull(clazz);
        final TableData tableData = TableData.from(clazz);
        final String select = GENERATOR.generateSelectSqlTemplate(tableData);
        return SUPPORT.builder(select).queryBinding(ps -> setParameter(ps, key, 1))
                .resultMapping(rs -> copyEntity(rs, tableData, clazz)).executeQuery(connection);
    }

    /**
     * execute a select filtering on a set of primary keys.
     * only known to work on postgres.
     *
     * @param connection db connection. close it yourself
     * @param keys       primary keys to filter on
     * @param clazz      entity type class
     * @param <T>        entity type
     * @return matching entities or empty set
     * @throws SQLException
     * @throws DormException
     */
    public <T> List<T> select(Connection connection, Collection<?> keys, Class<T> clazz) throws SQLException, DormException {
        Objects.requireNonNull(connection);
        Objects.requireNonNull(keys);
        Objects.requireNonNull(clazz);
        final TableData tableData = TableData.from(clazz);
        final String select = GENERATOR.generateBulkSelectSqlTemplate(tableData);
        return SUPPORT.builder(select).queryBinding(ps -> ps.setArray(1, keys))
                .resultMapping(rs -> copyEntity(rs, tableData, clazz)).executeQueryList(connection);
    }

    /**
     * execute insert.
     * for auto-generated keys, the primary key has to be null in the entity.
     * if the primary key is non-null, the insert will be formatted to insert with that specified primary key.
     * <p>
     * dorm will extract the generated key and return it in a copied entity object.
     * this can return a null object it fails to get the generated key for some reason.
     * if the primary key is non-null, the insert will simply return itself.
     *
     * @param connection db connection. close it yourself
     * @param entity     entity to insert
     * @param <T>        entity type
     * @return entity with generated primary key. null if there was no generated key returned. the entity itself if the primary key was non-null.
     * @throws SQLException
     * @throws DormException
     */
    public <T> T insert(Connection connection, T entity) throws SQLException, DormException {
        Objects.requireNonNull(connection);
        Objects.requireNonNull(entity);
        T result = null;
        final TableData tableData = TableData.from(entity.getClass());
        try {
            final Object pk = getPrimaryKeyValue(entity, tableData);
            final String insert = GENERATOR.generateInsertSqlTemplate(tableData, pk != null);
            final Object generatedKey = SUPPORT.insert(connection, insert, ps -> {
                int idx = 0;
                if (pk != null) {
                    setParameter(ps, tableData.getPrimaryKey(), entity, ++idx);
                }
                for (Field field : tableData.getColumns()) {
                    setParameter(ps, field, entity, ++idx);
                }
            });
            if (pk == null && generatedKey != null) {
                //noinspection unchecked
                result = constructNewInstance((Class<T>) entity.getClass());
                writeField(tableData.getPrimaryKey(), result, generatedKey);
                for (Field field : tableData.getColumns()) {
                    copyField(field, result, entity);
                }
            }
        } catch (IllegalAccessException | NoSuchMethodException | InstantiationException | InvocationTargetException e) {
            throw new DormException(e);
        }
        return result;
    }

    /**
     * execute update.
     *
     * @param connection db connection. close it yourself
     * @param entity     entity to update
     * @return number of rows updated
     * @throws SQLException
     * @throws DormException
     */
    public int update(Connection connection, Object entity) throws SQLException, DormException {
        Objects.requireNonNull(connection);
        Objects.requireNonNull(entity);
        final TableData tableData = TableData.from(entity.getClass());
        Objects.requireNonNull(getPrimaryKeyValue(entity, tableData), "primary key cannot be null");
        final String update = GENERATOR.generateUpdateSqlTemplate(tableData);
        return SUPPORT.update(connection, update, ps -> {
            int idx = 0;
            for (Field field : tableData.getColumns()) {
                setParameter(ps, field, entity, ++idx);
            }
            setParameter(ps, tableData.getPrimaryKey(), entity, ++idx);
        });
    }

    /**
     * execute delete.
     *
     * @param connection db connection. close it yourself
     * @param key        primary key to filter on
     * @param clazz      entity type class
     * @return number of rows updated
     * @throws SQLException
     * @throws DormException
     */
    public int delete(Connection connection, Object key, Class<?> clazz) throws SQLException, DormException {
        Objects.requireNonNull(connection);
        Objects.requireNonNull(key);
        Objects.requireNonNull(clazz);
        final TableData tableData = TableData.from(clazz);
        final String delete = GENERATOR.generateDeleteSqlTemplate(tableData);
        return SUPPORT.update(connection, delete, ps -> setParameter(ps, key, 1));
    }

    private Object getPrimaryKeyValue(Object entity, TableData tableData) throws DormException {
        try {
            return readField(tableData.getPrimaryKey(), entity);
        } catch (IllegalAccessException e) {
            throw new DormException(e);
        }
    }

    private <T> T copyEntity(BetterResultSet rs, TableData tableData, Class<T> clazz) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, SQLException {
        final T result = constructNewInstance(clazz);
        setField(rs, tableData.getPrimaryKey(), result, null);
        for (Field field : tableData.getColumns()) {
            setField(rs, field, result, null);
        }
        return result;
    }

    private void copyField(Field field, Object target, Object origin) throws IllegalAccessException {
        final TypeMappers.FieldCopier copier = TypeMappers.getFieldCopier(field.getType());
        if (copier != null) {
            copier.copy(field, target, origin);
        } else {
            writeField(field, target, readField(field, origin));
        }
    }

    private void setField(BetterResultSet rs, Field field, Object target, Integer idx) throws SQLException, IllegalAccessException {
        final TypeMappers.FieldResultWriter writer = TypeMappers.getFieldResultWriter(field.getType());
        if (writer != null) {
            writer.write(rs, field, target, idx);
        } else {
            final Object v = idx == null ? rs.getObject(TableData.getColumnName(field)) : rs.getObject(idx);
            writeField(field, target, v);
        }
    }

    private void setParameter(BetterPreparedStatement ps, Object value, int idx) throws SQLException {
        final TypeMappers.ObjectParamSetter setter = TypeMappers.getObjectParamSetter(value.getClass());
        if (setter != null) {
            setter.set(ps, value, idx);
        } else {
            ps.setObject(idx, value);
        }
    }

    private void setParameter(BetterPreparedStatement ps, Field field, Object target, int idx) throws IllegalAccessException, SQLException {
        final TypeMappers.FieldParamSetter setter = TypeMappers.getFieldParamSetter(field.getType());
        if (setter != null) {
            setter.set(ps, field, target, idx);
        } else {
            ps.setObject(idx, readField(field, target));
        }
    }
}
