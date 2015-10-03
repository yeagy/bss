package cyeagy.dorm;

import java.lang.reflect.Field;
import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static cyeagy.dorm.ReflectUtil.*;

/**
 * Joinless ORM. No setup required.
 */
public class DORM {
    private static final SqlGenerator GENERATOR = new SqlGenerator();

    /**
     * execute a select query filtering on the primary key.
     * @param connection db connection. close it yourself
     * @param key primary key to filter on
     * @param clazz entity type class
     * @param <T> entity type
     * @return matching entity or null
     * @throws Exception
     */
    public <T> T select(Connection connection, Object key, Class<T> clazz) throws Exception{
        final TableData tableData = TableData.analyze(clazz);
        final String select = GENERATOR.generateSelectSqlTemplate(tableData);
        try(final BetterPreparedStatement ps = BetterPreparedStatement.from(connection.prepareStatement(select))){
            setParameter(ps, key, 1);
            try(final BetterResultSet rs = BetterResultSet.from(ps.executeQuery())){
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

    /**
     * execute a select filtering on a set of primary keys.
     * only known to work on postgres.
     * @param connection db connection. close it yourself
     * @param keys primary keys to filter on
     * @param clazz entity type class
     * @param <T> entity type
     * @return matching entities or empty set
     * @throws Exception
     */
    public <T> Set<T> select(Connection connection, Collection<?> keys, Class<T> clazz) throws Exception{
        return select(connection, keys, clazz, null);
    }

    /**
     * execute a select filtering on a set of primary keys.
     * only known to work on postgres.
     * @param connection db connection. close it yourself
     * @param keys primary keys to filter on
     * @param clazz entity type class
     * @param keyCastType override the default primary key array cast type
     * @param <T> entity type
     * @return matching entities or empty set
     * @throws Exception
     */
    public <T> Set<T> select(Connection connection, Collection<?> keys, Class<T> clazz, String keyCastType) throws Exception{
        final TableData tableData = TableData.analyze(clazz);
        final String select = GENERATOR.generateBulkSelectSqlTemplate(tableData);
        final Set<?> keySet = keys instanceof Set ? (Set<?>) keys : new HashSet<>(keys);
        final Set<T> results = new HashSet<>(keySet.size());
        try(final BetterPreparedStatement ps = BetterPreparedStatement.from(connection.prepareStatement(select))){
            if(keyCastType == null) {
                keyCastType = TypeMappers.CLASS_SQL_TYPE_MAP.get(tableData.getPrimaryKey().getType());
            }
            final Array array = ps.createArrayOf(keyCastType, keySet.toArray());
            ps.setArray(1, array);
            try(final BetterResultSet rs = BetterResultSet.from(ps.executeQuery())){
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

    /**
     * execute insert.
     * for auto-generated keys, the primary key has to be null in the entity.
     * if the primary key is non-null, the insert will be formatted to insert with that specified primary key.
     *
     * dorm will extract the generated key and return it in a copied entity object. this can return a null object it fails to get the generated key for some reason.
     * if the primary key is non-null, the insert will simply return itself.
     * @param connection db connection. close it yourself
     * @param entity entity to insert
     * @param <T> entity type
     * @return entity with generated primary key. null if there was no generated key returned. the entity itself if the primary key was non-null.
     * @throws Exception
     */
    public <T> T insert(Connection connection, T entity) throws Exception {
        final TableData tableData = TableData.analyze(entity.getClass());
        final Object pk = tableData.getPrimaryKey().getType().isPrimitive() ? null : readField(tableData.getPrimaryKey(), entity);
        final String insert = GENERATOR.generateInsertSqlTemplate(tableData, pk != null);
        try(final BetterPreparedStatement ps = BetterPreparedStatement.from(connection.prepareStatement(insert, Statement.RETURN_GENERATED_KEYS))){
            int idx = 0;
            if(pk != null){
                setParameter(ps, tableData.getPrimaryKey(), entity, ++idx);
            }
            for (Field field : tableData.getColumns()) {
                setParameter(ps, field, entity, ++idx);
            }
            ps.execute();
            try(final BetterResultSet rs = BetterResultSet.from(ps.getGeneratedKeys())){
                if(rs.next()){
                    @SuppressWarnings("unchecked")
                    final T result = constructNewInstance((Class<T>) entity.getClass());
                    setField(rs, tableData.getPrimaryKey(), result, 1);
                    for (Field field : tableData.getColumns()) {
                        copyField(field, result, entity);
                    }
                    return result;
                }
            }
        }
        return null;
    }

    /**
     * execute update.
     * @param connection db connection. close it yourself
     * @param entity entity to update
     * @throws Exception
     */
    public void update(Connection connection, Object entity) throws Exception{
        final TableData tableData = TableData.analyze(entity.getClass());
        final String update = GENERATOR.generateUpdateSqlTemplate(tableData);
        try(final BetterPreparedStatement ps = BetterPreparedStatement.from(connection.prepareStatement(update))){
            int idx = 0;
            for (Field field : tableData.getColumns()) {
                setParameter(ps, field, entity, ++idx);
            }
            setParameter(ps, tableData.getPrimaryKey(), entity, ++idx);
            ps.execute();
        }
    }

    /**
     * execute delete.
     * @param connection db connection. close it yourself
     * @param key primary key to filter on
     * @param clazz entity type class
     * @throws Exception
     */
    public void delete(Connection connection, Object key, Class<?> clazz) throws Exception{
        final TableData tableData = TableData.analyze(clazz);
        final String delete = GENERATOR.generateDeleteSqlTemplate(tableData);
        try(final BetterPreparedStatement ps = BetterPreparedStatement.from(connection.prepareStatement(delete))){
            setParameter(ps, key, 1);
            ps.execute();
        }
    }

    private void copyField(Field field, Object target, Object origin) throws IllegalAccessException {
        final TypeMappers.FieldCopier copier = TypeMappers.FIELD_COPIER_MAP.get(field.getType());
        if(copier != null){
            copier.copy(field, target, origin);
        } else{
            writeField(field, target, readField(field, origin));
        }
    }

    private void setField(BetterResultSet rs, Field field, Object target, Integer idx) throws SQLException, IllegalAccessException {
        final TypeMappers.FieldResultWriter writer = TypeMappers.FIELD_RESULT_WRITER_MAP.get(field.getType());
        if(writer != null){
            writer.write(rs, field, target, idx);
        } else{
            final Object v = idx == null ? rs.getObject(TableData.getColumnName(field)) : rs.getObject(idx);
            writeField(field, target, v);
        }
    }

    private void setParameter(BetterPreparedStatement ps, Object value, int idx) throws SQLException {
        final TypeMappers.ObjectParamSetter setter = TypeMappers.OBJECT_PARAM_SETTER_MAP.get(value.getClass());
        if(setter != null){
            setter.set(ps, value, idx);
        } else{
            throw new IllegalArgumentException("prepared select parameter setting does not support class " + value.getClass().getName());
        }
    }

    private void setParameter(BetterPreparedStatement ps, Field field, Object target, int idx) throws IllegalAccessException, SQLException {
        final TypeMappers.FieldParamSetter setter = TypeMappers.FIELD_PARAM_SETTER_MAP.get(field.getType());
        if(setter != null){
            setter.set(ps, field, target, idx);
        } else{
            throw new IllegalArgumentException("prepared select parameter setting does not support class " + field.getType().getName());
        }
    }
}
