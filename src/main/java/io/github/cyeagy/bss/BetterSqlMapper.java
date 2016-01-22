package io.github.cyeagy.bss;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Joinless ORM. No setup required.
 */
public class BetterSqlMapper {
    private final BetterSqlGenerator generator;
    private final BetterSqlSupport support;

    private BetterSqlMapper(BetterOptions options) {
        generator = BetterSqlGenerator.from(options);
        support = BetterSqlSupport.from(options);
    }

    public static BetterSqlMapper fromDefaults() {
        return from(BetterOptions.fromDefaults());
    }

    public static BetterSqlMapper from(BetterOptions options){
        return new BetterSqlMapper(options);
    }

    /**
     * Find entity with matching primary key
     *
     * @param connection db connection. close it yourself
     * @param key        primary key to filter on
     * @param clazz      entity type class
     * @param <T>        entity type
     * @return entity or null
     * @throws SQLException
     * @throws BetterSqlException
     */
    public <T> T find(Connection connection, Object key, Class<T> clazz) throws SQLException, BetterSqlException {
        Objects.requireNonNull(connection);
        Objects.requireNonNull(key);
        Objects.requireNonNull(clazz);
        final TableData tableData = TableData.from(clazz);
        final String select = generator.generateSelectSqlTemplate(tableData);
        return support.builder(select).statementBinding(ps -> setParameter(ps, key, 1))
                .resultMapping(rs -> createEntity(rs, tableData, clazz)).executeQuery(connection);
    }

    /**
     * Find entities with matching primary keys
     *
     * @param connection db connection. close it yourself
     * @param keys       primary keys to filter on
     * @param clazz      entity type class
     * @param <T>        entity type
     * @return entities or empty set
     * @throws SQLException
     * @throws BetterSqlException
     */
    public <T> List<T> find(Connection connection, Collection<?> keys, Class<T> clazz) throws SQLException, BetterSqlException {
        Objects.requireNonNull(connection);
        Objects.requireNonNull(keys);
        Objects.requireNonNull(clazz);
        final TableData tableData = TableData.from(clazz);
        final String select = generator.generateBulkSelectSqlTemplate(tableData);
        return support.builder(select).statementBinding(ps -> ps.setArray(1, keys))
                .resultMapping(rs -> createEntity(rs, tableData, clazz)).executeQueryList(connection);
    }

    /**
     * Update entity.
     *
     * for auto-generated keys, the primary key has to be null in the entity.
     * if the primary key is non-null, the insert will be formatted to insert with that specified primary key.
     *
     * dorm will extract the generated key and return it in a copied entity object.
     * this can return a null object if it fails to get the generated key for some reason.
     * if the primary key is non-null, the insert will simply return itself.
     *
     * @param connection db connection. close it yourself
     * @param entity     entity to insert
     * @param <T>        entity type
     * @return entity with generated primary key. null if there was no generated key returned. the entity itself if the primary key was non-null.
     * @throws SQLException
     * @throws BetterSqlException
     */
    public <T> T insert(Connection connection, T entity) throws SQLException, BetterSqlException {
        Objects.requireNonNull(connection);
        Objects.requireNonNull(entity);
        T result = null;
        final TableData tableData = TableData.from(entity.getClass());
        final Object primaryKey = getPrimaryKeyValue(entity, tableData);
        final String insert = generator.generateInsertSqlTemplate(tableData, primaryKey != null);
        try {
            final Object generatedKey = support.insert(connection, insert, ps -> {
                int idx = 0;
                if (primaryKey != null) {
                    setParameter(ps, primaryKey, ++idx);
                }
                for (Field field : tableData.getColumns()) {
                    setParameter(ps, field, entity, ++idx);
                }
            });
            if (primaryKey == null && generatedKey != null) {
                //noinspection unchecked
                result = ReflectUtil.constructNewInstance((Class<T>) entity.getClass());
                ReflectUtil.writeField(tableData.getPrimaryKey(), result, generatedKey);
                for (Field field : tableData.getColumns()) {
                    copyField(field, result, entity);
                }
            }
        } catch (IllegalAccessException | NoSuchMethodException | InstantiationException | InvocationTargetException e) {
            throw new BetterSqlException(e);
        }
        return result;
    }

    /**
     * Update entity.
     *
     * @param connection db connection. close it yourself
     * @param entity     entity to update
     * @throws SQLException
     * @throws BetterSqlException if unexpected number of rows updated
     */
    public void update(Connection connection, Object entity) throws SQLException, BetterSqlException {
        Objects.requireNonNull(connection);
        Objects.requireNonNull(entity);
        final TableData tableData = TableData.from(entity.getClass());
        final Object primaryKey = getPrimaryKeyValue(entity, tableData);
        Objects.requireNonNull(primaryKey, "primary key cannot be null");
        final String update = generator.generateUpdateSqlTemplate(tableData);
        final int count = support.update(connection, update, ps -> {
            int idx = 0;
            for (Field field : tableData.getColumns()) {
                setParameter(ps, field, entity, ++idx);
            }
            setParameter(ps, primaryKey, ++idx);
        });
        if(count != 1){
            final String pkName = TableData.getColumnName(tableData.getPrimaryKey());
            throw new BetterSqlException(String.format("%s rows updated. 1 row expected. [table %s] primary key %s: %s",
                    count, tableData.getTableName(), pkName, primaryKey));
        }
    }

    /**
     * Delete entity.
     *
     * @param connection db connection. close it yourself
     * @param entity     entity to delete
     * @throws SQLException
     * @throws BetterSqlException if unexpected number of rows updated
     */
    public void delete(Connection connection, Object entity) throws SQLException, BetterSqlException {
        Objects.requireNonNull(connection);
        Objects.requireNonNull(entity);
        final TableData tableData = TableData.from(entity.getClass());
        final Object primaryKey = getPrimaryKeyValue(entity, tableData);
        Objects.requireNonNull(primaryKey, "primary key cannot be null");
        final String delete = generator.generateDeleteSqlTemplate(tableData);
        int count = support.update(connection, delete, ps -> setParameter(ps, primaryKey, 1));
        if(count != 1){
            final String pkName = TableData.getColumnName(tableData.getPrimaryKey());
            throw new BetterSqlException(String.format("%s rows deleted. 1 row expected. [table %s] primary key %s: %s",
                    count, tableData.getTableName(), pkName, primaryKey));
        }
    }

    /**
     * Delete entity with matching primary key.
     *
     * @param connection db connection. close it yourself
     * @param key        primary key to filter on
     * @param clazz      entity type class
     * @return number true if 1 row deleted
     * @throws SQLException
     * @throws BetterSqlException if unexpected number of rows updated
     */
    public boolean delete(Connection connection, Object key, Class<?> clazz) throws SQLException, BetterSqlException {
        Objects.requireNonNull(connection);
        Objects.requireNonNull(key);
        Objects.requireNonNull(clazz);
        final TableData tableData = TableData.from(clazz);
        final String delete = generator.generateDeleteSqlTemplate(tableData);
        int count = support.update(connection, delete, ps -> setParameter(ps, key, 1));
        if(count > 1){
            final String pkName = TableData.getColumnName(tableData.getPrimaryKey());
            throw new BetterSqlException(String.format("%s rows deleted. 0 or 1 row expected. [table %s] primary key %s: %s",
                    count, tableData.getTableName(), pkName, key));
        }
        return count == 1;
    }

    /**
     * Bulk delete entities with matching primary keys.
     *
     * @param connection db connection. close it yourself
     * @param keys       primary keys to filter on
     * @param clazz      entity type class
     * @return number of rows updated
     * @throws SQLException
     * @throws BetterSqlException
     */
    public int delete(Connection connection, Collection<?> keys, Class<?> clazz) throws SQLException, BetterSqlException {
        Objects.requireNonNull(connection);
        Objects.requireNonNull(keys);
        Objects.requireNonNull(clazz);
        final TableData tableData = TableData.from(clazz);
        final String delete = generator.generateBulkDeleteSqlTemplate(tableData);
        return support.update(connection, delete, ps -> ps.setArray(1, keys));
    }

    /**
     * Create a Select Builder. Use to automagically create an entity from an appropriate result set.
     *
     * @param sql   select statement
     * @param clazz entity type class
     * @param <T>   entity type
     * @return new SelectBuilder for entity type
     */
    public <T> SelectBuilder<T> select(String sql, Class<T> clazz) {
        return new SelectBuilder<>(sql, clazz);
    }

    public class SelectBuilder<T> {
        private final String sql;
        private final Class<T> clazz;
        private StatementBinding statementBinding = null;

        private SelectBuilder(String sql, Class<T> clazz) {
            Objects.requireNonNull(sql);
            Objects.requireNonNull(clazz);
            this.sql = sql;
            this.clazz = clazz;
        }

        /**
         * Optionally bind parameters to the PreparedStatement
         *
         * @param statementBinding bind parameter values to the prepared statement (optional)
         * @return this
         */
        public SelectBuilder<T> bind(StatementBinding statementBinding) {
            Objects.requireNonNull(statementBinding);
            this.statementBinding = statementBinding;
            return this;
        }

        /**
         * Query for a single result.
         *
         * @param connection db connection. close it yourself
         * @return entity or null
         * @throws SQLException
         * @throws BetterSqlException
         */
        public T one(Connection connection) throws SQLException, BetterSqlException {
            Objects.requireNonNull(connection);
            final TableData tableData = TableData.from(clazz);
            return support.builder(sql)
                    .statementBinding(statementBinding)
                    .resultMapping(rs -> createEntity(rs, tableData, clazz))
                    .executeQuery(connection);
        }

        /**
         * Query for a list of results.
         *
         * @param connection db connection. close it yourself
         * @return entities or empty set
         * @throws SQLException
         * @throws BetterSqlException
         */
        public List<T> list(Connection connection) throws SQLException, BetterSqlException {
            Objects.requireNonNull(connection);
            final TableData tableData = TableData.from(clazz);
            return support.builder(sql)
                    .statementBinding(statementBinding)
                    .resultMapping(rs -> createEntity(rs, tableData, clazz))
                    .executeQueryList(connection);
        }

        /**
         * Return a map of results.
         *
         * @param connection db connection. close it yourself
         * @param keyMapping map the ResultSet to a key
         * @param <K>        key type
         * @return entities or empty map
         * @throws SQLException
         * @throws BetterSqlException
         */
        public <K> Map<K, T> map(Connection connection, ResultMapping<K> keyMapping) throws SQLException, BetterSqlException {
            Objects.requireNonNull(connection);
            final TableData tableData = TableData.from(clazz);
            return support.builder(sql)
                    .statementBinding(statementBinding)
                    .resultMapping(rs -> createEntity(rs, tableData, clazz))
                    .keyMapping(keyMapping)
                    .executeQueryMapped(connection);
        }

    }

    private static Object getPrimaryKeyValue(Object entity, TableData tableData) throws BetterSqlException {
        try {
            return ReflectUtil.readField(tableData.getPrimaryKey(), entity);
        } catch (IllegalAccessException e) {
            throw new BetterSqlException(e);
        }
    }

    private static void copyField(Field field, Object target, Object origin) throws IllegalAccessException {
        final TypeMappers.FieldCopier copier = TypeMappers.getFieldCopier(field.getType());
        if (copier != null) {
            copier.copy(field, target, origin);
        } else {
            ReflectUtil.writeField(field, target, ReflectUtil.readField(field, origin));
        }
    }

    private static void setParameter(BetterPreparedStatement ps, Object value, int idx) throws SQLException {
        final TypeMappers.ObjectParamSetter setter = TypeMappers.getObjectParamSetter(value.getClass());
        if (setter != null) {
            setter.set(ps, value, idx);
        } else {
            ps.setObject(idx, value);
        }
    }

    private static void setParameter(BetterPreparedStatement ps, Field field, Object target, int idx) throws IllegalAccessException, SQLException {
        final TypeMappers.FieldParamSetter setter = TypeMappers.getFieldParamSetter(field.getType());
        if (setter != null) {
            setter.set(ps, field, target, idx);
        } else {
            ps.setObject(idx, ReflectUtil.readField(field, target));
        }
    }

    private static void setField(BetterResultSet rs, Field field, Object target, Integer idx) throws SQLException, IllegalAccessException {
        final TypeMappers.FieldResultWriter writer = TypeMappers.getFieldResultWriter(field.getType());
        if (writer != null) {
            writer.write(rs, field, target, idx);
        } else {
            final Object v = idx == null ? rs.getObject(TableData.getColumnName(field)) : rs.getObject(idx);
            ReflectUtil.writeField(field, target, v);
        }
    }

    private static <T> T createEntity(BetterResultSet rs, TableData tableData, Class<T> clazz) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, SQLException {
        final T result = ReflectUtil.constructNewInstance(clazz);
        setField(rs, tableData.getPrimaryKey(), result, null);
        for (Field field : tableData.getColumns()) {
            setField(rs, field, result, null);
        }
        return result;
    }
}
