package io.github.yeagy.bss;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Joinless ORM. No setup required.
 */
public final class BetterSqlMapper {
    private final BetterSqlGenerator generator;
    private final BetterSqlSupport support;

    private BetterSqlMapper(BetterOptions options) {
        generator = BetterSqlGenerator.from(options);
        support = BetterSqlSupport.from(options);
    }

    public static BetterSqlMapper fromDefaults() {
        return from(BetterOptions.fromDefaults());
    }

    public static BetterSqlMapper from(BetterOptions options) {
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
     */
    public <T> T find(Connection connection, Object key, Class<T> clazz) {
        Objects.requireNonNull(connection);
        Objects.requireNonNull(key);
        Objects.requireNonNull(clazz);
        final TableData tableData = TableData.from(clazz);
        if (tableData.hasCompositeKey()) {
            throw new BetterSqlException("method not supported for entities with composite keys. try the select builder");
        }
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
     */
    public <T> List<T> find(Connection connection, Collection<?> keys, Class<T> clazz) {
        Objects.requireNonNull(connection);
        Objects.requireNonNull(keys);
        Objects.requireNonNull(clazz);
        final TableData tableData = TableData.from(clazz);
        if (tableData.hasCompositeKey()) {
            throw new BetterSqlException("method not supported for entities with composite keys. try the select builder");
        }
        final String select = generator.generateBulkSelectSqlTemplate(tableData);
        return support.builder(select).statementBinding(ps -> ps.setArray(1, keys))
                .resultMapping(rs -> createEntity(rs, tableData, clazz)).executeQueryList(connection);
    }

    /**
     * Insert entity.
     * <p>
     * for auto-generated keys, the primary key has to be null in the entity.
     * if the primary key is non-null, the insert will be formatted to insert with that specified primary key.
     * <p>
     * dorm will extract the generated key and return it in a copied entity object.
     * this can return a null object if it fails to get the generated key for some reason.
     * if the primary key is non-null, the insert will simply return itself.
     *
     * @param connection db connection. close it yourself
     * @param entity     entity to insert
     * @param <T>        entity type
     * @return entity with generated primary key. null if there was no generated key returned. the entity itself if the primary key was non-null.
     */
    public <T> T insert(Connection connection, T entity) {
        Objects.requireNonNull(connection);
        Objects.requireNonNull(entity);
        T result = null;
        final TableData tableData = TableData.from(entity.getClass());
        final List<FieldValue> primaryKeyValues = getPrimaryKeyValues(entity, tableData);
        final String insert = generator.generateInsertSqlTemplate(tableData, !primaryKeyValues.isEmpty());
        try {
            final BetterSqlSupport.BoundBuilder builder = support.builder(insert).statementBinding(ps -> {
                int idx = 0;
                for (FieldValue primaryKeyValue : primaryKeyValues) {
                    setParameter(ps, primaryKeyValue.value, ++idx);
                }
                for (Field field : tableData.getColumns()) {
                    setParameter(ps, field, entity, ++idx);
                }
            });
            if (primaryKeyValues.isEmpty()) {
                final List<FieldValue> generatedKeys;
                if (!tableData.hasCompositeKey()) {
                    final Object generatedKey = builder.executeInsert(connection);
                    generatedKeys = Collections.singletonList(new FieldValue(tableData.getPrimaryKey(), generatedKey));
                } else {
                    generatedKeys = builder.keyMapping(rs -> {
                        final List<FieldValue> values = new ArrayList<>();
                        for (Field field : tableData.getPrimaryKeys()) {
                            values.add(new FieldValue(field, rs.getObject(TableData.getColumnName(field))));
                        }
                        return values;
                    }).executeInsert(connection);
                }
                if (!generatedKeys.isEmpty()) {
                    //noinspection unchecked
                    result = constructNewInstance((Class<T>) entity.getClass());
                    for (FieldValue key : generatedKeys) {
                        key.field.set(result, key.value);
                    }
                    for (Field field : tableData.getColumns()) {
                        copyField(field, result, entity);
                    }
                }
            } else {
                builder.executeUpdate(connection);//"update" because no need for generated keys
                result = entity;
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
     */
    public void update(Connection connection, Object entity) {
        Objects.requireNonNull(connection);
        Objects.requireNonNull(entity);
        final TableData tableData = TableData.from(entity.getClass());
        final List<FieldValue> primaryKeyValues = getPrimaryKeyValues(entity, tableData);
        if (primaryKeyValues.isEmpty()) {
            throw new BetterSqlException("primary key(s) cannot be null");
        }
        final String update = generator.generateUpdateSqlTemplate(tableData);
        final int count = support.update(connection, update, ps -> {
            int idx = 0;
            for (Field field : tableData.getColumns()) {
                setParameter(ps, field, entity, ++idx);
            }
            for (FieldValue primaryKeyValue : primaryKeyValues) {
                setParameter(ps, primaryKeyValue.value, ++idx);
            }
        });
        if (count != 1) {
            final String pks = primaryKeyValues.stream().map(pk -> TableData.getColumnName(pk.field) + ": " + pk.value).collect(Collectors.joining(", "));
            throw new BetterSqlException(String.format("%s rows updated. 1 row expected. [table %s] primary key(s) %s", count, tableData.getTableName(), pks));
        }
    }

    /**
     * Delete entity.
     *
     * @param connection db connection. close it yourself
     * @param entity     entity to delete
     */
    public void delete(Connection connection, Object entity) {
        Objects.requireNonNull(connection);
        Objects.requireNonNull(entity);
        final TableData tableData = TableData.from(entity.getClass());
        final List<FieldValue> primaryKeyValues = getPrimaryKeyValues(entity, tableData);
        if (primaryKeyValues.isEmpty()) {
            throw new BetterSqlException("primary key(s) cannot be null");
        }
        final String delete = generator.generateDeleteSqlTemplate(tableData);
        int count = support.update(connection, delete, ps -> {
            int idx = 0;
            for (FieldValue pk : primaryKeyValues) {
                setParameter(ps, pk.value, ++idx);
            }
        });
        if (count != 1) {
            final String pks = primaryKeyValues.stream().map(pk -> TableData.getColumnName(pk.field) + ": " + pk.value).collect(Collectors.joining(", "));
            throw new BetterSqlException(String.format("%s rows deleted. 1 row expected. [table %s] primary key(s) %s", count, tableData.getTableName(), pks));
        }
    }

    /**
     * Delete entity with matching primary key.
     *
     * @param connection db connection. close it yourself
     * @param key        primary key to filter on
     * @param clazz      entity type class
     * @return number of rows deleted
     */
    public int delete(Connection connection, Object key, Class<?> clazz) {
        Objects.requireNonNull(connection);
        Objects.requireNonNull(key);
        Objects.requireNonNull(clazz);
        final TableData tableData = TableData.from(clazz);
        if (tableData.hasCompositeKey()) {
            throw new BetterSqlException("method not supported for entities with composite keys");
        }
        final String delete = generator.generateDeleteSqlTemplate(tableData);
        return support.update(connection, delete, ps -> setParameter(ps, key, 1));
    }

    /**
     * Bulk delete entities with matching primary keys.
     *
     * @param connection db connection. close it yourself
     * @param keys       primary keys to filter on
     * @param clazz      entity type class
     * @return number of rows updated
     */
    public int delete(Connection connection, Collection<?> keys, Class<?> clazz) {
        Objects.requireNonNull(connection);
        Objects.requireNonNull(keys);
        Objects.requireNonNull(clazz);
        final TableData tableData = TableData.from(clazz);
        if (tableData.hasCompositeKey()) {
            throw new BetterSqlException("method not supported for entities with composite keys");
        }
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

    public final class SelectBuilder<T> {
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
         */
        public T one(Connection connection) {
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
         */
        public List<T> list(Connection connection) {
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
         */
        public <K> Map<K, T> map(Connection connection, ResultMapping<K> keyMapping) {
            Objects.requireNonNull(connection);
            final TableData tableData = TableData.from(clazz);
            return support.builder(sql)
                    .statementBinding(statementBinding)
                    .resultMapping(rs -> createEntity(rs, tableData, clazz))
                    .keyMapping(keyMapping)
                    .executeQueryMapped(connection);
        }

    }

    private static List<FieldValue> getPrimaryKeyValues(Object entity, TableData tableData) {
        try {
            if (!tableData.hasCompositeKey()) {
                final Field field = tableData.getPrimaryKey();
                final Object value = field.get(entity);
                return value != null ? Collections.singletonList(new FieldValue(field, value)) : Collections.EMPTY_LIST;
            } else {
                final List<FieldValue> values = new ArrayList<>();
                for (Field field : tableData.getPrimaryKeys()) {
                    final Object value = field.get(entity);
                    if (value != null) {
                        values.add(new FieldValue(field, value));
                    }
                }
                if (!values.isEmpty() && values.size() != tableData.getPrimaryKeys().size()) {
                    throw new BetterSqlException("composite keys must either be all null or all non-null");
                }
                return values;
            }
        } catch (IllegalAccessException e) {
            throw new BetterSqlException(e);
        }
    }

    private static void copyField(Field field, Object target, Object origin) throws IllegalAccessException {
        final TypeMappers.FieldCopier copier = TypeMappers.getFieldCopier(field.getType());
        if (copier != null) {
            copier.copy(field, target, origin);
        } else {
            field.set(target, field.get(origin));
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
            ps.setObject(idx, field.get(target));
        }
    }

    private static void setField(BetterResultSet rs, Field field, Object target, Integer idx) throws SQLException, IllegalAccessException {
        final TypeMappers.FieldResultWriter writer = TypeMappers.getFieldResultWriter(field.getType());
        if (writer != null) {
            writer.write(rs, field, target, idx);
        } else {
            final Object v = idx == null ? rs.getObject(TableData.getColumnName(field)) : rs.getObject(idx);
            field.set(target, v);
        }
    }

    private static <T> T createEntity(BetterResultSet rs, TableData tableData, Class<T> clazz) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, SQLException {
        final T result = constructNewInstance(clazz);
        for (Field field : tableData.getPrimaryKeys()) {
            setField(rs, field, result, null);
        }
        for (Field field : tableData.getColumns()) {
            setField(rs, field, result, null);
        }
        return result;
    }

    private static <T> T constructNewInstance(Class<T> clazz) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        final Constructor<T> constructor = clazz.getDeclaredConstructor();
        if (constructor == null) {
            throw new BetterSqlException("zero argument constructor not found on class " + clazz.getSimpleName());
        }
        if (!constructor.isAccessible()) {
            constructor.setAccessible(true);
        }
        return constructor.newInstance();
    }

    private static final class FieldValue {
        private final Field field;
        private final Object value;

        FieldValue(Field field, Object value) {
            this.field = field;
            this.value = value;
        }
    }
}
