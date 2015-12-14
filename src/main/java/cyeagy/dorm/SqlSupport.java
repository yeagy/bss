package cyeagy.dorm;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SqlSupport {

    public static SqlSupport fromDefaults() {
        return new SqlSupport();
    }

    private SqlSupport() { }

    /**
     * primarily for single SELECT. also useful for INSERT/UPDATE with RETURNING
     *
     * @param connection db connection. close it yourself
     * @param sql        sql template
     * @param binding    bind parameter values to the PreparedStatement (optional)
     * @param mapping    map ResultSet to return entity
     * @param <T>        entity type
     * @return entity or null
     * @throws SQLException
     */
    public <T> T query(Connection connection, String sql, QueryBinding binding, ResultMapping<T> mapping) throws SQLException, DormException {
        Objects.requireNonNull(connection);
        Objects.requireNonNull(sql);
        Objects.requireNonNull(mapping);
        T entity = null;
        try (final BetterPreparedStatement ps = BetterPreparedStatement.create(connection, sql)) {
            if (binding != null) {
                binding.bind(ps);
            }
            try (final BetterResultSet rs = BetterResultSet.from(ps.executeQuery())) {
                if (rs.next()) {
                    entity = mapping.map(rs, 0);
                }
            }
        } catch (SQLException e) {
            throw e;
        } catch (Throwable e) {
            throw new DormException(e);
        }
        return entity;
    }

    /**
     * primarily for bulk SELECT. also useful for INSERT/UPDATE with RETURNING
     *
     * @param connection db connection. close it yourself
     * @param sql        sql template
     * @param binding    bind parameter values to the PreparedStatement (optional)
     * @param mapping    map ResultSet to return entity
     * @param <T>        entity type
     * @return list of entity or empty list
     * @throws SQLException
     */
    public <T> List<T> queryList(Connection connection, String sql, QueryBinding binding, ResultMapping<T> mapping) throws SQLException, DormException {
        Objects.requireNonNull(connection);
        Objects.requireNonNull(sql);
        Objects.requireNonNull(mapping);
        final List<T> entities = new ArrayList<>();
        try (final BetterPreparedStatement ps = BetterPreparedStatement.create(connection, sql)) {
            if (binding != null) {
                binding.bind(ps);
            }
            try (final BetterResultSet rs = BetterResultSet.from(ps.executeQuery())) {
                int i = 0;
                while (rs.next()) {
                    entities.add(mapping.map(rs, i++));
                }
            }
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new DormException(e);
        }
        return Collections.unmodifiableList(entities);
    }

    /**
     * primarily for bulk SELECT. also useful for INSERT/UPDATE with RETURNING
     *
     * @param connection    db connection. close it yourself
     * @param sql           sql template
     * @param binding       bind parameter values to the PreparedStatement (optional)
     * @param resultMapping map ResultSet to return entity
     * @param keyMapping    map ResultSet to a key
     * @param <K>           key type
     * @param <T>           entity type
     * @return map of entities by key or empty map
     * @throws SQLException
     */
    public <K, T> Map<K, T> queryMap(Connection connection, String sql, QueryBinding binding, ResultMapping<T> resultMapping, ResultMapping<K> keyMapping) throws SQLException, DormException {
        Objects.requireNonNull(connection);
        Objects.requireNonNull(sql);
        Objects.requireNonNull(resultMapping);
        Objects.requireNonNull(keyMapping);
        final Map<K, T> map = new HashMap<>();
        try (final BetterPreparedStatement ps = BetterPreparedStatement.create(connection, sql)) {
            if (binding != null) {
                binding.bind(ps);
            }
            try (final BetterResultSet rs = BetterResultSet.from(ps.executeQuery())) {
                int i = 0;
                while (rs.next()) {
                    map.put(keyMapping.map(rs, i), resultMapping.map(rs, i++));
                }
            }
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new DormException(e);
        }
        return Collections.unmodifiableMap(map);
    }

    /**
     * primarily for INSERT/UPDATE/DELETE
     *
     * @param connection db connection. close it yourself
     * @param sql        sql template
     * @param binding    bind parameter values to the PreparedStatement (optional)
     * @return number of rows updated
     * @throws SQLException
     */
    public int update(Connection connection, String sql, QueryBinding binding) throws SQLException, DormException {
        Objects.requireNonNull(connection);
        Objects.requireNonNull(sql);
        try (final BetterPreparedStatement ps = BetterPreparedStatement.create(connection, sql)) {
            if (binding != null) {
                binding.bind(ps);
            }
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new DormException(e);
        }
    }

    /**
     * primarily for single INSERT returning the auto-generated key
     *
     * @param connection db connection. close it yourself
     * @param sql        sql template
     * @param binding    bind parameter values to the PreparedStatement
     * @param <K>        key type
     * @return key or null
     * @throws SQLException
     */
    public <K> K insert(Connection connection, String sql, QueryBinding binding) throws SQLException, DormException {
        Objects.requireNonNull(connection);
        Objects.requireNonNull(sql);
        Objects.requireNonNull(binding);
        K key = null;
        try (final BetterPreparedStatement ps = BetterPreparedStatement.create(connection, sql, true)) {
            binding.bind(ps);
            ps.executeUpdate();
            try (final BetterResultSet rs = BetterResultSet.from(ps.getGeneratedKeys())) {
                if (rs.next()) {
                    //noinspection unchecked
                    key = (K) rs.getObject(1);
                }
            }
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new DormException(e);
        }
        return key;
    }

    //all cascading builders below

    public Builder builder(String sql) {
        return new Builder(sql);
    }

    public static class Builder {
        private static final SqlSupport SQL_SUPPORT = new SqlSupport();
        private final String sql;

        private Builder(String sql) {
            this.sql = sql;
        }

        public int executeUpdate(Connection connection) throws SQLException, DormException {
            return SQL_SUPPORT.update(connection, sql, null);
        }

        public BoundBuilder queryBinding(QueryBinding queryBinding) {
            return new BoundBuilder(sql, queryBinding);
        }

        public <T> ResultBuilder<T> resultMapping(ResultMapping<T> resultMapping) {
            return new ResultBuilder<>(sql, resultMapping);
        }

        public <T> ResultBuilder<T> resultMapping(SimpleResultMapping<T> resultMapping) {
            return new ResultBuilder<>(sql, resultMapping);
        }
    }

    public static class BoundBuilder {
        private final String sql;
        private final QueryBinding queryBinding;

        private BoundBuilder(String sql, QueryBinding queryBinding) {
            this.sql = sql;
            this.queryBinding = queryBinding;
        }

        public int executeUpdate(Connection connection) throws SQLException, DormException {
            return Builder.SQL_SUPPORT.update(connection, sql, queryBinding);
        }

        public <K> K executeInsert(Connection connection) throws SQLException, DormException {
            return Builder.SQL_SUPPORT.insert(connection, sql, queryBinding);
        }

        public <T> BoundResultBuilder<T> resultMapping(ResultMapping<T> resultMapping) {
            return new BoundResultBuilder<>(sql, queryBinding, resultMapping);
        }

        public <T> BoundResultBuilder<T> resultMapping(SimpleResultMapping<T> resultMapping) {
            return new BoundResultBuilder<>(sql, queryBinding, resultMapping);
        }
    }

    public static class ResultBuilder<T> {
        private final String sql;
        private final ResultMapping<T> resultMapping;

        private ResultBuilder(String sql, ResultMapping<T> resultMapping) {
            this.sql = sql;
            this.resultMapping = resultMapping;
        }

        public T executeQuery(Connection connection) throws SQLException, DormException {
            return Builder.SQL_SUPPORT.query(connection, sql, null, resultMapping);
        }

        public List<T> executeQueryList(Connection connection) throws SQLException, DormException {
            return Builder.SQL_SUPPORT.queryList(connection, sql, null, resultMapping);
        }

        public BoundResultBuilder<T> queryBinding(QueryBinding queryBinding) {
            return new BoundResultBuilder<>(sql, queryBinding, resultMapping);
        }

        public <K> KeyedResultBuilder<K, T> keyMapping(ResultMapping<K> keyMapping) {
            return new KeyedResultBuilder<>(sql, resultMapping, keyMapping);
        }

        public <K> KeyedResultBuilder<K, T> keyMapping(SimpleResultMapping<K> keyMapping) {
            return new KeyedResultBuilder<>(sql, resultMapping, keyMapping);
        }
    }

    public static class BoundResultBuilder<T> {
        private final String sql;
        private final QueryBinding queryBinding;
        private final ResultMapping<T> resultMapping;

        private BoundResultBuilder(String sql, QueryBinding queryBinding, ResultMapping<T> resultMapping) {
            this.sql = sql;
            this.queryBinding = queryBinding;
            this.resultMapping = resultMapping;
        }

        public T executeQuery(Connection connection) throws SQLException, DormException {
            return Builder.SQL_SUPPORT.query(connection, sql, queryBinding, resultMapping);
        }

        public List<T> executeQueryList(Connection connection) throws SQLException, DormException {
            return Builder.SQL_SUPPORT.queryList(connection, sql, queryBinding, resultMapping);
        }

        public <K> BoundKeyedResultBuilder<K, T> keyMapping(ResultMapping<K> keyMapping) {
            return new BoundKeyedResultBuilder<>(sql, queryBinding, resultMapping, keyMapping);
        }

        public <K> BoundKeyedResultBuilder<K, T> keyMapping(SimpleResultMapping<K> keyMapping) {
            return new BoundKeyedResultBuilder<>(sql, queryBinding, resultMapping, keyMapping);
        }
    }

    public static class KeyedResultBuilder<K, T> {
        private final String sql;
        private final ResultMapping<T> resultMapping;
        private final ResultMapping<K> keyMapping;

        private KeyedResultBuilder(String sql, ResultMapping<T> resultMapping, ResultMapping<K> keyMapping) {
            this.sql = sql;
            this.resultMapping = resultMapping;
            this.keyMapping = keyMapping;
        }

        public Map<K, T> executeQueryMapped(Connection connection) throws SQLException, DormException {
            return Builder.SQL_SUPPORT.queryMap(connection, sql, null, resultMapping, keyMapping);
        }

        public BoundKeyedResultBuilder<K, T> queryBinding(QueryBinding queryBinding) {
            return new BoundKeyedResultBuilder<>(sql, queryBinding, resultMapping, keyMapping);
        }
    }

    public static class BoundKeyedResultBuilder<K, T> {
        private final String sql;
        private final QueryBinding queryBinding;
        private final ResultMapping<T> resultMapping;
        private final ResultMapping<K> keyMapping;

        private BoundKeyedResultBuilder(String sql, QueryBinding queryBinding, ResultMapping<T> resultMapping, ResultMapping<K> keyMapping) {
            this.sql = sql;
            this.queryBinding = queryBinding;
            this.resultMapping = resultMapping;
            this.keyMapping = keyMapping;
        }

        public Map<K, T> executeQueryMapped(Connection connection) throws SQLException, DormException {
            return Builder.SQL_SUPPORT.queryMap(connection, sql, queryBinding, resultMapping, keyMapping);
        }
    }
}
