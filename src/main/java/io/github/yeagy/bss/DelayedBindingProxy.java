package io.github.yeagy.bss;


import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is to support the SQL 'IN' clause in JDBC.
 * Parameter markers corresponding to a setArray calls will have their markers expanded by the size of the array/collection.
 * For better performance avoid this implementation and use a database that supports arrays, like Postgres.
 * <p>
 * PreparedStatement generally has a 2000 parameter limit
 */
final class DelayedBindingProxy implements BetterPreparedStatement {
    private Connection connection;//loathe having a reference to this
    private final String statement;
    private final boolean returnGeneratedKeys;
    private final NamedParameters namedParameters;
    private final Map<Integer, Binding> indexBindings = new HashMap<>();
    private BetterPreparedStatement bps;

    private Integer maxFieldSize;
    private Integer maxRows;
    private Boolean escapeProcessing;
    private Integer queryTimeout;
    private String cursorName;
    private Integer fetchDirection;
    private Integer fetchSize;
    private Boolean poolable;
    private Boolean closeOnCompletion;
    private Boolean batch;

    private DelayedBindingProxy(Connection connection, String statement, boolean returnGeneratedKeys, NamedParameters namedParameters) {
        this.connection = connection;
        this.statement = statement;
        this.returnGeneratedKeys = returnGeneratedKeys;
        this.namedParameters = namedParameters;
    }

    static DelayedBindingProxy from(Connection connection, String statement, boolean returnGeneratedKeys){
        final NamedParameters named = NamedParameters.from(statement);
        if(named != null){
            statement = named.getProcessedSql();
        }
        return new DelayedBindingProxy(connection, statement, returnGeneratedKeys, named);
    }

    @FunctionalInterface
    private interface Binding {
        void bind(BetterPreparedStatement ps) throws SQLException;
    }

    private abstract static class ArrayBinding implements Binding {
        private final int size;

        ArrayBinding(int size) {
            this.size = size;
        }
    }

    private BetterPreparedStatement expandPrepareBind() throws SQLException {
        final StringBuilder processed = new StringBuilder();
        int qIndex = 1;
        //todo surely there are edge case bugs here
        for (int i = 0; i < statement.length(); i++) {
            final char c = statement.charAt(i);
            if (c == '?') {
                Binding binding = indexBindings.get(qIndex++);
                if (binding instanceof ArrayBinding) {
                    processed.append(String.join(", ", Collections.nCopies(((ArrayBinding) binding).size, "?")));
                } else {
                    processed.append(c);
                }
            } else {
                processed.append(c);
            }
        }
        if(indexBindings.size() != qIndex - 1){
            throw new IllegalArgumentException("problem matching parameter markers to number of parameters");
        }
        final int returnKeys = returnGeneratedKeys ? RETURN_GENERATED_KEYS : NO_GENERATED_KEYS;
        final BetterPreparedStatement ps = new BetterPreparedStatementImpl(connection.prepareStatement(processed.toString(), returnKeys), null);
        connection = null;//huzzah to getting rid of this reference
        if (maxFieldSize != null) {
            ps.setMaxFieldSize(maxFieldSize);
        }
        if (maxRows != null) {
            ps.setMaxRows(maxRows);
        }
        if (escapeProcessing != null) {
            ps.setEscapeProcessing(escapeProcessing);
        }
        if (queryTimeout != null) {
            ps.setQueryTimeout(queryTimeout);
        }
        if (cursorName != null) {
            ps.setCursorName(cursorName);
        }
        if (fetchDirection != null) {
            ps.setFetchDirection(fetchDirection);
        }
        if (fetchSize != null) {
            ps.setFetchSize(fetchSize);
        }
        if (poolable != null) {
            ps.setPoolable(poolable);
        }
        if (closeOnCompletion != null && closeOnCompletion) {
            ps.closeOnCompletion();
        }
        if (batch != null && batch) {
            ps.addBatch();
        }
        for (Binding binding : indexBindings.values()) {
            binding.bind(ps);
        }
        return ps;
    }

    private List<Integer> namedParameterIndices(String namedParameter) {
        if (namedParameters == null) {
            throw new IllegalStateException("no named parameters found in statement");
        }
        final List<Integer> indices = namedParameters.getIndices(namedParameter);
        if (indices == null) {
            throw new IllegalStateException(String.format("no named parameter %s found in statement: %s", namedParameter, namedParameters.getUnprocessedSql()));
        }
        return indices;
    }

    @Override
    public Blob createBlob() throws SQLException {
        return connection.createBlob();
    }

    @Override
    public Clob createClob() throws SQLException {
        return connection.createClob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return connection.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return connection.createSQLXML();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new IllegalStateException("Cannot use SQL Array with DelayedBindingProxy. Try BetterOptions.Option.ARRAY_SUPPORT.");
    }

    @Override
    public Array createArray(Object[] elements) throws SQLException {
        throw new IllegalStateException("Cannot use SQL Array with DelayedBindingProxy. Try BetterOptions.Option.ARRAY_SUPPORT.");
    }

    @Override
    public Array createArray(Collection<?> elements) throws SQLException {
        throw new IllegalStateException("Cannot use SQL Array with DelayedBindingProxy. Try BetterOptions.Option.ARRAY_SUPPORT.");
    }

    @Override
    public void setArray(String namedParameter, Array x) throws SQLException {
        throw new IllegalStateException("Cannot use SQL Array with DelayedBindingProxy. Try BetterOptions.Option.ARRAY_SUPPORT.");
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new IllegalStateException("Cannot use SQL Array with DelayedBindingProxy. Try BetterOptions.Option.ARRAY_SUPPORT.");
    }

    @Override
    public void setArray(int parameterIndex, Collection<?> x) throws SQLException {
        indexBindings.put(parameterIndex, new ArrayBinding(x.size()) {
            @Override
            public void bind(BetterPreparedStatement ps) throws SQLException {
                int idx = parameterIndex;
                for (Object o : x) {
                    ps.setObject(idx++, o);
                }
            }
        });
    }

    @Override
    public void setArray(String namedParameter, Collection<?> x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            setArray(parameterIndex, x);
        }
    }

    @Override
    public void setArray(int parameterIndex, Object[] x) throws SQLException {
        indexBindings.put(parameterIndex, new ArrayBinding(x.length) {
            @Override
            public void bind(BetterPreparedStatement ps) throws SQLException {
                for (int i = 0; i < x.length; i++) {
                    ps.setObject(parameterIndex + i, x[i]);
                }
            }
        });
    }

    @Override
    public void setArray(String namedParameter, Object[] x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            setArray(parameterIndex, x);
        }
    }

    @Override
    public void setTime(int parameterIndex, LocalTime x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setTime(parameterIndex, x));
    }

    @Override
    public void setTime(String namedParameter, LocalTime x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setTime(parameterIndex, x));
        }
    }

    @Override
    public void setDate(int parameterIndex, LocalDate x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setDate(parameterIndex, x));
    }

    @Override
    public void setDate(String namedParameter, LocalDate x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setDate(parameterIndex, x));
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, LocalDateTime x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setTimestamp(parameterIndex, x));
    }

    @Override
    public void setTimestamp(String namedParameter, LocalDateTime x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setTimestamp(parameterIndex, x));
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, ZonedDateTime x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setTimestamp(parameterIndex, x));
    }

    @Override
    public void setTimestamp(String namedParameter, ZonedDateTime x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setTimestamp(parameterIndex, x));
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Instant x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setTimestamp(parameterIndex, x));
    }

    @Override
    public void setTimestamp(String namedParameter, Instant x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setTimestamp(parameterIndex, x));
        }
    }

    @Override
    public void setNull(String namedParameter, int sqlType) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setNull(parameterIndex, sqlType));
        }
    }

    @Override
    public void setBoolean(String namedParameter, boolean x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setBoolean(parameterIndex, x));
        }
    }

    @Override
    public void setByte(String namedParameter, byte x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setByte(parameterIndex, x));
        }
    }

    @Override
    public void setShort(String namedParameter, short x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setShort(parameterIndex, x));
        }
    }

    @Override
    public void setInt(String namedParameter, int x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setInt(parameterIndex, x));
        }
    }

    @Override
    public void setLong(String namedParameter, long x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setLong(parameterIndex, x));
        }
    }

    @Override
    public void setFloat(String namedParameter, float x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setFloat(parameterIndex, x));
        }
    }

    @Override
    public void setDouble(String namedParameter, double x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setDouble(parameterIndex, x));
        }
    }

    @Override
    public void setBigDecimal(String namedParameter, BigDecimal x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setBigDecimal(parameterIndex, x));
        }
    }

    @Override
    public void setString(String namedParameter, String x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setString(parameterIndex, x));
        }
    }

    @Override
    public void setBytes(String namedParameter, byte[] x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setBytes(parameterIndex, x));
        }
    }

    @Override
    public void setDate(String namedParameter, Date x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setDate(parameterIndex, x));
        }
    }

    @Override
    public void setTime(String namedParameter, Time x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setTime(parameterIndex, x));
        }
    }

    @Override
    public void setTimestamp(String namedParameter, Timestamp x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setTimestamp(parameterIndex, x));
        }
    }

    @Override
    public void setAsciiStream(String namedParameter, InputStream x, int length) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setAsciiStream(parameterIndex, x));
        }
    }

    @Override
    public void setBinaryStream(String namedParameter, InputStream x, int length) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setBinaryStream(parameterIndex, x));
        }
    }

    @Override
    public void setObject(String namedParameter, Object x, int targetSqlType) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setObject(parameterIndex, x, targetSqlType));
        }
    }

    @Override
    public void setObject(String namedParameter, Object x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setObject(parameterIndex, x));
        }
    }

    @Override
    public void setCharacterStream(String namedParameter, Reader reader, int length) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setCharacterStream(parameterIndex, reader, length));
        }
    }

    @Override
    public void setRef(String namedParameter, Ref x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setRef(parameterIndex, x));
        }
    }

    @Override
    public void setBlob(String namedParameter, Blob x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setBlob(parameterIndex, x));
        }
    }

    @Override
    public void setClob(String namedParameter, Clob x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setClob(parameterIndex, x));
        }
    }

    @Override
    public void setDate(String namedParameter, Date x, Calendar cal) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setDate(parameterIndex, x, cal));
        }
    }

    @Override
    public void setTime(String namedParameter, Time x, Calendar cal) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setTime(parameterIndex, x, cal));
        }
    }

    @Override
    public void setTimestamp(String namedParameter, Timestamp x, Calendar cal) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setTimestamp(parameterIndex, x, cal));
        }
    }

    @Override
    public void setNull(String namedParameter, int sqlType, String typeName) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setNull(parameterIndex, sqlType, typeName));
        }
    }

    @Override
    public void setURL(String namedParameter, URL x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setURL(parameterIndex, x));
        }
    }

    @Override
    public void setRowId(String namedParameter, RowId x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setRowId(parameterIndex, x));
        }
    }

    @Override
    public void setNString(String namedParameter, String value) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setNString(parameterIndex, value));
        }
    }

    @Override
    public void setNCharacterStream(String namedParameter, Reader value, long length) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setNCharacterStream(parameterIndex, value, length));
        }
    }

    @Override
    public void setNClob(String namedParameter, NClob value) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setNClob(parameterIndex, value));
        }
    }

    @Override
    public void setClob(String namedParameter, Reader reader, long length) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setClob(parameterIndex, reader, length));
        }
    }

    @Override
    public void setBlob(String namedParameter, InputStream inputStream, long length) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setBlob(parameterIndex, inputStream, length));
        }
    }

    @Override
    public void setNClob(String namedParameter, Reader reader, long length) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setNClob(parameterIndex, reader, length));
        }
    }

    @Override
    public void setSQLXML(String namedParameter, SQLXML xmlObject) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setSQLXML(parameterIndex, xmlObject));
        }
    }

    @Override
    public void setObject(String namedParameter, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setObject(parameterIndex, x, targetSqlType, scaleOrLength));
        }
    }

    @Override
    public void setAsciiStream(String namedParameter, InputStream x, long length) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setAsciiStream(parameterIndex, x, length));
        }
    }

    @Override
    public void setBinaryStream(String namedParameter, InputStream x, long length) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setBinaryStream(parameterIndex, x, length));
        }
    }

    @Override
    public void setCharacterStream(String namedParameter, Reader reader, long length) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setCharacterStream(parameterIndex, reader, length));
        }
    }

    @Override
    public void setAsciiStream(String namedParameter, InputStream x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setAsciiStream(parameterIndex, x));
        }
    }

    @Override
    public void setBinaryStream(String namedParameter, InputStream x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setBinaryStream(parameterIndex, x));
        }
    }

    @Override
    public void setCharacterStream(String namedParameter, Reader reader) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setCharacterStream(parameterIndex, reader));
        }
    }

    @Override
    public void setNCharacterStream(String namedParameter, Reader value) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setNCharacterStream(parameterIndex, value));
        }
    }

    @Override
    public void setClob(String namedParameter, Reader reader) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setClob(parameterIndex, reader));
        }
    }

    @Override
    public void setBlob(String namedParameter, InputStream inputStream) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setBlob(parameterIndex, inputStream));
        }
    }

    @Override
    public void setNClob(String namedParameter, Reader reader) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setNClob(parameterIndex, reader));
        }
    }

    @Override
    public void setObject(String namedParameter, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setObject(parameterIndex, x, targetSqlType, scaleOrLength));
        }
    }

    @Override
    public void setObject(String namedParameter, Object x, SQLType targetSqlType) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setObject(parameterIndex, x, targetSqlType));
        }
    }

    @Override
    public void setBooleanNullable(String namedParameter, Boolean x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setBooleanNullable(parameterIndex, x));
        }
    }

    @Override
    public void setByteNullable(String namedParameter, Byte x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setByteNullable(parameterIndex, x));
        }
    }

    @Override
    public void setShortNullable(String namedParameter, Short x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setShortNullable(parameterIndex, x));
        }
    }

    @Override
    public void setIntNullable(String namedParameter, Integer x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setIntNullable(parameterIndex, x));
        }
    }

    @Override
    public void setLongNullable(String namedParameter, Long x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setLongNullable(parameterIndex, x));
        }
    }

    @Override
    public void setFloatNullable(String namedParameter, Float x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setFloatNullable(parameterIndex, x));
        }
    }

    @Override
    public void setDoubleNullable(String namedParameter, Double x) throws SQLException {
        for (Integer parameterIndex : namedParameterIndices(namedParameter)) {
            indexBindings.put(parameterIndex, ps -> ps.setDoubleNullable(parameterIndex, x));
        }
    }

    @Override
    public void setBooleanNullable(int parameterIndex, Boolean x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setBooleanNullable(parameterIndex, x));
    }

    @Override
    public void setByteNullable(int parameterIndex, Byte x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setByteNullable(parameterIndex, x));
    }

    @Override
    public void setShortNullable(int parameterIndex, Short x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setShortNullable(parameterIndex, x));
    }

    @Override
    public void setIntNullable(int parameterIndex, Integer x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setIntNullable(parameterIndex, x));
    }

    @Override
    public void setLongNullable(int parameterIndex, Long x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setLongNullable(parameterIndex, x));
    }

    @Override
    public void setFloatNullable(int parameterIndex, Float x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setFloatNullable(parameterIndex, x));
    }

    @Override
    public void setDoubleNullable(int parameterIndex, Double x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setDoubleNullable(parameterIndex, x));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setNull(parameterIndex, sqlType));
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setBoolean(parameterIndex, x));
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setByte(parameterIndex, x));
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setShort(parameterIndex, x));
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setInt(parameterIndex, x));
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setLong(parameterIndex, x));
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setFloat(parameterIndex, x));
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setDouble(parameterIndex, x));
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setBigDecimal(parameterIndex, x));
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setString(parameterIndex, x));
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setBytes(parameterIndex, x));
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setDate(parameterIndex, x));
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setTime(parameterIndex, x));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setTimestamp(parameterIndex, x));
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setAsciiStream(parameterIndex, x, length));
    }

    @Override
    @Deprecated
    @SuppressWarnings("deprecation")
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setUnicodeStream(parameterIndex, x, length));
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setBinaryStream(parameterIndex, x, length));
    }

    @Override
    public void clearParameters() throws SQLException {
        indexBindings.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setObject(parameterIndex, x, targetSqlType));
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setObject(parameterIndex, x));
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setCharacterStream(parameterIndex, reader, length));
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setRef(parameterIndex, x));
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setBlob(parameterIndex, x));
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setClob(parameterIndex, x));
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setDate(parameterIndex, x, cal));
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setTime(parameterIndex, x, cal));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setTimestamp(parameterIndex, x, cal));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setNull(parameterIndex, sqlType, typeName));
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setURL(parameterIndex, x));
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setRowId(parameterIndex, x));
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setNString(parameterIndex, value));
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setNCharacterStream(parameterIndex, value, length));
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setNClob(parameterIndex, value));
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setClob(parameterIndex, reader, length));
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setBlob(parameterIndex, inputStream, length));
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setNClob(parameterIndex, reader, length));
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setSQLXML(parameterIndex, xmlObject));
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setObject(parameterIndex, x, targetSqlType, scaleOrLength));
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setAsciiStream(parameterIndex, x, length));
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setBinaryStream(parameterIndex, x, length));
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setCharacterStream(parameterIndex, reader, length));
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setAsciiStream(parameterIndex, x));
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setBinaryStream(parameterIndex, x));
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setCharacterStream(parameterIndex, reader));
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setNCharacterStream(parameterIndex, value));
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setClob(parameterIndex, reader));
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setBlob(parameterIndex, inputStream));
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setNClob(parameterIndex, reader));
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setObject(parameterIndex, x, targetSqlType, scaleOrLength));
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        indexBindings.put(parameterIndex, ps -> ps.setObject(parameterIndex, x, targetSqlType));
    }

    @Override
    public void addBatch() throws SQLException {
        batch = true;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new UnsupportedOperationException("Not legal on PreparedStatement. Try a vanilla Statement?");
    }

    @Override
    public void clearBatch() throws SQLException {
        batch = false;
    }

    @Override
    public int[] executeBatch() throws SQLException {
        bps = expandPrepareBind();
        return bps.executeBatch();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        bps = expandPrepareBind();
        return bps.executeQuery();
    }

    @Override
    public int executeUpdate() throws SQLException {
        bps = expandPrepareBind();
        return bps.executeUpdate();
    }

    @Override
    public boolean execute() throws SQLException {
        bps = expandPrepareBind();
        return bps.execute();
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        bps = expandPrepareBind();
        return bps.executeLargeUpdate();
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        throw new UnsupportedOperationException("Not legal on PreparedStatement. Try a vanilla Statement?");
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new UnsupportedOperationException("Not legal on PreparedStatement. Try a vanilla Statement?");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        throw new UnsupportedOperationException("Not legal on PreparedStatement. Try a vanilla Statement?");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException("Not legal on PreparedStatement. Try a vanilla Statement?");
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException("Not legal on PreparedStatement. Try a vanilla Statement?");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException("Not legal on PreparedStatement. Try a vanilla Statement?");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException("Not legal on PreparedStatement. Try a vanilla Statement?");
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException("Not legal on PreparedStatement. Try a vanilla Statement?");
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException("Not legal on PreparedStatement. Try a vanilla Statement?");
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        if (bps == null) {
            throw new IllegalStateException("DelayedBindingProxy limitation. Execute statement to call.");
        }
        return bps.getParameterMetaData();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (bps == null) {
            throw new IllegalStateException("DelayedBindingProxy limitation. Execute statement to call.");
        }
        return bps.getMetaData();
    }

    @Override
    public void close() throws SQLException {
        if (bps != null && !bps.isClosed()) {
            bps.close();
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        if (bps != null) {
            return bps.getMaxFieldSize();
        }
        return maxFieldSize;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        maxFieldSize = max;
    }

    @Override
    public int getMaxRows() throws SQLException {
        if (bps != null) {
            return bps.getMaxRows();
        }
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        escapeProcessing = enable;
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        if (bps != null) {
            return bps.getQueryTimeout();
        }
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        queryTimeout = seconds;
    }

    @Override
    public void cancel() throws SQLException {
        if (bps != null) {
            bps.cancel();
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        if (bps == null) {
            throw new IllegalStateException("DelayedBindingProxy limitation. Execute statement to call.");
        }
        return bps.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        if (bps != null) {
            bps.clearWarnings();
        }
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        cursorName = name;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        if (bps == null) {
            throw new IllegalStateException("DelayedBindingProxy limitation. Execute statement to call.");
        }
        return bps.getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        if (bps == null) {
            throw new IllegalStateException("DelayedBindingProxy limitation. Execute statement to call.");
        }
        return bps.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        if (bps == null) {
            throw new IllegalStateException("DelayedBindingProxy limitation. Execute statement to call.");
        }
        return bps.getMoreResults();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        fetchDirection = direction;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        if (bps != null) {
            return bps.getFetchDirection();
        }
        return fetchDirection;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        if (bps != null) {
            return bps.getFetchSize();
        }
        return fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        if (bps == null) {
            throw new IllegalStateException("DelayedBindingProxy limitation. Execute statement to call.");
        }
        return bps.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        if (bps == null) {
            throw new IllegalStateException("DelayedBindingProxy limitation. Execute statement to call.");
        }
        return bps.getResultSetType();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        if (bps == null) {
            throw new IllegalStateException("DelayedBindingProxy limitation. Execute statement to call.");
        }
        return bps.getMoreResults();
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        if (bps == null) {
            throw new IllegalStateException("DelayedBindingProxy limitation. Execute statement to call.");
        }
        return bps.getGeneratedKeys();
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        if (bps == null) {
            throw new IllegalStateException("DelayedBindingProxy limitation. Execute statement to call.");
        }
        return bps.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return bps == null || bps.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        this.poolable = poolable;
    }

    @Override
    public boolean isPoolable() throws SQLException {
        if (bps != null) {
            return bps.isPoolable();
        }
        return poolable;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        if (bps != null) {
            return bps.isCloseOnCompletion();
        }
        return closeOnCompletion;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (bps == null) {
            throw new IllegalStateException("DelayedBindingProxy limitation. Execute statement to call.");
        }
        return bps.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        if (bps == null) {
            throw new IllegalStateException("DelayedBindingProxy limitation. Execute statement to call.");
        }
        return bps.isWrapperFor(iface);
    }
}
