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
import java.sql.PreparedStatement;
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
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Enhanced delegate class for PreparedStatement
 * -- Null-safe primitive set methods
 * -- Forwards connection create methods
 * -- :named parameters
 */
class BetterPreparedStatementImpl implements BetterPreparedStatement {
    private final PreparedStatement ps;
    private final NamedParameters namedParameters;

    BetterPreparedStatementImpl(PreparedStatement ps, NamedParameters namedParameters) {
        this.ps = ps;
        this.namedParameters = namedParameters;
    }

    static BetterPreparedStatement from(Connection connection, String statement, boolean returnGeneratedKeys, boolean simulatedIn) throws SQLException {
        Objects.requireNonNull(statement);
        Objects.requireNonNull(connection);
        if (simulatedIn && detectInClause(statement)) {
            return DelayedBindingProxy.from(connection, statement, returnGeneratedKeys);
        }
        final NamedParameters named = NamedParameters.from(statement);
        if (named != null) {
            statement = named.getProcessedSql();
        }
        final int returnKeys = returnGeneratedKeys ? RETURN_GENERATED_KEYS : NO_GENERATED_KEYS;
        return new BetterPreparedStatementImpl(connection.prepareStatement(statement, returnKeys), named);
    }

    //todo beef this up
    private static boolean detectInClause(String statement) {
        return statement.toLowerCase().matches(".+[\t\n ]+in[\t\n ]+\\(.*");
    }

    @FunctionalInterface
    private interface NamedParameterSetter {
        void set(BetterPreparedStatement ps, int idx) throws SQLException;
    }

    private void setNamedParameter(String namedParameter, NamedParameterSetter setter) throws SQLException {
        if (namedParameters == null) {
            throw new IllegalStateException("no named parameters found in statement");
        }
        final List<Integer> indices = namedParameters.getIndices(namedParameter);
        if (indices != null) {
            for (Integer idx : indices) {
                setter.set(this, idx);
            }
        } else {
            throw new IllegalStateException(String.format("no named parameter %s found in statement: %s", namedParameter, namedParameters.getUnprocessedSql()));
        }
    }

    //connection forwards

    @Override
    public Blob createBlob() throws SQLException {
        return ps.getConnection().createBlob();
    }

    @Override
    public Clob createClob() throws SQLException {
        return ps.getConnection().createClob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return ps.getConnection().createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return ps.getConnection().createSQLXML();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return ps.getConnection().createArrayOf(typeName, elements);
    }

    //arrays convenience

    @Override
    public Array createArray(Object[] elements) throws SQLException {
        if (elements == null || elements.length == 0) {
            throw new IllegalArgumentException("array must have elements");
        }
        Class<?> clazz = elements[0].getClass();
        final String sqlType = TypeMappers.getSqlType(clazz);
        if (sqlType == null) {
            throw new SQLException("no sql type found for java class " + clazz.getName());
        }
        return createArrayOf(sqlType, elements);
    }

    @Override
    public Array createArray(Collection<?> elements) throws SQLException {
        return createArray(elements == null ? null : elements.toArray());
    }

    @Override
    public void setArray(int parameterIndex, Collection<?> x) throws SQLException {
        setArray(parameterIndex, createArray(x));
    }

    @Override
    public void setArray(String namedParameter, Collection<?> x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setArray(parameterIndex, x));
    }

    @Override
    public void setArray(int parameterIndex, Object[] x) throws SQLException {
        setArray(parameterIndex, createArray(x));
    }

    @Override
    public void setArray(String namedParameter, Object[] x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setArray(parameterIndex, x));
    }

    // java 8 time

    @Override
    public void setTime(int parameterIndex, LocalTime x) throws SQLException {
        setTime(parameterIndex, x == null ? null : Time.valueOf(x));
    }

    @Override
    public void setTime(String namedParameter, LocalTime x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setTime(parameterIndex, x));
    }

    @Override
    public void setDate(int parameterIndex, LocalDate x) throws SQLException {
        setDate(parameterIndex, x == null ? null : Date.valueOf(x));
    }

    @Override
    public void setDate(String namedParameter, LocalDate x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setDate(parameterIndex, x));
    }

    @Override
    public void setTimestamp(int parameterIndex, LocalDateTime x) throws SQLException {
        setTimestamp(parameterIndex, x == null ? null : Timestamp.valueOf(x));
    }

    @Override
    public void setTimestamp(String namedParameter, LocalDateTime x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setTimestamp(parameterIndex, x));
    }

    @Override
    public void setTimestamp(int parameterIndex, OffsetDateTime x) throws SQLException {
        setTimestamp(parameterIndex, x == null ? null : x.toInstant());
    }

    @Override
    public void setTimestamp(String namedParameter, OffsetDateTime x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setTimestamp(parameterIndex, x));
    }

    @Override
    public void setTimestamp(int parameterIndex, Instant x) throws SQLException {
        setTimestamp(parameterIndex, x == null ? null : Timestamp.from(x));
    }

    @Override
    public void setTimestamp(String namedParameter, Instant x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setTimestamp(parameterIndex, x));
    }

    //additional setters
    //:named setters

    @Override
    public void setNull(String namedParameter, int sqlType) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setNull(parameterIndex, sqlType));
    }

    @Override
    public void setBoolean(String namedParameter, boolean x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setBoolean(parameterIndex, x));
    }

    @Override
    public void setByte(String namedParameter, byte x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setByte(parameterIndex, x));
    }

    @Override
    public void setShort(String namedParameter, short x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setShort(parameterIndex, x));
    }

    @Override
    public void setInt(String namedParameter, int x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setInt(parameterIndex, x));
    }

    @Override
    public void setLong(String namedParameter, long x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setLong(parameterIndex, x));
    }

    @Override
    public void setFloat(String namedParameter, float x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setFloat(parameterIndex, x));
    }

    @Override
    public void setDouble(String namedParameter, double x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setDouble(parameterIndex, x));
    }

    @Override
    public void setBigDecimal(String namedParameter, BigDecimal x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setBigDecimal(parameterIndex, x));
    }

    @Override
    public void setString(String namedParameter, String x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setString(parameterIndex, x));
    }

    @Override
    public void setBytes(String namedParameter, byte[] x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setBytes(parameterIndex, x));
    }

    @Override
    public void setDate(String namedParameter, Date x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setDate(parameterIndex, x));
    }

    @Override
    public void setTime(String namedParameter, Time x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setTime(parameterIndex, x));
    }

    @Override
    public void setTimestamp(String namedParameter, Timestamp x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setTimestamp(parameterIndex, x));
    }

    @Override
    public void setAsciiStream(String namedParameter, InputStream x, int length) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setAsciiStream(parameterIndex, x, length));
    }

    @Override
    public void setBinaryStream(String namedParameter, InputStream x, int length) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setBinaryStream(parameterIndex, x, length));
    }

    @Override
    public void setObject(String namedParameter, Object x, int targetSqlType) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setObject(parameterIndex, x, targetSqlType));
    }

    @Override
    public void setObject(String namedParameter, Object x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setObject(parameterIndex, x));
    }

    @Override
    public void setCharacterStream(String namedParameter, Reader reader, int length) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setCharacterStream(parameterIndex, reader, length));
    }

    @Override
    public void setRef(String namedParameter, Ref x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setRef(parameterIndex, x));
    }

    @Override
    public void setBlob(String namedParameter, Blob x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setBlob(parameterIndex, x));
    }

    @Override
    public void setClob(String namedParameter, Clob x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setClob(parameterIndex, x));
    }

    @Override
    public void setArray(String namedParameter, Array x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setArray(parameterIndex, x));
    }

    @Override
    public void setDate(String namedParameter, Date x, Calendar cal) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setDate(parameterIndex, x, cal));
    }

    @Override
    public void setTime(String namedParameter, Time x, Calendar cal) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setTime(parameterIndex, x, cal));
    }

    @Override
    public void setTimestamp(String namedParameter, Timestamp x, Calendar cal) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setTimestamp(parameterIndex, x, cal));
    }

    @Override
    public void setNull(String namedParameter, int sqlType, String typeName) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setNull(parameterIndex, sqlType, typeName));
    }

    @Override
    public void setURL(String namedParameter, URL x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setURL(parameterIndex, x));
    }

    @Override
    public void setRowId(String namedParameter, RowId x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setRowId(parameterIndex, x));
    }

    @Override
    public void setNString(String namedParameter, String value) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setNString(parameterIndex, value));
    }

    @Override
    public void setNCharacterStream(String namedParameter, Reader value, long length) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setNCharacterStream(parameterIndex, value, length));
    }

    @Override
    public void setNClob(String namedParameter, NClob value) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setNClob(parameterIndex, value));
    }

    @Override
    public void setClob(String namedParameter, Reader reader, long length) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setClob(parameterIndex, reader, length));
    }

    @Override
    public void setBlob(String namedParameter, InputStream inputStream, long length) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setBlob(parameterIndex, inputStream, length));
    }

    @Override
    public void setNClob(String namedParameter, Reader reader, long length) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setNClob(parameterIndex, reader, length));
    }

    @Override
    public void setSQLXML(String namedParameter, SQLXML xmlObject) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setSQLXML(parameterIndex, xmlObject));
    }

    @Override
    public void setObject(String namedParameter, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setObject(parameterIndex, x, targetSqlType, scaleOrLength));
    }

    @Override
    public void setAsciiStream(String namedParameter, InputStream x, long length) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setAsciiStream(parameterIndex, x, length));
    }

    @Override
    public void setBinaryStream(String namedParameter, InputStream x, long length) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setBinaryStream(parameterIndex, x, length));
    }

    @Override
    public void setCharacterStream(String namedParameter, Reader reader, long length) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setCharacterStream(parameterIndex, reader, length));
    }

    @Override
    public void setAsciiStream(String namedParameter, InputStream x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setAsciiStream(parameterIndex, x));
    }

    @Override
    public void setBinaryStream(String namedParameter, InputStream x) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setBinaryStream(parameterIndex, x));
    }

    @Override
    public void setCharacterStream(String namedParameter, Reader reader) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setCharacterStream(parameterIndex, reader));
    }

    @Override
    public void setNCharacterStream(String namedParameter, Reader value) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setNCharacterStream(parameterIndex, value));
    }

    @Override
    public void setClob(String namedParameter, Reader reader) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setClob(parameterIndex, reader));
    }

    @Override
    public void setBlob(String namedParameter, InputStream inputStream) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setBlob(parameterIndex, inputStream));
    }

    @Override
    public void setNClob(String namedParameter, Reader reader) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setNClob(parameterIndex, reader));
    }

    @Override
    public void setObject(String namedParameter, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setObject(parameterIndex, x, targetSqlType, scaleOrLength));
    }

    @Override
    public void setObject(String namedParameter, Object x, SQLType targetSqlType) throws SQLException {
        setNamedParameter(namedParameter, (ps, parameterIndex) -> setObject(parameterIndex, x, targetSqlType));
    }

    @Override
    public void setBooleanNullable(String namedParameter, Boolean x) throws SQLException {
        if (x == null) {
            setNull(namedParameter, Types.BOOLEAN);
        } else {
            setBoolean(namedParameter, x);
        }
    }

    @Override
    public void setByteNullable(String namedParameter, Byte x) throws SQLException {
        if (x == null) {
            setNull(namedParameter, Types.TINYINT);
        } else {
            setByte(namedParameter, x);
        }
    }

    @Override
    public void setShortNullable(String namedParameter, Short x) throws SQLException {
        if (x == null) {
            setNull(namedParameter, Types.SMALLINT);
        } else {
            setShort(namedParameter, x);
        }
    }

    @Override
    public void setIntNullable(String namedParameter, Integer x) throws SQLException {
        if (x == null) {
            setNull(namedParameter, Types.INTEGER);
        } else {
            setInt(namedParameter, x);
        }
    }

    @Override
    public void setLongNullable(String namedParameter, Long x) throws SQLException {
        if (x == null) {
            setNull(namedParameter, Types.BIGINT);
        } else {
            setLong(namedParameter, x);
        }
    }

    @Override
    public void setFloatNullable(String namedParameter, Float x) throws SQLException {
        if (x == null) {
            setNull(namedParameter, Types.REAL);
        } else {
            setFloat(namedParameter, x);
        }
    }

    @Override
    public void setDoubleNullable(String namedParameter, Double x) throws SQLException {
        if (x == null) {
            setNull(namedParameter, Types.DOUBLE);
        } else {
            setDouble(namedParameter, x);
        }
    }

    //index setters

    @Override
    public void setBooleanNullable(int parameterIndex, Boolean x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.BOOLEAN);
        } else {
            ps.setBoolean(parameterIndex, x);
        }
    }

    @Override
    public void setByteNullable(int parameterIndex, Byte x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.TINYINT);
        } else {
            ps.setByte(parameterIndex, x);
        }
    }

    @Override
    public void setShortNullable(int parameterIndex, Short x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.SMALLINT);
        } else {
            ps.setShort(parameterIndex, x);
        }
    }

    @Override
    public void setIntNullable(int parameterIndex, Integer x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.INTEGER);
        } else {
            ps.setInt(parameterIndex, x);
        }
    }

    @Override
    public void setLongNullable(int parameterIndex, Long x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.BIGINT);
        } else {
            ps.setLong(parameterIndex, x);
        }
    }

    @Override
    public void setFloatNullable(int parameterIndex, Float x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.REAL);
        } else {
            ps.setFloat(parameterIndex, x);
        }
    }

    @Override
    public void setDoubleNullable(int parameterIndex, Double x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.DOUBLE);
        } else {
            ps.setDouble(parameterIndex, x);
        }
    }

    //ALL BELOW ARE DELEGATE METHODS

    @Override
    public ResultSet executeQuery() throws SQLException {
        return ps.executeQuery();
    }

    @Override
    public int executeUpdate() throws SQLException {
        return ps.executeUpdate();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        ps.setNull(parameterIndex, sqlType);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        ps.setBoolean(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        ps.setByte(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        ps.setShort(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        ps.setInt(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        ps.setLong(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        ps.setFloat(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        ps.setDouble(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        ps.setBigDecimal(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        ps.setString(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        ps.setBytes(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        ps.setDate(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        ps.setTime(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        ps.setTimestamp(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        ps.setAsciiStream(parameterIndex, x, length);
    }

    @Override
    @Deprecated
    @SuppressWarnings("deprecation")
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        ps.setUnicodeStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        ps.setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void clearParameters() throws SQLException {
        ps.clearParameters();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        ps.setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        ps.setObject(parameterIndex, x);
    }

    @Override
    public boolean execute() throws SQLException {
        return ps.execute();
    }

    @Override
    public void addBatch() throws SQLException {
        ps.addBatch();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        ps.setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        ps.setRef(parameterIndex, x);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        ps.setBlob(parameterIndex, x);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        ps.setClob(parameterIndex, x);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        ps.setArray(parameterIndex, x);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return ps.getMetaData();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        ps.setDate(parameterIndex, x, cal);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        ps.setTime(parameterIndex, x, cal);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        ps.setTimestamp(parameterIndex, x, cal);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        ps.setNull(parameterIndex, sqlType, typeName);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        ps.setURL(parameterIndex, x);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return ps.getParameterMetaData();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        ps.setRowId(parameterIndex, x);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        ps.setNString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        ps.setNCharacterStream(parameterIndex, value, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        ps.setNClob(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        ps.setClob(parameterIndex, reader, length);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        ps.setBlob(parameterIndex, inputStream, length);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        ps.setNClob(parameterIndex, reader, length);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        ps.setSQLXML(parameterIndex, xmlObject);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        ps.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        ps.setAsciiStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        ps.setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        ps.setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        ps.setAsciiStream(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        ps.setBinaryStream(parameterIndex, x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        ps.setCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        ps.setNCharacterStream(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        ps.setClob(parameterIndex, reader);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        ps.setBlob(parameterIndex, inputStream);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        ps.setNClob(parameterIndex, reader);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        ps.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        ps.setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        return ps.executeLargeUpdate();
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return ps.executeQuery(sql);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return ps.executeUpdate(sql);
    }

    @Override
    public void close() throws SQLException {
        ps.close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return ps.getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        ps.setMaxFieldSize(max);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return ps.getMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        ps.setMaxRows(max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        ps.setEscapeProcessing(enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return ps.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        ps.setQueryTimeout(seconds);
    }

    @Override
    public void cancel() throws SQLException {
        ps.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return ps.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        ps.clearWarnings();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        ps.setCursorName(name);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return ps.execute(sql);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return ps.getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return ps.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return ps.getMoreResults();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        ps.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ps.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        ps.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return ps.getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return ps.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return ps.getResultSetType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        ps.addBatch(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        ps.clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return ps.executeBatch();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return ps.getConnection();
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return ps.getMoreResults(current);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return ps.getGeneratedKeys();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return ps.executeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return ps.executeUpdate(sql, columnIndexes);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return ps.executeUpdate(sql, columnNames);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return ps.execute(sql, autoGeneratedKeys);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return ps.execute(sql, columnIndexes);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return ps.execute(sql, columnNames);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ps.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return ps.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        ps.setPoolable(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return ps.isPoolable();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        ps.closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return ps.isCloseOnCompletion();
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        return ps.getLargeUpdateCount();
    }

    @Override
    public void setLargeMaxRows(long max) throws SQLException {
        ps.setLargeMaxRows(max);
    }

    @Override
    public long getLargeMaxRows() throws SQLException {
        return ps.getLargeMaxRows();
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        return ps.executeLargeBatch();
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        return ps.executeLargeUpdate(sql);
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return ps.executeLargeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return ps.executeLargeUpdate(sql, columnIndexes);
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        return ps.executeLargeUpdate(sql, columnNames);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return ps.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return ps.isWrapperFor(iface);
    }
}
