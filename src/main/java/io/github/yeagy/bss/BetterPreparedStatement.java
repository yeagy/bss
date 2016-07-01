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
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
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

/**
 * Enhanced delegate class for PreparedStatement
 * -- Null-safe primitive set methods
 * -- Forwards connection create methods
 * -- :named parameters
 * -- java 8 time set methods
 */
public interface BetterPreparedStatement extends PreparedStatement {
    /**
     * use this factory method in order to use :named parameters. also works with standard ? params.
     *
     * this method does not return generated keys or attempt IN clause array simulation
     *
     * @param connection close this yourself.
     * @param statement  your sql statement
     * @return BetterPreparedStatement
     * @throws SQLException
     */
    static BetterPreparedStatement create(Connection connection, String statement) throws SQLException {
        return create(connection, statement, false, false);
    }

    /**
     * use this factory method in order to use :named parameters. also works with standard ? params.
     *
     * IN clause array simulation: This will allow use of the setArray methods without DB array support.
     * This uses parameter expansion at the time of the setArray call, by the array size. This is safe to use
     * with statements that do not have an IN clause.
     *
     * @param connection          close this yourself.
     * @param statement           your sql statement
     * @param returnGeneratedKeys true if you want to return generated keys
     * @param simulatedIn         true if you want IN clause array simulation
     * @return BetterPreparedStatement
     * @throws SQLException
     */
    static BetterPreparedStatement create(Connection connection, String statement, boolean returnGeneratedKeys, boolean simulatedIn) throws SQLException {
        return BetterPreparedStatementImpl.from(connection, statement, returnGeneratedKeys, simulatedIn);
    }

    Blob createBlob() throws SQLException;

    Clob createClob() throws SQLException;

    NClob createNClob() throws SQLException;

    SQLXML createSQLXML() throws SQLException;

    Array createArrayOf(String typeName, Object[] elements) throws SQLException;

    Array createArray(Object[] elements) throws SQLException;

    Array createArray(Collection<?> elements) throws SQLException;

    void setArray(int parameterIndex, Collection<?> x) throws SQLException;

    void setArray(String namedParameter, Collection<?> x) throws SQLException;

    void setArray(int parameterIndex, Object[] x) throws SQLException;

    void setArray(String namedParameter, Object[] x) throws SQLException;

    void setTime(int parameterIndex, LocalTime x) throws SQLException;

    void setTime(String namedParameter, LocalTime x) throws SQLException;

    void setDate(int parameterIndex, LocalDate x) throws SQLException;

    void setDate(String namedParameter, LocalDate x) throws SQLException;

    void setTimestamp(int parameterIndex, LocalDateTime x) throws SQLException;

    void setTimestamp(String namedParameter, LocalDateTime x) throws SQLException;

    void setTimestamp(int parameterIndex, ZonedDateTime x) throws SQLException;

    void setTimestamp(String namedParameter, ZonedDateTime x) throws SQLException;

    void setTimestamp(int parameterIndex, Instant x) throws SQLException;

    void setTimestamp(String namedParameter, Instant x) throws SQLException;

    void setNull(String namedParameter, int sqlType) throws SQLException;

    void setBoolean(String namedParameter, boolean x) throws SQLException;

    void setByte(String namedParameter, byte x) throws SQLException;

    void setShort(String namedParameter, short x) throws SQLException;

    void setInt(String namedParameter, int x) throws SQLException;

    void setLong(String namedParameter, long x) throws SQLException;

    void setFloat(String namedParameter, float x) throws SQLException;

    void setDouble(String namedParameter, double x) throws SQLException;

    void setBigDecimal(String namedParameter, BigDecimal x) throws SQLException;

    void setString(String namedParameter, String x) throws SQLException;

    void setBytes(String namedParameter, byte[] x) throws SQLException;

    void setDate(String namedParameter, Date x) throws SQLException;

    void setTime(String namedParameter, Time x) throws SQLException;

    void setTimestamp(String namedParameter, Timestamp x) throws SQLException;

    void setAsciiStream(String namedParameter, InputStream x, int length) throws SQLException;

    void setBinaryStream(String namedParameter, InputStream x, int length) throws SQLException;

    void setObject(String namedParameter, Object x, int targetSqlType) throws SQLException;

    void setObject(String namedParameter, Object x) throws SQLException;

    void setCharacterStream(String namedParameter, Reader reader, int length) throws SQLException;

    void setRef(String namedParameter, Ref x) throws SQLException;

    void setBlob(String namedParameter, Blob x) throws SQLException;

    void setClob(String namedParameter, Clob x) throws SQLException;

    void setArray(String namedParameter, Array x) throws SQLException;

    void setDate(String namedParameter, Date x, Calendar cal) throws SQLException;

    void setTime(String namedParameter, Time x, Calendar cal) throws SQLException;

    void setTimestamp(String namedParameter, Timestamp x, Calendar cal) throws SQLException;

    void setNull(String namedParameter, int sqlType, String typeName) throws SQLException;

    void setURL(String namedParameter, URL x) throws SQLException;

    void setRowId(String namedParameter, RowId x) throws SQLException;

    void setNString(String namedParameter, String value) throws SQLException;

    void setNCharacterStream(String namedParameter, Reader value, long length) throws SQLException;

    void setNClob(String namedParameter, NClob value) throws SQLException;

    void setClob(String namedParameter, Reader reader, long length) throws SQLException;

    void setBlob(String namedParameter, InputStream inputStream, long length) throws SQLException;

    void setNClob(String namedParameter, Reader reader, long length) throws SQLException;

    void setSQLXML(String namedParameter, SQLXML xmlObject) throws SQLException;

    void setObject(String namedParameter, Object x, int targetSqlType, int scaleOrLength) throws SQLException;

    void setAsciiStream(String namedParameter, InputStream x, long length) throws SQLException;

    void setBinaryStream(String namedParameter, InputStream x, long length) throws SQLException;

    void setCharacterStream(String namedParameter, Reader reader, long length) throws SQLException;

    void setAsciiStream(String namedParameter, InputStream x) throws SQLException;

    void setBinaryStream(String namedParameter, InputStream x) throws SQLException;

    void setCharacterStream(String namedParameter, Reader reader) throws SQLException;

    void setNCharacterStream(String namedParameter, Reader value) throws SQLException;

    void setClob(String namedParameter, Reader reader) throws SQLException;

    void setBlob(String namedParameter, InputStream inputStream) throws SQLException;

    void setNClob(String namedParameter, Reader reader) throws SQLException;

    void setObject(String namedParameter, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException;

    void setObject(String namedParameter, Object x, SQLType targetSqlType) throws SQLException;

    void setBooleanNullable(String namedParameter, Boolean x) throws SQLException;

    void setByteNullable(String namedParameter, Byte x) throws SQLException;

    void setShortNullable(String namedParameter, Short x) throws SQLException;

    void setIntNullable(String namedParameter, Integer x) throws SQLException;

    void setLongNullable(String namedParameter, Long x) throws SQLException;

    void setFloatNullable(String namedParameter, Float x) throws SQLException;

    void setDoubleNullable(String namedParameter, Double x) throws SQLException;

    void setBooleanNullable(int parameterIndex, Boolean x) throws SQLException;

    void setByteNullable(int parameterIndex, Byte x) throws SQLException;

    void setShortNullable(int parameterIndex, Short x) throws SQLException;

    void setIntNullable(int parameterIndex, Integer x) throws SQLException;

    void setLongNullable(int parameterIndex, Long x) throws SQLException;

    void setFloatNullable(int parameterIndex, Float x) throws SQLException;

    void setDoubleNullable(int parameterIndex, Double x) throws SQLException;
}
