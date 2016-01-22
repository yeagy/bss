package io.github.cyeagy.bss;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * Enhanced delegate class for ResultSet
 * -- Null-safe primitive get methods
 * -- SQL arrays casted to their java type
 * -- java 8 time get methods
 */
public interface BetterResultSet extends ResultSet {
    static BetterResultSet from(ResultSet resultSet){
        return BetterResultSetImpl.from(resultSet);
    }

    <T> T[] getArrayCasted(int columnIndex) throws SQLException;

    <T> T[] getArrayCasted(String columnLabel) throws SQLException;

    LocalTime getLocalTime(int columnIndex) throws SQLException;

    LocalTime getLocalTime(String columnLabel) throws SQLException;

    LocalDate getLocalDate(int columnIndex) throws SQLException;

    LocalDate getLocalDate(String columnLabel) throws SQLException;

    LocalDateTime getLocalDateTime(int columnIndex) throws SQLException;

    LocalDateTime getLocalDateTime(String columnLabel) throws SQLException;

    Boolean getBooleanNullable(int columnIndex) throws SQLException;

    Byte getByteNullable(int columnIndex) throws SQLException;

    Short getShortNullable(int columnIndex) throws SQLException;

    Integer getIntNullable(int columnIndex) throws SQLException;

    Long getLongNullable(int columnIndex) throws SQLException;

    Float getFloatNullable(int columnIndex) throws SQLException;

    Double getDoubleNullable(int columnIndex) throws SQLException;

    Boolean getBooleanNullable(String columnLabel) throws SQLException;

    Byte getByteNullable(String columnLabel) throws SQLException;

    Short getShortNullable(String columnLabel) throws SQLException;

    Integer getIntNullable(String columnLabel) throws SQLException;

    Long getLongNullable(String columnLabel) throws SQLException;

    Float getFloatNullable(String columnLabel) throws SQLException;

    Double getDoubleNullable(String columnLabel) throws SQLException;
}
