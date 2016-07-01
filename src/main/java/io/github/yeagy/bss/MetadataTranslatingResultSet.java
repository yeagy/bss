package io.github.yeagy.bss;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * this class is to abstract away needing to use the metadata to find the column names for generated compound keys
 */
public class MetadataTranslatingResultSet extends BetterResultSetImpl {
    private final Map<String, Integer> metadataColumnNames;

    private MetadataTranslatingResultSet(ResultSet rs, Map<String, Integer> metadataColumnNames) {
        super(rs);
        this.metadataColumnNames = metadataColumnNames;
    }

    static MetadataTranslatingResultSet fromGeneratedKeys(PreparedStatement ps) throws SQLException {
        final ResultSet rs = ps.getGeneratedKeys();
        final ResultSetMetaData metaData = rs.getMetaData();
        final Map<String, Integer> metadataColumnNames = new HashMap<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            metadataColumnNames.put(metaData.getColumnLabel(i), i);
        }
        return new MetadataTranslatingResultSet(rs, metadataColumnNames);
    }

    private int columnIndex(String columnLabel) {
        final Integer index = metadataColumnNames.get(columnLabel);
        if (index == null) {
            throw new IllegalArgumentException("metadata contained no index for column label: " + columnLabel);
        }
        return index;
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return super.getString(columnIndex(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return super.getBoolean(columnIndex(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return super.getByte(columnIndex(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return super.getShort(columnIndex(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return super.getInt(columnIndex(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return super.getLong(columnIndex(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return super.getFloat(columnIndex(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return super.getDouble(columnIndex(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return super.getBigDecimal(columnIndex(columnLabel), scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return super.getBytes(columnIndex(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return super.getDate(columnIndex(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return super.getTime(columnIndex(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return super.getTimestamp(columnIndex(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return super.getAsciiStream(columnIndex(columnLabel));
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return super.getUnicodeStream(columnIndex(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return super.getBinaryStream(columnIndex(columnLabel));
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return super.getObject(columnIndex(columnLabel));
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return super.getCharacterStream(columnIndex(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return super.getBigDecimal(columnIndex(columnLabel));
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        super.updateNull(columnIndex(columnLabel));
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        super.updateBoolean(columnIndex(columnLabel), x);
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        super.updateByte(columnIndex(columnLabel), x);
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        super.updateShort(columnIndex(columnLabel), x);
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        super.updateInt(columnIndex(columnLabel), x);
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        super.updateLong(columnIndex(columnLabel), x);
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        super.updateFloat(columnIndex(columnLabel), x);
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        super.updateDouble(columnIndex(columnLabel), x);
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        super.updateBigDecimal(columnIndex(columnLabel), x);
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        super.updateString(columnIndex(columnLabel), x);
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        super.updateBytes(columnIndex(columnLabel), x);
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        super.updateDate(columnIndex(columnLabel), x);
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        super.updateTime(columnIndex(columnLabel), x);
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        super.updateTimestamp(columnIndex(columnLabel), x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        super.updateAsciiStream(columnIndex(columnLabel), x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        super.updateBinaryStream(columnIndex(columnLabel), x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        super.updateCharacterStream(columnIndex(columnLabel), reader, length);
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        super.updateObject(columnIndex(columnLabel), x, scaleOrLength);
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        super.updateObject(columnIndex(columnLabel), x);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return super.getRef(columnIndex(columnLabel));
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return super.getObject(columnIndex(columnLabel), map);
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return super.getBlob(columnIndex(columnLabel));
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return super.getClob(columnIndex(columnLabel));
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return super.getArray(columnIndex(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return super.getDate(columnIndex(columnLabel), cal);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return super.getTime(columnIndex(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return super.getTimestamp(columnIndex(columnLabel), cal);
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return super.getURL(columnIndex(columnLabel));
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        super.updateRef(columnIndex(columnLabel), x);
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        super.updateBlob(columnIndex(columnLabel), x);
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        super.updateClob(columnIndex(columnLabel), x);
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        super.updateArray(columnIndex(columnLabel), x);
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return super.getRowId(columnIndex(columnLabel));
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        super.updateRowId(columnIndex(columnLabel), x);
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        super.updateNString(columnIndex(columnLabel), nString);
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        super.updateNClob(columnIndex(columnLabel), nClob);
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return super.getNClob(columnIndex(columnLabel));
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return super.getSQLXML(columnIndex(columnLabel));
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        super.updateSQLXML(columnIndex(columnLabel), xmlObject);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return super.getNString(columnIndex(columnLabel));
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return super.getNCharacterStream(columnIndex(columnLabel));
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        super.updateNCharacterStream(columnIndex(columnLabel), reader, length);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        super.updateAsciiStream(columnIndex(columnLabel), x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        super.updateBinaryStream(columnIndex(columnLabel), x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        super.updateCharacterStream(columnIndex(columnLabel), reader, length);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        super.updateBlob(columnIndex(columnLabel), inputStream, length);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        super.updateClob(columnIndex(columnLabel), reader, length);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        super.updateNClob(columnIndex(columnLabel), reader, length);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        super.updateNCharacterStream(columnIndex(columnLabel), reader);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        super.updateAsciiStream(columnIndex(columnLabel), x);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        super.updateBinaryStream(columnIndex(columnLabel), x);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        super.updateCharacterStream(columnIndex(columnLabel), reader);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        super.updateBlob(columnIndex(columnLabel), inputStream);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        super.updateClob(columnIndex(columnLabel), reader);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        super.updateNClob(columnIndex(columnLabel), reader);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return super.getObject(columnIndex(columnLabel), type);
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        super.updateObject(columnIndex(columnLabel), x, targetSqlType, scaleOrLength);
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
        super.updateObject(columnIndex(columnLabel), x, targetSqlType);
    }
}
