package io.github.cyeagy.bss;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

@FunctionalInterface
public interface ConnectionSupplier {
    Connection get() throws SQLException;

    default ConnectionSupplier from(DataSource dataSource) throws SQLException {
        return dataSource::getConnection;
    }
}
