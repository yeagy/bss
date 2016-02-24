package io.github.cyeagy.bss;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

import static java.sql.Connection.*;

public class BetterSqlTransaction {
    public enum Isolation {
        READ_UNCOMMITTED(TRANSACTION_READ_UNCOMMITTED),
        READ_COMMITTED(TRANSACTION_READ_COMMITTED),
        REPEATABLE_READ(TRANSACTION_REPEATABLE_READ),
        SERIALIZABLE(TRANSACTION_SERIALIZABLE);

        private final int isolationLevel;

        Isolation(int isolationLevel) {
            this.isolationLevel = isolationLevel;
        }
    }

    private BetterSqlTransaction() { }

    public static <T> ReturningTransaction<T> returning(TransactionConsumer<T> consumer) {
        return returning(null, consumer);
    }

    public static <T> ReturningTransaction<T> returning(Isolation isolation, TransactionConsumer<T> consumer) {
        Objects.requireNonNull(consumer);
        return new ReturningTransaction<>(consumer, isolation);
    }

    public static VoidTransaction with(VoidTransactionConsumer consumer) {
        return with(null, consumer);
    }

    public static VoidTransaction with(Isolation isolation, VoidTransactionConsumer consumer) {
        Objects.requireNonNull(consumer);
        return new VoidTransaction(consumer, isolation);
    }

    private static <T> T executeInternal(Connection connection, TransactionConsumer<T> consumer, Isolation isolation) throws SQLException {
        Objects.requireNonNull(connection);
        if (!connection.getAutoCommit()) {
            throw new SQLException("transaction on connection already started!");
        }

        Integer previousIsolation = null;
        if (isolation != null) {
            int iso = connection.getTransactionIsolation();
            if(iso != isolation.isolationLevel){
                previousIsolation = iso;
                connection.setTransactionIsolation(isolation.isolationLevel);
            }
        }

        T returning = null;
        SQLException caught = null;
        try {
            connection.setAutoCommit(false);
            returning = consumer.consumeAndReturn(connection);
            connection.commit();
        } catch (Throwable e) {
            if (e instanceof SQLException) {
                caught = (SQLException) e;
            } else {
                caught = new SQLException("uncaught exception in transaction", e);
            }
            try {
                connection.rollback();
            } catch (SQLException re) {
                caught.setNextException(re);
            }
        } finally {
            try {
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                caught = chainException(caught, e);
            }
            if (previousIsolation != null) {
                try {
                    connection.setTransactionIsolation(previousIsolation);
                } catch (SQLException e) {
                    caught = chainException(caught, e);
                }
            }
        }
        if (caught != null) {
            throw caught;
        }
        return returning;
    }

    private static <T> T executeInternal(ConnectionSupplier supplier, TransactionConsumer<T> consumer, Isolation isolation) throws SQLException {
        Objects.requireNonNull(supplier);

        Connection connection = null;
        T returning = null;
        SQLException caught = null;
        try {
            connection = supplier.get();
            returning = executeInternal(connection, consumer, isolation);
        } catch (SQLException e) {
            caught = e;
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    caught = chainException(caught, e);
                }
            }
        }
        if (caught != null) {
            throw caught;
        }
        return returning;
    }

    private static SQLException chainException(SQLException last, SQLException next) {
        if (last != null) {
            last.setNextException(next);
            return last;
        }
        return next;
    }

    public static class VoidTransaction {
        private final VoidTransactionConsumer consumer;
        private final Isolation isolation;

        private VoidTransaction(VoidTransactionConsumer consumer, Isolation isolation) {
            this.consumer = consumer;
            this.isolation = isolation;
        }

        public void execute(Connection connection) throws SQLException {
            executeInternal(connection, consumer, isolation);
        }

        public void execute(ConnectionSupplier supplier) throws SQLException {
            executeInternal(supplier, consumer, isolation);
        }
    }

    public static class ReturningTransaction<T> {
        private final TransactionConsumer<T> consumer;
        private final Isolation isolation;

        public ReturningTransaction(TransactionConsumer<T> consumer, Isolation isolation) {
            this.consumer = consumer;
            this.isolation = isolation;
        }

        public T execute(Connection connection) throws SQLException {
            return executeInternal(connection, consumer, isolation);
        }

        public T execute(ConnectionSupplier supplier) throws SQLException {
            return executeInternal(supplier, consumer, isolation);
        }
    }

    @FunctionalInterface
    public interface VoidTransactionConsumer extends TransactionConsumer<Void> {
        void consume(Connection connection) throws Exception;

        default Void consumeAndReturn(Connection connection) throws Exception {
            consume(connection);
            return null;
        }
    }

    @FunctionalInterface
    public interface TransactionConsumer<T> {
        T consumeAndReturn(Connection connection) throws Exception;
    }
}
