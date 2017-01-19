package io.github.yeagy.bss;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

import static java.sql.Connection.*;

/**
 * Simplified Transactions
 */
public final class BetterSqlTransaction {
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

    /**
     * Transaction that returns a value, like a select.
     *
     * @param transformer your logic goes here
     * @param <T>         return type
     * @return transaction context, abstracts away connection handling. call execute.
     */
    public static <T> ReturningTransaction<T> returning(TransactionTransformer<T> transformer) {
        return returning(null, transformer);
    }

    /**
     * Transaction that returns a value, like a select.
     *
     * @param isolation   isolation level
     * @param transformer your logic goes here
     * @param <T>         return type
     * @return transaction context, abstracts away connection handling. call execute.
     */
    public static <T> ReturningTransaction<T> returning(Isolation isolation, TransactionTransformer<T> transformer) {
        Objects.requireNonNull(transformer);
        return new ReturningTransaction<>(transformer, isolation);
    }

    /**
     * Transaction that does not return a value, like an insert.
     *
     * @param consumer your logic goes here
     * @return transaction context, abstracts away connection handling. call execute.
     */
    public static VoidTransaction with(TransactionConsumer consumer) {
        return with(null, consumer);
    }

    /**
     * Transaction that does not return a value, like an insert.
     *
     * @param isolation isolation level
     * @param consumer  your logic goes here
     * @return transaction context, abstracts away connection handling. call execute.
     */
    public static VoidTransaction with(Isolation isolation, TransactionConsumer consumer) {
        Objects.requireNonNull(consumer);
        return new VoidTransaction(consumer, isolation);
    }

    private static <T> T executeInternalChecked(Connection connection, TransactionTransformer<T> transformer, Isolation isolation) throws SQLException {
        Objects.requireNonNull(connection);
        if (!connection.getAutoCommit()) {
            throw new SQLException("transaction on connection already started!");
        }

        Integer previousIsolation = null;
        if (isolation != null) {
            int iso = connection.getTransactionIsolation();
            if (iso != isolation.isolationLevel) {
                previousIsolation = iso;
                connection.setTransactionIsolation(isolation.isolationLevel);
            }
        }

        T returning = null;
        SQLException caught = null;
        try {
            connection.setAutoCommit(false);
            returning = transformer.transform(connection);
            connection.commit();
        } catch (Exception e) {
            if (e instanceof SQLException) {
                caught = (SQLException) e;
            } else {
                caught = new SQLException("uncaught exception in transaction", e);
            }
            try {
                connection.rollback();
            } catch (SQLException re) {
                caught = chainException(caught, re);
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

    private static <T> T executeInternal(Connection connection, TransactionTransformer<T> transformer, Isolation isolation) {
        try {
            return executeInternalChecked(connection, transformer, isolation);
        } catch (SQLException e) {
            throw new BetterSqlException(e);
        }
    }

    private static <T> T executeInternal(ConnectionSupplier supplier, TransactionTransformer<T> transformer, Isolation isolation) {
        Objects.requireNonNull(supplier);

        Connection connection = null;
        T returning = null;
        SQLException caught = null;
        try {
            connection = supplier.get();
            returning = executeInternalChecked(connection, transformer, isolation);
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
            throw new BetterSqlException(caught);
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

    /**
     * This is a transaction context. It exists to abstract the connection away from the transaction logic.
     * Just call execute.
     */
    public final static class VoidTransaction {
        private final TransactionConsumer consumer;
        private final Isolation isolation;

        private VoidTransaction(TransactionConsumer consumer, Isolation isolation) {
            this.consumer = consumer;
            this.isolation = isolation;
        }

        public void execute(Connection connection) {
            executeInternal(connection, consumer, isolation);
        }

        public void execute(ConnectionSupplier supplier) {
            executeInternal(supplier, consumer, isolation);
        }
    }

    /**
     * This is a transaction context. It exists to abstract the connection away from the transaction logic.
     * Just call execute.
     */
    public final static class ReturningTransaction<T> {
        private final TransactionTransformer<T> transformer;
        private final Isolation isolation;

        public ReturningTransaction(TransactionTransformer<T> transformer, Isolation isolation) {
            this.transformer = transformer;
            this.isolation = isolation;
        }

        public T execute(Connection connection) {
            return executeInternal(connection, transformer, isolation);
        }

        public T execute(ConnectionSupplier supplier) {
            return executeInternal(supplier, transformer, isolation);
        }
    }

    @FunctionalInterface
    public interface TransactionConsumer extends TransactionTransformer<Void> {
        void consume(Connection connection) throws Exception;

        @Override
        default Void transform(Connection connection) throws Exception {
            consume(connection);
            return null;
        }
    }

    @FunctionalInterface
    public interface TransactionTransformer<T> {
        T transform(Connection connection) throws Exception;
    }
}
