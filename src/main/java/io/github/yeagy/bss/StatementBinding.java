package io.github.yeagy.bss;

/**
 * bind parameter values to the statement.
 * both index (?) and named (:foo) parameter tokens are supported.
 */
@FunctionalInterface
public interface StatementBinding {
    void bind(BetterPreparedStatement ps) throws Exception;
}
