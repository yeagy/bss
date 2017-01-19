package io.github.yeagy.bss;

/**
 * extract a values from a SINGLE result set row.
 *
 * DO NOT advance the result set cursor, BSS will do this for you to produce collections.
 * @param <T> result object
 */
@FunctionalInterface
public interface ResultMapping<T> {
    T map(BetterResultSet rs) throws Exception;
}
