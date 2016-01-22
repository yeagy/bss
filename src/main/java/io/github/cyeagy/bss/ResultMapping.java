package io.github.cyeagy.bss;

@FunctionalInterface
public interface ResultMapping<T> {
    T map(BetterResultSet rs, int idx) throws Exception;
}
