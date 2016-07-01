package io.github.yeagy.bss;

@FunctionalInterface
public interface ResultMapping<T> {
    T map(BetterResultSet rs) throws Exception;
}
