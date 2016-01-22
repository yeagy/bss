package io.github.cyeagy.bss;

//can be used inline on the builders, makes for simpler lambdas as the result index is rarely needed
@FunctionalInterface
public interface SimpleResultMapping<T> extends ResultMapping<T> {
    T map(BetterResultSet rs) throws Exception;

    @Override
    default T map(BetterResultSet rs, int idx) throws Exception {
        return map(rs);
    }
}
