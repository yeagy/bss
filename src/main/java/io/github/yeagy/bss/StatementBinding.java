package io.github.yeagy.bss;

@FunctionalInterface
public interface StatementBinding {
    void bind(BetterPreparedStatement ps) throws Exception;
}
