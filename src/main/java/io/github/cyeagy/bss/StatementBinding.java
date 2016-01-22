package io.github.cyeagy.bss;

@FunctionalInterface
public interface StatementBinding {
    void bind(BetterPreparedStatement ps) throws Exception;
}
