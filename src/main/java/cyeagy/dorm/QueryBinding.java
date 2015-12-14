package cyeagy.dorm;

@FunctionalInterface
public interface QueryBinding {
    void bind(BetterPreparedStatement ps) throws Exception;
}
