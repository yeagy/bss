package cyeagy.dorm;

@FunctionalInterface
public interface ResultMapping<T> {
    T map(BetterResultSet rs, int idx) throws Exception;
}
