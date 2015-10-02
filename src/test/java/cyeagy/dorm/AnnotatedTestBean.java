package cyeagy.dorm;

import java.sql.Timestamp;

@Table(name = "test_bean")
public class AnnotatedTestBean {
    @Column(name="some_long")
    private long legacyLong;
    @Column(name="some_int")
    private int legacyInt;
    @Column(name="some_string")
    private String legacyString;
    @Column(name="some_dtm")
    private Timestamp legacyTimestamp;
    @Id
    @Column(name="test_key")
    private Long legacyKey;

    private AnnotatedTestBean() {}

    public AnnotatedTestBean(Long legacyKey, long legacyLong, int legacyInt, String legacyString, Timestamp legacyTimestamp) {
        this.legacyKey = legacyKey;
        this.legacyLong = legacyLong;
        this.legacyInt = legacyInt;
        this.legacyString = legacyString;
        this.legacyTimestamp = legacyTimestamp;
    }

    public Long getLegacyKey() {
        return legacyKey;
    }

    public long getLegacyLong() {
        return legacyLong;
    }

    public int getLegacyInt() {
        return legacyInt;
    }

    public String getLegacyString() {
        return legacyString;
    }

    public Timestamp getLegacyTimestamp() {
        return legacyTimestamp;
    }
}
