package cyeagy.dorm;

import java.sql.Timestamp;

public class TestBean {
    private Long test_key;
    private long some_long;
    private int some_int;
    private String some_string;
    private Timestamp some_dtm;

    public TestBean() { }

    public TestBean(Long test_key, long some_long, int some_int, String some_string, Timestamp some_dtm) {
        this.test_key = test_key;
        this.some_long = some_long;
        this.some_int = some_int;
        this.some_string = some_string;
        this.some_dtm = some_dtm;
    }

    public Long getTest_key() {
        return test_key;
    }

    public long getSome_long() {
        return some_long;
    }

    public int getSome_int() {
        return some_int;
    }

    public String getSome_string() {
        return some_string;
    }

    public Timestamp getSome_dtm() {
        return some_dtm;
    }
}
