package io.github.yeagy.bss;

import java.sql.Timestamp;

public class TestBean {
    @Id private Long testKey;
    private long someLong;
    private int someInt;
    private String someString;
    private Timestamp someDtm;
    private transient double transientDouble;//ignored by mapper

    private TestBean() { }

    public TestBean(Long testKey, long someLong, int someInt, String someString, Timestamp someDtm, double transientDouble) {
        this.testKey = testKey;
        this.someLong = someLong;
        this.someInt = someInt;
        this.someString = someString;
        this.someDtm = someDtm;
        this.transientDouble = transientDouble;
    }

    public Long getTestKey() {
        return testKey;
    }

    public long getSomeLong() {
        return someLong;
    }

    public int getSomeInt() {
        return someInt;
    }

    public String getSomeString() {
        return someString;
    }

    public Timestamp getSomeDtm() {
        return someDtm;
    }

    public double getTransientDouble() {
        return transientDouble;
    }
}
