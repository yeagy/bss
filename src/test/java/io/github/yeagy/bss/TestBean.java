package io.github.yeagy.bss;

import java.sql.Timestamp;

public class TestBean {
    public enum Status{ON, OFF}

    @Id private Long testKey;
    private long someLong;
    private int someInt;
    private String someString;
    private Timestamp someDtm;
    private transient double transientDouble;//ignored by mapper
    private Status someEnum;

    private TestBean() { }

    public TestBean(Long testKey, long someLong, int someInt, String someString, Timestamp someDtm, double transientDouble, Status someEnum) {
        this.testKey = testKey;
        this.someLong = someLong;
        this.someInt = someInt;
        this.someString = someString;
        this.someDtm = someDtm;
        this.transientDouble = transientDouble;
        this.someEnum = someEnum;
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

    public Status getSomeEnum() {
        return someEnum;
    }
}
