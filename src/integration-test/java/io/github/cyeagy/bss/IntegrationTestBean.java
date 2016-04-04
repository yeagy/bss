package io.github.cyeagy.bss;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

@Table(schema = "bss_test", name = "integration_test")
public class IntegrationTestBean {
    @Id private Long testKey;
    private long someLong;
    private int someInt;
    private short someShort;
    private double someDouble;
    private float someFloat;
    private boolean someBool;
    private String someString;
    private BigDecimal someBd;
    private Time someTime;
    private Date someDate;
    private Timestamp someDtm;

    private IntegrationTestBean() { }

    public IntegrationTestBean(Long testKey, long someLong, int someInt, short someShort, double someDouble, float someFloat, boolean someBool, String someString, BigDecimal someBd, Time someTime, Date someDate, Timestamp someDtm) {
        this.testKey = testKey;
        this.someLong = someLong;
        this.someInt = someInt;
        this.someShort = someShort;
        this.someDouble = someDouble;
        this.someFloat = someFloat;
        this.someBool = someBool;
        this.someString = someString;
        this.someBd = someBd;
        this.someTime = someTime;
        this.someDate = someDate;
        this.someDtm = someDtm;
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

    public short getSomeShort() {
        return someShort;
    }

    public double getSomeDouble() {
        return someDouble;
    }

    public float getSomeFloat() {
        return someFloat;
    }

    public boolean isSomeBool() {
        return someBool;
    }

    public String getSomeString() {
        return someString;
    }

    public BigDecimal getSomeBd() {
        return someBd;
    }

    public Time getSomeTime() {
        return someTime;
    }

    public Date getSomeDate() {
        return someDate;
    }

    public Timestamp getSomeDtm() {
        return someDtm;
    }
}
