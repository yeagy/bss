package io.github.yeagy.bss;

@Table(schema = "bss_test", name = "composite_key_test")
public class CompositeKeyBean {
    @Id private Long keyA;
    @Id private Long keyB;
    private long someLong;
    private int someInt;
    private String someString;

    private CompositeKeyBean() { }

    public CompositeKeyBean(Long keyA, Long keyB, long someLong, int someInt, String someString) {
        this.keyA = keyA;
        this.keyB = keyB;
        this.someLong = someLong;
        this.someInt = someInt;
        this.someString = someString;
    }

    public Long getKeyA() {
        return keyA;
    }

    public Long getKeyB() {
        return keyB;
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
}
