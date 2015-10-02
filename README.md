# dorm -- dumb object relational mapper
##### Joinless ORM. This library can automatically do single table/object CRUD operations. For more *exotic* things like joins, this library can generate SQL that can then be handed edited and combined with enhanced JDBC wrappers for explicit control. Dorm is a complement to, and not replacement of, JDBC.

I was inspired to write this library because I hate Hibernate, but I also hate writing boilerplate JDBC. My opinion is that Hibernate is an overly complex piece of a software where the productivity benefits are outweighed by maintenance problems. My opinion is also that writing JDBC is time consuming, and the most mundane can largely be metaprogrammed by convention.

#### Dependencies
 * Java 8
 * Guava 18

## Usage
##### Convention -> Table is in lower snake_case, POJO in CamelCase. Primary Key is first/top field.
Dorm is designed to be as minimal as possible. As such, it can work on POJO's without need for annotations if a simple convention is followed. Annotations can be used to stray from convention.

Example table:
```
CREATE TABLE test_bean(
  test_key BIGINT PRIMARY KEY AUTO_INCREMENT,
  some_long BIGINT,
  some_int INT,
  some_string VARCHAR,
  some_dtm TIMESTAMP
);
```
Example POJO
```
import java.sql.Timestamp;

public class TestBean {
    private Long testKey;
    private long someLong;
    private int someInt;
    private String someString;
    private Timestamp someDtm;

    private TestBean() { }
    public TestBean(Long testKey, long someLong, int someInt, String someString, Timestamp someDtm) {
        this.testKey = testKey;
        this.someLong = someLong;
        this.someInt = someInt;
        this.someString = someString;
        this.someDtm = someDtm;
    }
    public Long getTestKey() { return testKey; }
    public long getSomeLong() { return someLong; }
    public int getSomeInt() { return someInt; }
    public String getSomeString() { return someString; }
    public Timestamp getSomeDtm() { return someDtm; }
}
```
Dorm usage:
```
TestBean bean = new TestBean(null, Long.MAX_VALUE, Integer.MAX_VALUE, "test string", Timestamp.from(Instant.now()));
TestBean result = DORM.insert(connection, bean);
assertNotNull(result.getTestKey());

TestBean readBean = DORM.select(connection, result.getTestKey(), TestBean.class);
assertThat(readBean.getSomeString(), equalTo(bean.getSomeString()));

DORM.update(connection, new TestBean(result.getTestKey(), bean.getSomeLong(), bean.getSomeInt(), "changed string", bean.getSomeDtm()));

readBean = DORM.select(connection, result.getTestKey(), TestBean.class);
assertThat(bean.getSomeString(), not(equalTo(readBean.getSomeString())));

DORM.delete(connection, result.getTestKey(), TestBean.class);

readBean = DORM.select(connection, result.getTestKey(), TestBean.class);
assertNull(readBean);
```
Annotation example:
```
import java.sql.Timestamp;

@Table(name = "test_bean", schema = "")
public class AnnotatedTestBean {
    @Column(name="some_long") private long legacyLong;
    @Column(name="some_int") private int legacyInt;
    @Column(name="some_string") private String legacyString;
    @Column(name="some_dtm") private Timestamp legacyTimestamp;
    @Id @Column(name="test_key") private Long legacyKey;

    private AnnotatedTestBean() {}
    public AnnotatedTestBean(Long legacyKey, long legacyLong, int legacyInt, String legacyString, Timestamp legacyTimestamp) {
        this.legacyKey = legacyKey;
        this.legacyLong = legacyLong;
        this.legacyInt = legacyInt;
        this.legacyString = legacyString;
        this.legacyTimestamp = legacyTimestamp;
    }
    public Long getLegacyKey() { return legacyKey; }
    public long getLegacyLong() { return legacyLong; }
    public int getLegacyInt() { return legacyInt; }
    public String getLegacyString() { return legacyString; }
    public Timestamp getLegacyTimestamp() { return legacyTimestamp; }
}
```
You can also use the SqlGenerator class to generate DML based on your pojo. Supports both with ? parameters and :named parameters.