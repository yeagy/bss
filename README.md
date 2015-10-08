# dorm -- dumb object relational mapper
##### Joinless ORM. This library can automagically do single table/object CRUD operations. For more *exotic* things like joins, this library can generate SQL that can then be handed edited and combined with enhanced JDBC wrappers for explicit control. Dorm is a complement to, and not replacement of, JDBC.

I was inspired to write this library because I hate Hibernate, but I also hate writing boilerplate JDBC.
My opinion is that Hibernate is a bloated piece of software where the productivity benefits are not worth the various trade-offs.
My opinion is also that writing JDBC is time consuming, and the most mundane can largely be meta-programmed by convention.

#### Dependencies
 * Java 8

## Usage
##### Convention -> Table is in lower snake_case, POJO in CamelCase. Primary Key is first/top field. No parameter constructor (any scope). Primitive fields are non-null columns.
Dorm is designed to be as minimal as possible.
As such, it can work on POJO's without need for annotations if a simple convention is followed.
Annotations can be used to stray from convention.

Example table (postgres syntax):
```
CREATE TABLE test_bean(
  test_key BIGSERIAL PRIMARY KEY,
  some_long BIGINT,
  some_string VARCHAR,
  some_dtm TIMESTAMP
);
```
Example POJO:
```
import java.sql.Timestamp;

public class TestBean {
    private Long testKey;
    private long someLong;
    private String someString;
    private Timestamp someDtm;

    private TestBean() { }
    public TestBean(Long testKey, long someLong, String someString, Timestamp someDtm) {
        this.testKey = testKey;
        this.someLong = someLong;
        this.someString = someString;
        this.someDtm = someDtm;
    }
    public Long getTestKey() { return testKey; }
    public long getSomeLong() { return someLong; }
    public String getSomeString() { return someString; }
    public Timestamp getSomeDtm() { return someDtm; }
}
```
Dorm usage:
```
TestBean bean = new TestBean(null, Long.MAX_VALUE, "test string", Timestamp.from(Instant.now()));
TestBean result = DORM.insert(connection, bean);
assertNotNull(result.getTestKey());

result = DORM.select(connection, result.getTestKey(), TestBean.class);
assertThat(result.getSomeString(), equalTo(bean.getSomeString()));

DORM.update(connection, new TestBean(result.getTestKey(), bean.getSomeLong(), bean.getSomeInt(), "changed string", bean.getSomeDtm()));

result = DORM.select(connection, result.getTestKey(), TestBean.class);
assertThat(bean.getSomeString(), not(equalTo(result.getSomeString())));

DORM.delete(connection, result.getTestKey(), TestBean.class);

result = DORM.select(connection, result.getTestKey(), TestBean.class);
assertNull(result);
```
Annotation example:
```
import java.sql.Timestamp;

@Table(name = "test_bean", schema = "")
public class AnnotatedTestBean {
    @Column(name="some_long") private long legacyLong;
    @Column(name="some_string") private String legacyString;
    @Column(name="some_dtm") private Timestamp legacyTimestamp;
    @Id @Column(name="test_key") private Long legacyKey;

    private AnnotatedTestBean() {}
    public AnnotatedTestBean(Long legacyKey, long legacyLong, String legacyString, Timestamp legacyTimestamp) {
        this.legacyKey = legacyKey;
        this.legacyLong = legacyLong;
        this.legacyString = legacyString;
        this.legacyTimestamp = legacyTimestamp;
    }
    public Long getLegacyKey() { return legacyKey; }
    public long getLegacyLong() { return legacyLong; }
    public String getLegacyString() { return legacyString; }
    public Timestamp getLegacyTimestamp() { return legacyTimestamp; }
}
```
#### Enchanced JDBC Support
Dorm provides tools to make the JDBC you still have to do manually easier.
Classes BetterPreparedStatement and BetterResultSet feature null-safe primitive setting, as well as :named parameters.
Class SqlSupport features a simple lambda based API as well as a Builder object for one line JDBC calls.



You can use the SqlGenerator class to generate DDL/DML based on your POJO.<br/>
Supports both ? parameters and :named parameters.
