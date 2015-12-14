# dorm -- dumb object relational mapper
##### Joinless ORM. This library can automagically do single table/object CRUD operations using reflection. For more *exotic* things like joins, this library can generate SQL that can then be edited and leveraged with enhanced JDBC utilities for simplified data access. Dorm is a complement to, and not replacement of, JDBC.

This is ORM for people that hate Hibernate and it's ilk. Dorm is not going to prescribe your data architecture, your cache layer(s), or help you ignore thinking about transaction boundaries. Dorm will do your simple rote JDBC for you, and ease more complex usage.
#### Dependencies
 * Java 8

## Usage
##### Convention -> Table is in lower snake_case, POJO in CamelCase. Primary Key is first/top field. No parameter constructor. Primitive fields are non-null columns.
Dorm is designed to be as quite minimal, it can work on POJO's without need for annotations if a simple convention is followed.
Annotations can be used to stray from convention.

Example table (postgres syntax):
``` sql
CREATE TABLE test_bean (
  test_key    BIGSERIAL PRIMARY KEY,
  some_long   BIGINT    NOT NULL,
  some_string VARCHAR,
  some_dtm    TIMESTAMP
);
```
Example POJO:
``` java
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
``` java
Dorm DORM = Dorm.fromDefaults();

TestBean bean = new TestBean(null, Long.MAX_VALUE, "test string", Timestamp.from(Instant.now()));
TestBean result = DORM.insert(connection, bean);
assertNotNull(result.getTestKey());

result = DORM.select(connection, result.getTestKey(), TestBean.class);
assertThat(result.getSomeString(), equalTo(bean.getSomeString()));

DORM.update(connection, new TestBean(result.getTestKey(), bean.getSomeLong(), "changed string", bean.getSomeDtm()));

result = DORM.select(connection, result.getTestKey(), TestBean.class);
assertThat(result.getSomeString(), not(equalTo(bean.getSomeString())));

DORM.delete(connection, result.getTestKey(), TestBean.class);

result = DORM.select(connection, result.getTestKey(), TestBean.class);
assertNull(result);
```
Annotation example:
``` java
import java.sql.Timestamp;

@Table(name = "test_bean", schema = "")
public class AnnotatedTestBean {
    @Column(name="some_long") private long legacyLong;
    @Column(name="some_string") private String legacyString;
    @Column(name="some_dtm") private Timestamp legacyTimestamp;
    @Id @Column(name="test_key") private Long legacyKey;

    private AnnotatedTestBean() { }
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
Dorm provides utilities to ease the JDBC you still have to do manually.
**BetterPreparedStatement** and **BetterResultSet** classes feature :named parameters, null-safe boxed primitive getting/setting, array type conversion, and java 8 time.
Class **SqlSupport** features a simple lambda based API as well as a cascading builder object for one line JDBC calls.

Example of method and builder styles:
``` java
SqlSupport SQL_SUPPORT = SqlSupport.fromDefaults();
Timestamp now = Timestamp.from(Instant.now());

//METHOD STYLE
String insert = "INSERT INTO test_bean (some_long, some_string, some_dtm) VALUES (:some_long, :some_string, :some_dtm)";
Long key = SQL_SUPPORT.insert(connection, insert, ps -> {
    ps.setLong("some_long", Long.MAX_VALUE);
    ps.setString("some_string", "test string");
    ps.setTimestamp("some_dtm", now);
});
assertNotNull(key);
assertThat(key, not(equalTo(0)));

String select = "SELECT * FROM test_bean WHERE test_key = :test_key";
TestBean bean = SQL_SUPPORT.query(connection, select, ps -> ps.setLong("test_key", key),
        (rs, i) -> new TestBean(rs.getLong("test_key"), rs.getLong("some_long"), rs.getString("some_string"), rs.getTimestamp("some_dtm")));
assertNotNull(bean);
assertThat(bean.getSomeLong(), equalTo(Long.MAX_VALUE));
assertThat(bean.getSomeString(), equalTo("test string"));
assertThat(bean.getSomeDtm(), equalTo(now));

//BUILDER STYLE
String update = "UPDATE test_bean SET some_long = :some_long, some_string = :some_string, some_dtm = :some_dtm WHERE test_key = :test_key";
int rowsUpdated = SQL_SUPPORT.builder(update)
        .queryBinding(ps -> {
            ps.setLong("some_long", bean.getSomeLong());
            ps.setString("some_string", "changed string");
            ps.setTimestamp("some_dtm", bean.getSomeDtm());
            ps.setLong("test_key", key);
        })
        .executeUpdate(connection);
assertThat(rowsUpdated, equalTo(1));

String delete = "DELETE FROM test_bean WHERE test_key = :test_key";
int rowsDeleted = SQL_SUPPORT.builder(delete)
        .queryBinding(ps -> ps.setLong("test_key", key))
        .executeUpdate(connection);
assertThat(rowsDeleted, equalTo(1));
```
Class **SqlTransaction** provides one line transactions with lambdas.
``` java
TestBean testBean = SqlTransaction.returning(conn -> {
    TestBean insert = DORM.insert(conn, new TestBean(null, Long.MAX_VALUE, "test string", Timestamp.from(Instant.now())));
    DORM.update(conn, new TestBean(insert.getTestKey(), insert.getSomeLong(), "changed string", insert.getSomeDtm()));
    return DORM.select(conn, insert.getTestKey(), TestBean.class);
}).execute(connection);
assertNotNull(testBean);
assertThat(testBean.getSomeString(), equalTo("changed string"));
```

Class **SqlGenerator** generates CREATE TABLE and CRUD based on your POJO.<br/>
Supports both ? parameters and :named parameters.
``` java
SqlGenerator GENERATOR = SqlGenerator.fromDefaults();
TableData tableData = TableData.from(TestBean.class);
String insert = GENERATOR.generateInsertSqlTemplateNamed(tableData);
String select = GENERATOR.generateSelectSqlTemplate(tableData);
String update = GENERATOR.generateUpdateSqlTemplate(tableData);
String delete = GENERATOR.generateDeleteSqlTemplateNamed(tableData);
String create = GENERATOR.generateCreateStatement(tableData);
```
``` sql
INSERT INTO test_bean (some_long, some_string, some_dtm) VALUES (:some_long, :some_string, :some_dtm)
SELECT test_key, some_long, some_string, some_dtm FROM test_bean WHERE test_key = ?
UPDATE test_bean SET some_long = ?, some_string = ?, some_dtm = ? WHERE test_key = ?
DELETE FROM test_bean WHERE test_key = :test_key
CREATE TABLE test_bean (
  test_key BIGINT PRIMARY KEY,
  some_long BIGINT NOT NULL,
  some_string VARCHAR,
  some_dtm TIMESTAMP
)
```
