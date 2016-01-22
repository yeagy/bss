# BSS - Better SQL Support
### Lightweight JDBC enhancement library. Joinless ORM.
###### Dependencies: Java 8.
Vanilla JDBC is a utilitarian and lackluster experience.
The early java API is dated.
Lots of common usages are easy to screw up, like null handling primitives, or properly executing a transaction and rollback.
The index based parameter API is error prone, and lacks support for the common IN clause!<br>
This library can enhance the JDBC experience to one that's bearable! Near decent even!

**BetterPreparedStatement**
 * :named parameters!!
 * null-safe primitive set methods
 * java 8 time set methods
 * automagic create/set array methods!!
 * IN clause support with array simulation!!!! (for DBs that don't support arrays)

**BetterResultSet**
 * null-safe primitive get methods
 * java 8 time get methods
 * SQL arrays casted to their java type

**BetterSqlSupport**
 * provides a java 8 lambda based API encapsulating common CRUD usage of JDBC.
 * additionally features a fluent statement builder API for added flexibility.

**BetterSqlMapper**
 * joinless ORM
 * simple convention with annotation overrides
 * additional fluent select builder API allows any query to automagically map to a POJO

**BetterSqlTransaction**
 * encapsulates the JDBC transaction process into a simple lambda based API.

**BetterSqlGenerator**
 * generate basic CRUD SQL from a POJO

#### Tested Databases: PostgreSQL. MySQL/MariaDB. H2.
## Usage
##### ORM Simplified
BSS includes a joinless ORM, the anti-JPA. This will not prescribe your object structure, your caching layer(s), your transaction strategies, or lock you into any kind of framework. It can work on vanilla POJO's if a simple convention is followed. Annotations can be used to stray from convention.

**Convention -> Table is in lower snake_case, POJO in CamelCase. Primary Key is first/top field. Zero-parameter constructor. Primitive fields are non-null columns.**

BetterSqlMapper usage:
``` java
BetterSqlMapper sqlMapper = BetterSqlMapper.fromDefaults();

TestBean bean = new TestBean(null, Long.MAX_VALUE, "test string", Timestamp.from(Instant.now()));
TestBean result = sqlMapper.insert(connection, bean);
assertNotNull(result.getTestKey());

result = sqlMapper.find(connection, result.getTestKey(), TestBean.class);
assertThat(result.getSomeString(), equalTo(bean.getSomeString()));

sqlMapper.update(connection, new TestBean(result.getTestKey(), bean.getSomeLong(), "changed string", bean.getSomeDtm()));

result = sqlMapper.find(connection, result.getTestKey(), TestBean.class);
assertThat(result.getSomeString(), not(equalTo(bean.getSomeString())));

sqlMapper.delete(connection, result);

result = sqlMapper.find(connection, result.getTestKey(), TestBean.class);
assertNull(result);
```
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
Annotation example:
``` java
import java.sql.Timestamp;

@Table(name = "test_bean")
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
BSS provides utilities to ease the JDBC you still have to do manually.
**BetterPreparedStatement** and **BetterResultSet** classes feature :named parameters, null-safe boxed primitive getting/setting, array type conversion, and java 8 time.
Class **BetterSqlSupport** features a simple lambda based API as well as a cascading builder for one-line JDBC calls.

Example of mixed method and builder styles:
``` java
BetterSqlSupport sqlSupport = BetterSqlSupport.fromDefaults();
Timestamp now = Timestamp.from(Instant.now());

String insert = "INSERT INTO test_bean (some_long, some_string, some_dtm) VALUES (:some_long, :some_string, :some_dtm)";
Long key = sqlSupport.insert(connection, insert, ps -> {
    ps.setLong("some_long", Long.MAX_VALUE);
    ps.setString("some_string", "test string");
    ps.setTimestamp("some_dtm", now);
});
assertThat(key, not(equalTo(0)));

String select = "SELECT * FROM test_bean WHERE test_key = :test_key";
TestBean bean = sqlSupport.builder(select)
        .statementBinding(ps -> ps.setLong("test_key", key))
        .resultMapping(rs -> new TestBean(rs.getLong("test_key"), rs.getLong("some_long"), rs.getString("some_string"), rs.getTimestamp("some_dtm")))
        .executeQuery(connection);
assertThat(bean.getSomeString(), equalTo("test string"));

String update = "UPDATE test_bean SET some_long = :some_long, some_string = :some_string, some_dtm = :some_dtm WHERE test_key = :test_key";
int rowsUpdated = sqlSupport.update(connection, update, ps -> {
    ps.setLong("some_long", bean.getSomeLong());
    ps.setString("some_string", "changed string");
    ps.setTimestamp("some_dtm", bean.getSomeDtm());
    ps.setLong("test_key", key);
});
assertThat(rowsUpdated, equalTo(1));

String delete = "DELETE FROM test_bean WHERE test_key = :test_key";
int rowsDeleted = sqlSupport.builder(delete)
        .statementBinding(ps -> ps.setLong("test_key", key))
        .executeUpdate(connection);
assertThat(rowsDeleted, equalTo(1));
```


Class **BetterSqlTransaction** provides one line transactions with lambdas.
``` java
BetterSqlMapper sqlMapper = BetterSqlMapper.fromDefaults();

TestBean testBean = BetterSqlTransaction.returning(conn -> {
    TestBean insert = sqlMapper.insert(conn, new TestBean(null, Long.MAX_VALUE, "test string", Timestamp.from(Instant.now())));
    sqlMapper.update(conn, new TestBean(insert.getTestKey(), insert.getSomeLong(), "changed string", insert.getSomeDtm()));
    return sqlMapper.select(conn, insert.getTestKey(), TestBean.class);
}).execute(connection);
assertNotNull(testBean);
assertThat(testBean.getSomeString(), equalTo("changed string"));
```

Class **BetterSqlGenerator** generates CREATE TABLE and CRUD based on your POJO.<br/>
Supports both ? parameters and :named parameters.
``` java
BetterSqlGenerator GENERATOR = BetterSqlGenerator.fromDefaults();
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
