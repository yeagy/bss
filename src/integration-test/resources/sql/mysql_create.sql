DROP DATABASE IF EXISTS bss_test;

CREATE DATABASE bss_test;

CREATE TABLE bss_test.integration_test (
  test_key    BIGINT AUTO_INCREMENT PRIMARY KEY,
  some_long   BIGINT   NOT NULL,
  some_int    INTEGER  NOT NULL,
  some_short  SMALLINT NOT NULL,
  some_double DOUBLE   NOT NULL,
  some_float  REAL     NOT NULL,
  some_bool   BOOLEAN  NOT NULL,
  some_string VARCHAR(255),
  some_bd     NUMERIC,
  some_time   TIME,
  some_date   DATE,
  some_dtm    TIMESTAMP
);
