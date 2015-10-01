# dorm -- dumb object relational mapper (java8)
Joinless ORM. This library can automatically do single table/object CRUD operations. For more exotic things like joins, this library can generate SQL that can then be easily handed edited and combined with enhanced JDBC utilities for a experience more pleasurable than plain JDBC. Dorm is a complement to, and not replacement of, JDBC.

I was inspired to write this library because I hate Hibernate, but I also hate writing boilerplate JDBC. My opinion is that Hibernate is an overly complex piece of a software where the productivity benefits are outweighed by maintenance problems. My opinion is also that writing JDBC is time consuming, and the most mundane can largely be metaprogrammed by convention.
