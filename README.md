# DBConnectionPool
DBConnectionPool is a database connection pool framework in Java. Those who write standalone Java applications that does not run on somekind of container can use this framework to efficiently manage connections to database. This framework supports

- Maintain min, max connections to a database
- Automatic management of connections, restoring bad/broken connections with good one.
- Monitor connections, provides a way to identify which thread is using a connection at any given time
- No depedency other than log4j.
- Tested and proven to work great for a commercial application of 100million database transactions a day.
Can wrap and manage any Database connection driver.
