SQL currently supports query clauses and kvpair clauses. The query clauses are used to build SQL statements, and the kvpair clauses are used to build some related parameters.

The path parameter is set to /sql in a SQL query. The path parameter is set to / in an HA3 query.



## Run queries

If you have the service IP address and port of the HA3 QRS, you can run the curl command to manually call the query interface.



```

curl "ip:port/sql?query=SELECT brand, COUNT(*) FROM phone GROUP BY (brand)&&kvpair=trace:INFO;formatType:json"

```



## Additional considerations

Spaces cannot exist between kvpair and && or between kvpair clauses. Otherwise, the query fails.
