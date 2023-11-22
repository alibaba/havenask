---

toc:  content
order: -1
---

# Method to obtain the parameter value
currently, sql supports the query clause and kvpair clause. the query clause is used to spell sql query, and the kvpair clause is used to spell some related parameters.
Different from ha3, the path of sql query is "/sql", while ha3 is "/"

## Execute queries
If you know the service IP address and port number of HA3 Qrs, you can use the curl command to manually call the query interface.

```
curl "ip:port/sql?query=SELECT brand, COUNT(*) FROM phone GROUP BY (brand)&&kvpair=trace:INFO;formatType:json"
```

## Note
Spaces are not allowed between kvpair and && or between kvpair clauses. If a space exists between kvpair and && or between kvpair clauses, the query fails to be resolved.

## View cluster registration information
You can curl \<host\>:\<ip\>/sqlClientInfo of cluster qrs to view the tables and functions registered in the cluster