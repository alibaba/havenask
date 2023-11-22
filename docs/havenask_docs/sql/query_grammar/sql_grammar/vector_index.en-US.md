---
toc: content
order: 30336
---

# Vector-based query
## Syntax for a common query
```
query=select id from in0 where MATCHINDEX('index_name', '0.1,0.2,0.98,0.6;0.3,0.4,0.98,0.6...')

Note: in0 is the name of the table to be queried, index_name is the name of the vector index, followed by the vector to be queried.
```

## Specify top n query
```
query=select id from in0 where MATCHINDEX('index_name', '0.1,0.2,0.98,0.6;0.3,0.4,0.98,0.6&n=10')

Note: in0 is the name of the table to be queried, index_name is the name of the vector index, followed by the vector to be queried, and n specifies the number of top results returned by vector retrieval.
```

## Syntax for a query that includes the specified threshold value
```
query=select id from in0 where MATCHINDEX('index_name', '0.1,0.2,0.98,0.6;0.3,0.4,0.98,0.6&sf=0.8')

Note: in0 is the name of the table to be queried, index_name is the name of the vector index, followed by the vector to be queried, and sf specifies the threshold for the score to be filtered. If you set the search_type parameter to ip in the schema.json file, documents whose inner product is less than 4.0 are filtered out. When the search_type parameter in the user schema is l2 (Euclidean distance), the documents whose Euclidean distance score is higher than 2.0 will be filtered out. The reason why it is 2.0 rather than 4.0 is that the engine calculates the score based on the square of the Euclidean distance.
```

## Syntax for a query based on categories
```
query=select id from in0 where MATCHINDEX('index_name', '16#0.1,0.2,0.98,0.6;0.3,0.4,0.98,0.6;1512#0.3,0.4,0.98,0.6&n=200')
// The vector to be queried needs to be urlencode.
query=select id from in0 where MATCHINDEX('index_name', '16%230.1%2c0.2%2c0.98%2c0.6%3b1512%230.3%2c0.4%2c0.98%2c0.6%26n%3d200')
Note: If you want to distinguish a category, you must specify the category ID and the vector to be queried in the parameter value. Separate the category ID and vector with '#' (URLEncode '#' in the query). Separate multiple categories with commas (,) and separate multiple vectors with semicolons.
```
