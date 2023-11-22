---
toc: content
order: 4
---

# Use Havenask to query UDFs
* Some UDFs in SQL support native queries using havenask
## Syntax
### Simple queries
Syntax:
`query=Index name:'Search query'^boost Logical operator Index name:'Search query'^boost`

- **Index name**: the index based on which you want to perform the query. The specified index must be included in the index schema that you create. The system scans the values of the field based on which you create the specified index and returns the documents in which the values of the field match the search query that you specify.
- **Search query**: the content for which you want to search.
- **boost**: indicates the weight of the query term. The value is of the int type and ranges from [0 to 99]. If you do not set this parameter, the default value is 99.
- You can specify multiple query conditions. The supported query conditions include `()`, `AND`, `OR`, `ANDNOT`, and `RANK` (which must be uppercase). The priority of these query conditions in descending order is RANK,OR,AND,ANDNOT,().
   - AND indicates the intersection of two query terms. For example, query=default: 'mobile' AND default: 'bluetooth 'indicates that documents that contain both mobile and bluetooth terms are queried.
   - OR indicates the union of two query terms. For example, query=default: 'cellphone' or default: 'bluetooth.
   - ANDNOT indicates the set where the first query is valid and the second query is not valid. For example, query=default: 'mobile' ANDNOT default: 'bluetooth 'indicates that documents that contain "mobile phones" but not "bluetooth" are queried.
   - RANK represents the set where the first query holds true and the second query does not necessarily hold true. For example, query=default: 'mobile' RANK default: 'bluetooth 'indicates that documents that contain "mobile phone" are queried. The documents do not necessarily contain "bluetooth". You can use this operator if specific search queries are required to calculate relevance scores, and these search queries do not affect the query results. In the preceding example, the default:'Bluetooth' condition is used to calculate relevance scores based on text in documents. In this case, the documents that contain Bluetooth are assigned high ranks.
### Advanced queries
#### Multiple search queries based on the same index
Syntax:
`query=Index name: 'Search query '^ boost | 'Search query' ^ boost
qeury=index name: 'query term' ^ boost &'query term' ^ boost `

The vertical bar (|) specifies the OR operator. The ampersand (&) specifies the AND operator.

#### Phrase queries
Syntax:
`query=Index name:"Search query"^boost Operator Index name:"Search query"^boost`

When you write a phrase query statement, enclose each search query in double quotation marks (`" "`). After the system converts a search query to terms, all terms that are obtained are connected and arranged in the same order as the order of items in the search query.

- When you write a phrase query statement, enclose each search query in double quotation marks (`" "`). In a phrase query, all the terms are connected and arranged in the same order before and after analysis.
- Range queries include geography queries and value range queries. For more information, see the following sections.

#### Geography queries
Syntax:
`query=Index name:'SHAPE(ARGS...)'`

You can specify the following SHAPE(ARGS...) in the query clause:

- Point: You can specify a point in the point(LON LAT) format. LON specifies the longitude value of the point, and LAT specifies the latitude value of the point. Separate the longitude value and the latitude value with a space.
- Circle: You can specify a circle in the circle(LON LAT,Radius) format. LON specifies the longitude value of the center for the circle, LAT specifies the latitude value of the center for the circle, and Radius specifies the radius of the circle. Unit: meter.
- Rectangle: You can specify a rectangle in the rectangle(minLON minLAT,maxLON maxLAT) format. Take note that for latitude values, the value of maxLAT must be greater than or equal to minLAT. If the maxLAT value is smaller than the minLAT value, the system automatically exchanges the values. For longitude values, the value of minLON must be smaller than the value of maxLON. If the specified value of minLON is greater than the specified value of maxLON, the result is incorrect.
- Polygon: You can specify a polygon in the polygon(LON1 LAT1,LON2 LAT2,LON3 LAT3,LON4 LAT4,...) format. You can specify a convex polygon or a concave polygon. The start point and the end point of the polygon must be the same point. You cannot specify adjacent sides that are collinear or sides that intersect.

Note:

- The index that you specify must be of the SPATIAL type.
- The expression that you use to define the circle must be included in single quotation marks ('). Example: query=spatial_index:'circle(130.0 10.0,1000.0)'.
- The point coordinates of lines and polygons are mapped to a flat world map to determine the scope of line queries and polygon queries, regardless of the case of crossing 180 degrees longitude. The query results of the inverted index on the location field are accurate. The query results of the inverted index on the line and polygon fields need to be filtered.

#### Value range query:
Syntax:
`query=Index name:(Numeric 1,Numeric 2]`

Numeric 1 specifies the start of the value range, and Numeric 2 specifies the end of the value range. You can use open endpoints and closed endpoints to specify ranges. In the sample code, the opening parenthesis ( before Numeric 1 specifies that Numeric 1 is an open endpoint, and the closing bracket ] next to Numeric 2 specifies that Numeric 2 is a closed endpoint.
Examples:
query=price:(3,100): queries values that are greater than 3 and smaller than 100.
Closed-interval query: query = price: [3,100], which indicates that the data of 3<=x<=100 is queried.
query=price:(3,100]: queries values that are greater than 3 and smaller than or equal to 100.
query=price:(,100): queries values that are smaller than 100. In this clause, the start of the value range is not specified.

Note:

- The index field must be of a numeric type.
- Values in the specified field must be integers. Floating-point numbers are not supported.

#### Date (date) query:
Syntax:
`query=Index name:(Start time,End time)`.
The timestamps of the start time and end time must be of the INTEGER type and accurate to milliseconds. If you do not specify a start time, the system starts the scan from the value 0. If you do not specify an end time, the default value 4102416000000 is used. The default value indicates the following timestamp: 2100-01-01 00:00. You can use open endpoints and closed endpoints to specify ranges.


Note:

- The index must be of the DATE type.
- The timestamps must be of the INTEGER type and accurate to milliseconds. If a timestamp that you specify exceeds 4102416000000, the system uses 4102416000000 as the timestamp.

