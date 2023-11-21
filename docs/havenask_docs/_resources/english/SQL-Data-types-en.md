<div class="lake-content" typography="classic"><h2 id="C5gSW" style="font-size: 24px; line-height: 32px; margin: 21px 0 5px 0"><span class="ne-text">Data types</span></h2>



Data type | Description | Support for multiple values | Used for attribute indexes | Used for summary indexes | Used for inverted indexes
-- | -- | -- | -- | -- | --
TEXT | Stores text data. | No | No | Yes | Yes
STRING | Stores string data. | Yes | Yes | Yes | Yes
INT8 | Stores 8-bit signed integers. | Yes | Yes | Yes | Yes
UINT8 | Stores the 8-bit unsigned integers. | Yes | Yes | Yes | Yes
INT16 | Stores the 16-bit signed integers. | Yes | Yes | Yes | Yes
UINT16 | Stores the 16-bit unsigned integers. | Yes | Yes | Yes | Yes
INTEGER | Stores the 32-bit signed integers. | Yes | Yes | Yes | Yes
UINT32 | Stores the 32-bit unsigned integers. | Yes | Yes | Yes | Yes
INT64 | Stores the 64-bit signed integers. | Yes | Yes | Yes | Yes
UINT64 | Stores the 64-bit unsigned integers. | Yes | Yes | Yes | Yes
FLOAT | Stores the single-precision 32-bit floating points. | Yes | Yes | Yes | No
DOUBLE | Stores the double-precision 64-bit floating points. | Yes | Yes | Yes | No
LOCATION | Stores the longitude and latitude of a point. | Yes | Yes | Yes | Yes
LINE | Stores the latitude and longitude of the points that form a polyline. The first value represents the number of points on the polyline. | Yes | Yes | Yes | Yes | Yes
POLYGON | Stores the longitude and latitude of the points that form a polygon. The value is a combination of multiple polylines. For each polyline, the first value represents the number of points on the polyline. | Yes | Yes | Yes | Yes | Yes
DATE | Stores date values. | No | No | Yes | Yes
TIME | Stores time values. | No | No | Yes | Yes
TIMESTMP | Stores timestamps.Format: {DATE} {TIME} [TIMEZONE (optional)] | No | No | Yes | Yes



<ul class="ne-ul" style="margin: 0; padding-left: 23px"><li id="u805de80d"><span class="ne-text">TEXT: You must specify an analyzer for fields of the TEXT type when you configure a schema.</span> </li><li id="ufb6e6171"><span class="ne-text">You can specify values for fields of the LOCATION data type in the location={Longitude} {Latitude} format, such as location=116 40.</span></li><li id="ub5b9d535"><span class="ne-text">You can specify values for fields of the LINE data type in the line=location,location,location...^] format, such as line=116 40,117 41,118 42 ^].</span></li><li id="ua7764e9b"><span class="ne-text">You can specify values for fields f the POLYGON data type in the format of polygon=location1,location2,...location1^]...</span></li><li id="u22ab2c49"><span class="ne-text">You can specify values for fields of the DATE data type in the year-month-day format, such as 2020-08-19.</span></li><li id="u0237178a"><span class="ne-text">You can specify values for fields of the TIME data type in the hour:minute:second[.milliSeconds] format. The milliSeconds parameter is optional. For example, a time value can be 11:40:00.234 or 12:00:00.</span></li><li id="uae34c0e2"><span class="ne-text">You can specify values for fields of the TIMESTAMP data type in the {DATE} {TIME} [TIMEZONE] format. The TIMEZONE parameter specifies a time zone and is optional. For example, a timestamp value can be 2020-08-19 11:40:00.234 or 2020-08-19 11:40.00.234+0800. By default, the time zone is UTC. You can change the time zone.</span></li></ul></div>