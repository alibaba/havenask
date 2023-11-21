---
toc: content
order: 2
---

# types in the system
<div class="lake-content" typography="classic"><h2 id="s6y5o" style="font-size: 24px; line-height: 32px; margin: 21px 0 5px 0"><span class="ne-text">Mapping between engine fields and MaxCompute fields</span></h2>

Engine field type | ODPS field type
-- | --
TEXT | String types such as VARCHAR and STRING
STRING | String types such as VARCHAR and STRING
INT8 | Single value corresponds to TINYINT. Multiple values correspond to strings such as VARCHAR and STRING. Separate multiple values with "\x1D"('^]').
UINT8 | Single value corresponds to TINYINT. Note that multiple values correspond to strings such as VARCHAR and STRING. Separate multiple values with "\x1D"('^]').
INT16 | A single value corresponds to TINYINT and a SMALLINT value corresponds to VARCHAR and STRING strings. Separate multiple values with "\x1D"('^]').
UINT16 | Single value corresponds to TINYINT and SMALLINT. Note that multiple values correspond to strings such as VARCHAR and STRING. Separate multiple values with "\x1D"('^]').
INTEGER | A single value corresponds to a string type such as TINYINT, SMALLINT, and INT. Multiple values correspond to a string type such as VARCHAR and STRING. Separate multiple values with "\x1D"('^]').
UINT32 | Single value corresponds to TINYINT, SMALLINT, and INT. Note that multiple values correspond to strings such as VARCHAR and STRING. Separate multiple values with "\x1D"('^]').
INT64 | A single value corresponds to TINYINT, SMALLINT, INT, or BIGINT. Multiple values correspond to strings such as VARCHAR and STRING. Separate multiple values with "\x1D"('^]').
UINT64 | Single value corresponds to TINYINT, SMALLINT, INT, and BIGINT. Note that multiple values correspond to strings such as VARCHAR and STRING. Separate multiple values with "\x1D"('^]').
FLOAT | A single value corresponds to FLOAT or a string of the VARCHAR or STRING type if multiple values of the integer type within the FLOAT range are used. Separate multiple values with "\x1D"('^]').
DOUBLE | A single value corresponds to a value of the DOUBLE or FLOAT type. A single value corresponds to a string of the VARCHAR or STRING type. Separate multiple values with "\x1D"('^]').
LOCATION | String types such as VARCHAR and STRING
LINE | VARCHAR, STRING, and other string types
String types such as POLYGON | VARCHAR and STRING
DATE | DATE
TIME | String types such as VARCHAR and STRING
TIMESTMP | DATETIME, TIMESTAMP

</div>


<div class="lake-content" typography="classic"><h2 id="K7MHB" style="font-size: 24px; line-height: 32px; margin: 21px 0 5px 0"><span class="ne-text">Mapped between engine fields and RDS fields</span></h2>

Engine field type | RDS field type
-- | --
String types such as TEXT | VARCHAR
STRING | VARCHAR and other string types
INT8 | Single value corresponds to TINYINT. Multiple values correspond to strings such as VARCHAR and STRING. Separate multiple values with "\x1D"('^]').
UINT8 | Single value corresponds to TINYINT. Note that multiple values correspond to strings such as VARCHAR and STRING. Separate multiple values with "\x1D"('^]').
INT16 | A single value corresponds to TINYINT and a SMALLINT value corresponds to VARCHAR and STRING strings. Separate multiple values with "\x1D"('^]').
UINT16 | Single value corresponds to TINYINT and SMALLINT. Note that multiple values correspond to strings such as VARCHAR and STRING. Separate multiple values with "\x1D"('^]').
INTEGER | Single value corresponds to TINYINT, SMALLINT, and INTEGER. Multiple values correspond to strings such as VARCHAR and STRING. Separate multiple values with "\x1D"('^]').
UINT32 | Single value corresponds to TINYINT, SMALLINT, and INTEGER. Note that multiple values correspond to strings such as VARCHAR and STRING. Separate multiple values with "\x1D"('^]').
INT64 | Single value corresponds to TINYINT, SMALLINT, INTEGER, or BIGINT. Multiple values correspond to strings such as VARCHAR and STRING. Separate multiple values with "\x1D"('^]').
UINT64 | Single value corresponds to TINYINT, SMALLINT, INTEGER, or BIGINT. Note that multiple values correspond to strings such as VARCHAR and STRING. Separate multiple values with "\x1D"('^]').
FLOAT | A single value corresponds to FLOAT, NUMERIC, or a string of the VARCHAR or STRING type when multiple values of the integer type within the FLOAT range. Separate multiple values with "\x1D"('^]').
DOUBLE | A single value corresponds to a string of the DOUBLE, NUMERIC, or FLOAT type. A single value corresponds to a string of the VARCHAR, STRING type. Separate multiple values with "\x1D"('^]').
LOCATION | String types such as VARCHAR and STRING
LINE | VARCHAR, STRING, and other string types
String types such as POLYGON | VARCHAR and STRING
DATE | DATE
TIME | TIME
TIMESTMP | DATETIME, TIMESTAMP

</div>