---
toc: content
order: 2
---

# 系统中的各种类型
<div class="lake-content" typography="classic"><h2 id="s6y5o" style="font-size: 24px; line-height: 32px; margin: 21px 0 5px 0"><span class="ne-text">引擎字段与ODPS字段的对应关系</span></h2>

引擎字段类型 | ODPS字段类型
-- | --
TEXT | VARCHAR、STRING等字符串类型
STRING | VARCHAR、STRING等字符串类型
INT8 | 单值时对应TINYINT多值时对应VARCHAR、STRING等字符串类型，多值之间用"\x1D"（'^]')分隔
UINT8 | 单值时对应TINYINT，注意取值范围多值时对应VARCHAR、STRING等字符串类型，多值之间用"\x1D"（'^]')分隔
INT16 | 单值时对应TINYINT、SMALLINT多值时对应VARCHAR、STRING等字符串类型，多值之间用"\x1D"（'^]')分隔
UINT16 | 单值时对应TINYINT、SMALLINT，注意取值范围多值时对应VARCHAR、STRING等字符串类型，多值之间用"\x1D"（'^]')分隔
INTEGER | 单值时对应TINYINT、SMALLINT、INT多值时对应VARCHAR、STRING等字符串类型，多值之间用"\x1D"（'^]')分隔
UINT32 | 单值时对应TINYINT、SMALLINT、INT，注意取值范围多值时对应VARCHAR、STRING等字符串类型，多值之间用"\x1D"（'^]')分隔
INT64 | 单值时对应TINYINT、SMALLINT、INT、BIGINT多值时对应VARCHAR、STRING等字符串类型，多值之间用"\x1D"（'^]')分隔
UINT64 | 单值时对应TINYINT、SMALLINT、INT、BIGINT，注意取值范围多值时对应VARCHAR、STRING等字符串类型，多值之间用"\x1D"（'^]')分隔
FLOAT | 单值时对应FLOAT或者在FLOAT范围内的整型多值时对应VARCHAR、STRING等字符串类型，多值之间用"\x1D"（'^]')分隔
DOUBLE | 单值时对应DOUBLE、FLOAT或者整型多值时对应VARCHAR、STRING等字符串类型，多值之间用"\x1D"（'^]')分隔
LOCATION | VARCHAR、STRING等字符串类型
LINE | VARCHAR、STRING等字符串类型
POLYGON | VARCHAR、STRING等字符串类型
DATE | DATE
TIME | VARCHAR、STRING等字符串类型
TIMESTMP | DATETIME、TIMESTAMP

</div>


<div class="lake-content" typography="classic"><h2 id="K7MHB" style="font-size: 24px; line-height: 32px; margin: 21px 0 5px 0"><span class="ne-text">引擎字段与RDS字段的对应关系</span></h2>

引擎字段类型 | RDS字段类型
-- | --
TEXT | VARCHAR等字符串类型
STRING | VARCHAR等字符串类型
INT8 | 单值时对应TINYINT多值时对应VARCHAR、STRING等字符串类型，多值之间用"\x1D"（'^]')分隔
UINT8 | 单值时对应TINYINT，注意取值范围多值时对应VARCHAR、STRING等字符串类型，多值之间用"\x1D"（'^]')分隔
INT16 | 单值时对应TINYINT、SMALLINT多值时对应VARCHAR、STRING等字符串类型，多值之间用"\x1D"（'^]')分隔
UINT16 | 单值时对应TINYINT、SMALLINT，注意取值范围多值时对应VARCHAR、STRING等字符串类型，多值之间用"\x1D"（'^]')分隔
INTEGER | 单值时对应TINYINT、SMALLINT、INTEGER多值时对应VARCHAR、STRING等字符串类型，多值之间用"\x1D"（'^]')分隔
UINT32 | 单值时对应TINYINT、SMALLINT、INTEGER，注意取值范围多值时对应VARCHAR、STRING等字符串类型，多值之间用"\x1D"（'^]')分隔
INT64 | 单值时对应TINYINT、SMALLINT、INTEGER、BIGINT多值时对应VARCHAR、STRING等字符串类型，多值之间用"\x1D"（'^]')分隔
UINT64 | 单值时对应TINYINT、SMALLINT、INTEGER、BIGINT，注意取值范围多值时对应VARCHAR、STRING等字符串类型，多值之间用"\x1D"（'^]')分隔
FLOAT | 单值时对应FLOAT、NUMERIC或者在FLOAT范围内的整型多值时对应VARCHAR、STRING等字符串类型，多值之间用"\x1D"（'^]')分隔
DOUBLE | 单值时对应DOUBLE、NUMERIC、FLOAT或者整型多值时对应VARCHAR、STRING等字符串类型，多值之间用"\x1D"（'^]')分隔
LOCATION | VARCHAR、STRING等字符串类型
LINE | VARCHAR、STRING等字符串类型
POLYGON | VARCHAR、STRING等字符串类型
DATE | DATE
TIME | TIME
TIMESTMP | DATETIME、TIMESTAMP

</div>