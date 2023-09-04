cc_library(
    name='table_json',
    hdrs=['table/TableJson.h'],
    include_prefix='table',
    strip_include_prefix='table',
    visibility=['//visibility:public'],
    deps=['//aios/autil:json']
)
cc_library(
    name='table',
    srcs=[
        'table/Column.cpp', 'table/ColumnData.cpp', 'table/ColumnSchema.cpp',
        'table/ComboComparator.cpp', 'table/ComparatorCreator.cpp',
        'table/Table.cpp', 'table/TableFormatter.cpp', 'table/TableSchema.cpp',
        'table/TableUtil.cpp', 'table/ValueTypeSwitch.cpp'
    ],
    hdrs=[
        'table/Column.h', 'table/ColumnComparator.h', 'table/ColumnData.h',
        'table/ColumnSchema.h', 'table/ComboComparator.h', 'table/Comparator.h',
        'table/ComparatorCreator.h', 'table/Row.h', 'table/Table.h',
        'table/TableFormatter.h', 'table/TableSchema.h', 'table/TableUtil.h',
        'table/ValueTypeSwitch.h'
    ],
    include_prefix='table',
    strip_include_prefix='table',
    visibility=['//visibility:public'],
    deps=[
        'table_json', '//aios/autil:compression', '//aios/autil:json',
        '//aios/autil:log', '//aios/autil:string_helper', '//aios/autil:time',
        '//aios/matchdoc'
    ]
)
