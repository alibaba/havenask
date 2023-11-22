cc_library(
    name='table_json',
    hdrs=['TableJson.h'],
    include_prefix='table',
    visibility=['//visibility:public'],
    deps=['//aios/autil:json']
)
cc_library(
    name='table',
    srcs=[
        'BaseColumnData.cpp', 'Column.cpp', 'ColumnSchema.cpp',
        'ComboComparator.cpp', 'ComparatorCreator.cpp', 'Table.cpp',
        'TableFormatter.cpp', 'TableSchema.cpp', 'TableUtil.cpp',
        'UserTypeColumnData.cpp', 'ValueTypeSwitch.cpp'
    ],
    hdrs=[
        'BaseColumnData.h', 'Column.h', 'ColumnComparator.h', 'ColumnData.h',
        'ColumnDataTraits.h', 'ColumnSchema.h', 'ComboComparator.h',
        'Comparator.h', 'ComparatorCreator.h', 'HllCtxColumnData.h',
        'ListColumnData.h', 'ListDataHolder.h', 'Row.h', 'SimpleColumnData.h',
        'Table.h', 'TableFormatter.h', 'TableSchema.h', 'TableUtil.h',
        'UserTypeColumnData.h', 'ValueTypeSwitch.h'
    ],
    include_prefix='table',
    visibility=['//visibility:public'],
    deps=[
        'table_json', '//aios/autil:compression', '//aios/autil:json',
        '//aios/autil:log', '//aios/autil:result', '//aios/autil:string_helper',
        '//aios/autil:time', '//aios/matchdoc'
    ]
)
