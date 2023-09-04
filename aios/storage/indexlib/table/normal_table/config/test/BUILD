load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='normal_table_config_unittest',
    srcs=['SegmentGroupConfigTest.cpp'],
    data=glob(['testdata/*']),
    deps=[
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/framework:tablet_schema_loader',
        '//aios/storage/indexlib/table/normal_table:Common',
        '//aios/storage/indexlib/table/normal_table:NormalTableFactory',
        '//aios/storage/indexlib/table/normal_table/config:SegmentGroupConfig',
        '//aios/unittest_framework'
    ]
)
