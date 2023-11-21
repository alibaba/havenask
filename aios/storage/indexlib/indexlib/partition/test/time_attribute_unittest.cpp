#include "indexlib/partition/test/time_attribute_unittest.h"

#include "indexlib/config/test/schema_maker.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/util/test/build_test_util.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::config;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, TimeAttributeTest);

TimeAttributeTest::TimeAttributeTest() {}

TimeAttributeTest::~TimeAttributeTest() {}

void TimeAttributeTest::TestTimeField()
{
    string field = "time:time;pk:string";
    string index = "pk:primarykey64:pk";
    string attribute = "time";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");

    string fullDocString = "cmd=add,time=12:13:14.326,pk=a,ts=1;"
                           "cmd=add,time=14:15:38.10,pk=b,ts=1;"
                           "cmd=add,time=09:10:10.1,pk=c,ts=1;"
                           "cmd=update_field,pk=c,time=09:00:00.0,ts=1:";

    string incDocString = "cmd=update_field,time=15:00:00,pk=a,ts=2;"
                          "cmd=add,time=23:59:59.999,pk=d,ts=2;";

    string patchIncStr = "cmd=update_field,time=0:0:0.0,pk=d,ts=3;";

    string rtDocString = "cmd=add,time=21:12:00,pk=e,ts=4;"
                         "cmd=update_field,time=13:00:00,pk=a,ts=5;"
                         "cmd=update_field,time=12:00:00,pk=b,ts=6;";

    string rtDocString2 = "cmd=update_field,time=12:00:00,pk=a,ts=7;"
                          "cmd=update_field,time=11:00:00,pk=b,ts=8;";

    PartitionStateMachine psm;
    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "pk:a", "time=12:13:14.326"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:c", "time=09:00:00.000"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDocString, "pk:a", "time=15:00:00.000"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:d", "time=23:59:59.999"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, patchIncStr, "pk:d", "time=00:00:00.000"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocString, "pk:b", "time=12:00:00.000"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:e", "time=21:12:00.000"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString2, "pk:a", "time=12:00:00.000"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:b", "time=11:00:00.000"));
    // redo
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, rtDocString, "pk:a", "time=12:00:00.000"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:b", "time=11:00:00.000"));
}

void TimeAttributeTest::TestTimestampField()
{
    string field = "time:timestamp;pk:string";
    string index = "pk:primarykey64:pk";
    string attribute = "time";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");

    string fullDocString = "cmd=add,time=2010-11-12 12:13:14.326,pk=a,ts=1;"
                           "cmd=add,time=2019-10-01 14:15:38.10,pk=b,ts=1;"
                           "cmd=add,time=2020-07-08 14:00:12.1,pk=c,ts=1;"
                           "cmd=update_field,time=2000-01-01 12:00:00,pk=c,ts=1";

    string incDocString = "cmd=update_field,time=1999-01-01 15:00:00,pk=a,ts=2;"
                          "cmd=add,time=2008-05-11 02:10:01.100,pk=d,ts=2;";

    string patchIncStr = "cmd=update_field,time=2018-05-11 02:10:01.100,pk=d,ts=3;";

    string rtDocString = "cmd=add,time=2020-02-11 21:12:00,pk=e,ts=4;"
                         "cmd=update_field,time=1998-01-01 15:00:00,pk=a,ts=5;"
                         "cmd=update_field,time=2031-11-12 12:00:00,pk=b,ts=6;";

    string rtDocString2 = "cmd=update_field,time=1980-01-01 12:00:00,pk=a,ts=7;"
                          "cmd=update_field,time=2010-11-01 11:00:10,pk=b,ts=8;";

    PartitionStateMachine psm;
    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "pk:a", "time=2010-11-12 12:13:14.326"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:c", "time=2000-01-01 12:00:00.000"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDocString, "pk:a", "time=1999-01-01 15:00:00.000"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:d", "time=2008-05-11 02:10:01.100"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, patchIncStr, "pk:d", "time=2018-05-11 02:10:01.100"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocString, "pk:b", "time=2031-11-12 12:00:00.000"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:e", "time=2020-02-11 21:12:00.000"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString2, "pk:a", "time=1980-01-01 12:00:00.000"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:b", "time=2010-11-01 11:00:10.000"));

    // redo
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, rtDocString, "pk:a", "time=1980-01-01 12:00:00.000"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:b", "time=2010-11-01 11:00:10.000"));
}

void TimeAttributeTest::TestDateField()
{
    string field = "date:date;pk:string";
    string index = "pk:primarykey64:pk";
    string attribute = "date";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");

    string fullDocString = "cmd=add,date=2012-06-17,pk=a,ts=1;"
                           "cmd=add,date=2020-11-21,pk=b,ts=1;"
                           "cmd=add,date=2000-01-01,pk=c,ts=1;"
                           "cmd=update_field,date=2010-01-01,pk=c,ts=1;";

    string incDocString = "cmd=update_field,date=2011-10-01,pk=a,ts=2;"
                          "cmd=add,date=2009-12-12,pk=d,ts=2;";

    string patchIncStr = "cmd=update_field,date=1997-10-01,pk=d,ts=3;";

    string rtDocString = "cmd=add,date=2030-11-11,pk=e,ts=4;"
                         "cmd=update_field,date=1988-11-11,pk=a,ts=5;"
                         "cmd=update_field,date=1989-11-01,pk=b,ts=6;";

    string rtDocString2 = "cmd=update_field,date=2088-11-11,pk=a,ts=7;"
                          "cmd=update_field,date=2089-11-01,pk=b,ts=8;";

    PartitionStateMachine psm;
    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "pk:a", "date=2012-06-17"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:c", "date=2010-01-01"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDocString, "pk:a", "date=2011-10-01"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:d", "date=2009-12-12"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, patchIncStr, "pk:d", "date=1997-10-01"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocString, "pk:b", "date=1989-11-01"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:e", "date=2030-11-11"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString2, "pk:a", "date=2088-11-11"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:b", "date=2089-11-01"));

    // redo
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, rtDocString, "pk:a", "date=2088-11-11"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:b", "date=2089-11-01"));
}

void TimeAttributeTest::TestNullTimeField()
{
    string field = "time:time;pk:string";
    string index = "pk:primarykey64:pk";
    string attribute = "time";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    SchemaMaker::EnableNullFields(schema, "time");
    string fullDocString = "cmd=add,time=12:13:14.326,pk=a,ts=1;"
                           "cmd=add,time=14:15:38.10,pk=b,ts=1;"
                           "cmd=add,time=09:10:10.1,pk=c,ts=1;"
                           "cmd=update_field,pk=c,time=__NULL__,ts=1:";

    string incDocString = "cmd=update_field,time=__NULL__,pk=a,ts=2;"
                          "cmd=add,time=23:59:59.999,pk=d,ts=2;";

    string patchIncStr = "cmd=update_field,time=__NULL__,pk=d,ts=3;";

    string rtDocString = "cmd=add,pk=e,ts=4;"
                         "cmd=update_field,time=13:00:00,pk=a,ts=5;"
                         "cmd=update_field,time=__NULL__,pk=b,ts=6;";

    string rtDocString2 = "cmd=update_field,time=__NULL__,pk=a,ts=7;"
                          "cmd=update_field,time=11:00:00,pk=b,ts=8;";

    PartitionStateMachine psm;
    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "pk:a", "time=12:13:14.326"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:c", "time=__NULL__"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDocString, "pk:a", "time=__NULL__"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:d", "time=23:59:59.999"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, patchIncStr, "pk:d", "time=__NULL__"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocString, "pk:b", "time=__NULL__"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:e", "time=__NULL__"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString2, "pk:a", "time=__NULL__"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:b", "time=11:00:00.000"));
    // redo
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, rtDocString, "pk:a", "time=__NULL__"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:b", "time=11:00:00.000"));
}

void TimeAttributeTest::TestNullTimestampField()
{
    string field = "time:timestamp;pk:string";
    string index = "pk:primarykey64:pk";
    string attribute = "time";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    SchemaMaker::EnableNullFields(schema, "time");
    string fullDocString = "cmd=add,pk=a,ts=1;"
                           "cmd=add,time=2019-10-01 14:15:38.10,pk=b,ts=1;"
                           "cmd=add,time=2020-07-08 14:00:12.1,pk=c,ts=1;"
                           "cmd=update_field,time=__NULL__,pk=c,ts=1";

    string incDocString = "cmd=update_field,time=1999-01-01 15:00:00,pk=a,ts=2;"
                          "cmd=add,time=__NULL__,pk=d,ts=2;";

    string patchIncStr = "cmd=update_field,time=2018-05-11 02:10:01.100,pk=d,ts=3;";

    string rtDocString = "cmd=add,pk=e,ts=4;"
                         "cmd=update_field,time=__NULL__,pk=a,ts=5;"
                         "cmd=update_field,time=2031-11-12 12:00:00,pk=b,ts=6;";

    string rtDocString2 = "cmd=update_field,time=1980-01-01 12:00:00,pk=a,ts=7;"
                          "cmd=update_field,time=__NULL__,pk=b,ts=8;";

    PartitionStateMachine psm;
    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "pk:a", "time=__NULL__"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:c", "time=__NULL__"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDocString, "pk:a", "time=1999-01-01 15:00:00.000"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:d", "time=__NULL__"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, patchIncStr, "pk:d", "time=2018-05-11 02:10:01.100"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocString, "pk:b", "time=2031-11-12 12:00:00.000"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:e", "time=__NULL__"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString2, "pk:a", "time=1980-01-01 12:00:00.000"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:b", "time=__NULL__"));

    // redo
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, rtDocString, "pk:a", "time=1980-01-01 12:00:00.000"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:b", "time=__NULL__"));
}

void TimeAttributeTest::TestNullDateField()
{
    string field = "date:date;pk:string";
    string index = "pk:primarykey64:pk";
    string attribute = "date";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    SchemaMaker::EnableNullFields(schema, "date");
    string fullDocString = "cmd=add,pk=a,ts=1;"
                           "cmd=add,date=2020-11-21,pk=b,ts=1;"
                           "cmd=add,date=2000-01-01,pk=c,ts=1;"
                           "cmd=update_field,date=__NULL__,pk=c,ts=1;";

    string incDocString = "cmd=update_field,date=2011-10-01,pk=a,ts=2;"
                          "cmd=add,date=2009-12-12,pk=d,ts=2;";

    string patchIncStr = "cmd=update_field,date=__NULL__,pk=d,ts=3;";

    string rtDocString = "cmd=add,pk=e,ts=4;"
                         "cmd=update_field,date=__NULL__,pk=a,ts=5;"
                         "cmd=update_field,date=1989-11-01,pk=b,ts=6;";

    string rtDocString2 = "cmd=update_field,date=2088-11-11,pk=a,ts=7;"
                          "cmd=update_field,date=__NULL__,pk=b,ts=8;";

    PartitionStateMachine psm;
    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "pk:a", "date=__NULL__"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:c", "date=__NULL__"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDocString, "pk:a", "date=2011-10-01"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:d", "date=2009-12-12"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, patchIncStr, "pk:d", "date=__NULL__"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocString, "pk:a", "date=__NULL__"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:e", "date=__NULL__"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString2, "pk:a", "date=2088-11-11"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:b", "date=__NULL__"));

    // redo
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, rtDocString, "pk:a", "date=2088-11-11"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:b", "date=__NULL__"));
}

void TimeAttributeTest::TestTimestampFieldWithDefaultTimeZoneDelta()
{
    string field = "time:timestamp;pk:string";
    string index = "pk:primarykey64:pk";
    string attribute = "time";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    FieldConfigPtr fieldConfig = schema->GetFieldConfig("time");
    fieldConfig->SetDefaultTimeZoneDelta(-3600 * 1000); // GMT+0100
    string fullDocString = "cmd=add,time=2010-11-12 12:11:14.326 +0100,pk=a,ts=1;"
                           "cmd=add,time=2019-10-01 14:15:38.10,pk=b,ts=1;"
                           "cmd=add,time=2020-07-08 15:00:12.1 +0200,pk=c,ts=1;"
                           "cmd=update_field,time=2000-01-01 12:00:00,pk=c,ts=1";

    string incDocString = "cmd=update_field,time=1999-01-01 02:00:00 -1200,pk=a,ts=2;"
                          "cmd=add,time=2008-05-11 02:10:01.100,pk=d,ts=2;";

    string patchIncStr = "cmd=update_field,time=2018-05-11 00:10:01.100 -0100,pk=d,ts=3;";

    string rtDocString = "cmd=add,time=2020-02-11 21:12:00 +0000,pk=e,ts=4;"
                         "cmd=update_field,time=1998-01-01 15:00:00,pk=a,ts=5;"
                         "cmd=update_field,time=2031-11-12 12:00:00,pk=b,ts=6;";

    string rtDocString2 = "cmd=update_field,time=1980-01-01 12:00:00,pk=a,ts=7;"
                          "cmd=update_field,time=2010-11-01 22:00:10 +1200,pk=b,ts=8;";

    PartitionStateMachine psm;
    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "pk:a", "time=2010-11-12 12:11:14.326"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:c", "time=2000-01-01 12:00:00.000"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDocString, "pk:a", "time=1999-01-01 15:00:00.000"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:d", "time=2008-05-11 02:10:01.100"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, patchIncStr, "pk:d", "time=2018-05-11 02:10:01.100"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocString, "pk:b", "time=2031-11-12 12:00:00.000"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:e", "time=2020-02-11 22:12:00.000"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString2, "pk:a", "time=1980-01-01 12:00:00.000"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:b", "time=2010-11-01 11:00:10.000"));

    // redo
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, rtDocString, "pk:a", "time=1980-01-01 12:00:00.000"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:b", "time=2010-11-01 11:00:10.000"));
}
}} // namespace indexlib::partition
