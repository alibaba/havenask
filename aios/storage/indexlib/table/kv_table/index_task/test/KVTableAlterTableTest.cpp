#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/Tablet.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/table/kv_table/test/KVTableTestHelper.h"
#include "indexlib/table/kv_table/test/KVTabletSchemaMaker.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace table {

class KVTableAlterTableTest : public TESTBASE
{
public:
    KVTableAlterTableTest();
    ~KVTableAlterTableTest();

public:
    void setUp() override
    {
        std::string field = "string1:string;string2:string";
        _tabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "string2", -1, _valueFormat);
        _docs1 = "cmd=add,string1=1,string2=abc,ts=1,locator=0:1;"
                 "cmd=add,string1=2,string2=cde,ts=2,locator=0:2;"
                 "cmd=add,string1=3,string2=fgh,ts=3,locator=0:3;"
                 "cmd=add,string1=321,string2=fgh,ts=3,locator=0:3;"
                 "cmd=add,string1=4,string2=ijk,ts=4,locator=0:4;";
        _docs2 = "cmd=add,string1=1,string2=ijk,ts=5,locator=0:5;"
                 "cmd=add,string1=12,string2=Acde,ts=6,locator=0:6;"
                 "cmd=add,string1=3,string2=Afgh,ts=7,locator=0:7;"
                 "cmd=add,string1=14,string2=Aijk,ts=8,locator=0:8;"
                 "cmd=add,string1=15,string2=15,ts=9,locator=0:9;"
                 "cmd=delete,string1=15,ts=10,locator=0:10;"
                 "cmd=delete,string1=2,ts=13,locator=0:11;";
    }

    void tearDown() override {}

protected:
    void DoTestSimpleAddField(const std::string& valueFormat);
    void DoTestSimpleDeleteField(const std::string& valueFormat);
    void DoTestMergeSegment(const std::string& valueFormat);
    void DoTestUpdate2Add(const std::string& valueFormat);
    void DoTestDuplicateAdd(const std::string& valueFormat);

    std::shared_ptr<config::TabletOptions> makeTableOptions();

private:
    std::string _docs1;
    std::string _docs2;
    std::shared_ptr<config::ITabletSchema> _tabletSchema;
    std::string _valueFormat;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.table, KVTableAlterTableTest);

KVTableAlterTableTest::KVTableAlterTableTest() {}

KVTableAlterTableTest::~KVTableAlterTableTest() {}

TEST_F(KVTableAlterTableTest, TestAlterTableWithAlterTable_1)
{
    // fix bug: 连续AlterTable schema1， schema2。中间没有build数据。commit出来的version的roadMap不对，缺少schema1信息。
    auto tabletOptions = makeTableOptions();
    // 1. build segments with schema 0
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(indexRoot, _tabletSchema, std::move(tabletOptions), INVALID_VERSIONID).IsOK());

    string docs1 = "cmd=add,string1=1,string2=abc,ts=1,locator=0:1;"
                   "cmd=add,string1=2,string2=cde,ts=2,locator=0:2;";

    ASSERT_TRUE(helper.BuildSegment(docs1).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=abc"));

    auto tablet = helper.GetTablet();
    ASSERT_EQ(0, tablet->GetTabletReader()->GetSchema()->GetSchemaId());
    ASSERT_EQ(0, tablet->GetTabletSchema()->GetSchemaId());

    // 2. alterTablet with schema 1, delete string2 & add new fields
    std::string field = "string1:string;value:uint64;str:string";
    auto newTabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "value;str", -1, _valueFormat);
    newTabletSchema->GetFieldConfig("value")->SetDefaultValue("10");
    newTabletSchema->GetFieldConfig("str")->SetDefaultValue("default");
    newTabletSchema->SetSchemaId(1);
    TableTestHelper::StepInfo firstAlterTableStep;
    Status status;
    // TableTestHelper::StepInfo secondAlterTableStep;
    std::tie(status, firstAlterTableStep) = helper.StepAlterTable(firstAlterTableStep, newTabletSchema, {});
    ASSERT_TRUE(status.IsOK());
    // 3. alterTablet with schema2, deplicated add string2 & change type of value
    field = "string1:string;string2:string;str:string";
    newTabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "string2;str", -1, _valueFormat);
    newTabletSchema->GetFieldConfig("str")->SetDefaultValue("default");
    newTabletSchema->GetFieldConfig("string2")->SetDefaultValue("pujian");
    newTabletSchema->SetSchemaId(2);
    ASSERT_TRUE(helper.AlterTable(newTabletSchema, {}).IsOK());
    auto version = helper.GetCurrentVersion();
    std::cerr << ToJsonString(version) << std::endl;
    ASSERT_TRUE(version.IsInSchemaVersionRoadMap(0));
    ASSERT_TRUE(version.IsInSchemaVersionRoadMap(1));
    ASSERT_TRUE(version.IsInSchemaVersionRoadMap(2));
}

TEST_F(KVTableAlterTableTest, TestAlterTableWithAlterTable)
{
    // fix bug：连续AlterTable，更新schema1
    // 后build了几篇doc，没有dump，更新schema2失败。原因是加载schema1的segment时从tabletData中找不到schema1
    //错误信息如下：
    // AdapterIgnoreFieldCalculator Init:28 [get tablet schema[1] fail from tabletData]
    auto tabletOptions = makeTableOptions();
    // 1. build segments with schema 0
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(indexRoot, _tabletSchema, std::move(tabletOptions), INVALID_VERSIONID).IsOK());

    string docs1 = "cmd=add,string1=1,string2=abc,ts=1,locator=0:1;"
                   "cmd=add,string1=2,string2=cde,ts=2,locator=0:2;";

    ASSERT_TRUE(helper.BuildSegment(docs1).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=abc"));

    auto tablet = helper.GetTablet();
    ASSERT_EQ(0, tablet->GetTabletReader()->GetSchema()->GetSchemaId());
    ASSERT_EQ(0, tablet->GetTabletSchema()->GetSchemaId());

    // 2. alterTablet with schema 1, delete string2 & add new fields
    std::string field = "string1:string;value:uint64;str:string";
    auto newTabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "value;str", -1, _valueFormat);
    newTabletSchema->GetFieldConfig("value")->SetDefaultValue("10");
    newTabletSchema->GetFieldConfig("str")->SetDefaultValue("default");
    newTabletSchema->SetSchemaId(1);
    TableTestHelper::StepInfo firstAlterTableStep;
    Status status;
    // TableTestHelper::StepInfo secondAlterTableStep;
    std::tie(status, firstAlterTableStep) = helper.StepAlterTable(firstAlterTableStep, newTabletSchema, {});
    ASSERT_TRUE(status.IsOK());
    // 3. build with schema 1
    string docs2 = "cmd=add,string1=3,string2=ijk,value=4,str=124,ts=12,locator=0:3;"
                   "cmd=add,string1=4,string2=12,value=8,str=abc,ts=13,locator=0:4;";
    ASSERT_TRUE(helper.Build(docs2).IsOK());

    // 4. alterTablet with schema2, deplicated add string2 & change type of value
    // 4.1 first delete origin value
    field = "string1:string;string2:string;str:string";
    newTabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "string2;str", -1, _valueFormat);
    newTabletSchema->GetFieldConfig("str")->SetDefaultValue("default");
    newTabletSchema->GetFieldConfig("string2")->SetDefaultValue("pujian");
    newTabletSchema->SetSchemaId(2);
    ASSERT_TRUE(helper.AlterTable(newTabletSchema, {}).IsOK());

    // 4.2 readd new value
    field = "string1:string;string2:string;str:string;value:uint32";
    newTabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "string2;str;value", -1, _valueFormat);
    newTabletSchema->GetFieldConfig("value")->SetDefaultValue("20");
    newTabletSchema->GetFieldConfig("str")->SetDefaultValue("default");
    newTabletSchema->GetFieldConfig("string2")->SetDefaultValue("pujian");
    newTabletSchema->SetSchemaId(3);
    ASSERT_TRUE(helper.AlterTable(newTabletSchema, {}).IsOK());

    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=pujian,value=20,str=default"));
    ASSERT_TRUE(helper.Query("kv", "string1", "3", "string2=pujian,value=20,str=124"));

    string docs3 = "cmd=add,string1=3,string2=ijk,value=4,str=124,ts=12,locator=0:5;";
    ASSERT_TRUE(helper.BuildSegment(docs3).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "3", "string2=ijk,value=4,str=124"));

    // 5. merge with new schema 2
    ASSERT_TRUE(helper.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(true)).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=pujian,value=20,str=default"));
    ASSERT_TRUE(helper.Query("kv", "string1", "3", "string2=ijk,value=4,str=124"));
    ASSERT_TRUE(helper.Query("kv", "string1", "4", "string2=pujian,value=20,str=abc"));

    string docs4 = "cmd=add,string1=4,string2=12,value=8,str=abc,ts=13,locator=0:6;";
    ASSERT_TRUE(helper.Build(docs4).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "4", "string2=12,value=8,str=abc"));

    vector<schemaid_t> expectSchemaRoadMap = {0, 1, 2, 3};
    ASSERT_EQ(expectSchemaRoadMap, helper.GetCurrentVersion().GetSchemaVersionRoadMap());
}

TEST_F(KVTableAlterTableTest, TestSimpleAddField) { DoTestSimpleAddField("default"); }

TEST_F(KVTableAlterTableTest, TestSimpleAddFieldWithPlainFormat) { DoTestSimpleAddField("plain"); }

TEST_F(KVTableAlterTableTest, TestSimpleDeleteField) { DoTestSimpleDeleteField("default"); }

TEST_F(KVTableAlterTableTest, TestSimpleDeleteFieldWithPlainFormat) { DoTestSimpleDeleteField("plain"); }

TEST_F(KVTableAlterTableTest, TestMergeSegment) { DoTestMergeSegment("default"); }

TEST_F(KVTableAlterTableTest, TestMergeSegmentWithPlainFormat) { DoTestMergeSegment("plain"); }

TEST_F(KVTableAlterTableTest, TestUpdate2Add) { DoTestUpdate2Add("default"); }

TEST_F(KVTableAlterTableTest, TestUpdate2AddWithPlainFormat) { DoTestUpdate2Add("plain"); }

TEST_F(KVTableAlterTableTest, TestDuplicateAdd) { DoTestDuplicateAdd("default"); }

TEST_F(KVTableAlterTableTest, TestDuplicateAddWithPlainFormat) { DoTestDuplicateAdd("plain"); }

TEST_F(KVTableAlterTableTest, TestNotSupportAlterTableForImpactValue)
{
    std::string field = "string1:string;string2:string";
    auto tabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "string2", -1, "impact");
    auto tabletOptions = makeTableOptions();
    // 1. build segments for schema 0
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(indexRoot, tabletSchema, std::move(tabletOptions), INVALID_VERSIONID).IsOK());
    ASSERT_TRUE(helper.BuildSegment(_docs1).IsOK());
    ASSERT_TRUE(helper.BuildSegment(_docs2).IsOK());

    field = "string1:string;string2:string;value:uint64;str:string";
    auto newTabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "string2;value;str", -1, "impact");
    newTabletSchema->GetFieldConfig("value")->SetDefaultValue("10");
    newTabletSchema->GetFieldConfig("str")->SetDefaultValue("default");
    newTabletSchema->SetSchemaId(1);
    ASSERT_FALSE(helper.AlterTable(newTabletSchema, {}).IsOK());
}

TEST_F(KVTableAlterTableTest, TestAlterTableDisableSimpleValue)
{
    // 0. current schema is simple value, set schemaId != 0, make disable simple value effective
    std::string field = "string1:string;string2:string;value:uint64;str:string";
    auto tabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "value");
    tabletSchema->SetSchemaId(1);
    string schemaStr;
    ASSERT_TRUE(tabletSchema->Serialize(false, &schemaStr));
    tabletSchema.reset(framework::TabletSchemaLoader::LoadSchema(schemaStr).release());
    ASSERT_TRUE(tabletSchema);
    auto tabletOptions = makeTableOptions();
    KVTableTestHelper helper;
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    ASSERT_TRUE(helper.Open(indexRoot, tabletSchema, std::move(tabletOptions), INVALID_VERSIONID).IsOK());

    // 1. alter table from disabled simple value schema
    auto newTabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "string2;value;str");
    newTabletSchema->SetSchemaId(2);
    ASSERT_TRUE(helper.AlterTable(newTabletSchema, {}).IsOK());
    ASSERT_TRUE(helper.BuildSegment(_docs1).IsOK());
    ASSERT_TRUE(helper.BuildSegment(_docs2).IsOK());

    // 2. target schema is simple value, alterTable to disabled simple value
    field = "string1:string;string2:string;value:uint64";
    newTabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "value");
    newTabletSchema->SetSchemaId(3);
    ASSERT_TRUE(newTabletSchema->Serialize(false, &schemaStr));
    newTabletSchema.reset(framework::TabletSchemaLoader::LoadSchema(schemaStr).release());
    ASSERT_TRUE(newTabletSchema);

    ASSERT_TRUE(helper.AlterTable(newTabletSchema, {}).IsOK());

    ASSERT_TRUE(helper.Query("kv", "string1", "4", "value=0"));

    string docs3 = "cmd=add,string1=1,value=31,ts=12,locator=0:12;";
    ASSERT_TRUE(helper.Build(docs3).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "value=31"));

    // 3. add new field str
    field = "string1:string;string2:string;value:uint64;str:string";
    newTabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "value;str");
    newTabletSchema->GetFieldConfig("str")->SetDefaultValue("pujian");
    newTabletSchema->SetSchemaId(4);

    ASSERT_TRUE(helper.AlterTable(newTabletSchema, {}).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "value=31,str=pujian"));
    ASSERT_TRUE(helper.Query("kv", "string1", "4", "value=0,str=pujian"));

    string docs4 = "cmd=add,string1=1,value=32,str=hello,ts=12,locator=0:13;";
    ASSERT_TRUE(helper.Build(docs4).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "value=32,str=hello"));
}

TEST_F(KVTableAlterTableTest, TestAlterTableWithNoVarlenAttributes)
{
    std::string field = "string1:string;string2:string;value1:uint64;value2:uint32;value3:int16";
    auto tabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "value1;value2");
    auto tabletOptions = makeTableOptions();
    // 1. build segments for schema 0
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(indexRoot, tabletSchema, std::move(tabletOptions), INVALID_VERSIONID).IsOK());
    std::string docs1 = "cmd=add,string1=1,value1=1,value2=2,ts=1,locator=0:1;"
                        "cmd=add,string1=2,value1=21,value2=22,ts=2,locator=0:2;"
                        "cmd=add,string1=3,value1=31,value2=32,ts=3,locator=0:3;";
    ASSERT_TRUE(helper.BuildSegment(docs1).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "2", "value1=21,value2=22"));

    // 2. add var len attribute from all fix length attributes
    auto newTabletSchema1 = table::KVTabletSchemaMaker::Make(field, "string1", "value1;value2;value3;string2");
    newTabletSchema1->SetSchemaId(1);
    ASSERT_TRUE(helper.AlterTable(newTabletSchema1, {}).IsOK());

    std::string docs2 = "cmd=add,string1=4,value1=41,value2=42,value3=43,string2=abc,ts=4,locator=0:4;"
                        "cmd=add,string1=5,value1=51,value2=52,value3=53,string2=def,ts=5,locator=0:5;"
                        "cmd=add,string1=6,value1=61,value2=62,value3=63,string2=hello,ts=6,locator=0:6;";
    ASSERT_TRUE(helper.BuildSegment(docs2).IsOK());
    ASSERT_TRUE(helper.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(true)).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "2", "value1=21,value2=22,value3=0"));
    ASSERT_TRUE(helper.Query("kv", "string1", "4", "value1=41,value2=42,value3=43,string2=abc"));

    // 3. remove var len attributes
    auto newTabletSchema2 = table::KVTabletSchemaMaker::Make(field, "string1", "value1;value3");
    newTabletSchema2->SetSchemaId(2);
    ASSERT_TRUE(helper.AlterTable(newTabletSchema2, {}).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "2", "value1=21,value3=0"));
    ASSERT_TRUE(helper.Query("kv", "string1", "4", "value1=41,value3=43"));
    ASSERT_FALSE(helper.Query("kv", "string1", "4", "value1=41,value2=42,value3=43,string2=abc"));

    auto currentVersion = helper.GetCurrentVersion();
    ASSERT_EQ(2, currentVersion.GetIndexTaskHistory().GetTaskLogs("alter_table@alter_table").size());
    ASSERT_EQ(1, currentVersion.GetIndexTaskHistory().GetTaskLogs("merge@_default_").size());
}

void KVTableAlterTableTest::DoTestSimpleAddField(const std::string& valueFormat)
{
    _valueFormat = valueFormat;
    tearDown();
    setUp();
    auto tabletOptions = makeTableOptions();

    // 1. build segments for schema 0
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(indexRoot, _tabletSchema, std::move(tabletOptions), INVALID_VERSIONID).IsOK());
    ASSERT_TRUE(helper.BuildSegment(_docs1).IsOK());
    ASSERT_TRUE(helper.BuildSegment(_docs2).IsOK());
    auto tablet = helper.GetTablet();
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=ijk"));
    ASSERT_EQ(0, tablet->GetTabletReader()->GetSchema()->GetSchemaId());
    ASSERT_EQ(0, tablet->GetTabletSchema()->GetSchemaId());

    // 2. alterTable with schema 1
    std::string field = "string1:string;string2:string;value:uint64;str:string";
    auto newTabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "string2;value;str", -1, _valueFormat);
    newTabletSchema->GetFieldConfig("value")->SetDefaultValue("10");
    newTabletSchema->GetFieldConfig("str")->SetDefaultValue("default");
    newTabletSchema->SetSchemaId(1);
    auto oldVersion = helper.GetCurrentVersion();
    ASSERT_EQ(0, oldVersion.GetReadSchemaId());
    for (auto [segId, schemaId] : oldVersion) {
        ASSERT_EQ(0, schemaId);
    }

    ASSERT_TRUE(helper.AlterTable(newTabletSchema, {}).IsOK());
    auto currentVersion = helper.GetCurrentVersion();
    ASSERT_EQ(1, currentVersion.GetSchemaId());
    ASSERT_EQ(1, currentVersion.GetReadSchemaId());
    for (auto [segId, schemaId] : currentVersion) {
        ASSERT_EQ(0, schemaId);
    }
    ASSERT_EQ(1, tablet->GetTabletReader()->GetSchema()->GetSchemaId());
    ASSERT_EQ(1, tablet->GetTabletSchema()->GetSchemaId());

    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=ijk,value=10,str=default"));
    string docs3 = "cmd=add,string1=1,string2=ijk,str=124,ts=12,locator=0:12;"
                   "cmd=add,string1=12,string2=12,value=8,str=abc,ts=13,locator=0:13;"
                   "cmd=add,string1=3,string2=Afgh,ts=14,value=14,locator=0:14;"
                   "cmd=add,string1=15,string2=15,str=im15,ts=15,locator=0:15;"
                   "cmd=delete,string1=321,ts=16,locator=0:16;";

    // 3. build with new schema
    ASSERT_TRUE(helper.Build(docs3).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=ijk,value=10,str=124"));
    ASSERT_TRUE(helper.Query("kv", "string1", "12", "string2=12,value=8,str=abc"));
    ASSERT_TRUE(helper.Query("kv", "string1", "3", "string2=Afgh,value=14"));
    ASSERT_TRUE(helper.Query("kv", "string1", "15", "string2=15,str=im15,value=10"));
    ASSERT_TRUE(helper.Query("kv", "string1", "321", ""));
}

void KVTableAlterTableTest::DoTestSimpleDeleteField(const std::string& valueFormat)
{
    _valueFormat = valueFormat;
    tearDown();
    setUp();
    auto tabletOptions = makeTableOptions();

    std::string field = "string1:string;string2:string;value:uint64;str:string";
    auto tabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "string2;value;str", -1, _valueFormat);
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(indexRoot, tabletSchema, std::move(tabletOptions), INVALID_VERSIONID).IsOK());

    // 1. build segment with schema 0
    string docs = "cmd=add,string1=1,string2=ijk,str=124,ts=12,locator=0:12;"
                  "cmd=add,string1=12,string2=12,value=8,str=abc,ts=13,locator=0:13;"
                  "cmd=add,string1=3,string2=Afgh,ts=14,value=14,locator=0:14;"
                  "cmd=add,string1=15,string2=15,str=im15,ts=15,locator=0:15;";
    ASSERT_TRUE(helper.BuildSegment(docs).IsOK());
    auto tablet = helper.GetTablet();
    ASSERT_EQ(0, tablet->GetTabletReader()->GetSchema()->GetSchemaId());
    ASSERT_EQ(0, tablet->GetTabletSchema()->GetSchemaId());
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=ijk,str=124,value=0"));

    // 2. alterTable with schema 1 with deleted fields, remove value & str
    field = "string1:string;string2:string";
    auto newTabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "string2", -1, _valueFormat);
    newTabletSchema->SetSchemaId(1);
    ASSERT_TRUE(helper.AlterTable(newTabletSchema, {}).IsOK());
    ASSERT_FALSE(helper.Query("kv", "string1", "1", "string2=ijk,str=124,value=0"));
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=ijk"));

    ASSERT_EQ(1, tablet->GetTabletReader()->GetSchema()->GetSchemaId());
    ASSERT_EQ(1, tablet->GetTabletSchema()->GetSchemaId());

    // 3. build with new schema 1
    string docs2 = "cmd=add,string1=17,string2=ijk,str=124,ts=17,locator=0:17;";
    ASSERT_TRUE(helper.Build(docs2).IsOK());
    ASSERT_FALSE(helper.Query("kv", "string1", "17", "string2=ijk,str=124"));
    ASSERT_TRUE(helper.Query("kv", "string1", "17", "string2=ijk"));
}

void KVTableAlterTableTest::DoTestMergeSegment(const std::string& valueFormat)
{
    _valueFormat = valueFormat;
    tearDown();
    setUp();

    auto tabletOptions = makeTableOptions();
    // 1. build segments with schema 0
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(indexRoot, _tabletSchema, std::move(tabletOptions), INVALID_VERSIONID).IsOK());
    ASSERT_TRUE(helper.BuildSegment(_docs1).IsOK());
    ASSERT_TRUE(helper.BuildSegment(_docs2).IsOK());
    auto tablet = helper.GetTablet();
    ASSERT_EQ(0, tablet->GetTabletReader()->GetSchema()->GetSchemaId());
    ASSERT_EQ(0, tablet->GetTabletSchema()->GetSchemaId());

    // 2. alterTable with schema 1
    std::string field = "string1:string;string2:string;value:uint64;str:string";
    auto newTabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "string2;value;str", -1, _valueFormat);
    newTabletSchema->GetFieldConfig("value")->SetDefaultValue("10");
    newTabletSchema->GetFieldConfig("str")->SetDefaultValue("default");
    newTabletSchema->SetSchemaId(1);
    auto oldVersion = helper.GetCurrentVersion();
    ASSERT_EQ(0, oldVersion.GetReadSchemaId());
    for (auto [segId, schemaId] : oldVersion) {
        ASSERT_EQ(0, schemaId);
    }
    ASSERT_TRUE(helper.AlterTable(newTabletSchema, {}).IsOK());
    ASSERT_EQ(1, tablet->GetTabletReader()->GetSchema()->GetSchemaId());
    ASSERT_EQ(1, tablet->GetTabletSchema()->GetSchemaId());

    // 3. build with new schema 1
    string docs3 = "cmd=add,string1=1,string2=ijk,str=124,ts=12,locator=0:12;"
                   "cmd=add,string1=12,string2=12,value=8,str=abc,ts=13,locator=0:13;"
                   "cmd=add,string1=3,string2=Afgh,ts=14,value=14,locator=0:14;"
                   "cmd=add,string1=15,string2=15,str=im15,ts=15,locator=0:15;"
                   "cmd=delete,string1=321,ts=16,locator=0:16;";
    ASSERT_TRUE(helper.BuildSegment(docs3).IsOK());
    auto currentVersion = helper.GetCurrentVersion();
    ASSERT_EQ(1, currentVersion.GetSchemaId());
    ASSERT_EQ(1, currentVersion.GetReadSchemaId());
    size_t idx = 0;
    for (auto [segId, schemaId] : currentVersion) {
        if (idx == currentVersion.GetSegmentCount() - 1) {
            ASSERT_EQ(1, schemaId);
        } else {
            ASSERT_EQ(0, schemaId);
        }
        idx++;
    }

    // 4. merge with new schema 1
    ASSERT_TRUE(helper.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(true)).IsOK());
    const auto& mergeVersion = helper.GetCurrentVersion();
    ASSERT_EQ(1, mergeVersion.GetSchemaId());
    ASSERT_EQ(1, mergeVersion.GetReadSchemaId());
    for (auto [segId, schemaId] : mergeVersion) {
        ASSERT_EQ(1, schemaId);
    }
    ASSERT_EQ(1, tablet->GetTabletReader()->GetSchema()->GetSchemaId());
    ASSERT_EQ(1, tablet->GetTabletSchema()->GetSchemaId());

    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=ijk,str=124,value=10"));
    ASSERT_TRUE(helper.Query("kv", "string1", "14", "string2=Aijk,str=default,value=10"));
}

void KVTableAlterTableTest::DoTestUpdate2Add(const std::string& valueFormat)
{
    _valueFormat = valueFormat;
    tearDown();
    setUp();

    auto tabletOptions = makeTableOptions();
    // 1. build segments with schema 0
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(indexRoot, _tabletSchema, std::move(tabletOptions), INVALID_VERSIONID).IsOK());
    ASSERT_TRUE(helper.BuildSegment(_docs1).IsOK());
    ASSERT_TRUE(helper.BuildSegment(_docs2).IsOK());

    auto tablet = helper.GetTablet();
    ASSERT_EQ(0, tablet->GetTabletReader()->GetSchema()->GetSchemaId());
    ASSERT_EQ(0, tablet->GetTabletSchema()->GetSchemaId());

    // 2. alterTablet with schema 1
    std::string field = "string1:string;string2:string;value:uint64;str:string";
    auto newTabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "string2;value;str", -1, _valueFormat);
    newTabletSchema->GetFieldConfig("value")->SetDefaultValue("10");
    newTabletSchema->GetFieldConfig("str")->SetDefaultValue("default");
    newTabletSchema->SetSchemaId(1);
    ASSERT_TRUE(helper.AlterTable(newTabletSchema, {}).IsOK());

    // 3. build with schema 1
    string docs3 = "cmd=add,string1=1,string2=ijk,str=124,ts=12,locator=0:12;"
                   "cmd=add,string1=12,string2=12,value=8,str=abc,ts=13,locator=0:13;"
                   "cmd=add,string1=3,string2=Afgh,ts=14,value=14,locator=0:14;"
                   "cmd=add,string1=15,string2=15,str=im15,ts=15,locator=0:15;"
                   "cmd=update_field,string1=1,string2=hello,str=world,ts=16,locator=0:16;" // update building segment
                   "cmd=update_field,string1=4,string2=4ijk,value=20,ts=17,locator=0:17;";  // update built segment
    ASSERT_TRUE(helper.Build(docs3).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=hello,value=10,str=world"));
    ASSERT_TRUE(helper.Query("kv", "string1", "4", "string2=4ijk,value=20,str=default"));

    ASSERT_EQ(1, tablet->GetTabletReader()->GetSchema()->GetSchemaId());
    ASSERT_EQ(1, tablet->GetTabletSchema()->GetSchemaId());
}

void KVTableAlterTableTest::DoTestDuplicateAdd(const std::string& valueFormat)
{
    _valueFormat = valueFormat;
    tearDown();
    setUp();

    auto tabletOptions = makeTableOptions();
    // 1. build segments with schema 0
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(indexRoot, _tabletSchema, std::move(tabletOptions), INVALID_VERSIONID).IsOK());

    string docs1 = "cmd=add,string1=1,string2=abc,ts=1,locator=0:1;"
                   "cmd=add,string1=2,string2=cde,ts=2,locator=0:2;";

    ASSERT_TRUE(helper.BuildSegment(docs1).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=abc"));

    auto tablet = helper.GetTablet();
    ASSERT_EQ(0, tablet->GetTabletReader()->GetSchema()->GetSchemaId());
    ASSERT_EQ(0, tablet->GetTabletSchema()->GetSchemaId());

    // 2. alterTablet with schema 1, delete string2 & add new fields
    std::string field = "string1:string;value:uint64;str:string";
    auto newTabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "value;str", -1, _valueFormat);
    newTabletSchema->GetFieldConfig("value")->SetDefaultValue("10");
    newTabletSchema->GetFieldConfig("str")->SetDefaultValue("default");
    newTabletSchema->SetSchemaId(1);
    ASSERT_TRUE(helper.AlterTable(newTabletSchema, {}).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "value=10,str=default"));
    ASSERT_FALSE(helper.Query("kv", "string1", "1", "string2=abc"));

    // 3. build with schema 1
    string docs2 = "cmd=add,string1=3,string2=ijk,value=4,str=124,ts=12,locator=0:3;"
                   "cmd=add,string1=4,string2=12,value=8,str=abc,ts=13,locator=0:4;";
    ASSERT_TRUE(helper.BuildSegment(docs2).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "3", "value=4,str=124"));

    // 4. alterTablet with schema2, deplicated add string2 & change type of value
    // 4.1 first delete origin value
    field = "string1:string;string2:string;str:string";
    newTabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "string2;str", -1, _valueFormat);
    newTabletSchema->GetFieldConfig("str")->SetDefaultValue("default");
    newTabletSchema->GetFieldConfig("string2")->SetDefaultValue("pujian");
    newTabletSchema->SetSchemaId(2);
    ASSERT_TRUE(helper.AlterTable(newTabletSchema, {}).IsOK());

    // 4.2 readd new value
    field = "string1:string;string2:string;str:string;value:uint32";
    newTabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "string2;str;value", -1, _valueFormat);
    newTabletSchema->GetFieldConfig("value")->SetDefaultValue("20");
    newTabletSchema->GetFieldConfig("str")->SetDefaultValue("default");
    newTabletSchema->GetFieldConfig("string2")->SetDefaultValue("pujian");
    newTabletSchema->SetSchemaId(3);
    ASSERT_TRUE(helper.AlterTable(newTabletSchema, {}).IsOK());

    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=pujian,value=20,str=default"));
    ASSERT_TRUE(helper.Query("kv", "string1", "3", "string2=pujian,value=20,str=124"));

    string docs3 = "cmd=add,string1=3,string2=ijk,value=4,str=124,ts=12,locator=0:5;";
    ASSERT_TRUE(helper.BuildSegment(docs3).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "3", "string2=ijk,value=4,str=124"));

    // 5. merge with new schema 2
    ASSERT_TRUE(helper.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(true)).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=pujian,value=20,str=default"));
    ASSERT_TRUE(helper.Query("kv", "string1", "3", "string2=ijk,value=4,str=124"));
    ASSERT_TRUE(helper.Query("kv", "string1", "4", "string2=pujian,value=20,str=abc"));

    string docs4 = "cmd=add,string1=4,string2=12,value=8,str=abc,ts=13,locator=0:6;";
    ASSERT_TRUE(helper.Build(docs4).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "4", "string2=12,value=8,str=abc"));

    vector<schemaid_t> expectSchemaRoadMap = {0, 1, 2, 3};
    ASSERT_EQ(expectSchemaRoadMap, helper.GetCurrentVersion().GetSchemaVersionRoadMap());
}

std::shared_ptr<config::TabletOptions> KVTableAlterTableTest::makeTableOptions()
{
    std::string jsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "level_num" : 3
        }
    }
    } )";

    std::shared_ptr<config::TabletOptions> tabletOptions = std::make_shared<config::TabletOptions>();
    FromJsonString(*tabletOptions, jsonStr);
    tabletOptions->SetIsOnline(true);
    tabletOptions->SetIsLeader(true);
    tabletOptions->SetFlushLocal(false);
    tabletOptions->SetFlushRemote(true);
    tabletOptions->TEST_GetOfflineConfig().TEST_GetMergeConfig().TEST_SetMergeStrategyStr("shard_based");
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetBuildingMemoryLimit(1 * 1024 * 1024);
    return tabletOptions;
}

}} // namespace indexlibv2::table
