#include "indexlib/table/kv_table/KVTabletDocIterator.h"

#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/document/raw_document/DefaultRawDocument.h"
#include "indexlib/framework/Tablet.h"
#include "indexlib/framework/test/TabletTestAgent.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/KVIndexPreference.h"
#include "indexlib/table/kv_table/test/KVTableTestHelper.h"
#include "indexlib/table/kv_table/test/KVTabletSchemaMaker.h"
#include "unittest/unittest.h"

namespace indexlibv2 { namespace table {

class KVTabletDocIteratorTest : public TESTBASE
{
public:
    struct SimpleValueResultData {
        std::string key;
        std::string value;
        uint32_t timestamp;
    };

public:
    KVTabletDocIteratorTest() {}
    ~KVTabletDocIteratorTest() {}

public:
private:
    void InnerTest(bool isSingleShard, bool needMerge, bool hasTTL, bool needStorePKValue);
    void PrepareTabletOption();
    void PrepareSingleShardOptions();
    void PrepareMultiShardOptions();
    void PrepareSchema(const std::string& fields, bool hasTTL = true, bool needStorePKValue = false);
    void PrepareIterator(KVTabletDocIterator& kvtabletDocIterator, const std::vector<std::string>& docStrings,
                         table::KVTableTestHelper& helper, const std::pair<uint32_t, uint32_t> rangeInRatio,
                         Status& status, bool needMerge = false,
                         const std::optional<std::vector<std::string>>& fields = std::nullopt);
    void checkResult(KVTabletDocIterator& kvtabletDocIterator, const std::vector<SimpleValueResultData>& expectResults,
                     bool hasTTL = true, bool needStorePKValue = false);
    void testPackValue(const std::string& valueFormat);
    void testAlterTablet(const std::string& valueFormat);

private:
    std::string _valueFormat;
    std::shared_ptr<config::TabletOptions> _tabletOptions = std::make_shared<config::TabletOptions>();
    std::shared_ptr<config::ITabletSchema> _tabletSchema;
    std::map<std::string, std::string> _params;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlibv2.table, KVTabletDocIteratorTest);

void KVTabletDocIteratorTest::PrepareTabletOption()
{
    _tabletOptions->SetIsOnline(true);
    _tabletOptions->SetIsLeader(true);
    _tabletOptions->SetFlushLocal(false);
    _tabletOptions->SetFlushRemote(true);
    _tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetBuildingMemoryLimit(2 * 1024 * 1024);
}

void KVTabletDocIteratorTest::PrepareSingleShardOptions()
{
    std::string jsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "sharding_column_num" : 1,
            "level_num" : 3
        }
    }
    } )";
    FromJsonString(*_tabletOptions, jsonStr);
    PrepareTabletOption();
}

void KVTabletDocIteratorTest::PrepareMultiShardOptions()
{
    std::string jsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "sharding_column_num" : 2,
            "level_num" : 3
        }
    }
    } )";
    FromJsonString(*_tabletOptions, jsonStr);
    PrepareTabletOption();
}

void KVTabletDocIteratorTest::PrepareSchema(const std::string& fields, bool hasTTL, bool needStorePKValue)
{
    _tabletSchema = table::KVTabletSchemaMaker::Make(fields, "pkey", "value", -1, _valueFormat, needStorePKValue);
    if (hasTTL) {
        auto indexConfigs = _tabletSchema->GetIndexConfigs();
        ASSERT_FALSE(indexConfigs.empty());
        for (auto& indexConfig : indexConfigs) {
            auto kvConfig = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfig);
            kvConfig->SetTTL(3);
        }
    }
    _params["reader_timestamp"] = "0";
}

void KVTabletDocIteratorTest::PrepareIterator(KVTabletDocIterator& kvtabletDocIterator,
                                              const std::vector<std::string>& docStrings,
                                              table::KVTableTestHelper& helper,
                                              const std::pair<uint32_t, uint32_t> rangeInRatio, Status& status,
                                              bool needMerge, const std::optional<std::vector<std::string>>& fields)
{
    fslib::fs::FileSystem::remove(GET_TEMP_DATA_PATH());
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    ASSERT_TRUE(helper.Open(indexRoot, _tabletSchema, _tabletOptions).IsOK());
    for (const std::string& doc : docStrings) {
        ASSERT_TRUE(helper.BuildSegment(doc).IsOK());
    }
    if (needMerge) {
        TableTestHelper::StepInfo info;
        info.specifyEpochId = "12345";
        ASSERT_TRUE(helper.Merge(true, &info).IsOK());
    }
    std::vector<std::string> fieldNames;
    if (fields) {
        fieldNames = fields.value();
    } else {
        for (auto fieldConfig : _tabletSchema->GetFieldConfigs()) {
            fieldNames.push_back(fieldConfig->GetFieldName());
        }
    }

    status = kvtabletDocIterator.Init(helper.GetTablet()->GetTabletData(), rangeInRatio, nullptr, fieldNames, _params);
}

void KVTabletDocIteratorTest::checkResult(KVTabletDocIterator& kvtabletDocIterator,
                                          const std::vector<SimpleValueResultData>& expectResults, bool hasTTL,
                                          bool needStorePKValue)
{
    indexlibv2::document::DefaultRawDocument doc;
    std::string checkpoint;
    framework::Locator::DocInfo docInfo(0, 0, 0, 0);
    auto kvConfig = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(_tabletSchema->GetIndexConfigs()[0]);
    auto keyType = kvConfig->GetFieldConfig()->GetFieldType();
    for (auto& expectResult : expectResults) {
        ASSERT_TRUE(kvtabletDocIterator.HasNext());
        ASSERT_TRUE(kvtabletDocIterator.Next(&doc, &checkpoint, &docInfo).IsOK());
        ASSERT_EQ(doc.getFieldCount(), (uint32_t)2);
        if (needStorePKValue) {
            ASSERT_EQ(doc.getField("pkey"), expectResult.key);
        } else {
            dictkey_t expectedHash = -1;
            indexlib::index::KeyHasherWrapper::GetHashKey(keyType, kvConfig->UseNumberHash(), expectResult.key.data(),
                                                          expectResult.key.size(), expectedHash);
            ASSERT_EQ(doc.getField("pkey"), std::to_string(expectedHash));
        }
        ASSERT_EQ(doc.getField("value"), expectResult.value);
        if (hasTTL) {
            ASSERT_EQ(docInfo.timestamp, expectResult.timestamp);
        } else {
            ASSERT_EQ(docInfo.timestamp, 0);
        }
    }
    ASSERT_FALSE(kvtabletDocIterator.HasNext());
}

void KVTabletDocIteratorTest::testPackValue(const std::string& valueFormat)
{
    fslib::fs::FileSystem::remove(GET_TEMP_DATA_PATH());
    std::string fields = "pkey:int32;longval:uint64;strval:string;int8val:int8;uint8val:uint8";
    _tabletSchema = table::KVTabletSchemaMaker::Make(fields, "pkey", "int8val;uint8val;longval;strval");
    std::string indexPath = GET_TEMP_DATA_PATH();
    auto indexConfigs = _tabletSchema->GetIndexConfigs();
    ASSERT_FALSE(indexConfigs.empty());
    auto kvConfig = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfigs[0]);
    kvConfig->SetTTL(3);
    indexlib::config::KVIndexPreference& kvIndexPreference = kvConfig->GetIndexPreference();
    indexlib::config::KVIndexPreference::ValueParam& valueParam = kvIndexPreference.GetValueParam();
    if (valueFormat == "plain") {
        valueParam.EnablePlainFormat(true);
        indexPath += "/plain";
    } else if (valueFormat == "impact") {
        valueParam.EnableValueImpact(true);
        indexPath += "/impact";
    }
    framework::IndexRoot indexRoot(indexPath, indexPath);
    _params["reader_timestamp"] = "0";
    std::vector<std::string> docStrings = {
        "cmd=add,pkey=2,longval=1,strval=val2,int8val=-1,uint8val=1,ts=50000000,locator=0:5;"
        "cmd=add,pkey=4,longval=4,strval=val4,int8val=-4,uint8val=4,ts=60000000,locator=0:6;"
        "cmd=delete,pkey=3,ts=70000000,locator=0:7;"
        "cmd=add,pkey=5,longval=5,strval=val5,int8val=-11,uint8val=11,ts=80000000,locator=0:8;"
        "cmd=add,pkey=7,longval=7,strval=val7,int8val=-7,uint8val=7,ts=90000000,locator=0:9;"
        "cmd=add,pkey=7,longval=17,strval=val7_2,int8val=-17,uint8val=17,ts=100000000,locator=0:10;",
        "cmd=add,pkey=1,longval=1,strval=val1,int8val=-1,uint8val=1,ts=10000000,locator=0:11;"
        "cmd=add,pkey=2,longval=2,strval=val2,int8val=-2,uint8val=2,ts=20000000,locator=0:12;"
        "cmd=add,pkey=3,longval=3,strval=val3,int8val=-4,uint8val=4,ts=30000000,locator=0:13;"
        "cmd=add,pkey=2,longval=2,strval=val2_2,int8val=-11,uint8val=11,ts=40000000,locator=0:14;"
        "cmd=delete,pkey=5,ts=40000000,locator=0:15;",
    };

#define CHECK_PACK_RAW_DOC(ts, int8val, uint8val, longval, strval)                                                     \
    {                                                                                                                  \
        indexlibv2::document::DefaultRawDocument doc;                                                                  \
        std::string checkpoint1;                                                                                       \
        framework::Locator::DocInfo docInfo(0, 0, 0, 0);                                                               \
        ASSERT_TRUE(kvtabletDocIterator.HasNext());                                                                    \
        ASSERT_TRUE(kvtabletDocIterator.Next(&doc, &checkpoint1, &docInfo).IsOK());                                    \
        ASSERT_EQ(int8val, doc.getField("int8val"));                                                                   \
        ASSERT_EQ(uint8val, doc.getField("uint8val"));                                                                 \
        ASSERT_EQ(longval, doc.getField("longval"));                                                                   \
        ASSERT_EQ(strval, doc.getField("strval"));                                                                     \
        ASSERT_EQ(ts, docInfo.timestamp);                                                                              \
    }

    KVTabletDocIterator kvtabletDocIterator;
    table::KVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(indexRoot, _tabletSchema, _tabletOptions).IsOK());
    for (const std::string& doc : docStrings) {
        ASSERT_TRUE(helper.BuildSegment(doc).IsOK());
    }
    std::vector<std::string> fieldNames;
    for (auto fieldConfig : _tabletSchema->GetFieldConfigs()) {
        fieldNames.push_back(fieldConfig->GetFieldName());
    }

    ASSERT_TRUE(kvtabletDocIterator
                    .Init(helper.GetTablet()->GetTabletData(), std::make_pair(0, 99), nullptr, fieldNames, _params)
                    .IsOK());
    CHECK_PACK_RAW_DOC(40, "-11", "11", "2", "val2_2");
    CHECK_PACK_RAW_DOC(60, "-4", "4", "4", "val4");
    CHECK_PACK_RAW_DOC(10, "-1", "1", "1", "val1");
    CHECK_PACK_RAW_DOC(30, "-4", "4", "3", "val3");
    CHECK_PACK_RAW_DOC(100, "-17", "17", "17", "val7_2");
#undef CHECK_PACK_RAW_DOC
}

void KVTabletDocIteratorTest::testAlterTablet(const std::string& valueFormat)
{
    _valueFormat = valueFormat;
    std::string fields = "pkey:int32;value:string";
    PrepareMultiShardOptions();
    PrepareSchema(fields);
    std::vector<std::string> docStrings = {"cmd=add,pkey=2,value=Aijk,ts=50000000,locator=0:5;"
                                           "cmd=add,pkey=4,value=Acde,ts=60000000,locator=0:6;"
                                           "cmd=delete,pkey=3,ts=70000000,locator=0:7;"
                                           "cmd=add,pkey=5,value=Almn,ts=80000000,locator=0:8;",
                                           "cmd=add,pkey=1,value=abc,ts=10000000,locator=0:11;"
                                           "cmd=add,pkey=2,value=cde,ts=20000000,locator=0:12;"
                                           "cmd=add,pkey=3,value=fgh,ts=30000000,locator=0:13;"
                                           "cmd=add,pkey=2,value=ijk,ts=40000000,locator=0:14;"
                                           "cmd=delete,pkey=5,ts=40000000,locator=0:17;"};
    KVTabletDocIterator kvtabletDocIterator;
    table::KVTableTestHelper helper;
    Status status;
    PrepareIterator(kvtabletDocIterator, docStrings, helper, std::make_pair(0, 99), status);
    ASSERT_TRUE(status.IsOK());
    std::vector<SimpleValueResultData> expectResults = {
        {"2", "ijk", 40}, {"4", "Acde", 60}, {"1", "abc", 10}, {"3", "fgh", 30}};
    checkResult(kvtabletDocIterator, expectResults);
    auto oldVersion = helper.GetCurrentVersion();
    ASSERT_EQ(0, oldVersion.GetReadSchemaId());
    for (auto [segId, schemaId] : oldVersion) {
        ASSERT_EQ(0, schemaId);
    }

    fields = "pkey:int32;value:string;value2:string";
    auto newTabletSchema = table::KVTabletSchemaMaker::Make(fields, "pkey", "value;value2", -1, _valueFormat);
    auto indexConfigs = newTabletSchema->GetIndexConfigs();
    ASSERT_FALSE(indexConfigs.empty());
    auto kvConfig = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfigs[0]);
    kvConfig->SetTTL(3);
    newTabletSchema->GetFieldConfig("value2")->SetDefaultValue("A");
    newTabletSchema->SetSchemaId(1);
    ASSERT_TRUE(helper.AlterTable(newTabletSchema).IsOK());
    auto currentVersion = helper.GetCurrentVersion();
    ASSERT_EQ(1, currentVersion.GetSchemaId());
    ASSERT_EQ(1, currentVersion.GetReadSchemaId());

    std::vector<std::string> fieldNames;
    for (auto fieldConfig : newTabletSchema->GetFieldConfigs()) {
        fieldNames.push_back(fieldConfig->GetFieldName());
    }

    status = kvtabletDocIterator.Init(helper.GetTablet()->GetTabletData(), std::make_pair(0, 99), nullptr, fieldNames,
                                      _params);
    ASSERT_TRUE(status.IsOK());
#define CHECK_ALTER_RAW_DOC(ts, pkey, value)                                                                           \
    {                                                                                                                  \
        indexlibv2::document::DefaultRawDocument doc;                                                                  \
        std::string checkpoint1;                                                                                       \
        framework::Locator::DocInfo docInfo(0, 0, 0, 0);                                                               \
        ASSERT_TRUE(kvtabletDocIterator.HasNext());                                                                    \
        ASSERT_TRUE(kvtabletDocIterator.Next(&doc, &checkpoint1, &docInfo).IsOK());                                    \
        ASSERT_EQ(pkey, doc.getField("pkey"));                                                                         \
        ASSERT_EQ(value, doc.getField("value"));                                                                       \
        ASSERT_EQ("A", doc.getField("value2"));                                                                        \
        ASSERT_EQ(ts, docInfo.timestamp);                                                                              \
    }
    CHECK_ALTER_RAW_DOC(40, "2", "ijk")
    CHECK_ALTER_RAW_DOC(60, "4", "Acde")
    CHECK_ALTER_RAW_DOC(10, "1", "abc")
    CHECK_ALTER_RAW_DOC(30, "3", "fgh")
#undef CHECK_ALTER_RAW_DOC
}

void KVTabletDocIteratorTest::InnerTest(bool isSingleShard, bool needMerge, bool hasTTL, bool needStorePKValue)
{
    std::vector<SimpleValueResultData> expectSingleShardResults = {
        {"a", "abc", 10}, {"c", "fgh", 30}, {"b", "ijk", 40}, {"d", "Acde", 60}};
    std::vector<SimpleValueResultData> expectMultiShardResults = {
        {"a", "abc", 10}, {"b", "ijk", 40}, {"c", "fgh", 30}, {"d", "Acde", 60}};

    std::string field = "pkey:string;value:string";
    if (isSingleShard) {
        PrepareSingleShardOptions();
    } else {
        PrepareMultiShardOptions();
    }
    std::vector<std::string> docStrings = {"cmd=add,pkey=b,value=Aijk,ts=50000000,locator=0:5;"
                                           "cmd=add,pkey=d,value=Acde,ts=60000000,locator=0:6;"
                                           "cmd=delete,pkey=c,ts=70000000,locator=0:7;"
                                           "cmd=add,pkey=e,value=Aijk,ts=80000000,locator=0:8;",
                                           "cmd=add,pkey=a,value=abc,ts=10000000,locator=0:11;"
                                           "cmd=add,pkey=b,value=cde,ts=20000000,locator=0:12;"
                                           "cmd=add,pkey=c,value=fgh,ts=30000000,locator=0:13;"
                                           "cmd=add,pkey=b,value=ijk,ts=40000000,locator=0:14;"
                                           "cmd=delete,pkey=e,ts=40000000,locator=0:17;"};

#define INNER_TEST(valueFormat)                                                                                        \
    {                                                                                                                  \
        _valueFormat = valueFormat;                                                                                    \
        PrepareSchema(field, hasTTL, needStorePKValue);                                                                \
        ASSERT_TRUE(_tabletSchema);                                                                                    \
        KVTabletDocIterator kvtabletDocIterator;                                                                       \
        table::KVTableTestHelper helper;                                                                               \
        Status status;                                                                                                 \
        PrepareIterator(kvtabletDocIterator, docStrings, helper, std::make_pair(0, 99), status, needMerge);            \
        ASSERT_TRUE(status.IsOK());                                                                                    \
        if (hasTTL && needMerge) {                                                                                     \
            ASSERT_EQ(kvtabletDocIterator.TEST_GetShardIterators().size(), 0);                                         \
        } else if (isSingleShard) {                                                                                    \
            ASSERT_EQ(kvtabletDocIterator.TEST_GetShardIterators().size(), 1);                                         \
            checkResult(kvtabletDocIterator, expectSingleShardResults, hasTTL && !needMerge, needStorePKValue);        \
        } else {                                                                                                       \
            ASSERT_EQ(kvtabletDocIterator.TEST_GetShardIterators().size(), 2);                                         \
            checkResult(kvtabletDocIterator, expectMultiShardResults, hasTTL && !needMerge, needStorePKValue);         \
        }                                                                                                              \
    }

    // test var len
    INNER_TEST("");
    INNER_TEST("impack");
    INNER_TEST("plain");

    // test fixed len
    expectMultiShardResults = {{"a", "11", 10}, {"b", "22", 40}, {"c", "13", 30}, {"d", "14", 60}};
    expectSingleShardResults = {{"a", "11", 10}, {"c", "13", 30}, {"b", "22", 40}, {"d", "14", 60}};
    docStrings = {"cmd=add,pkey=b,value=32,ts=50000000,locator=0:5;"
                  "cmd=add,pkey=d,value=14,ts=60000000,locator=0:6;"
                  "cmd=delete,pkey=c,ts=70000000,locator=0:7;"
                  "cmd=add,pkey=e,value=15,ts=80000000,locator=0:8;",
                  "cmd=add,pkey=a,value=11,ts=10000000,locator=0:11;"
                  "cmd=add,pkey=b,value=12,ts=20000000,locator=0:12;"
                  "cmd=add,pkey=c,value=13,ts=30000000,locator=0:13;"
                  "cmd=add,pkey=b,value=22,ts=40000000,locator=0:14;"
                  "cmd=delete,pkey=e,ts=40000000,locator=0:17;"};
    INNER_TEST("");
#undef INNER_TEST
}

TEST_F(KVTabletDocIteratorTest, TestInit)
{
    std::string field = "pkey:int32;value:string";
    PrepareMultiShardOptions();
    PrepareSchema(field);
    std::vector<std::string> docStrings = {"cmd=add,pkey=1,value=abc,ts=10000000,locator=0:1;"
                                           "cmd=add,pkey=2,value=cde,ts=20000000,locator=0:2;"
                                           "cmd=add,pkey=3,value=fgh,ts=30000000,locator=0:3;"
                                           "cmd=add,pkey=2,value=ijk,ts=40000000,locator=0:4;"
                                           "cmd=delete,pkey=5,ts=40000000,locator=0:5;",
                                           "cmd=add,pkey=2,value=Aijk,ts=50000000,locator=0:6;"
                                           "cmd=add,pkey=4,value=Acde,ts=60000000,locator=0:7;"
                                           "cmd=delete,pkey=3,ts=70000000,locator=0:8;"
                                           "cmd=add,pkey=5,value=Aijk,ts=80000000,locator=0:9;"};
    // test init normal
    {
        KVTabletDocIterator kvtabletDocIterator;
        table::KVTableTestHelper helper;
        Status status;
        PrepareIterator(kvtabletDocIterator, docStrings, helper, std::make_pair(0, 99), status);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(kvtabletDocIterator.TEST_GetShardIterators().size(), 2);
    }

    // test out of range
    {
        KVTabletDocIterator kvtabletDocIterator;
        table::KVTableTestHelper helper;
        Status status;
        PrepareIterator(kvtabletDocIterator, docStrings, helper, std::make_pair(0, 100), status);
        ASSERT_TRUE(status.IsInvalidArgs());
    }
    {
        KVTabletDocIterator kvtabletDocIterator;
        table::KVTableTestHelper helper;
        Status status;
        PrepareIterator(kvtabletDocIterator, docStrings, helper, std::make_pair(-1, 99), status);
        ASSERT_TRUE(status.IsInvalidArgs());
    }
    {
        KVTabletDocIterator kvtabletDocIterator;
        table::KVTableTestHelper helper;
        Status status;
        PrepareIterator(kvtabletDocIterator, docStrings, helper, std::make_pair(99, 0), status);
        ASSERT_TRUE(status.IsInvalidArgs());
    }

    // test select shard
    {
        KVTabletDocIterator kvtabletDocIterator;
        table::KVTableTestHelper helper;
        Status status;
        PrepareIterator(kvtabletDocIterator, docStrings, helper, std::make_pair(0, 0), status);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(kvtabletDocIterator.TEST_GetShardIterators().size(), 1);
    }
    {
        KVTabletDocIterator kvtabletDocIterator;
        table::KVTableTestHelper helper;
        Status status;
        PrepareIterator(kvtabletDocIterator, docStrings, helper, std::make_pair(0, 49), status);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(kvtabletDocIterator.TEST_GetShardIterators().size(), 1);
    }
    {
        KVTabletDocIterator kvtabletDocIterator;
        table::KVTableTestHelper helper;
        Status status;
        PrepareIterator(kvtabletDocIterator, docStrings, helper, std::make_pair(0, 50), status);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(kvtabletDocIterator.TEST_GetShardIterators().size(), 2);
    }
    {
        KVTabletDocIterator kvtabletDocIterator;
        table::KVTableTestHelper helper;
        Status status;
        PrepareIterator(kvtabletDocIterator, docStrings, helper, std::make_pair(51, 99), status);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(kvtabletDocIterator.TEST_GetShardIterators().size(), 0);
    }
}

TEST_F(KVTabletDocIteratorTest, TestNextAndSeek)
{
    std::string field = "pkey:int32;value:string";
    PrepareMultiShardOptions();
    PrepareSchema(field);
    std::vector<std::string> docStrings = {"cmd=add,pkey=2,value=Aijk,ts=50000000,locator=0:5;"
                                           "cmd=add,pkey=4,value=Acde,ts=60000000,locator=0:6;"
                                           "cmd=delete,pkey=3,ts=70000000,locator=0:7;"
                                           "cmd=add,pkey=5,value=Almn,ts=80000000,locator=0:8;",
                                           "cmd=add,pkey=1,value=abc,ts=10000000,locator=0:11;"
                                           "cmd=add,pkey=2,value=cde,ts=20000000,locator=0:12;"
                                           "cmd=add,pkey=3,value=fgh,ts=30000000,locator=0:13;"
                                           "cmd=add,pkey=2,value=ijk,ts=40000000,locator=0:14;"
                                           "cmd=delete,pkey=5,ts=40000000,locator=0:17;"};
    KVTabletDocIterator kvtabletDocIterator;
    table::KVTableTestHelper helper;
    Status status;
    PrepareIterator(kvtabletDocIterator, docStrings, helper, std::make_pair(0, 99), status);
    ASSERT_TRUE(status.IsOK());

    // test next
    indexlibv2::document::DefaultRawDocument doc;
    framework::Locator::DocInfo docInfo(0, 0, 0, 0);
    std::string checkpoint0;
    index::IShardRecordIterator::ShardCheckpoint shardCheckpoint;
    std::string defaultCheckpoint((char*)&shardCheckpoint, sizeof(shardCheckpoint));
    kvtabletDocIterator.SetCheckPoint(defaultCheckpoint, &checkpoint0);

    std::string checkpoint1;
    ASSERT_TRUE(kvtabletDocIterator.HasNext());
    ASSERT_TRUE(kvtabletDocIterator.Next(&doc, &checkpoint1, &docInfo).IsOK());

    std::string checkpoint2;
    ASSERT_TRUE(kvtabletDocIterator.HasNext());
    ASSERT_TRUE(kvtabletDocIterator.Next(&doc, &checkpoint2, &docInfo).IsOK());

    std::string checkpoint3;
    ASSERT_TRUE(kvtabletDocIterator.HasNext());
    ASSERT_TRUE(kvtabletDocIterator.Next(&doc, &checkpoint3, &docInfo).IsOK());

    std::string checkpoint4;
    ASSERT_TRUE(kvtabletDocIterator.HasNext());
    ASSERT_TRUE(kvtabletDocIterator.Next(&doc, &checkpoint4, &docInfo).IsOK());

    ASSERT_FALSE(kvtabletDocIterator.HasNext());

    // test seek
    ASSERT_TRUE(kvtabletDocIterator.Seek(checkpoint0).IsOK());
    std::vector<SimpleValueResultData> expectResults = {
        {"2", "ijk", 40}, {"4", "Acde", 60}, {"1", "abc", 10}, {"3", "fgh", 80}};

    ASSERT_TRUE(kvtabletDocIterator.Seek(checkpoint2).IsOK());
    expectResults = {{"1", "abc", 10}, {"3", "fgh", 30}};
    checkResult(kvtabletDocIterator, expectResults);

    ASSERT_TRUE(kvtabletDocIterator.Seek(checkpoint1).IsOK());
    expectResults = {{"4", "Acde", 60}, {"1", "abc", 10}, {"3", "fgh", 30}};
    checkResult(kvtabletDocIterator, expectResults);

    ASSERT_TRUE(kvtabletDocIterator.Seek(checkpoint4).IsOK());
    ASSERT_FALSE(kvtabletDocIterator.HasNext());

    ASSERT_TRUE(kvtabletDocIterator.Seek(checkpoint3).IsOK());
    expectResults = {{"3", "fgh", 30}};
    checkResult(kvtabletDocIterator, expectResults);
}

TEST_F(KVTabletDocIteratorTest, TestSimple)
{
    InnerTest(false, false, false, false);
    InnerTest(false, false, false, true);
    InnerTest(false, false, true, false);
    InnerTest(false, false, true, true);
    InnerTest(false, true, false, false);
    InnerTest(false, true, false, true);
    InnerTest(false, true, true, false);
    InnerTest(false, true, true, true);
    InnerTest(true, false, false, false);
    InnerTest(true, false, false, true);
    InnerTest(true, false, true, false);
    InnerTest(true, false, true, true);
    InnerTest(true, true, false, false);
    InnerTest(true, true, false, true);
    InnerTest(true, true, true, false);
    InnerTest(true, true, true, true);
}

TEST_F(KVTabletDocIteratorTest, TestTTL)
{
    std::string field = "pkey:int32;value:int8";
    PrepareMultiShardOptions();
    PrepareSchema(field);
    _params["reader_timestamp"] = "20000000";
    std::vector<std::string> docStrings = {"cmd=add,pkey=2,value=32,ts=50000000,locator=0:5;"
                                           "cmd=add,pkey=4,value=14,ts=60000000,locator=0:6;"
                                           "cmd=delete,pkey=3,ts=70000000,locator=0:7;"
                                           "cmd=add,pkey=5,value=15,ts=80000000,locator=0:8;",
                                           "cmd=add,pkey=1,value=11,ts=10000000,locator=0:11;"
                                           "cmd=add,pkey=2,value=12,ts=20000000,locator=0:12;"
                                           "cmd=add,pkey=3,value=13,ts=30000000,locator=0:13;"
                                           "cmd=add,pkey=2,value=22,ts=40000000,locator=0:14;"
                                           "cmd=delete,pkey=5,ts=40000000,locator=0:17;"};
    KVTabletDocIterator kvtabletDocIterator;
    table::KVTableTestHelper helper;
    Status status;
    PrepareIterator(kvtabletDocIterator, docStrings, helper, std::make_pair(0, 99), status);
    ASSERT_TRUE(status.IsOK());
    std::vector<SimpleValueResultData> expectResults = {{"2", "22", 40}, {"4", "14", 60}, {"3", "13", 30}};
    checkResult(kvtabletDocIterator, expectResults);
}

TEST_F(KVTabletDocIteratorTest, TestPackValue)
{
    PrepareMultiShardOptions();
    AUTIL_LOG(INFO, "test default pack value");
    testPackValue("");
    AUTIL_LOG(INFO, "test impack pack value");
    testPackValue("impack");
    AUTIL_LOG(INFO, "test plain pack value");
    testPackValue("plain");
}

TEST_F(KVTabletDocIteratorTest, TestFields)
{
    std::string field = "pkey:int32;value:int8;value2:int8";
    PrepareMultiShardOptions();
    _tabletSchema = table::KVTabletSchemaMaker::Make(field, "pkey", "value;value2");
    std::vector<std::string> requiredFields = {"value", "pkey"};
    std::vector<std::string> docStrings = {
        "cmd=add,pkey=2,value=32,value2=32,ts=50000000,locator=0:5;"
        "cmd=add,pkey=4,value=14,value2=14,ts=60000000,locator=0:6;"
        "cmd=delete,pkey=3,ts=70000000,locator=0:7;"
        "cmd=add,pkey=5,value=15,value2=15,ts=80000000,locator=0:8;",
        "cmd=add,pkey=1,value=11,value2=11,ts=10000000,locator=0:11;"
        "cmd=add,pkey=2,value=12,value2=12,ts=20000000,locator=0:12;"
        "cmd=add,pkey=3,value=13,value2=13,ts=30000000,locator=0:13;"
        "cmd=add,pkey=2,value=22,value2=22,ts=40000000,locator=0:14;"
        "cmd=delete,pkey=5,ts=40000000,locator=0:17;",
    };
    KVTabletDocIterator kvtabletDocIterator;
    table::KVTableTestHelper helper;
    Status status;
    PrepareIterator(kvtabletDocIterator, docStrings, helper, std::make_pair(0, 99), status, false, requiredFields);
    ASSERT_TRUE(status.IsOK());
    std::vector<SimpleValueResultData> expectResults = {{"2", "22", 0}, {"4", "14", 0}, {"1", "11", 0}, {"3", "13", 0}};
    checkResult(kvtabletDocIterator, expectResults);
}

TEST_F(KVTabletDocIteratorTest, TestEmptyShard)
{
    std::string field = "pkey:int32;value:string";
    PrepareMultiShardOptions();
    PrepareSchema(field);
    index::IShardRecordIterator::ShardCheckpoint shardCheckpoint;
    std::string defaultCheckpoint((char*)&shardCheckpoint, sizeof(shardCheckpoint));
    {
        std::vector<std::string> docStrings = {"cmd=delete,pkey=3,ts=70000000,locator=0:7;"
                                               "cmd=add,pkey=5,value=Aijk,ts=80000000,locator=0:8;",
                                               "cmd=add,pkey=1,value=abc,ts=10000000,locator=0:11;"
                                               "cmd=add,pkey=3,value=fgh,ts=30000000,locator=0:13;"
                                               "cmd=delete,pkey=5,ts=40000000,locator=0:17;"};
        KVTabletDocIterator kvtabletDocIterator;
        table::KVTableTestHelper helper;
        Status status;
        PrepareIterator(kvtabletDocIterator, docStrings, helper, std::make_pair(0, 99), status);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(kvtabletDocIterator.TEST_GetShardIterators().size(), 2);
        indexlibv2::document::DefaultRawDocument doc;
        std::string checkpoint0;
        kvtabletDocIterator.SetCheckPoint(defaultCheckpoint, &checkpoint0);
        ASSERT_TRUE(kvtabletDocIterator.Seek(checkpoint0).IsOK());

        std::string checkpoint;
        framework::Locator::DocInfo docInfo(0, 0, 0, 0);
        ASSERT_TRUE(kvtabletDocIterator.HasNext());
        ASSERT_TRUE(kvtabletDocIterator.Next(&doc, &checkpoint, &docInfo).IsOK());
        ASSERT_TRUE(kvtabletDocIterator.Seek(checkpoint).IsOK());
        std::vector<SimpleValueResultData> expectResults = {{"3", "fgh", 30}};
        checkResult(kvtabletDocIterator, expectResults);
    }
    {
        std::vector<std::string> docStrings = {"cmd=add,pkey=2,value=Aijk,ts=50000000,locator=0:25;"
                                               "cmd=add,pkey=4,value=Acde,ts=60000000,locator=0:26;",
                                               "cmd=add,pkey=2,value=cde,ts=20000000,locator=0:27;"
                                               "cmd=add,pkey=2,value=ijk,ts=40000000,locator=0:28;"};
        KVTabletDocIterator kvtabletDocIterator;
        table::KVTableTestHelper helper;
        Status status;
        PrepareIterator(kvtabletDocIterator, docStrings, helper, std::make_pair(0, 99), status);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(kvtabletDocIterator.TEST_GetShardIterators().size(), 2);
        indexlibv2::document::DefaultRawDocument doc;
        std::string checkpoint0;
        kvtabletDocIterator.SetCheckPoint(defaultCheckpoint, &checkpoint0);
        ASSERT_TRUE(kvtabletDocIterator.Seek(checkpoint0).IsOK());

        std::string checkpoint;
        framework::Locator::DocInfo docInfo(0, 0, 0, 0);
        ASSERT_TRUE(kvtabletDocIterator.HasNext());
        ASSERT_TRUE(kvtabletDocIterator.Next(&doc, &checkpoint, &docInfo).IsOK());
        ASSERT_TRUE(kvtabletDocIterator.Seek(checkpoint).IsOK());
        std::vector<SimpleValueResultData> expectResults = {{"4", "Acde", 60}};
        checkResult(kvtabletDocIterator, expectResults);
    }
}

TEST_F(KVTabletDocIteratorTest, TestEmptySegment)
{
    std::string field = "pkey:int32;value:string";
    PrepareMultiShardOptions();
    PrepareSchema(field);
    index::IShardRecordIterator::ShardCheckpoint shardCheckpoint;
    std::string defaultCheckpoint((char*)&shardCheckpoint, sizeof(shardCheckpoint));
    {
        std::vector<std::string> docStrings = {"cmd=delete,pkey=3,ts=70000000,locator=0:7;"
                                               "cmd=add,pkey=5,value=Aijk,ts=80000000,locator=0:8;",
                                               "cmd=add,pkey=1,value=abc,ts=10000000,locator=0:11;"
                                               "cmd=add,pkey=2,value=cde,ts=20000000,locator=0:12;"
                                               "cmd=add,pkey=3,value=fgh,ts=30000000,locator=0:13;"
                                               "cmd=delete,pkey=5,ts=40000000,locator=0:17;"};
        KVTabletDocIterator kvtabletDocIterator;
        table::KVTableTestHelper helper;
        Status status;
        PrepareIterator(kvtabletDocIterator, docStrings, helper, std::make_pair(0, 99), status);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(kvtabletDocIterator.TEST_GetShardIterators().size(), 2);
        indexlibv2::document::DefaultRawDocument doc;
        std::string checkpoint0;
        kvtabletDocIterator.SetCheckPoint(defaultCheckpoint, &checkpoint0);
        ASSERT_TRUE(kvtabletDocIterator.Seek(checkpoint0).IsOK());

        std::string checkpoint;
        framework::Locator::DocInfo docInfo(0, 0, 0, 0);
        ASSERT_TRUE(kvtabletDocIterator.HasNext());
        ASSERT_TRUE(kvtabletDocIterator.Next(&doc, &checkpoint, &docInfo).IsOK());
        ASSERT_TRUE(kvtabletDocIterator.Seek(checkpoint).IsOK());
        std::vector<SimpleValueResultData> expectResults = {{"1", "abc", 10}, {"3", "fgh", 30}};
        checkResult(kvtabletDocIterator, expectResults);
    }
    {
        std::vector<std::string> docStrings = {"cmd=add,pkey=2,value=Aijk,ts=50000000,locator=0:25;"
                                               "cmd=add,pkey=4,value=Acde,ts=60000000,locator=0:26;",
                                               "cmd=add,pkey=2,value=cde,ts=20000000,locator=0:27;"
                                               "cmd=add,pkey=3,value=fgh,ts=30000000,locator=0:28;"
                                               "cmd=add,pkey=2,value=ijk,ts=40000000,locator=0:29;"};
        KVTabletDocIterator kvtabletDocIterator;
        table::KVTableTestHelper helper;
        Status status;
        PrepareIterator(kvtabletDocIterator, docStrings, helper, std::make_pair(0, 99), status);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(kvtabletDocIterator.TEST_GetShardIterators().size(), 2);
        indexlibv2::document::DefaultRawDocument doc;
        std::string checkpoint0;
        kvtabletDocIterator.SetCheckPoint(defaultCheckpoint, &checkpoint0);
        ASSERT_TRUE(kvtabletDocIterator.Seek(checkpoint0).IsOK());

        std::string checkpoint;
        framework::Locator::DocInfo docInfo(0, 0, 0, 0);
        ASSERT_TRUE(kvtabletDocIterator.HasNext());
        ASSERT_TRUE(kvtabletDocIterator.Next(&doc, &checkpoint, &docInfo).IsOK());
        ASSERT_TRUE(kvtabletDocIterator.Seek(checkpoint).IsOK());
        std::vector<SimpleValueResultData> expectResults = {{"4", "Acde", 60}, {"3", "fgh", 30}};
        checkResult(kvtabletDocIterator, expectResults);
    }
}

TEST_F(KVTabletDocIteratorTest, TestMergeSegment)
{
    std::string field = "pkey:int32;value:string";
    PrepareMultiShardOptions();
    PrepareSchema(field, false);
    std::vector<std::string> docStrings = {
        "cmd=add,pkey=2,value=Aijk,ts=50000000,locator=0:5;"
        "cmd=add,pkey=4,value=Acde,ts=60000000,locator=0:6;"
        "cmd=delete,pkey=3,ts=70000000,locator=0:7;"
        "cmd=add,pkey=5,value=Aijk,ts=80000000,locator=0:8;",
        "cmd=add,pkey=1,value=abc,ts=10000000,locator=0:11;"
        "cmd=add,pkey=2,value=cde,ts=20000000,locator=0:12;"
        "cmd=add,pkey=3,value=fgh,ts=30000000,locator=0:13;"
        "cmd=add,pkey=2,value=ijk,ts=40000000,locator=0:14;"
        "cmd=delete,pkey=5,ts=40000000,locator=0:17;",
    };
    KVTabletDocIterator kvtabletDocIterator;
    table::KVTableTestHelper helper;
    Status status;
    PrepareIterator(kvtabletDocIterator, docStrings, helper, std::make_pair(0, 99), status, true);
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(kvtabletDocIterator.TEST_GetShardIterators().size(), 2);
    std::vector<SimpleValueResultData> expectResults = {
        {"2", "ijk", 0}, {"4", "Acde", 0}, {"1", "abc", 0}, {"3", "fgh", 0}};
    checkResult(kvtabletDocIterator, expectResults);
}

TEST_F(KVTabletDocIteratorTest, TestAlterTablet)
{
    AUTIL_LOG(INFO, "test default alter tablet");
    testAlterTablet("");
    AUTIL_LOG(INFO, "test impack alter tablet");
    testAlterTablet("impack");
    AUTIL_LOG(INFO, "test plain alter tablet");
    testAlterTablet("plain");
}

TEST_F(KVTabletDocIteratorTest, TestNoField)
{
    std::string field = "pkey:int32;value:int8;value2:int8;value3:string;value4:int32";
    PrepareMultiShardOptions();
    _tabletSchema = table::KVTabletSchemaMaker::Make(field, "pkey", "value;value2;value3;value4", -1, "plain");
    std::vector<std::string> requiredFields;
    std::vector<std::string> docStrings = {
        "cmd=add,pkey=2,value=32,value2=32,value3=123123,ts=50000000,locator=0:5;"
        "cmd=add,pkey=4,value=14,value2=14,ts=60000000,locator=0:6;"
        "cmd=delete,pkey=3,ts=70000000,locator=0:7;"
        "cmd=add,pkey=5,value=15,value2=15,ts=80000000,locator=0:8;",
        "cmd=add,pkey=1,value=11,value2=11,ts=10000000,locator=0:11;"
        "cmd=add,pkey=2,value=12,value2=12,ts=20000000,locator=0:12;"
        "cmd=add,pkey=3,value=13,value2=13,ts=30000000,locator=0:13;"
        "cmd=add,pkey=2,value=22,value2=22,ts=40000000,locator=0:14;"
        "cmd=delete,pkey=5,ts=40000000,locator=0:17;",
    };
    KVTabletDocIterator kvtabletDocIterator;
    table::KVTableTestHelper helper;
    Status status;
    PrepareIterator(kvtabletDocIterator, docStrings, helper, std::make_pair(0, 99), status, false, requiredFields);
    ASSERT_TRUE(status.IsOK());

    size_t docCount = 0;
    while (kvtabletDocIterator.HasNext()) {
        indexlibv2::document::DefaultRawDocument doc;
        std::string checkPoint;
        indexlibv2::framework::Locator::DocInfo docInfo;
        auto status = kvtabletDocIterator.Next(&doc, &checkPoint, &docInfo);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(0, doc.getFieldCount());
        docCount++;
    }
    ASSERT_EQ(4, docCount);
}

TEST_F(KVTabletDocIteratorTest, TestEmptyTablet)
{
    std::string field = "pkey:int32;value:int8;value2:int8;value3:string;value4:int32";
    PrepareMultiShardOptions();
    _tabletSchema = table::KVTabletSchemaMaker::Make(field, "pkey", "value;value2;value3;value4", -1, "plain");
    std::vector<std::string> requiredFields;

    KVTabletDocIterator kvtabletDocIterator;
    table::KVTableTestHelper helper;
    Status status;
    PrepareIterator(kvtabletDocIterator, {}, helper, std::make_pair(0, 99), status, false, requiredFields);
    ASSERT_TRUE(status.IsOK());
    ASSERT_FALSE(kvtabletDocIterator.HasNext());
}
}} // namespace indexlibv2::table
