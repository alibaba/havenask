#include "indexlib/table/kkv_table/KKVTabletDocIterator.h"

#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/document/raw_document/DefaultRawDocument.h"
#include "indexlib/framework/Tablet.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/framework/test/TabletTestAgent.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/table/kkv_table/test/KKVTableTestHelper.h"
#include "indexlib/table/kkv_table/test/KKVTabletSchemaMaker.h"
#include "unittest/unittest.h"

namespace indexlibv2 { namespace table {

class KKVTabletDocIteratorTest : public TESTBASE
{
public:
    struct SimpleValueResultData {
        std::string pkey = 0;
        index::keytype_t skey = 0;
        std::string value;
        uint32_t timestamp;
        std::string ttl;
    };

public:
    KKVTabletDocIteratorTest() {}
    ~KKVTabletDocIteratorTest() {}

public:
private:
    void InnerTest(bool isSingleShard, bool needMerge, bool hasTTL, bool needStorePKValue, bool isOptimizeMerge);
    void PrepareTabletOption();
    void PrepareSingleShardOptions();
    void PrepareMultiShardOptions();
    void PrepareSchema(const std::string& fields, bool hasTTL = true, bool ttlFromDoc = false,
                       bool needStorePKValue = false);
    void PrepareIterator(KKVTabletDocIterator& kkvtabletDocIterator, const std::vector<std::string>& docStrings,
                         table::KKVTableTestHelper& helper, const std::pair<uint32_t, uint32_t> rangeInRatio,
                         Status& status, bool needMerge, bool isOptimizeMerge,
                         const std::optional<std::vector<std::string>>& fieldNames);
    void checkResult(KKVTabletDocIterator& kkvTabletDocIterator,
                     const std::vector<SimpleValueResultData>& expectResults, int64_t mergedDefaultTs = 0,
                     bool checkTTLField = false, bool needStorePKValue = false);
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

AUTIL_LOG_SETUP(indexlib.table, KKVTabletDocIteratorTest);

void KKVTabletDocIteratorTest::PrepareTabletOption()
{
    _tabletOptions->SetIsOnline(true);
    _tabletOptions->SetIsLeader(true);
    _tabletOptions->SetFlushLocal(false);
    _tabletOptions->SetFlushRemote(true);
    _tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetBuildingMemoryLimit(2 * 1024 * 1024);
}

void KKVTabletDocIteratorTest::PrepareSingleShardOptions()
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

void KKVTabletDocIteratorTest::PrepareMultiShardOptions()
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

void KKVTabletDocIteratorTest::PrepareSchema(const std::string& fields, bool hasTTL, bool ttlFromDoc,
                                             bool needStorePKValue)
{
    if (ttlFromDoc) {
        _tabletSchema = KKVTableTestHelper::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "kkv_schema_ttl_from_doc.json");
        ASSERT_TRUE(_tabletSchema->GetLegacySchema() == nullptr);
        _params["reader_timestamp"] = "0";
        return;
    }
    _tabletSchema =
        table::KKVTabletSchemaMaker::Make(fields, "pkey", "skey", "value", -1, _valueFormat, needStorePKValue);
    ASSERT_TRUE(framework::TabletSchemaLoader::ResolveSchema(nullptr, "", _tabletSchema.get()).IsOK());
    if (hasTTL) {
        auto indexConfigs = _tabletSchema->GetIndexConfigs();
        ASSERT_FALSE(indexConfigs.empty());
        auto kkvConfig = std::dynamic_pointer_cast<indexlibv2::config::KKVIndexConfig>(indexConfigs[0]);
        kkvConfig->SetTTL(3);
        if (needStorePKValue) {
            auto kvConfig = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfigs[1]);
            kvConfig->SetTTL(3);
        }
    }
    _params["reader_timestamp"] = "0";
}

void KKVTabletDocIteratorTest::PrepareIterator(KKVTabletDocIterator& kkvTabletDocIterator,
                                               const std::vector<std::string>& docStrings,
                                               table::KKVTableTestHelper& helper,
                                               const std::pair<uint32_t, uint32_t> rangeInRatio, Status& status,
                                               bool needMerge, bool isOptimizeMerge,
                                               const std::optional<std::vector<std::string>>& fieldNames)
{
    fslib::fs::FileSystem::remove(GET_TEMP_DATA_PATH());
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    ASSERT_TRUE(helper.Open(indexRoot, _tabletSchema, _tabletOptions).IsOK());
    for (const std::string& doc : docStrings) {
        ASSERT_TRUE(helper.BuildSegment(doc).IsOK());
    }
    if (needMerge) {
        TableTestHelper::MergeOption mergeOption;
        mergeOption.mergeAutoReload = true;
        mergeOption.isOptimizeMerge = isOptimizeMerge;
        ASSERT_TRUE(helper.Merge(mergeOption).IsOK());
    }
    std::vector<std::string> finalFields;
    if (fieldNames) {
        finalFields = fieldNames.value();
    } else {
        for (auto fieldConfig : _tabletSchema->GetFieldConfigs()) {
            finalFields.push_back(fieldConfig->GetFieldName());
        }
    }

    status =
        kkvTabletDocIterator.Init(helper.GetTablet()->GetTabletData(), rangeInRatio, nullptr, finalFields, _params);
}

void KKVTabletDocIteratorTest::checkResult(KKVTabletDocIterator& kkvTabletDocIterator,
                                           const std::vector<SimpleValueResultData>& expectResults,
                                           int64_t mergedDefaultTs, bool checkTTLField, bool needStorePKValue)
{
    document::DefaultRawDocument doc;
    std::string checkpoint;
    document::IDocument::DocInfo docInfo(0, 0, 0);
    auto kkvConfig = std::dynamic_pointer_cast<indexlibv2::config::KKVIndexConfig>(_tabletSchema->GetIndexConfigs()[0]);
    ASSERT_TRUE(kkvConfig);
    auto keyType = kkvConfig->GetPrefixFieldConfig()->GetFieldType();
    for (auto& expectResult : expectResults) {
        ASSERT_TRUE(kkvTabletDocIterator.HasNext());
        ASSERT_TRUE(kkvTabletDocIterator.Next(&doc, &checkpoint, &docInfo).IsOK());
        if (!checkTTLField) {
            ASSERT_EQ(doc.getFieldCount(), (uint32_t)3);
        } else {
            ASSERT_EQ(doc.getFieldCount(), (uint32_t)4);
            ASSERT_EQ(doc.getField("field_for_ttl"), expectResult.ttl);
        }
        if (needStorePKValue) {
            ASSERT_EQ(doc.getField("pkey"), expectResult.pkey);
        } else {
            dictkey_t expectedHash = -1;
            indexlib::index::KeyHasherWrapper::GetHashKey(keyType, keyType != ft_string, expectResult.pkey.data(),
                                                          expectResult.pkey.size(), expectedHash);
            dictkey_t hashKey = -1;
            indexlib::util::MurmurHasher::GetHashKey(expectResult.pkey.data(), expectResult.pkey.size(), hashKey);
            auto pkeyHasherType = kkvConfig->GetPrefixHashFunctionType();
            indexlib::util::GetHashKey(pkeyHasherType, expectResult.pkey, hashKey);
            EXPECT_EQ(doc.getField("pkey"), std::to_string(expectedHash));
        }
        ASSERT_EQ(doc.getField("skey"), std::to_string(expectResult.skey));
        ASSERT_EQ(doc.getField("value"), expectResult.value);
        if (mergedDefaultTs == 0) {
            ASSERT_EQ(docInfo.timestamp, expectResult.timestamp);
        } else {
            ASSERT_EQ(docInfo.timestamp, mergedDefaultTs);
        }
    }
    ASSERT_FALSE(kkvTabletDocIterator.HasNext());
}

void KKVTabletDocIteratorTest::testPackValue(const std::string& valueFormat)
{
    fslib::fs::FileSystem::remove(GET_TEMP_DATA_PATH());
    std::string fields = "pkey:int32;skey:int32;longval:uint64;strval:string;int8val:int8;uint8val:uint8";
    _tabletSchema = table::KKVTabletSchemaMaker::Make(fields, "pkey", "skey", "int8val;uint8val;longval;strval");
    std::string indexPath = GET_TEMP_DATA_PATH();
    auto indexConfigs = _tabletSchema->GetIndexConfigs();
    ASSERT_FALSE(indexConfigs.empty());
    auto kkvConfig = std::dynamic_pointer_cast<indexlibv2::config::KKVIndexConfig>(indexConfigs[0]);
    kkvConfig->SetTTL(3);
    auto& kkvIndexPreference = kkvConfig->GetIndexPreference();
    auto& valueParam = kkvIndexPreference.GetValueParam();
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
        "cmd=add,pkey=2,skey=2,longval=1,strval=val2,int8val=-1,uint8val=1,ts=10000000,locator=0:5;"
        "cmd=add,pkey=4,skey=4,longval=4,strval=val4,int8val=-4,uint8val=4,ts=20000000,locator=0:6;"
        "cmd=delete,pkey=3,ts=20000000,locator=0:7;"
        "cmd=add,pkey=5,skey=4,longval=5,strval=val5,int8val=-11,uint8val=11,ts=30000000,locator=0:8;"
        "cmd=add,pkey=7,skey=7,longval=7,strval=val7,int8val=-7,uint8val=7,ts=40000000,locator=0:9;"
        "cmd=add,pkey=7,skey=7,longval=17,strval=val7_2,int8val=-17,uint8val=17,ts=50000000,locator=0:10;",
        "cmd=add,pkey=1,skey=1,longval=1,strval=val1,int8val=-1,uint8val=1,ts=60000000,locator=0:11;"
        "cmd=add,pkey=2,skey=2,longval=2,strval=val2,int8val=-2,uint8val=2,ts=60000000,locator=0:12;"
        "cmd=add,pkey=3,skey=3,longval=3,strval=val3,int8val=-4,uint8val=4,ts=80000000,locator=0:13;"
        "cmd=add,pkey=2,skey=2,longval=2,strval=val2_2,int8val=-11,uint8val=11,ts=90000000,locator=0:14;"
        "cmd=delete,pkey=5,ts=90000000,locator=0:15;"};

#define CHECK_PACK_RAW_DOC(ts, int8val, uint8val, longval, strval)                                                     \
    {                                                                                                                  \
        document::DefaultRawDocument doc;                                                                              \
        std::string checkpoint1;                                                                                       \
        document::IDocument::DocInfo docInfo(0, 0, 0);                                                                 \
        ASSERT_TRUE(kkvTabletDocIterator.HasNext());                                                                   \
        ASSERT_TRUE(kkvTabletDocIterator.Next(&doc, &checkpoint1, &docInfo).IsOK());                                   \
        ASSERT_EQ(int8val, doc.getField("int8val"));                                                                   \
        ASSERT_EQ(uint8val, doc.getField("uint8val"));                                                                 \
        ASSERT_EQ(longval, doc.getField("longval"));                                                                   \
        ASSERT_EQ(strval, doc.getField("strval"));                                                                     \
        ASSERT_EQ(ts, docInfo.timestamp);                                                                              \
    }

    KKVTabletDocIterator kkvTabletDocIterator;
    table::KKVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(indexRoot, _tabletSchema, _tabletOptions).IsOK());
    for (const std::string& doc : docStrings) {
        ASSERT_TRUE(helper.BuildSegment(doc).IsOK());
    }
    std::vector<std::string> fieldNames;
    for (auto fieldConfig : _tabletSchema->GetFieldConfigs()) {
        fieldNames.push_back(fieldConfig->GetFieldName());
    }

    ASSERT_TRUE(kkvTabletDocIterator
                    .Init(helper.GetTablet()->GetTabletData(), std::make_pair(0, 99), nullptr, fieldNames, _params)
                    .IsOK());
    CHECK_PACK_RAW_DOC(90, "-11", "11", "2", "val2_2");
    CHECK_PACK_RAW_DOC(20, "-4", "4", "4", "val4");
    CHECK_PACK_RAW_DOC(60, "-1", "1", "1", "val1");
    CHECK_PACK_RAW_DOC(80, "-4", "4", "3", "val3");
    CHECK_PACK_RAW_DOC(50, "-17", "17", "17", "val7_2");
#undef CHECK_PACK_RAW_DOC
}

void KKVTabletDocIteratorTest::testAlterTablet(const std::string& valueFormat)
{
    _valueFormat = valueFormat;
    std::string fields = "pkey:int32;skey:int32;value:string";
    PrepareMultiShardOptions();
    PrepareSchema(fields);
    std::vector<std::string> docStrings = {"cmd=add,pkey=2,skey=2,value=Aijk,ts=10000000,locator=0:1;"
                                           "cmd=add,pkey=4,skey=4,value=Acde,ts=20000000,locator=0:2;"
                                           "cmd=delete,pkey=3,skey=3,ts=30000000,locator=0:3;"
                                           "cmd=add,pkey=5,skey=5,value=Almn,ts=40000000,locator=0:4;",
                                           "cmd=add,pkey=1,skey=1,value=abc,ts=50000000,locator=0:5;"
                                           "cmd=add,pkey=2,skey=2,value=cde,ts=60000000,locator=0:6;"
                                           "cmd=add,pkey=3,skey=3,value=fgh,ts=70000000,locator=0:7;"
                                           "cmd=add,pkey=2,skey=2,value=ijk,ts=80000000,locator=0:8;"
                                           "cmd=delete,pkey=5,ts=90000000,locator=0:9;"};
    KKVTabletDocIterator kkvTabletDocIterator;
    table::KKVTableTestHelper helper;
    Status status;
    PrepareIterator(kkvTabletDocIterator, docStrings, helper, std::make_pair(0, 99), status, false, true, std::nullopt);
    ASSERT_TRUE(status.IsOK());
    std::vector<SimpleValueResultData> expectResults = {
        {"2", 2, "ijk", 80}, {"4", 4, "Acde", 20}, {"1", 1, "abc", 50}, {"3", 3, "fgh", 70}};
    checkResult(kkvTabletDocIterator, expectResults);
    auto oldVersion = helper.GetCurrentVersion();
    ASSERT_EQ(0, oldVersion.GetReadSchemaId());
    for (auto [segId, schemaId] : oldVersion) {
        ASSERT_EQ(0, schemaId);
    }

    fields = "pkey:int32;skey:int32;value:string;value2:string";
    auto newTabletSchema = table::KKVTabletSchemaMaker::Make(fields, "pkey", "skey", "value;value2", -1, _valueFormat);
    auto indexConfigs = newTabletSchema->GetIndexConfigs();
    ASSERT_FALSE(indexConfigs.empty());
    auto kkvConfig = std::dynamic_pointer_cast<indexlibv2::config::KKVIndexConfig>(indexConfigs[0]);
    kkvConfig->SetTTL(3);
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

    status = kkvTabletDocIterator.Init(helper.GetTablet()->GetTabletData(), std::make_pair(0, 99), nullptr, fieldNames,
                                       _params);
    ASSERT_TRUE(status.IsOK());

#define CHECK_ALTER_RAW_DOC(ts, pkey, value)                                                                           \
    {                                                                                                                  \
        document::DefaultRawDocument doc;                                                                              \
        std::string checkpoint1;                                                                                       \
        document::IDocument::DocInfo docInfo(0, 0, 0);                                                                 \
        ASSERT_TRUE(kkvTabletDocIterator.HasNext());                                                                   \
        ASSERT_TRUE(kkvTabletDocIterator.Next(&doc, &checkpoint1, &docInfo).IsOK());                                   \
        ASSERT_EQ(pkey, doc.getField("pkey"));                                                                         \
        ASSERT_EQ(pkey, doc.getField("skey"));                                                                         \
        ASSERT_EQ(value, doc.getField("value"));                                                                       \
        ASSERT_EQ("A", doc.getField("value2"));                                                                        \
        ASSERT_EQ(ts, docInfo.timestamp);                                                                              \
    }

    CHECK_ALTER_RAW_DOC(80, "2", "ijk");
    CHECK_ALTER_RAW_DOC(20, "4", "Acde");
    CHECK_ALTER_RAW_DOC(50, "1", "abc");
    CHECK_ALTER_RAW_DOC(70, "3", "fgh");
#undef CHECK_ALTER_RAW_DOC
}

void KKVTabletDocIteratorTest::InnerTest(bool isSingleShard, bool needMerge, bool hasTTL, bool needStorePKValue,
                                         bool isOptimizeMerge)
{
    std::vector<SimpleValueResultData> expectMultiShardResults = {
        {"2", 2, "ijk", 80}, {"1", 1, "abc", 50}, {"4", 4, "Acde", 20}, {"3", 3, "fgh", 70}};
    std::vector<SimpleValueResultData> expectSingleShardResults = {
        {"4", 4, "Acde", 20}, {"2", 2, "ijk", 80}, {"1", 1, "abc", 50}, {"3", 3, "fgh", 70}};

    std::string field = "pkey:string;skey:int32;value:string";
    if (isSingleShard) {
        PrepareSingleShardOptions();
    } else {
        PrepareMultiShardOptions();
    }
    std::vector<std::string> docStrings = {"cmd=add,pkey=2,skey=2,value=Aijk,ts=10000000,locator=0:1;"
                                           "cmd=add,pkey=4,skey=4,value=Acde,ts=20000000,locator=0:2;"
                                           "cmd=delete,pkey=3,ts=30000000,locator=0:3;"
                                           "cmd=add,pkey=5,skey=5,value=Almn,ts=40000000,locator=0:4;",

                                           "cmd=add,pkey=1,skey=1,value=abc,ts=50000000,locator=0:5;"
                                           "cmd=add,pkey=2,skey=2,value=cde,ts=60000000,locator=0:6;"
                                           "cmd=add,pkey=3,skey=3,value=fgh,ts=70000000,locator=0:7;"
                                           "cmd=add,pkey=2,skey=2,value=ijk,ts=80000000,locator=0:8;"
                                           "cmd=delete,pkey=5,ts=90000000,locator=0:10;"};

#define INNER_TEST(valueFormat)                                                                                        \
    {                                                                                                                  \
        _valueFormat = valueFormat;                                                                                    \
        PrepareSchema(field, hasTTL, false, needStorePKValue);                                                         \
        KKVTabletDocIterator kkvTabletDocIterator;                                                                     \
        table::KKVTableTestHelper helper;                                                                              \
        Status status;                                                                                                 \
        PrepareIterator(kkvTabletDocIterator, docStrings, helper, std::make_pair(0, 99), status, needMerge,            \
                        isOptimizeMerge, std::nullopt);                                                                \
        ASSERT_TRUE(status.IsOK());                                                                                    \
        if (hasTTL && needMerge) {                                                                                     \
            ASSERT_EQ(kkvTabletDocIterator.TEST_GetShardIterators().size(), 0);                                        \
        } else if (isSingleShard) {                                                                                    \
            ASSERT_EQ(kkvTabletDocIterator.TEST_GetShardIterators().size(), 1);                                        \
            checkResult(kkvTabletDocIterator, expectSingleShardResults, (needMerge && isOptimizeMerge) ? 90 : 0,       \
                        false, needStorePKValue);                                                                      \
        } else {                                                                                                       \
            ASSERT_EQ(kkvTabletDocIterator.TEST_GetShardIterators().size(), 2);                                        \
            checkResult(kkvTabletDocIterator, expectMultiShardResults, (needMerge && isOptimizeMerge) ? 90 : 0, false, \
                        needStorePKValue);                                                                             \
        }                                                                                                              \
    }

    // test var len
    INNER_TEST("");
    INNER_TEST("impack");
    INNER_TEST("plain");
#undef INNER_TEST
}

TEST_F(KKVTabletDocIteratorTest, TestInit)
{
    std::string field = "pkey:int32;skey:int32;value:string";
    PrepareMultiShardOptions();
    PrepareSchema(field);
    std::vector<std::string> docStrings = {"cmd=add,pkey=1,skey=1,value=abc,ts=10000000,locator=0:1;"
                                           "cmd=add,pkey=2,skey=2,value=cde,ts=20000000,locator=0:2;"
                                           "cmd=add,pkey=3,skey=3,value=fgh,ts=30000000,locator=0:3;"
                                           "cmd=add,pkey=2,skey=2,value=ijk,ts=40000000,locator=0:4;"
                                           "cmd=delete,pkey=5,skey=1,ts=40000000,locator=0:7;",
                                           "cmd=add,pkey=2,skey=2,value=Aijk,ts=50000000,locator=0:5;"
                                           "cmd=add,pkey=4,skey=4,value=Acde,ts=60000000,locator=0:6;"
                                           "cmd=delete,pkey=3,ts=70000000,locator=0:7;"
                                           "cmd=add,pkey=5,skey=5,value=Aijk,ts=80000000,locator=0:8;"};
    // test init normal
    {
        KKVTabletDocIterator kkvTabletDocIterator;
        table::KKVTableTestHelper helper;
        Status status;
        PrepareIterator(kkvTabletDocIterator, docStrings, helper, std::make_pair(0, 99), status, false, true,
                        std::nullopt);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(kkvTabletDocIterator.TEST_GetShardIterators().size(), 2);
    }
    // test out of range
    {
        KKVTabletDocIterator kkvTabletDocIterator;
        table::KKVTableTestHelper helper;
        Status status;
        PrepareIterator(kkvTabletDocIterator, docStrings, helper, std::make_pair(0, 100), status, false, true,
                        std::nullopt);
        ASSERT_TRUE(status.IsInvalidArgs());
    }
    {
        KKVTabletDocIterator kkvTabletDocIterator;
        table::KKVTableTestHelper helper;
        Status status;
        PrepareIterator(kkvTabletDocIterator, docStrings, helper, std::make_pair(-1, 99), status, false, true,
                        std::nullopt);
        ASSERT_TRUE(status.IsInvalidArgs());
    }
    {
        KKVTabletDocIterator kkvTabletDocIterator;
        table::KKVTableTestHelper helper;
        Status status;
        PrepareIterator(kkvTabletDocIterator, docStrings, helper, std::make_pair(99, 0), status, false, true,
                        std::nullopt);
        ASSERT_TRUE(status.IsInvalidArgs());
    }
    // test select shard
    {
        KKVTabletDocIterator kkvTabletDocIterator;
        table::KKVTableTestHelper helper;
        Status status;
        PrepareIterator(kkvTabletDocIterator, docStrings, helper, std::make_pair(0, 0), status, false, true,
                        std::nullopt);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(kkvTabletDocIterator.TEST_GetShardIterators().size(), 1);
    }
    {
        KKVTabletDocIterator kkvTabletDocIterator;
        table::KKVTableTestHelper helper;
        Status status;
        PrepareIterator(kkvTabletDocIterator, docStrings, helper, std::make_pair(0, 49), status, false, true,
                        std::nullopt);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(kkvTabletDocIterator.TEST_GetShardIterators().size(), 1);
    }
    {
        KKVTabletDocIterator kkvTabletDocIterator;
        table::KKVTableTestHelper helper;
        Status status;
        PrepareIterator(kkvTabletDocIterator, docStrings, helper, std::make_pair(0, 50), status, false, true,
                        std::nullopt);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(kkvTabletDocIterator.TEST_GetShardIterators().size(), 2);
    }
    {
        KKVTabletDocIterator kkvTabletDocIterator;
        table::KKVTableTestHelper helper;
        Status status;
        PrepareIterator(kkvTabletDocIterator, docStrings, helper, std::make_pair(51, 99), status, false, true,
                        std::nullopt);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(kkvTabletDocIterator.TEST_GetShardIterators().size(), 0);
    }
}

TEST_F(KKVTabletDocIteratorTest, TestNextAndSeek)
{
    std::string field = "pkey:int32;skey:int32;value:string";
    PrepareMultiShardOptions();
    PrepareSchema(field);
    std::vector<std::string> docStrings = {"cmd=add,pkey=2,skey=2,value=Aijk,ts=10000000,locator=0:1;"
                                           "cmd=add,pkey=4,skey=4,value=Acde,ts=20000000,locator=0:2;"
                                           "cmd=delete,pkey=3,ts=30000000,locator=0:3;"
                                           "cmd=add,pkey=5,skey=5,value=Almn,ts=40000000,locator=0:4;",
                                           "cmd=add,pkey=5,skey=6,value=Almn66,ts=50000000,locator=0:5;",

                                           "cmd=add,pkey=1,skey=1,value=abc,ts=60000000,locator=0:6;"
                                           "cmd=add,pkey=2,skey=2,value=cde,ts=70000000,locator=0:7;"
                                           "cmd=add,pkey=3,skey=3,value=fgh,ts=80000000,locator=0:8;"
                                           "cmd=add,pkey=3,skey=4,value=fgh4,ts=90000000,locator=0:9;"
                                           "cmd=add,pkey=3,skey=5,value=fgh5,ts=100000000,locator=0:10;"
                                           "cmd=add,pkey=2,skey=2,value=ijk,ts=110000000,locator=0:11;"
                                           "cmd=delete,pkey=5,ts=120000000,locator=0:12;"
                                           "cmd=delete,pkey=3,skey=4,ts=130000000,locator=0:13;"
                                           "cmd=delete,pkey=3,skey=6,ts=140000000,locator=0:14;"};
    table::KKVTableTestHelper helper;
    Status status;
    KKVTabletDocIterator kkvTabletDocIterator;
    PrepareIterator(kkvTabletDocIterator, docStrings, helper, std::make_pair(0, 99), status, false, true, std::nullopt);
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(kkvTabletDocIterator.TEST_GetShardIterators().size(), 2);

    // test next
    document::DefaultRawDocument doc;
    document::IDocument::DocInfo docInfo(0, 0, 0);
    std::string checkpoint0;
    index::IShardRecordIterator::ShardCheckpoint shardCheckpoint;
    std::string defaultCheckpoint((char*)&shardCheckpoint, sizeof(shardCheckpoint));
    kkvTabletDocIterator.SetCheckPoint(defaultCheckpoint, &checkpoint0);

    std::string checkpoint1;
    ASSERT_TRUE(kkvTabletDocIterator.HasNext());
    ASSERT_TRUE(kkvTabletDocIterator.Next(&doc, &checkpoint1, &docInfo).IsOK());

    std::string checkpoint2;
    ASSERT_TRUE(kkvTabletDocIterator.HasNext());
    ASSERT_TRUE(kkvTabletDocIterator.Next(&doc, &checkpoint2, &docInfo).IsOK());

    std::string checkpoint3;
    ASSERT_TRUE(kkvTabletDocIterator.HasNext());
    ASSERT_TRUE(kkvTabletDocIterator.Next(&doc, &checkpoint3, &docInfo).IsOK());

    std::string checkpoint4;
    ASSERT_TRUE(kkvTabletDocIterator.HasNext());
    ASSERT_TRUE(kkvTabletDocIterator.Next(&doc, &checkpoint4, &docInfo).IsOK());

    std::string checkpoint5;
    ASSERT_TRUE(kkvTabletDocIterator.HasNext());
    ASSERT_TRUE(kkvTabletDocIterator.Next(&doc, &checkpoint5, &docInfo).IsOK());
    ASSERT_FALSE(kkvTabletDocIterator.HasNext());

    // test seek
    ASSERT_TRUE(kkvTabletDocIterator.Seek(checkpoint0).IsOK());
    std::vector<SimpleValueResultData> expectResults = {
        {"2", 2, "ijk", 110}, {"4", 4, "Acde", 20}, {"1", 1, "abc", 60}, {"3", 3, "fgh", 80}, {"3", 5, "fgh5", 100}};
    checkResult(kkvTabletDocIterator, expectResults);

    ASSERT_TRUE(kkvTabletDocIterator.Seek(checkpoint2).IsOK());
    expectResults = {{"1", 1, "abc", 60}, {"3", 3, "fgh", 80}, {"3", 5, "fgh5", 100}};
    checkResult(kkvTabletDocIterator, expectResults);

    ASSERT_TRUE(kkvTabletDocIterator.Seek(checkpoint1).IsOK());
    expectResults = {{"4", 4, "Acde", 20}, {"1", 1, "abc", 60}, {"3", 3, "fgh", 80}, {"3", 5, "fgh5", 100}};
    checkResult(kkvTabletDocIterator, expectResults);

    ASSERT_TRUE(kkvTabletDocIterator.Seek(checkpoint4).IsOK());
    expectResults = {{"3", 5, "fgh5", 100}};
    checkResult(kkvTabletDocIterator, expectResults);

    ASSERT_TRUE(kkvTabletDocIterator.Seek(checkpoint5).IsOK());
    ASSERT_FALSE(kkvTabletDocIterator.HasNext());

    ASSERT_TRUE(kkvTabletDocIterator.Seek(checkpoint3).IsOK());
    expectResults = {{"3", 3, "fgh", 80}, {"3", 5, "fgh5", 100}};
    checkResult(kkvTabletDocIterator, expectResults);
    ASSERT_TRUE(kkvTabletDocIterator.Seek(checkpoint5).IsOK());
    expectResults = {};
    checkResult(kkvTabletDocIterator, expectResults);
}

TEST_F(KKVTabletDocIteratorTest, TestSimple)
{
    InnerTest(false, false, false, false, true);
    InnerTest(false, false, false, true, true);
    InnerTest(false, false, true, false, true);
    InnerTest(false, false, true, true, true);
    InnerTest(false, true, false, false, true);
    InnerTest(false, true, false, true, true);
    InnerTest(false, true, true, false, true);
    InnerTest(false, true, true, true, true);
    InnerTest(true, false, false, false, true);
    InnerTest(true, false, false, true, true);
    InnerTest(true, false, true, false, true);
    InnerTest(true, false, true, true, true);
    InnerTest(true, true, false, false, true);
    InnerTest(true, true, false, true, true);
    InnerTest(true, true, true, false, true);
    InnerTest(true, true, true, true, true);
    InnerTest(false, false, false, false, false);
    InnerTest(false, false, false, true, false);
    InnerTest(false, false, true, false, false);
    InnerTest(false, false, true, true, false);
    InnerTest(false, true, false, false, false);
    InnerTest(false, true, false, true, false);
    InnerTest(false, true, true, false, false);
    InnerTest(false, true, true, true, false);
    InnerTest(true, false, false, false, false);
    InnerTest(true, false, false, true, false);
    InnerTest(true, false, true, false, false);
    InnerTest(true, false, true, true, false);
    InnerTest(true, true, false, false, false);
    InnerTest(true, true, false, true, false);
    InnerTest(true, true, true, false, false);
    InnerTest(true, true, true, true, false);
}

TEST_F(KKVTabletDocIteratorTest, TestTTL)
{
    std::string field = "pkey:int32;field_for_ttl:int32;skey:int32;value:int8";
    std::vector<std::string> docStrings = {"cmd=add,pkey=2,skey=2,value=32,ts=10000000,field_for_ttl=10,locator=0:1;"
                                           "cmd=add,pkey=4,skey=4,value=14,ts=20000000,field_for_ttl=20,locator=0:2;"
                                           "cmd=delete,pkey=3,ts=9000000,locator=0:3;"
                                           "cmd=add,pkey=5,skey=5,value=15,ts=30000000,field_for_ttl=30,locator=0:4;",

                                           "cmd=add,pkey=1,skey=1,value=11,ts=40000000,field_for_ttl=40,locator=0:5;"
                                           "cmd=add,pkey=2,skey=2,value=12,ts=50000000,field_for_ttl=50,locator=0:6;"
                                           "cmd=add,pkey=3,skey=3,value=13,ts=60000000,field_for_ttl=35,locator=0:7;"
                                           "cmd=add,pkey=2,skey=2,value=22,ts=70000000,field_for_ttl=50,locator=0:8;"
                                           "cmd=delete,pkey=5,ts=80000000,locator=0:9;"};
    PrepareMultiShardOptions();
    {
        PrepareSchema(field, true, false);
        KKVTabletDocIterator kkvTabletDocIterator;
        table::KKVTableTestHelper helper;
        Status status;
        _params["reader_timestamp"] = "30000000";
        PrepareIterator(kkvTabletDocIterator, docStrings, helper, std::make_pair(0, 99), status, false, true,
                        std::nullopt);
        ASSERT_TRUE(status.IsOK());
        std::vector<SimpleValueResultData> expectResults = {{"2", 2, "22", 70}, {"1", 1, "11", 40}, {"3", 3, "13", 60}};
        checkResult(kkvTabletDocIterator, expectResults);
    }
    // doc from ttl1
    {
        PrepareSchema(field, true, true);
        KKVTabletDocIterator kkvTabletDocIterator;
        table::KKVTableTestHelper helper;
        Status status;
        _params["reader_timestamp"] = "40000000";
        PrepareIterator(kkvTabletDocIterator, docStrings, helper, std::make_pair(0, 99), status, false, true,
                        std::nullopt);
        ASSERT_TRUE(status.IsOK());
        std::vector<SimpleValueResultData> expectResults = {
            {"2", 2, "22", 70, "50"}, {"4", 4, "14", 20, "20"}, {"1", 1, "11", 40, "40"}, {"3", 3, "13", 60, "35"}};
        checkResult(kkvTabletDocIterator, expectResults, 0, true);
    }
    // doc from ttl2
    {
        PrepareSchema(field, true, true);
        KKVTabletDocIterator kkvTabletDocIterator;
        table::KKVTableTestHelper helper;
        Status status;
        _params["reader_timestamp"] = "41000000";
        PrepareIterator(kkvTabletDocIterator, docStrings, helper, std::make_pair(0, 99), status, false, true,
                        std::nullopt);
        ASSERT_TRUE(status.IsOK());
        std::vector<SimpleValueResultData> expectResults = {
            {"2", 2, "22", 70, "50"}, {"1", 1, "11", 40, "40"}, {"3", 3, "13", 60, "35"}};
        checkResult(kkvTabletDocIterator, expectResults, 0, true);
    }
}

TEST_F(KKVTabletDocIteratorTest, TestPackkvalue)
{
    PrepareMultiShardOptions();
    AUTIL_LOG(INFO, "test default pack value");
    testPackValue("");
    AUTIL_LOG(INFO, "test impack pack value");
    testPackValue("impack");
    AUTIL_LOG(INFO, "test plain pack value");
    testPackValue("plain");
}

TEST_F(KKVTabletDocIteratorTest, TestFields)
{
    std::string field = "pkey:int32;skey:int32;value:string;value2:string";
    PrepareMultiShardOptions();
    _tabletSchema = table::KKVTabletSchemaMaker::Make(field, "pkey", "skey", "value;value2");
    std::vector<std::string> requiredFields = {"value"};
    std::vector<std::string> docStrings = {"cmd=add,pkey=2,skey=2,value=Aijk,value2=Aijk,ts=10000000,locator=0:1;"
                                           "cmd=add,pkey=4,skey=4,value=Acde,value2=Acde,ts=20000000,locator=0:2;"
                                           "cmd=delete,pkey=3,ts=30000000,locator=0:3;"
                                           "cmd=add,pkey=5,skey=5,value=Aijk,value2=Aijk,ts=40000000,locator=0:4;",

                                           "cmd=add,pkey=1,skey=1,value=abc,value2=abc,ts=50000000,locator=0:5;"
                                           "cmd=add,pkey=2,skey=2,value=cde,value2=cde,ts=60000000,locator=0:6;"
                                           "cmd=add,pkey=3,skey=3,value=fgh,value2=fgh,ts=70000000,locator=0:7;"
                                           "cmd=add,pkey=2,skey=2,value=ijk,value2=ijk,ts=80000000,locator=0:8;"
                                           "cmd=delete,pkey=5,ts=90000000,locator=0:9;"};
    KKVTabletDocIterator kkvTabletDocIterator;
    table::KKVTableTestHelper helper;
    Status status;
    PrepareIterator(kkvTabletDocIterator, docStrings, helper, std::make_pair(0, 99), status, false, true,
                    requiredFields);
    ASSERT_TRUE(status.IsOK());
    std::vector<SimpleValueResultData> expectResults = {
        {"2", 2, "ijk", 80}, {"4", 4, "Acde", 20}, {"1", 1, "abc", 50}, {"3", 3, "fgh", 70}};
    checkResult(kkvTabletDocIterator, expectResults);
}

TEST_F(KKVTabletDocIteratorTest, TestEmptyShard)
{
    std::string field = "pkey:int32;skey:int32;value:string";
    PrepareMultiShardOptions();
    PrepareSchema(field);
    index::IShardRecordIterator::ShardCheckpoint shardCheckpoint;
    std::string defaultCheckpoint((char*)&shardCheckpoint, sizeof(shardCheckpoint));
    {
        std::vector<std::string> docStrings = {"cmd=delete,pkey=3,skey=3,ts=10000000,locator=0:1;"
                                               "cmd=add,pkey=5,skey=5,value=Aijk,ts=20000000,locator=0:2;",
                                               "cmd=add,pkey=1,skey=1,value=abc,ts=30000000,locator=0:3;"
                                               "cmd=add,pkey=3,skey=3,value=fgh,ts=40000000,locator=0:4;"
                                               "cmd=delete,pkey=5,ts=50000000,locator=0:5;"};
        KKVTabletDocIterator kkvTabletDocIterator;
        table::KKVTableTestHelper helper;
        Status status;
        PrepareIterator(kkvTabletDocIterator, docStrings, helper, std::make_pair(0, 99), status, false, true,
                        std::nullopt);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(kkvTabletDocIterator.TEST_GetShardIterators().size(), 2);
        document::DefaultRawDocument doc;
        std::string checkpoint0;
        kkvTabletDocIterator.SetCheckPoint(defaultCheckpoint, &checkpoint0);
        ASSERT_TRUE(kkvTabletDocIterator.Seek(checkpoint0).IsOK());

        std::string checkpoint;
        document::IDocument::DocInfo docInfo(0, 0, 0);
        ASSERT_TRUE(kkvTabletDocIterator.HasNext());
        ASSERT_TRUE(kkvTabletDocIterator.Next(&doc, &checkpoint, &docInfo).IsOK());
        ASSERT_TRUE(kkvTabletDocIterator.Seek(checkpoint).IsOK());
        std::vector<SimpleValueResultData> expectResults = {{"3", 3, "fgh", 40}};
        checkResult(kkvTabletDocIterator, expectResults);
    }
    {
        std::vector<std::string> docStrings = {"cmd=add,pkey=2,skey=2,value=Aijk,ts=10000000,locator=0:1;"
                                               "cmd=add,pkey=4,skey=4,value=Acde,ts=20000000,locator=0:2;",
                                               "cmd=add,pkey=2,skey=2,value=cde,ts=30000000,locator=0:3;"
                                               "cmd=add,pkey=2,skey=2,value=ijk,ts=40000000,locator=0:4;"};
        KKVTabletDocIterator kkvTabletDocIterator;
        table::KKVTableTestHelper helper;
        Status status;
        PrepareIterator(kkvTabletDocIterator, docStrings, helper, std::make_pair(0, 99), status, false, true,
                        std::nullopt);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(kkvTabletDocIterator.TEST_GetShardIterators().size(), 2);
        document::DefaultRawDocument doc;
        std::string checkpoint0;
        kkvTabletDocIterator.SetCheckPoint(defaultCheckpoint, &checkpoint0);
        ASSERT_TRUE(kkvTabletDocIterator.Seek(checkpoint0).IsOK());

        std::string checkpoint;
        document::IDocument::DocInfo docInfo(0, 0, 0);
        ASSERT_TRUE(kkvTabletDocIterator.HasNext());
        ASSERT_TRUE(kkvTabletDocIterator.Next(&doc, &checkpoint, &docInfo).IsOK());
        ASSERT_TRUE(kkvTabletDocIterator.Seek(checkpoint).IsOK());
        std::vector<SimpleValueResultData> expectResults = {{"4", 4, "Acde", 20}};
        checkResult(kkvTabletDocIterator, expectResults);
    }
}

TEST_F(KKVTabletDocIteratorTest, TestEmptySegment)
{
    std::string field = "pkey:int32;skey:int32;value:string";
    PrepareMultiShardOptions();
    PrepareSchema(field);
    index::IShardRecordIterator::ShardCheckpoint shardCheckpoint;
    std::string defaultCheckpoint((char*)&shardCheckpoint, sizeof(shardCheckpoint));
    {
        std::vector<std::string> docStrings = {"cmd=delete,pkey=3,skey=3,ts=10000000,locator=0:1;"
                                               "cmd=add,pkey=5,skey=5,value=Aijk,ts=20000000,locator=0:2;",
                                               "cmd=add,pkey=1,skey=1,value=abc,ts=30000000,locator=0:3;"
                                               "cmd=add,pkey=2,skey=2,value=cde,ts=40000000,locator=0:4;"
                                               "cmd=add,pkey=3,skey=3,value=fgh,ts=50000000,locator=0:5;"
                                               "cmd=delete,pkey=5,ts=60000000,locator=0:6;"};
        KKVTabletDocIterator kkvTabletDocIterator;
        table::KKVTableTestHelper helper;
        Status status;
        PrepareIterator(kkvTabletDocIterator, docStrings, helper, std::make_pair(0, 99), status, false, true,
                        std::nullopt);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(kkvTabletDocIterator.TEST_GetShardIterators().size(), 2);
        document::DefaultRawDocument doc;
        std::string checkpoint0;
        kkvTabletDocIterator.SetCheckPoint(defaultCheckpoint, &checkpoint0);
        ASSERT_TRUE(kkvTabletDocIterator.Seek(checkpoint0).IsOK());

        std::string checkpoint;
        document::IDocument::DocInfo docInfo(0, 0, 0);
        ASSERT_TRUE(kkvTabletDocIterator.HasNext());
        ASSERT_TRUE(kkvTabletDocIterator.Next(&doc, &checkpoint, &docInfo).IsOK());
        ASSERT_TRUE(kkvTabletDocIterator.Seek(checkpoint).IsOK());
        std::vector<SimpleValueResultData> expectResults = {{"1", 1, "abc", 30}, {"3", 3, "fgh", 50}};
        checkResult(kkvTabletDocIterator, expectResults);
    }
    {
        std::vector<std::string> docStrings = {"cmd=add,pkey=2,skey=2,value=Aijk,ts=10000000,locator=0:1;"
                                               "cmd=add,pkey=4,skey=4,value=Acde,ts=20000000,locator=0:2;",
                                               "cmd=add,pkey=2,skey=2,value=cde,ts=30000000,locator=0:3;"
                                               "cmd=add,pkey=3,skey=3,value=fgh,ts=40000000,locator=0:4;"
                                               "cmd=add,pkey=2,skey=2,value=ijk,ts=50000000,locator=0:5;"};
        KKVTabletDocIterator kkvTabletDocIterator;
        table::KKVTableTestHelper helper;
        Status status;
        PrepareIterator(kkvTabletDocIterator, docStrings, helper, std::make_pair(0, 99), status, false, true,
                        std::nullopt);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(kkvTabletDocIterator.TEST_GetShardIterators().size(), 2);
        document::DefaultRawDocument doc;
        std::string checkpoint0;
        kkvTabletDocIterator.SetCheckPoint(defaultCheckpoint, &checkpoint0);
        ASSERT_TRUE(kkvTabletDocIterator.Seek(checkpoint0).IsOK());

        std::string checkpoint;
        document::IDocument::DocInfo docInfo(0, 0, 0);
        ASSERT_TRUE(kkvTabletDocIterator.HasNext());
        ASSERT_TRUE(kkvTabletDocIterator.Next(&doc, &checkpoint, &docInfo).IsOK());
        ASSERT_TRUE(kkvTabletDocIterator.Seek(checkpoint).IsOK());
        std::vector<SimpleValueResultData> expectResults = {{"4", 4, "Acde", 20}, {"3", 3, "fgh", 40}};
        checkResult(kkvTabletDocIterator, expectResults);
    }
}

// TODO(shouya) wait kkv support alter tablet
// TEST_F(KKVTabletDocIteratorTest, TestAlterTablet)
// {
//     AUTIL_LOG(INFO, "test default alter tablet");
//     testAlterTablet("");
//     AUTIL_LOG(INFO, "test impack alter tablet");
//     testAlterTablet("impack");
//     AUTIL_LOG(INFO, "test plain alter tablet");
//     testAlterTablet("plain");
// }

}} // namespace indexlibv2::table
