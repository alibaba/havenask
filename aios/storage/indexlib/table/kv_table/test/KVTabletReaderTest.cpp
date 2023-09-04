#include "autil/HashAlgorithm.h"
#include "future_lite/CoroInterface.h"
#include "future_lite/executors/SimpleExecutor.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/framework/IndexRoot.h"
#include "indexlib/framework/Tablet.h"
#include "indexlib/framework/test/TabletTestAgent.h"
#include "indexlib/index/kv/KVIndexReader.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/table/kv_table/KVReaderImpl.h"
#include "indexlib/table/kv_table/KVTabletFactory.h"
#include "indexlib/table/kv_table/KVTabletSessionReader.h"
#include "indexlib/table/kv_table/test/KVTableTestHelper.h"
#include "indexlib/table/kv_table/test/KVTabletSchemaMaker.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib;
using namespace indexlib::util;
using namespace indexlib::index_base;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlibv2::index;
using namespace indexlibv2::framework;

namespace indexlibv2 { namespace table {

class KVTabletReaderTest : public TESTBASE
{
public:
    KVTabletReaderTest() {}
    ~KVTabletReaderTest() {}

public:
    void setUp() override
    {
        _pool = new autil::mem_pool::Pool();
        setenv("DISABLE_CODEGEN", "true", 1);
    }
    void tearDown() override
    {
        DELETE_AND_SET_NULL(_pool);
        unsetenv("DISABLE_CODEGEN");
    }

private:
    void CompareQueryResult(const KVIndexReaderPtr& kvIndexReader, const string keyStr, uint64_t ts, bool exist,
                            bool isDeleted, const string expectedValue);
    void CompareQueryResultAndMetric(const KVIndexReaderPtr& kvIndexReader, const string keyStr,
                                     const string expectedValue, int64_t expectedBlockHit, int64_t expectedBlockMiss);

private:
    autil::mem_pool::Pool* _pool = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, KVTabletReaderTest);

void KVTabletReaderTest::CompareQueryResult(const KVIndexReaderPtr& kvIndexReader, const string keyStr, uint64_t ts,
                                            bool exist, bool isDeleted, const string expectedValue)
{
    future_lite::executors::SimpleExecutor ex(1);
    autil::StringView key(keyStr);
    vector<autil::StringView> keys = {key};
    autil::StringView value;
    KVMetricsCollector collector;
    KVReadOptions readOptions;
    readOptions.timestamp = ts;
    readOptions.pool = _pool;
    readOptions.metricsCollector = &collector;
    if (exist) {
        auto status = future_lite::interface::syncAwait(kvIndexReader->GetAsync(key, value, readOptions), &ex);
        ASSERT_EQ(status, KVResultStatus::FOUND);
        ASSERT_GT(value.size(), expectedValue.length());
        ASSERT_NE(value.to_string().find(expectedValue), std::string::npos);

        auto result = future_lite::interface::syncAwait(kvIndexReader->GetAsync(key, readOptions), &ex);
        ASSERT_EQ(result.status, KVResultStatus::FOUND);
        auto packValue = result.valueExtractor.GetPackValue();
        ASSERT_GT(packValue.size(), expectedValue.length());
        ASSERT_NE(packValue.to_string().find(expectedValue), std::string::npos);

        auto resultVec = future_lite::interface::syncAwait(kvIndexReader->BatchGetAsync(keys, readOptions), &ex);
        ASSERT_EQ(1, resultVec.size());
        ASSERT_EQ(future_lite::interface::getTryValue(resultVec[0]).status, KVResultStatus::FOUND);
        packValue = future_lite::interface::getTryValue(resultVec[0]).valueExtractor.GetPackValue();
        ASSERT_GT(packValue.size(), expectedValue.length());
        ASSERT_NE(packValue.to_string().find(expectedValue), std::string::npos);
    } else {
        auto status = future_lite::interface::syncAwait(kvIndexReader->GetAsync(key, value, readOptions), &ex);
        if (isDeleted) {
            ASSERT_EQ(status, KVResultStatus::DELETED);
        } else {
            ASSERT_EQ(status, KVResultStatus::NOT_FOUND);
        }
    }
}

void KVTabletReaderTest::CompareQueryResultAndMetric(const KVIndexReaderPtr& kvIndexReader, const string keyStr,
                                                     const string expectedValue, int64_t expectedBlockHit,
                                                     int64_t expectedBlockMiss)
{
    future_lite::executors::SimpleExecutor ex(1);
    autil::StringView key(keyStr);
    autil::StringView value;
    KVMetricsCollector collector;
    KVReadOptions readOptions;
    readOptions.pool = _pool;
    readOptions.metricsCollector = &collector;
    auto status = future_lite::interface::syncAwait(kvIndexReader->GetAsync(key, value, readOptions), &ex);
    ASSERT_EQ(status, KVResultStatus::FOUND);
    ASSERT_GT(value.size(), expectedValue.length());
    ASSERT_NE(value.to_string().find(expectedValue), std::string::npos);

    ASSERT_LT(0L, collector.GetSSTableLatency());
    ASSERT_LE(0L, collector.GetMemTableLatency());
    // FIXME: refactor block cache counter
    EXPECT_EQ(expectedBlockHit, collector.GetBlockCacheHitCount());
    EXPECT_EQ(expectedBlockMiss, collector.GetBlockCacheMissCount());
    ASSERT_EQ(0L, collector.GetSearchCacheMissCount());
    ASSERT_EQ(0L, collector.GetSearchCacheHitCount());
    ASSERT_EQ(1L, collector.GetSSTableCount());
    ASSERT_EQ(0L, collector.GetMemTableCount());
}

TEST_F(KVTabletReaderTest, TestSameKeyAndDelete)
{
    std::string jsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "sharding_column_num" : 2,
            "level_num" : 3
        }
    }
    } )";

    std::shared_ptr<config::TabletOptions> tabletOptions = std::make_shared<config::TabletOptions>();
    FromJsonString(*tabletOptions, jsonStr);
    tabletOptions->SetIsOnline(true);
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetMaxDocCount(1);
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetBuildingMemoryLimit(64 * 1024 * 1024);

    std::string field = "string1:string;string2:string";
    auto tabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "string2");
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());

    table::KVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(indexRoot, tabletSchema, std::move(tabletOptions)).IsOK());
    auto tablet = helper.GetITablet();
    ASSERT_TRUE(helper.Build("cmd=add,string1=1,string2=abc,ts=1000000;").IsOK()); // dump
    ASSERT_TRUE(helper.Build("cmd=add,string1=2,string2=cde,ts=1000000;").IsOK()); // dump
    ASSERT_TRUE(helper.Build("cmd=delete,string1=1,ts=2000000;").IsOK());
    ASSERT_TRUE(helper.Build("cmd=add,string1=2,string2=fgh,ts=3000000;").IsOK()); // dump
    ASSERT_TRUE(tablet->Seal().IsOK());
    ASSERT_TRUE(tablet->NeedCommit());
    auto pair = tablet->Commit(framework::CommitOptions().SetNeedPublish(true));
    ASSERT_TRUE(pair.first.IsOK());
    ASSERT_TRUE(tablet->Reopen(framework::ReopenOptions(), pair.second.GetVersionId()).IsOK());

    auto reader = tablet->GetTabletReader();
    auto kvTabletReader = std::dynamic_pointer_cast<table::KVTabletSessionReader>(reader);
    auto kvIndexReader =
        std::dynamic_pointer_cast<index::KVIndexReader>(kvTabletReader->GetIndexReader("kv", "string1"));

    ASSERT_EQ(2, dynamic_pointer_cast<KVReaderImpl>(kvIndexReader)->TEST_GetDiskShardReaders().size());
    ASSERT_EQ(4, dynamic_pointer_cast<KVReaderImpl>(kvIndexReader)->TEST_GetDiskShardReaders()[0].size());
    ASSERT_EQ(4, dynamic_pointer_cast<KVReaderImpl>(kvIndexReader)->TEST_GetDiskShardReaders()[1].size());

    CompareQueryResult(kvIndexReader, "1", 0, false, true, "");
    CompareQueryResult(kvIndexReader, "2", 0, true, false, "fgh");
}

TEST_F(KVTabletReaderTest, TestTTL)
{
    std::string jsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "sharding_column_num" : 4,
            "level_num" : 3
        }
    }
    } )";

    std::shared_ptr<config::TabletOptions> tabletOptions = std::make_shared<config::TabletOptions>();
    FromJsonString(*tabletOptions, jsonStr);
    tabletOptions->SetIsOnline(true);
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetMaxDocCount(1);
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetBuildingMemoryLimit(64 * 1024 * 1024);

    std::string field = "string1:string;string2:string";
    auto tabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "string2");
    auto indexConfig = tabletSchema->GetIndexConfig("kv", "string1");
    auto kvIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfig);
    kvIndexConfig->SetTTL(100);
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());

    table::KVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(indexRoot, tabletSchema, std::move(tabletOptions)).IsOK());
    auto tablet = helper.GetITablet();
    ASSERT_TRUE(helper.Build("cmd=add,string1=1,string2=abc,ts=1000000;").IsOK());
    ASSERT_TRUE(tablet->Seal().IsOK());
    ASSERT_TRUE(tablet->NeedCommit());
    auto pair = tablet->Commit(framework::CommitOptions().SetNeedPublish(true));
    ASSERT_TRUE(pair.first.IsOK());
    ASSERT_TRUE(tablet->Reopen(framework::ReopenOptions(), pair.second.GetVersionId()).IsOK());

    auto reader = tablet->GetTabletReader();
    auto kvTabletReader = std::dynamic_pointer_cast<table::KVTabletSessionReader>(reader);
    auto kvIndexReader = kvTabletReader->GetIndexReader<index::KVIndexReader>("kv", "string1");

    ASSERT_EQ(4, dynamic_pointer_cast<KVReaderImpl>(kvIndexReader)->TEST_GetDiskShardReaders().size());

    CompareQueryResult(kvIndexReader, "1", 100000000, true, false, "abc");
    CompareQueryResult(kvIndexReader, "1", 200000000, false, false, "");
    CompareQueryResult(kvIndexReader, "1", 0, true, false, "abc");
}

TEST_F(KVTabletReaderTest, TestBlockCache)
{
    std::string jsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "sharding_column_num" : 2,
            "level_num" : 3
        }
    }
    } )";

    std::shared_ptr<config::TabletOptions> tabletOptions = std::make_shared<config::TabletOptions>();
    FromJsonString(*tabletOptions, jsonStr);

    jsonStr = R"({ "load_config" :
        [{ "file_patterns" : ["key", "value"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : true }}]
    })";
    tabletOptions->TEST_GetOnlineConfig().MutableLoadConfigList() =
        LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);

    tabletOptions->SetIsOnline(true);
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetMaxDocCount(1);
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetEnablePackageFile(false);
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetBuildingMemoryLimit(64 * 1024 * 1024);

    std::string field = "string1:string;string2:string";
    auto tabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "string2");
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());

    table::KVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(indexRoot, tabletSchema, std::move(tabletOptions)).IsOK());
    auto tablet = helper.GetITablet();
    ASSERT_TRUE(helper.Build("cmd=add,string1=1,string2=abc,ts=1;").IsOK());
    ASSERT_TRUE(helper.Build("cmd=add,string1=2,string2=cde,ts=1;").IsOK());
    ASSERT_TRUE(tablet->Seal().IsOK());
    ASSERT_TRUE(tablet->NeedCommit());
    auto pair = tablet->Commit(framework::CommitOptions().SetNeedPublish(true));
    ASSERT_TRUE(pair.first.IsOK());
    ASSERT_TRUE(tablet->Reopen(framework::ReopenOptions(), pair.second.GetVersionId()).IsOK());

    auto reader = tablet->GetTabletReader();
    auto kvTabletReader = std::dynamic_pointer_cast<table::KVTabletSessionReader>(reader);
    auto kvIndexReader = kvTabletReader->GetIndexReader<index::KVIndexReader>("kv", "string1");

    CompareQueryResultAndMetric(kvIndexReader, "1", "abc", 1, 3);
    CompareQueryResultAndMetric(kvIndexReader, "1", "abc", 4, 0);
    CompareQueryResultAndMetric(kvIndexReader, "2", "cde", 1, 2);
    CompareQueryResultAndMetric(kvIndexReader, "2", "cde", 3, 0);
}

TEST_F(KVTabletReaderTest, TestValueAdapter)
{
    std::string jsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "sharding_column_num" : 2,
            "level_num" : 3
        }
    }
    } )";

    std::shared_ptr<config::TabletOptions> tabletOptions = std::make_shared<config::TabletOptions>();
    FromJsonString(*tabletOptions, jsonStr);

    jsonStr = R"({ "load_config" :
        [{ "file_patterns" : ["key", "value"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : true }}]
    })";
    tabletOptions->TEST_GetOnlineConfig().MutableLoadConfigList() =
        LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);

    tabletOptions->SetIsOnline(true);
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetMaxDocCount(1);
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetEnablePackageFile(false);
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetBuildingMemoryLimit(64 * 1024 * 1024);

    std::string field = "string1:string;string2:string";
    auto tabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "string2");
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());

    table::KVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(indexRoot, tabletSchema, std::move(tabletOptions)).IsOK());
    auto tablet = helper.GetITablet();
    ASSERT_TRUE(helper.Build("cmd=add,string1=1,string2=abc,ts=1;").IsOK());
    ASSERT_TRUE(helper.Build("cmd=add,string1=2,string2=cde,ts=1;").IsOK());
    ASSERT_TRUE(tablet->Seal().IsOK());
    ASSERT_TRUE(tablet->NeedCommit());
    auto pair = tablet->Commit(framework::CommitOptions().SetNeedPublish(true));
    ASSERT_TRUE(pair.first.IsOK());
    ASSERT_TRUE(tablet->Reopen(framework::ReopenOptions(), pair.second.GetVersionId()).IsOK());

    auto tabletData = framework::TabletTestAgent(helper.GetTablet()).TEST_GetTabletData();
    // create new schema
    std::string newField = "string1:string;string2:string;int32_single:int32;str_multi:string:true";
    auto newTabletSchema = table::KVTabletSchemaMaker::Make(newField, "string1", "string2;int32_single;str_multi");

    newTabletSchema->GetFieldConfig("int32_single")->SetDefaultValue("10");
    newTabletSchema->GetFieldConfig("str_multi")->SetDefaultValue("abc123");

    auto newKvIndexConfig =
        std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(newTabletSchema->GetIndexConfig("kv", "string1"));

    KVReaderImpl newReader(1);
    ASSERT_TRUE(newReader.Open(newKvIndexConfig, tabletData.get()).IsOK());

    string keyStr = "1";
    future_lite::executors::SimpleExecutor ex(1);
    autil::StringView key(keyStr);
    KVMetricsCollector collector;
    KVReadOptions readOptions;
    readOptions.timestamp = 1000000;
    readOptions.pool = _pool;
    readOptions.metricsCollector = &collector;

    auto result = future_lite::interface::syncAwait(newReader.GetAsync(key, readOptions), &ex);
    ASSERT_EQ(result.status, KVResultStatus::FOUND);
    ASSERT_EQ(3, result.valueExtractor.GetFieldCount());

    int32_t i32;
    ASSERT_TRUE(result.valueExtractor.GetTypedValue("int32_single", i32));
    ASSERT_EQ(10, i32);

    autil::MultiString multiStr;
    ASSERT_TRUE(result.valueExtractor.GetTypedValue("str_multi", multiStr));
    ASSERT_EQ(2, multiStr.size());
    ASSERT_EQ(string("abc"), string(multiStr[0].getData(), multiStr[0].getDataSize()));
    ASSERT_EQ(string("123"), string(multiStr[1].getData(), multiStr[1].getDataSize()));
}

TEST_F(KVTabletReaderTest, TestMultiIndex)
{
    std::string jsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "sharding_column_num" : 2,
            "level_num" : 3
        }
    }
    } )";
    std::shared_ptr<config::TabletOptions> tabletOptions = std::make_shared<config::TabletOptions>();
    FromJsonString(*tabletOptions, jsonStr);
    tabletOptions->SetIsOnline(true);
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetMaxDocCount(1);
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetBuildingMemoryLimit(64 * 1024 * 1024);
    auto tabletSchema = table::KVTableTestHelper::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "/multi_kv_schema.json");
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    table::KVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(indexRoot, tabletSchema, std::move(tabletOptions)).IsOK());
    auto tablet = helper.GetITablet();
    stringstream docs;
    docs << "cmd=add,weights=1.1 1.11 1.111,key1=1,ts=10000000;"
         << "cmd=add,weights=1.2 1.22 1.222,key1=2,ts=20000000;"
         << "cmd=add,weights=2.1 2.11 2.111,key2=1,ts=10000000;"
         << "cmd=add,weights=2.2 2.22 2.222,key2=2,ts=20000000;";
    ASSERT_TRUE(helper.Build(docs.str()).IsOK()); // dump
    ASSERT_TRUE(tablet->Seal().IsOK());
    ASSERT_TRUE(tablet->NeedCommit());
    auto pair = tablet->Commit(framework::CommitOptions().SetNeedPublish(true));
    ASSERT_TRUE(pair.first.IsOK());
    ASSERT_TRUE(tablet->Reopen(framework::ReopenOptions(), pair.second.GetVersionId()).IsOK());

    auto tabletReader = tablet->GetTabletReader();
    auto kvTabletReader = std::dynamic_pointer_cast<table::KVTabletSessionReader>(tabletReader);
    auto kvIndexReader =
        std::dynamic_pointer_cast<index::KVIndexReader>(kvTabletReader->GetIndexReader("kv", "index1"));
    auto kvIndexReader2 =
        std::dynamic_pointer_cast<index::KVIndexReader>(kvTabletReader->GetIndexReader("kv", "index2"));

    {
        ASSERT_EQ(2, dynamic_pointer_cast<KVReaderImpl>(kvIndexReader)->TEST_GetDiskShardReaders().size());
        ASSERT_EQ(4, dynamic_pointer_cast<KVReaderImpl>(kvIndexReader)->TEST_GetDiskShardReaders()[0].size());
        ASSERT_EQ(4, dynamic_pointer_cast<KVReaderImpl>(kvIndexReader)->TEST_GetDiskShardReaders()[1].size());

        ASSERT_EQ(2, dynamic_pointer_cast<KVReaderImpl>(kvIndexReader2)->TEST_GetDiskShardReaders().size());
        ASSERT_EQ(4, dynamic_pointer_cast<KVReaderImpl>(kvIndexReader2)->TEST_GetDiskShardReaders()[0].size());
        ASSERT_EQ(4, dynamic_pointer_cast<KVReaderImpl>(kvIndexReader2)->TEST_GetDiskShardReaders()[1].size());
    }
    string key1 = "1";
    string key2 = "2";
    string notFound = "notFound";
    KVReadOptions readOptions;
    readOptions.timestamp = 1000000;
    readOptions.pool = _pool;
    { // 测试index1 key1查询
        autil::StringView key(key1);
        auto result = kvIndexReader->Get(key, readOptions);
        ASSERT_EQ(result.status, KVResultStatus::FOUND);
        ASSERT_EQ(1, result.valueExtractor.GetFieldCount());
        autil::MultiValueType<float> multiValue;
        ASSERT_TRUE(result.valueExtractor.GetTypedValue("weights", multiValue));
        ASSERT_EQ(multiValue.size(), 3);
        auto values = multiValue.data();
        ASSERT_EQ(values[0], (float)1.1);
        ASSERT_EQ(values[1], (float)1.11);
        ASSERT_EQ(values[2], (float)1.111);
    }
    { // 测试index1 key2查询
        autil::StringView key(key2);
        auto result = kvIndexReader->Get(key, readOptions);
        ASSERT_EQ(result.status, KVResultStatus::FOUND);
        ASSERT_EQ(1, result.valueExtractor.GetFieldCount());
        autil::MultiValueType<float> multiValue;
        ASSERT_TRUE(result.valueExtractor.GetTypedValue("weights", multiValue));
        ASSERT_EQ(multiValue.size(), 3);
        auto values = multiValue.data();
        ASSERT_EQ(values[0], (float)1.2);
        ASSERT_EQ(values[1], (float)1.22);
        ASSERT_EQ(values[2], (float)1.222);
    }
    { // 测试index1 非法key查询
        autil::StringView key(notFound);
        auto result = kvIndexReader->Get(key, readOptions);
        ASSERT_EQ(result.status, KVResultStatus::NOT_FOUND);
    }
    { // 测试index2 key1查询
        autil::StringView key(key1);
        auto result = kvIndexReader2->Get(key, readOptions);
        ASSERT_EQ(result.status, KVResultStatus::FOUND);
        ASSERT_EQ(1, result.valueExtractor.GetFieldCount());
        autil::MultiValueType<float> multiValue;
        ASSERT_TRUE(result.valueExtractor.GetTypedValue("weights", multiValue));
        ASSERT_EQ(multiValue.size(), 3);
        auto values = multiValue.data();
        ASSERT_EQ(values[0], (float)2.1);
        ASSERT_EQ(values[1], (float)2.11);
        ASSERT_EQ(values[2], (float)2.111);
    }
    { // 测试index2 key2查询
        autil::StringView key(key2);
        auto result = kvIndexReader2->Get(key, readOptions);
        ASSERT_EQ(result.status, KVResultStatus::FOUND);
        ASSERT_EQ(1, result.valueExtractor.GetFieldCount());
        autil::MultiValueType<float> multiValue;
        ASSERT_TRUE(result.valueExtractor.GetTypedValue("weights", multiValue));
        ASSERT_EQ(multiValue.size(), 3);
        auto values = multiValue.data();
        ASSERT_EQ(values[0], (float)2.2);
        ASSERT_EQ(values[1], (float)2.22);
        ASSERT_EQ(values[2], (float)2.222);
    }
    { // 测试index2 非法key查询
        autil::StringView key(notFound);
        auto result = kvIndexReader2->Get(key, readOptions);
        ASSERT_EQ(result.status, KVResultStatus::NOT_FOUND);
    }
}

}} // namespace indexlibv2::table
