#include "future_lite/CoroInterface.h"
#include "future_lite/executors/SimpleExecutor.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/framework/IndexRoot.h"
#include "indexlib/index/kv/KVIndexReader.h"
#include "indexlib/table/kv_table/KVCacheReaderImpl.h"
#include "indexlib/table/kv_table/KVTabletReader.h"
#include "indexlib/table/kv_table/KVTabletSessionReader.h"
#include "indexlib/table/kv_table/test/KVTableTestHelper.h"
#include "indexlib/table/kv_table/test/KVTabletSchemaMaker.h"
#include "indexlib/util/cache/SearchCacheCreator.h"
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

class KVTabletCacheReaderTest : public TESTBASE
{
public:
    KVTabletCacheReaderTest() {}
    ~KVTabletCacheReaderTest() {}

public:
    void setUp() override
    {
        _pool = new autil::mem_pool::Pool();
        SearchCachePtr searchCache(SearchCacheCreator::Create(
            "cache_size=1024;num_shard_bits=10", MemoryQuotaControllerPtr(), std::shared_ptr<TaskScheduler>(), NULL));
        _searchCacheWrapper = std::make_shared<indexlib::util::SearchCachePartitionWrapper>(searchCache, "part0");
    }
    void tearDown() override { DELETE_AND_SET_NULL(_pool); }

private:
    void CompareQueryResult(const KVIndexReaderPtr& kvIndexReader, const string& keyStr, uint64_t ts, bool exist,
                            const string& expectedValue);
    void CheckCacheResult(const string& keyStr, bool exist, const string& expectedValue,
                          const string& expectedLocatorStr = "");
    std::shared_ptr<KVCacheReaderImpl> GetTabletReader(const table::KVTableTestHelper& helper);

private:
    autil::mem_pool::Pool* _pool = nullptr;
    indexlib::util::SearchCachePartitionWrapperPtr _searchCacheWrapper;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, KVTabletCacheReaderTest);

TEST_F(KVTabletCacheReaderTest, TestGet)
{
    std::string jsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "sharding_column_num" : 1,
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
    ASSERT_TRUE(helper.BuildSegment("cmd=add,string1=1,string2=abc,ts=1000000,locator=0:1;").IsOK()); // dump
    ASSERT_TRUE(helper.BuildSegment("cmd=add,string1=2,string2=cde,ts=1000000,locator=0:2;").IsOK()); // dump
    ASSERT_TRUE(helper.BuildSegment("cmd=delete,string1=1,ts=2000000,locator=0:3;").IsOK());
    ASSERT_TRUE(helper.BuildSegment("cmd=add,string1=2,string2=fgh,ts=3000000,locator=0:4;").IsOK()); // dump

    {
        auto kvCacheReader = GetTabletReader(helper);
        ASSERT_EQ(size_t(1), kvCacheReader->TEST_GetDiskShardReaders().size());
        ASSERT_EQ(size_t(4), kvCacheReader->TEST_GetDiskShardReaders()[0].size());
        CheckCacheResult("1", false, "");
        CompareQueryResult(kvCacheReader, "1", 0, false, "");
        CheckCacheResult("1", true, "", "0:4:[{0,65535,4,0}]");

        CheckCacheResult("2", false, "");
        CompareQueryResult(kvCacheReader, "2", 0, true, "fgh");
        CheckCacheResult("2", true, "fgh", "0:4:[{0,65535,4,0}]");
    }

    {
        ASSERT_TRUE(helper.BuildSegment("cmd=add,string1=2,string2=cde,ts=4000000,locator=0:5;").IsOK()); // dump
        auto kvCacheReader = GetTabletReader(helper);
        ASSERT_EQ(size_t(5), kvCacheReader->TEST_GetDiskShardReaders()[0].size());
        ASSERT_TRUE(kvCacheReader->TEST_GetMemoryShardReaders().empty());
        CheckCacheResult("2", true, "fgh", "0:4:[{0,65535,4,0}]");
        CompareQueryResult(kvCacheReader, "2", 0, true, "cde");
        CheckCacheResult("2", true, "cde", "0:5:[{0,65535,5,0}]");

        CompareQueryResult(kvCacheReader, "1", 0, false, "");
        CheckCacheResult("1", true, "", "0:5:[{0,65535,5,0}]");
    }

    {
        ASSERT_TRUE(helper.Build("cmd=add,string1=1,string2=abc,ts=1000000,locator=0:6;").IsOK());
        auto kvCacheReader = GetTabletReader(helper);
        ASSERT_EQ(size_t(5), kvCacheReader->TEST_GetDiskShardReaders()[0].size());
        ASSERT_EQ(size_t(1), kvCacheReader->TEST_GetMemoryShardReaders()[0].size());
        CheckCacheResult("1", true, "", "0:5:[{0,65535,5,0}]");
        CompareQueryResult(kvCacheReader, "1", 0, true, "abc");
        CheckCacheResult("1", true, "", "0:5:[{0,65535,5,0}]");
        CompareQueryResult(kvCacheReader, "2", 0, true, "cde");
        CheckCacheResult("2", true, "cde", "0:5:[{0,65535,5,0}]");
    }

    {
        ASSERT_TRUE(helper.BuildSegment("cmd=add,string1=1,string2=ab,ts=1000000,locator=0:7;").IsOK());
        auto kvCacheReader = GetTabletReader(helper);
        ASSERT_EQ(size_t(7), kvCacheReader->TEST_GetDiskShardReaders()[0].size());
        ASSERT_TRUE(kvCacheReader->TEST_GetMemoryShardReaders().empty());
        CheckCacheResult("1", true, "", "0:5:[{0,65535,5,0}]");
        CompareQueryResult(kvCacheReader, "1", 0, true, "ab");
        CheckCacheResult("1", true, "ab", "0:7:[{0,65535,7,0}]");
    }

    {
        ASSERT_TRUE(helper.BuildSegment("cmd=delete,string1=1,ts=2000000,locator=0:8;").IsOK());
        auto kvCacheReader = GetTabletReader(helper);
        ASSERT_EQ(size_t(8), kvCacheReader->TEST_GetDiskShardReaders()[0].size());
        ASSERT_TRUE(kvCacheReader->TEST_GetMemoryShardReaders().empty());
        CheckCacheResult("1", true, "ab", "0:7:[{0,65535,7,0}]");
        CompareQueryResult(kvCacheReader, "1", 0, false, "");
        CheckCacheResult("1", true, "", "0:8:[{0,65535,8,0}]");
        CompareQueryResult(kvCacheReader, "1", 0, false, "");
    }
}

void KVTabletCacheReaderTest::CompareQueryResult(const KVIndexReaderPtr& kvIndexReader, const string& keyStr,
                                                 uint64_t ts, bool exist, const string& expectedValue)
{
    future_lite::executors::SimpleExecutor ex(1);
    autil::StringView key(keyStr);
    vector<autil::StringView> keys = {key};
    autil::StringView value;
    KVMetricsCollector collector;
    KVReadOptions readOptions;
    readOptions.timestamp = ts;
    readOptions.searchCacheType = indexlib::tsc_default;
    readOptions.pool = _pool;
    readOptions.metricsCollector = &collector;
    if (exist) {
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
        ASSERT_EQ(future_lite::interface::syncAwait(kvIndexReader->GetAsync(key, value, readOptions), &ex),
                  KVResultStatus::NOT_FOUND);
    }
}

void KVTabletCacheReaderTest::CheckCacheResult(const string& keyStr, bool exist, const string& expectedValue,
                                               const string& expectedLocatorStr)
{
    keytype_t hashKey = 0;
    ASSERT_TRUE(indexlib::util::GetHashKey(hft_murmur, autil::StringView(keyStr), hashKey));
    indexlib::util::SearchCachePartitionWrapperPtr cacheWrapper(
        _searchCacheWrapper->CreateExclusiveCacheWrapper("0@string1:string2;"));
    std::unique_ptr<indexlib::util::CacheItemGuard> cacheItemGuard = cacheWrapper->Get(hashKey, 0, nullptr);
    if (!exist) {
        ASSERT_FALSE(cacheItemGuard);
    } else {
        ASSERT_TRUE(cacheItemGuard);
        auto kvCacheItem = cacheItemGuard->GetCacheItem<KVCacheItem>();
        bool hasValue = kvCacheItem->HasValue();
        auto value = kvCacheItem->GetValue();
        auto locator = kvCacheItem->GetLocator();
        if (expectedValue.empty()) {
            ASSERT_FALSE(hasValue);
            ASSERT_TRUE(value.empty());
            ASSERT_EQ(expectedLocatorStr, locator->DebugString());
        } else {
            ASSERT_TRUE(hasValue);
            ASSERT_FALSE(value.empty());
            ASSERT_GT(value.size(), expectedValue.length());
            ASSERT_NE(value.to_string().find(expectedValue), std::string::npos);
            ASSERT_EQ(expectedLocatorStr, locator->DebugString());
        }
    }
}

std::shared_ptr<KVCacheReaderImpl> KVTabletCacheReaderTest::GetTabletReader(const table::KVTableTestHelper& helper)
{
    auto tablet = helper.GetITablet();
    auto reader = tablet->GetTabletReader();
    auto kvTabletReader = std::dynamic_pointer_cast<table::KVTabletSessionReader>(reader);
    auto kvCacheReader = kvTabletReader->GetIndexReader<KVCacheReaderImpl>("kv", "string1");
    kvCacheReader->SetSearchCache(_searchCacheWrapper);
    return kvCacheReader;
}

}} // namespace indexlibv2::table
