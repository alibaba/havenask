#include "indexlib/partition/test/search_cache_intetest.h"

#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/index/kkv/kkv_reader.h"
#include "indexlib/index/kv/kv_reader.h"

using namespace std;
using namespace autil::mem_pool;
using namespace autil;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::util;
using namespace indexlib::common;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, SearchCacheTest);

SearchCacheTest::SearchCacheTest() {}

SearchCacheTest::~SearchCacheTest() {}

void SearchCacheTest::CaseSetUp()
{
    mOptions = IndexPartitionOptions();
    mOptions.GetBuildConfig(false).keepVersionCount = 100;
    mOptions.GetBuildConfig(true).buildTotalMemory = 22;
    mOptions.GetBuildConfig(false).buildTotalMemory = 40;

    mKVSchema = SchemaMaker::MakeKVSchema("key:int32;value:uint64;", "key", "value");
    mKKVSchema = SchemaMaker::MakeKKVSchema("pkey:string;skey:int32;value:uint32;", "pkey", "skey", "value;skey;");
    util::SearchCachePtr searchCache(
        new util::SearchCache(1024 * 1024 * 1024, util::MemoryQuotaControllerPtr(), util::TaskSchedulerPtr(), NULL, 6));
    mResource.searchCache.reset(new util::SearchCachePartitionWrapper(searchCache, ""));
}

void SearchCacheTest::CaseTearDown() {}

void SearchCacheTest::TestKVSearchWithOnlyInc()
{
    PartitionStateMachine psm(false, mResource);
    mOptions.GetOnlineConfig().ttl = 2;
    mKVSchema->SetEnableTTL(true);
    ASSERT_TRUE(psm.Init(mKVSchema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,key=1,value=1,ts=1000000;"
                       "cmd=add,key=2,value=2,ts=1000000;"
                       "cmd=delete,key=2,ts=3000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    SearchCachePartitionWrapperPtr searchCache = psm.GetIndexPartitionResource().searchCache;
    ASSERT_TRUE(searchCache);
    ASSERT_EQ((size_t)0, searchCache->GetUsage());
    // search from index, keys will be in cache after search
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));
    ASSERT_LT((size_t)0, searchCache->GetUsage());

    // search from cache
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1#4", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));

    ASSERT_TRUE(psm.Transfer(QUERY_ONLY_CACHE, "", "1", "value=1"));
    string incDocStr = "cmd=add,key=1,value=2,ts=4000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocStr, "", ""));

    ASSERT_TRUE(psm.Transfer(QUERY_NO_CACHE, "", "1", "value=2"));
    // no rt, inc cover all rt
    ASSERT_TRUE(psm.Transfer(QUERY_ONLY_CACHE, "", "1", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=2"));
    ASSERT_TRUE(psm.Transfer(QUERY_ONLY_CACHE, "", "1", "value=2"));
}

void SearchCacheTest::TestKVSearchCacheRtLackSomeDoc()
{
    PartitionStateMachine psm(false, mResource);
    ASSERT_TRUE(psm.Init(mKVSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT,
                             "cmd=add,key=1,value=1,ts=1000000,locator=0:1000000;"
                             "cmd=add,key=2,value=2,ts=3000000,locator=0:1000000",
                             "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=add,key=2,value=1,ts=4000000,locator=0:1000000", "1", "value=1"));
    ASSERT_TRUE(psm.Transfer(
        BUILD_INC,
        "cmd=add,key=1,value=1,ts=1000000,locator=0:1000000;cmd=add,key=1,value=2,ts=2000000,locator=0:2000000", "1",
        "value=2"));
}

void SearchCacheTest::TestKVSearchWithIncRtBuilding()
{
    PartitionStateMachine psm(false, mResource);
    mOptions.GetOnlineConfig().ttl = 2;
    mKVSchema->SetEnableTTL(true);
    ASSERT_TRUE(psm.Init(mKVSchema, mOptions, GET_TEMP_DATA_PATH()));
    string fullDocString = "cmd=add,key=1,value=1,ts=1000000;"
                           "cmd=add,key=2,value=2,ts=1000000;"
                           "cmd=add,key=3,value=3,ts=1000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "", ""));
    string rtDocString = "cmd=add,key=1,value=2,ts=2000000;"
                         "cmd=delete,key=2,ts=2000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocString, "", ""));
    string buildingDocString = "cmd=add,key=1,value=3,ts=3000000;"
                               "cmd=delete,key=3,ts=3000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, buildingDocString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=3;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1#6", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));

    // building delete cache
    ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=delete,key=1,ts=4000000", "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", ""));
}

void SearchCacheTest::TestKVSearchAfterDumpRtSegment()
{
    PartitionStateMachine psm(false, mResource);
    ASSERT_TRUE(psm.Init(mKVSchema, mOptions, GET_TEMP_DATA_PATH()));
    string fullDocString = "cmd=add,key=1,value=1,ts=1000000;"
                           "cmd=add,key=2,value=2,ts=1000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "", ""));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "value=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));

    string rtDocString = "cmd=add,key=1,value=2,ts=2000000;"
                         "cmd=delete,key=2,value=2,ts=2000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocString, "", ""));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));
}

void SearchCacheTest::TestKVSearchFromBuilding()
{
    PartitionStateMachine psm(false, mResource);
    ASSERT_TRUE(psm.Init(mKVSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,key=1,value=1,ts=1000000", "", ""));
    // key 1 is in cache after query
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"));

    // building segment has newer value
    ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=add,key=1,value=2,ts=2000000", "", ""));
    // search from building
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=2"));
}

void SearchCacheTest::TestKVIncPartlyCoverBuildingSegment()
{
    PartitionStateMachine psm(false, mResource);
    ASSERT_TRUE(psm.Init(mKVSchema, mOptions, GET_TEMP_DATA_PATH()));

    string fullStr = "cmd=add,key=1,value=1,ts=1000000;"
                     "cmd=add,key=2,value=2,ts=2000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullStr, "1", "value=1"));

    string rtStr = "cmd=add,key=1,value=2,ts=3000000;"
                   "cmd=add,key=2,value=4,ts=4000000;"
                   "cmd=delete,key=2,ts=5000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtStr, "1", "value=2"));

    string incStr = "cmd=add,key=1,value=2,ts=3000000;"
                    "cmd=add,key=2,value=4,ts=4000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=2"));
}

void SearchCacheTest::TestKVCacheItemBeenInvalidated()
{
    PartitionStateMachine psm(false, mResource);
    ASSERT_TRUE(psm.Init(mKVSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,key=1,value=1,ts=1000000", "", ""));
    // key 1 is in cache after query
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=1"));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,key=2,value=2,ts=2000000", "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,key=3,value=3,ts=3000000", "", ""));
    // inc segment cover rt  has newer value
    ASSERT_TRUE(psm.Transfer(BUILD_INC, "cmd=add,key=1,value=2,ts=2000000", "", ""));
    // cache item been invalidated, search from index
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "value=2"));
}

void SearchCacheTest::TestKVSearchNotFoundKeyInCache()
{
    PartitionStateMachine psm(false, mResource);
    ASSERT_TRUE(psm.Init(mKVSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,key=1,value=1,ts=1000000", "", ""));
    // key 10 is in cache after query
    ASSERT_TRUE(psm.Transfer(QUERY, "", "10", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,key=2,value=2,ts=2000000", "", ""));
    // search newer rt segments
    ASSERT_TRUE(psm.Transfer(QUERY, "", "10", ""));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "20", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "20", ""));
}

void SearchCacheTest::TestKVSearchDeletedKeyInCache()
{
    PartitionStateMachine psm(false, mResource);
    ASSERT_TRUE(psm.Init(mKVSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,key=1,value=1,ts=1000000", "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=delete,key=1,ts=2000000", "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", ""));
}

void SearchCacheTest::GetKVValue(PartitionStateMachine& psm, const string& key, StringView& value,
                                 autil::mem_pool::Pool* pool)
{
    auto indexPartition = psm.GetIndexPartition();
    auto reader = indexPartition->GetReader();
    auto kvReader = reader->GetKVReader();

    ASSERT_TRUE(future_lite::interface::syncAwait(
        kvReader->GetAsync(StringView("0"), value, 1000000, TableSearchCacheType::tsc_default, pool)));
}

// TODO change case name
// run with valgrind, expect no invalid read found
void SearchCacheTest::TestKVSearchCacheExpired()
{
    StringView oldValue;
    Pool pool;

    util::SearchCachePtr searchCache(
        new util::SearchCache(1024 * 1024, util::MemoryQuotaControllerPtr(), util::TaskSchedulerPtr(), NULL, 6));
    mResource.searchCache.reset(new util::SearchCachePartitionWrapper(searchCache, ""));
    PartitionStateMachine psm(false, mResource);
    mKVSchema = SchemaMaker::MakeKVSchema("key:int32;value:string", "key", "value");
    ASSERT_TRUE(psm.Init(mKVSchema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "";

    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,key=0,value=hello,ts=1000000", "", ""));

    GetKVValue(psm, "0", oldValue, &pool);

    GetKVValue(psm, "0", oldValue, &pool); // get from cache

    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,key=0,value=world,ts=1000000", "", ""));

    StringView newValue;

    GetKVValue(psm, "0", newValue, &pool); // envict old value from lru cache

    // cerr << "VALUE: [ " << oldValue <<"]" <<endl; // value addr should be valid

    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,key=3,value=helloworld,ts=1000000", "", ""));
    GetKVValue(psm, "0", oldValue, &pool);
    GetKVValue(psm, "0", oldValue, &pool);

    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,key=0,value=helloworld,ts=1000000", "", ""));

    GetKVValue(psm, "0", newValue, &pool);

    // cerr << "VALUE: [ " << oldValue <<"]" <<endl; // value addr should be valid
}

void SearchCacheTest::TestKVSearchWithTTL()
{
    PartitionStateMachine psm(false, mResource);
    mOptions.GetOnlineConfig().ttl = 20;
    mKVSchema->SetEnableTTL(true);
    ASSERT_TRUE(psm.Init(mKVSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,key=1,value=1,ts=10000000", "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,key=2,value=2,ts=20000000", "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1#20", "value=1;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2#20", "value=2;"));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "1#35", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2#35", "value=2"));
}

void SearchCacheTest::TestKKVSearchWithOnlyInc()
{
    PartitionStateMachine psm(false, mResource);
    mOptions.GetOnlineConfig().ttl = 2;
    ASSERT_TRUE(psm.Init(mKKVSchema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,pkey=1,skey=1,value=1,ts=1000000;"
                       "cmd=add,pkey=1,skey=2,value=2,ts=1000000;"
                       "cmd=add,pkey=2,skey=1,value=1,ts=1000000;"
                       "cmd=delete,pkey=2,ts=1000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    SearchCachePartitionWrapperPtr searchCache = psm.GetIndexPartitionResource().searchCache;
    ASSERT_TRUE(searchCache);
    ASSERT_EQ((size_t)0, searchCache->GetUsage());
    // search from index
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "skey=1,value=1,ts=1000000;skey=2,value=2,ts=1000000"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));
    ASSERT_LT((size_t)0, searchCache->GetUsage());
    // search from cache
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "skey=1,value=1,ts=1000000;skey=2,value=2,ts=1000000"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1#4", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));
    ASSERT_TRUE(psm.Transfer(QUERY_ONLY_CACHE, "", "1", "skey=1,value=1,ts=1000000;skey=2,value=2,ts=1000000"));

    string incDocString = "cmd=add,pkey=1,skey=3,value=3,ts=2000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY_NO_CACHE, "", "1",
                             "skey=1,value=1,ts=1000000;skey=2,value=2,ts=1000000;skey=3,value=3,ts=2000000"));
    // no rt, inc cover all rt
    ASSERT_TRUE(psm.Transfer(QUERY_ONLY_CACHE, "", "1", ""));
    ASSERT_TRUE(
        psm.Transfer(QUERY, "", "1", "skey=1,value=1,ts=1000000;skey=2,value=2,ts=1000000;skey=3,value=3,ts=2000000"));
    ASSERT_TRUE(psm.Transfer(QUERY_ONLY_CACHE, "", "1",
                             "skey=1,value=1,ts=1000000;skey=2,value=2,ts=1000000;skey=3,value=3,ts=2000000"));
}

void SearchCacheTest::TestKKVSearchWithIncRtBuilding()
{
    PartitionStateMachine psm(false, mResource);
    mOptions.GetOnlineConfig().ttl = 2;
    ASSERT_TRUE(psm.Init(mKKVSchema, mOptions, GET_TEMP_DATA_PATH()));
    string fullDocString = "cmd=add,pkey=1,skey=1,value=1,ts=1000000;"
                           "cmd=add,pkey=2,skey=1,value=1,ts=1000000;"
                           "cmd=add,pkey=3,skey=1,value=1,ts=1000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "", ""));
    string rtDocString = "cmd=add,pkey=1,skey=2,value=2,ts=2000000;"
                         "cmd=delete,pkey=2,ts=2000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocString, "", ""));
    string buildingDocString = "cmd=add,pkey=1,skey=3,value=3,ts=3000000;"
                               "cmd=delete,pkey=3,ts=3000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, buildingDocString, "", ""));
    SearchCachePartitionWrapperPtr searchCache = psm.GetIndexPartitionResource().searchCache;
    ASSERT_TRUE(searchCache);
    ASSERT_EQ((size_t)0, searchCache->GetUsage());
    // search from index
    ASSERT_TRUE(
        psm.Transfer(QUERY, "", "1", "skey=1,value=1,ts=1000000;skey=2,value=2,ts=2000000;skey=3,value=3,ts=3000000"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));
    ASSERT_LT((size_t)0, searchCache->GetUsage());
    // search from cache
    ASSERT_TRUE(
        psm.Transfer(QUERY, "", "1", "skey=1,value=1,ts=1000000;skey=2,value=2,ts=2000000;skey=3,value=3,ts=3000000"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1#5", "skey=3,value=3,ts=3000000"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));

    // building delete cache
    ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=delete,pkey=1,skey=1,ts=4000000", "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "skey=2,value=2,ts=2000000;skey=3,value=3,ts=3000000"));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=delete,pkey=1,ts=4000000", "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", ""));
}

void SearchCacheTest::TestKKVSearchAfterDumpRtSegment()
{
    PartitionStateMachine psm(false, mResource);
    mOptions.GetOnlineConfig().ttl = 2;
    ASSERT_TRUE(psm.Init(mKKVSchema, mOptions, GET_TEMP_DATA_PATH()));
    string fullDocString = "cmd=add,pkey=1,skey=1,value=1,ts=1000000;"
                           "cmd=add,pkey=2,skey=1,value=1,ts=1000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "", ""));

    // put cache
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "skey=1,value=1,ts=1000000"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "skey=1,value=1,ts=1000000"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));

    string rtDocString = "cmd=add,pkey=1,skey=2,value=2,ts=2000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocString, "", ""));

    string rtDocString2 = "cmd=add,pkey=1,skey=3,value=3,ts=3000000;"
                          "cmd=delete,pkey=2,ts=3000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocString2, "", ""));

    ASSERT_TRUE(
        psm.Transfer(QUERY, "", "1", "skey=1,value=1,ts=1000000;skey=2,value=2,ts=2000000;skey=3,value=3,ts=3000000"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1#5", "skey=3,value=3,ts=3000000"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));

    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=delete,pkey=1,skey=1,ts=4000000", "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "skey=2,value=2,ts=2000000;skey=3,value=3,ts=3000000"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1#5", "skey=3,value=3,ts=3000000"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=delete,pkey=1,ts=4000000", "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", ""));
}

void SearchCacheTest::TestKKVSearchFromBuilding()
{
    mOptions.GetOfflineConfig().fullIndexStoreKKVTs = true;
    PartitionStateMachine psm(false, mResource);
    ASSERT_TRUE(psm.Init(mKKVSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, "cmd=add,pkey=1,skey=1,value=1,ts=1000000", "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_INC, "cmd=add,pkey=1,skey=2,value=2,ts=2000000", "", ""));
    // search from index
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "skey=1,value=1,ts=1000000;skey=2,value=2,ts=2000000"));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=add,pkey=1,skey=1,value=11,ts=11000000", "", ""));
    // search from cache and newer rt segments
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "skey=1,value=11,ts=11000000;skey=2,value=2,ts=2000000;"));
}

void SearchCacheTest::TestKKVCacheItemBeenInvalidated()
{
    PartitionStateMachine psm(false, mResource);
    ASSERT_TRUE(psm.Init(mKKVSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,pkey=1,skey=1,value=1,ts=1000000", "", ""));
    // skey 1 is in cache after query
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "skey=1,value=1;"));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,pkey=1,skey=2,value=2,ts=2000000", "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,pkey=1,skey=3,value=3,ts=3000000", "", ""));
    // inc segment cover rt  has newer value
    ASSERT_TRUE(psm.Transfer(BUILD_INC, "cmd=add,pkey=1,skey=1,value=2,ts=2000000", "", ""));
    // cache item been invalidated, search from index
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "skey=1,value=2,ts=2000000;skey=3,value=3,ts=3000000;"));
}

void SearchCacheTest::TestKKVIncCoverRtWithCacheItemValid()
{
    PartitionStateMachine psm(false, mResource);
    ASSERT_TRUE(psm.Init(mKKVSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,pkey=1,skey=1,value=1,ts=1000000;", "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT,
                             "cmd=add,pkey=1,skey=2,value=2,ts=2000000;"
                             "cmd=add,pkey=1,skey=3,value=3,ts=3000000;",
                             "", ""));

    // skey 1 is in cache after query
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1",
                             "skey=1,value=1,ts=1000000;"
                             "skey=2,value=2,ts=2000000;"
                             "skey=3,value=3,ts=3000000;"));
    string incDoc = "cmd=add,pkey=1,skey=1,value=11,ts=1000000;"
                    "cmd=add,pkey=1,skey=2,value=22,ts=2000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "", ""));
    // cache is valid, so value 2 is not been filtered.
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1",
                             "skey=1,value=1,ts=1000000;"
                             "skey=2,value=2,ts=2000000;"
                             "skey=3,value=3,ts=3000000;"));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "", "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1",
                             "skey=1,value=1,ts=1000000;"
                             "skey=2,value=2,ts=2000000;"
                             "skey=3,value=3,ts=3000000;"));
    // inc cover all rt
    ASSERT_TRUE(psm.Transfer(BUILD_INC, "cmd=add,pkey=1,skey=3,value=33,ts=3000000;", "", ""));
    // cache item is invalidated
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1",
                             "skey=1,value=11,ts=1000000;"
                             "skey=2,value=22,ts=2000000;"
                             "skey=3,value=33,ts=3000000;"));
}

void SearchCacheTest::TestKKVIncCoverAllRt()
{
    PartitionStateMachine psm(false, mResource);
    ASSERT_TRUE(psm.Init(mKKVSchema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,pkey=1,skey=1,value=1,ts=1000000;"
                       "cmd=add,pkey=2,skey=1,value=1,ts=1000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, docString, "", ""));
    // pkey 1 and pkey2 in cache after query
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "skey=1,value=1,ts=1000000;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "skey=1,value=1,ts=1000000;"));

    ASSERT_TRUE(psm.Transfer(BUILD_INC, "cmd=add,pkey=1,skey=1,value=9,ts=9000000", "", ""));
    // cache item been invalidated, search from index
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "skey=1,value=9,ts=9000000;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
}

void SearchCacheTest::TestKKVIncCoverAllRtWithBuilding()
{
    PartitionStateMachine psm(false, mResource);
    ASSERT_TRUE(psm.Init(mKKVSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,pkey=1,skey=1,value=1,ts=1000000", "", ""));
    // skey 1 is in cache after query
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "skey=1,value=1,ts=1000000;"));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=add,pkey=1,skey=2,value=2,ts=2000000", "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_INC, "cmd=add,pkey=1,skey=1,value=9,ts=9000000", "", ""));
    // cache item been invalidated, search from index
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "skey=1,value=9,ts=9000000;"));
}

void SearchCacheTest::TestKKVIncPartlyCoverBuildingSegment()
{
    PartitionStateMachine psm(false, mResource);
    ASSERT_TRUE(psm.Init(mKKVSchema, mOptions, GET_TEMP_DATA_PATH()));

    string fullStr = "cmd=add,pkey=1,skey=1,value=1,ts=1000000,locator=1:1000000;"
                     "cmd=add,pkey=2,skey=2,value=2,ts=2000000,locator=1:2000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullStr, "1", "skey=1,value=1"));

    string rtStr = "cmd=add,pkey=1,skey=1,value=2,ts=3000000,locator=1:3000000;"
                   "cmd=add,pkey=2,skey=3,value=4,ts=4000000,locator=1:4000000;"
                   "cmd=delete,pkey=2,skey=2,ts=5000000,locator=1:5000000;"
                   "cmd=add,pkey=3,skey=3,value=3,ts=9000000,locator=1:5000000;"
                   "cmd=add,pkey=4,skey=4,value=4,ts=7000000,locator=1:5000000;";

    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtStr, "1", "skey=1,value=2,ts=3000000"));

    string incStr = "cmd=add,pkey=1,skey=1,value=2,ts=3000000,locator=1:3000000;"
                    "cmd=add,pkey=2,skey=3,value=4,ts=4000000,locator=1:4000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "skey=1,value=2;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "skey=3,value=4"));

    string incStr1 = "cmd=delete,pkey=2,skey=2,ts=5000000,locator=1:5000000;"
                     "cmd=add,pkey=2,skey=7,value=7,ts=8000000,locator=1:6000000;"
                     "cmd=add,pkey=3,skey=3,value=3,ts=6000000,locator=1:6000000;"
                     "cmd=add,pkey=4,skey=4,value=4,ts=7000000,locator=1:5000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incStr1, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "skey=3,value=4;skey=7,value=7;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "skey=3,value=3,ts=9000000;"));
}

void SearchCacheTest::TestKKVSearchNotFoundKeyInCache()
{
    PartitionStateMachine psm(false, mResource);
    ASSERT_TRUE(psm.Init(mKKVSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,pkey=1,skey=1,value=1,ts=1000000", "", ""));
    // skey 10 is in cache after query
    ASSERT_TRUE(psm.Transfer(QUERY, "", "10", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,pkey=1,skey=2,value=2,ts=2000000", "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "10", ""));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "20", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "20", ""));
}

void SearchCacheTest::TestKKVSearchDeletedKeyInCache()
{
    PartitionStateMachine psm(false, mResource);
    ASSERT_TRUE(psm.Init(mKKVSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,pkey=1,skey=1,value=1,ts=1000000", "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=delete,pkey=1,ts=2000000", "", ""));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", ""));
}

void SearchCacheTest::TestKKVSearchWithSKey()
{
    PartitionStateMachine psm(false, mResource);
    ASSERT_TRUE(psm.Init(mKKVSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,pkey=1,skey=1,value=1,ts=1000000", "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,pkey=1,skey=2,value=2,ts=2000000", "", ""));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "1:1", "skey=1,value=1,ts=1000000;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1:2", "skey=2,value=2,ts=2000000;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "skey=1,value=1,ts=1000000;skey=2,value=2,ts=2000000;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1:1", "skey=1,value=1,ts=1000000;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1:2", "skey=2,value=2,ts=2000000;"));
}

void SearchCacheTest::TestKKVSearchWithTTL()
{
    PartitionStateMachine psm(false, mResource);
    mOptions.GetOnlineConfig().ttl = 20;
    ASSERT_TRUE(psm.Init(mKKVSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,pkey=1,skey=1,value=1,ts=10000000", "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,pkey=1,skey=2,value=2,ts=20000000", "", ""));
    // skey 1 and skey 2 will be in cache after search
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1#20", "skey=1,value=1,ts=10000000;skey=2,value=2,ts=20000000;"));
    // skey 1 will be filter when searching from cache
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1#35", "skey=2,value=2,ts=20000000;"));
}

void SearchCacheTest::TestKKVSearchBuiltSegmentHasDeletedPKey()
{
    PartitionStateMachine psm(false, mResource);
    ASSERT_TRUE(psm.Init(mKKVSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,pkey=1,skey=1,value=1,ts=1000000", "", ""));
    // skey 1 will be in cache after search
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "skey=1,value=1,ts=1000000"));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=delete,pkey=1,ts=20000000", "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", ""));
}

void SearchCacheTest::TestKKVSearchBuiltSegmentHasDeletedSKey()
{
    PartitionStateMachine psm(false, mResource);
    ASSERT_TRUE(psm.Init(mKKVSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,pkey=1,skey=1,value=1,ts=1000000", "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,pkey=1,skey=2,value=2,ts=2000000", "", ""));
    // skey 1 and skey 2 will be in cache after search
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "skey=1,value=1,ts=1000000;skey=2,value=2,ts=2000000"));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=delete,pkey=1,skey=1,ts=3000000", "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "skey=2,value=2,ts=2000000"));
}

void SearchCacheTest::TestKKVSearchWithTruncateLimits()
{
    KKVIndexFieldInfo suffixFieldInfo;
    suffixFieldInfo.fieldName = "skey";
    suffixFieldInfo.keyType = KKVKeyType::SUFFIX;
    suffixFieldInfo.countLimits = 2;
    suffixFieldInfo.sortParams = StringToSortParams("-$TIME_STAMP");

    const IndexSchemaPtr& indexSchema = mKKVSchema->GetIndexSchema();
    KKVIndexConfigPtr kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    kkvConfig->SetSuffixFieldInfo(suffixFieldInfo);

    mOptions.GetOnlineConfig().ttl = 20;
    PartitionStateMachine psm(false, mResource);
    ASSERT_TRUE(psm.Init(mKKVSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,pkey=1,skey=1,value=1,ts=10000000", "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,pkey=1,skey=2,value=2,ts=20000000", "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,pkey=1,skey=3,value=3,ts=30000000", "", ""));

    // search built : skey2 and skey3 in cache
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "skey=2,ts=20000000;skey=3,ts=30000000;"));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=add,pkey=1,skey=4,value=4,ts=40000000", "", ""));
    // search building and cache : skey4 from building and skey3 from cache
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "skey=3,ts=30000000;skey=4,ts=40000000;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1#55", "skey=4,ts=40000000;"));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=delete,pkey=1,skey=3,ts=50000000", "", ""));
    // search building and cache : skey4 from building and skey2 from cache
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "skey=2,ts=20000000;skey=4,ts=40000000;"));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=delete,pkey=1,skey=4,ts=60000000", "", ""));
    // search building and cache : nothing from building and skey2 from cache
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "skey=2,ts=20000000;"));
}

void SearchCacheTest::TestKKVSearchWithKeepSortSequence()
{
    KKVIndexFieldInfo suffixFieldInfo;
    suffixFieldInfo.fieldName = "skey";
    suffixFieldInfo.keyType = KKVKeyType::SUFFIX;
    suffixFieldInfo.sortParams = StringToSortParams("-$TIME_STAMP");
    suffixFieldInfo.enableKeepSortSequence = true;

    const IndexSchemaPtr& indexSchema = mKKVSchema->GetIndexSchema();
    KKVIndexConfigPtr kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    kkvConfig->SetSuffixFieldInfo(suffixFieldInfo);

    PartitionStateMachine psm(false, mResource);

    IndexPartitionOptions options = mOptions;
    options.GetBuildConfig(false).shardingColumnNum = 2;
    options.GetBuildConfig(false).levelTopology = indexlibv2::framework::topo_hash_mod;
    options.GetBuildConfig(false).levelNum = 2;
    ASSERT_TRUE(psm.Init(mKKVSchema, options, GET_TEMP_DATA_PATH()));

    string fullDocs = "cmd=add,pkey=1,skey=1,value=1,ts=10000000;"
                      "cmd=add,pkey=1,skey=2,value=2,ts=20000000;"
                      "cmd=add,pkey=1,skey=3,value=3,ts=30000000;";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocs, "", ""));

    OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(OnlinePartition, psm.GetIndexPartition());
    KKVReaderPtr kkvReader = onlinePart->GetReader()->GetKKVReader();
    ASSERT_FALSE(kkvReader->GetSortParams().empty());

    string pkey = "1";
    StringView pkeyStr(pkey);
    CheckKKVReader(kkvReader, pkeyStr, "", "3,3;2,2;1,1", tsc_default);
    CheckKKVReader(kkvReader, pkeyStr, "", "3,3;2,2;1,1", tsc_only_cache);

    CheckKKVReader(kkvReader, pkeyStr, "1", "1,1", tsc_no_cache);
    CheckKKVReader(kkvReader, pkeyStr, "2,3", "3,3;2,2", tsc_no_cache);
}

void SearchCacheTest::CheckKKVReader(const KKVReaderPtr& kkvReader, StringView& pkeyStr, const string& skeyInfos,
                                     const string& expectResult, TableSearchCacheType type)
{
    const IndexSchemaPtr& indexSchema = mKKVSchema->GetIndexSchema();
    KKVIndexConfigPtr kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    ValueConfigPtr valueConfig = kkvConfig->GetValueConfig();
    PackAttributeFormatterPtr formatter(new PackAttributeFormatter);
    formatter->Init(valueConfig->CreatePackAttributeConfig());
    AttributeReferenceTyped<uint32_t>* valueRef = formatter->GetAttributeReferenceTyped<uint32_t>("value");
    assert(valueRef);

    vector<string> skeyVec;
    StringUtil::fromString(skeyInfos, skeyVec, ",");

    Pool pool;
    KKVIterator* iter = NULL;
    if (skeyVec.empty()) {
        iter = future_lite::interface::syncAwait(kkvReader->LookupAsync(pkeyStr, 0, type, &pool));
    } else {
        vector<StringView> skeyStrVec;
        for (size_t i = 0; i < skeyVec.size(); i++) {
            skeyStrVec.push_back(StringView(skeyVec[i]));
        }
        iter = future_lite::interface::syncAwait(kkvReader->LookupAsync(pkeyStr, skeyStrVec, 0, type, &pool));
    }
    ASSERT_TRUE(iter);

    vector<vector<uint64_t>> resultInfos;
    StringUtil::fromString(expectResult, resultInfos, ",", ";");

    for (size_t i = 0; i < resultInfos.size(); i++) {
        assert(resultInfos[i].size() == 2);
        ASSERT_TRUE(iter->IsValid());
        ASSERT_EQ(resultInfos[i][0], iter->GetCurrentSkey());

        StringView valueStr;
        iter->GetCurrentValue(valueStr);

        uint32_t value;
        valueRef->GetValue(valueStr.data(), value);
        ASSERT_EQ(resultInfos[i][1], (uint64_t)value);
        iter->MoveToNext();
    }
    ASSERT_FALSE(iter->IsValid());
    POOL_COMPATIBLE_DELETE_CLASS(&pool, iter);
}

void SearchCacheTest::TestKKVSearchCacheExpired()
{
    StringView value;
    Pool pool;

    mOptions.GetOnlineConfig().ttl = DEFAULT_TIME_TO_LIVE;
    util::SearchCachePtr searchCache(
        new util::SearchCache(1024 * 1024, util::MemoryQuotaControllerPtr(), util::TaskSchedulerPtr(), NULL, 6));
    mResource.searchCache.reset(new util::SearchCachePartitionWrapper(searchCache, ""));
    PartitionStateMachine psm(false, mResource);
    mKKVSchema = SchemaMaker::MakeKKVSchema("pkey:string;skey:int32;value:string;", "pkey", "skey", "value;skey;");
    ASSERT_TRUE(psm.Init(mKKVSchema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "";

    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,pkey=0,skey=0,value=hello,ts=1000000", "", ""));

    auto indexPartition = psm.GetIndexPartition();
    auto reader = indexPartition->GetReader();
    auto kkvReader = reader->GetKKVReader();

    std::unique_ptr<KKVIterator, std::function<void(KKVIterator*)>> iter(
        nullptr, [&pool](KKVIterator* p) { IE_POOL_COMPATIBLE_DELETE_CLASS(&pool, p); });

    iter.reset(future_lite::interface::syncAwait(
        kkvReader->LookupAsync(StringView("0"), 0, TableSearchCacheType::tsc_default, &pool)));
    ASSERT_TRUE(iter.get());
    ASSERT_TRUE(iter->IsValid());
    iter->GetCurrentValue(value);

    // cerr << "VALUE: [ " << value <<"]"<< endl;

    iter.reset(future_lite::interface::syncAwait(
        kkvReader->LookupAsync(StringView("0"), 0, TableSearchCacheType::tsc_default, &pool)));
    ASSERT_TRUE(iter.get());
    ASSERT_TRUE(iter->IsValid());
    iter->GetCurrentValue(value);

    // cerr << "VALUE: [ " << value <<"]"<< endl;

    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "cmd=add,pkey=0,skey=1,value=world,ts=1000000", "", ""));

    indexPartition = psm.GetIndexPartition();
    reader = indexPartition->GetReader();
    kkvReader = reader->GetKKVReader();

    StringView newValue;
    iter.reset(future_lite::interface::syncAwait(
        kkvReader->LookupAsync(StringView("0"), 0, TableSearchCacheType::tsc_default, &pool)));
    ASSERT_TRUE(iter.get());
    ASSERT_TRUE(iter->IsValid());
    iter->GetCurrentValue(newValue);
    // cerr << "VALUE: [ " << value <<"]" <<endl;
}

void SearchCacheTest::TestDirtyCache()
{
    auto searchCache = mResource.searchCache->GetSearchCache();
    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    mOptions.GetOnlineConfig().loadRemainFlushRealtimeIndex = true;
    mResource.searchCache.reset(new util::SearchCachePartitionWrapper(searchCache, ""));
    {
        PartitionStateMachine psm(false, mResource);
        ASSERT_TRUE(psm.Init(mKKVSchema, mOptions, GET_TEMP_DATA_PATH()));
        string docString = "cmd=add,pkey=1,skey=1,value=1,ts=1000000;";
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        // skey=1 in cache now
        ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "skey=1,value=1"));
        ASSERT_TRUE(psm.Transfer(QUERY_ONLY_CACHE, "", "1", "skey=1,value=1"));

        string rtDocs = "cmd=add,pkey=1,skey=2,value=2,ts=2000000;";
        // "cmd=add,pkey=2,skey=2,value=2,ts=4000000;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocs, "", ""));
        docString = "cmd=add,pkey=1,skey=2,value=2,ts=2000000;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC, docString, "", ""));
    }
    mResource.searchCache.reset(new util::SearchCachePartitionWrapper(searchCache, ""));
    {
        PartitionStateMachine psm(false, mResource);
        ASSERT_TRUE(psm.Init(mKKVSchema, mOptions, GET_TEMP_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "skey=1,value=1;skey=2,value=2"));
        ASSERT_TRUE(psm.Transfer(QUERY_ONLY_CACHE, "", "1", "skey=1,value=1;skey=2,value=2"));
    }
}

void SearchCacheTest::TestBug21328825()
{
    auto searchCache = mResource.searchCache->GetSearchCache();
    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    mOptions.GetOnlineConfig().loadRemainFlushRealtimeIndex = true;
    mOptions.GetBuildConfig(false).levelNum = 3;
    mResource.searchCache.reset(new util::SearchCachePartitionWrapper(searchCache, ""));
    mOptions.GetMergeConfig().mergeStrategyStr = "shard_based";
    mOptions.GetMergeConfig().mergeStrategyParameter.strategyConditions = "space_amplification=997.3";

    PartitionStateMachine psm(false, mResource);
    ASSERT_TRUE(psm.Init(mKKVSchema, mOptions, GET_TEMP_DATA_PATH()));
    string docString = "cmd=add,pkey=2,skey=2,value=2,ts=1000000;";
    // IncTs = 1
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));

    docString = "cmd=delete,pkey=1,ts=4000000;"
                "cmd=add,pkey=1,skey=1,value=1,ts=5000000;"
                "cmd=add,pkey=71,skey=1,value=1,ts=7000000;";

    ASSERT_TRUE(psm.Transfer(BUILD_RT, docString, "", ""));

    docString = "cmd=add,pkey=21,skey=1,value=1,ts=9000000;";

    // rt ts = 9
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, docString, "", ""));

    docString = "cmd=delete,pkey=1,ts=4000000;"
                "cmd=add,pkey=1,skey=1,value=1,ts=5000000;"
                "cmd=add,pkey=71,skey=1,value=1,ts=7000000;";

    // inc ts = 7
    EXPECT_TRUE(psm.Transfer(BUILD_INC, docString, "", ""));

    EXPECT_TRUE(psm.Transfer(QUERY, "", "1", "skey=1,value=1"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "1", "skey=1,value=1"));

    docString = "cmd=add,pkey=101,skey=1,value=1,ts=988100000;";
    EXPECT_TRUE(psm.Transfer(BUILD_RT, docString, "", ""));

    docString = "cmd=add,pkey=71,skey=1,value=1,ts=88100000;";
    EXPECT_TRUE(psm.Transfer(BUILD_INC, docString, "", ""));

    EXPECT_TRUE(psm.Transfer(QUERY, "", "1", "skey=1,value=1"));
    EXPECT_TRUE(psm.Transfer(QUERY_NO_CACHE, "", "1", "skey=1,value=1"));
}
}} // namespace indexlib::partition
