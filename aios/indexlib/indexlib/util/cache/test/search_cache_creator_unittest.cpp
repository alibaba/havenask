#include "indexlib/util/cache/test/search_cache_creator_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, SearchCacheCreatorTest);

SearchCacheCreatorTest::SearchCacheCreatorTest()
{
}

SearchCacheCreatorTest::~SearchCacheCreatorTest()
{
}

void SearchCacheCreatorTest::CaseSetUp()
{
}

void SearchCacheCreatorTest::CaseTearDown()
{
}

void SearchCacheCreatorTest::TestEmptyParam()
{
    ASSERT_EQ(NULL, Create(""));
}

void SearchCacheCreatorTest::TestInvalidParam()
{
    ASSERT_EQ(NULL, Create("abc=123"));
    ASSERT_EQ(NULL, Create("cache_size=abc"));
    ASSERT_EQ(NULL, Create("cache_size=-2"));
    ASSERT_EQ(NULL, Create("cache_size=0"));
    ASSERT_EQ(NULL, Create("cache_size=100;num_shard_bits=-9"));
}

void SearchCacheCreatorTest::TestValidParam()
{
    SearchCachePtr searchCache(Create("cache_size=1024;num_shard_bits=10"));
    ASSERT_EQ((uint64_t)1024*1024*1024, searchCache->GetCacheSize());
    ASSERT_EQ(10, searchCache->mLRUCache.num_shard_bits_);
}

SearchCache* SearchCacheCreatorTest::Create(const std::string& param)
{
    return SearchCacheCreator::Create(param,
            MemoryQuotaControllerPtr(),
            TaskSchedulerPtr(),
            NULL);
}

IE_NAMESPACE_END(util);

