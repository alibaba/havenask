#include "indexlib/util/cache/SearchCacheCreator.h"

#include "autil/Log.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace util {

class SearchCacheCreatorTest : public INDEXLIB_TESTBASE
{
public:
    SearchCacheCreatorTest();
    ~SearchCacheCreatorTest();

    DECLARE_CLASS_NAME(SearchCacheCreatorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestEmptyParam();
    void TestInvalidParam();
    void TestValidParam();

private:
    SearchCache* Create(const std::string& param);

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SearchCacheCreatorTest, TestEmptyParam);
INDEXLIB_UNIT_TEST_CASE(SearchCacheCreatorTest, TestInvalidParam);
INDEXLIB_UNIT_TEST_CASE(SearchCacheCreatorTest, TestValidParam);

SearchCacheCreatorTest::SearchCacheCreatorTest() {}

SearchCacheCreatorTest::~SearchCacheCreatorTest() {}

void SearchCacheCreatorTest::CaseSetUp() {}

void SearchCacheCreatorTest::CaseTearDown() {}

void SearchCacheCreatorTest::TestEmptyParam() { ASSERT_EQ(NULL, Create("")); }

void SearchCacheCreatorTest::TestInvalidParam()
{
    ASSERT_EQ(NULL, Create("abc=123"));
    ASSERT_EQ(NULL, Create("cache_size=abc"));
    ASSERT_EQ(NULL, Create("cache_size=-2"));
    ASSERT_EQ(NULL, Create("cache_size=0"));
    ASSERT_EQ(NULL, Create("cache_size=100;num_shard_bits=-9"));
    ASSERT_EQ(NULL, Create("cache_size=100;num_shard_bits=20"));
}

void SearchCacheCreatorTest::TestValidParam()
{
    SearchCachePtr searchCache(Create("cache_size=1024;num_shard_bits=10"));
    ASSERT_EQ((uint64_t)1024 * 1024 * 1024, searchCache->GetCacheSize());
    // ASSERT_EQ(10, searchCache->_cache->num_shard_bits_);
}

SearchCache* SearchCacheCreatorTest::Create(const std::string& param)
{
    return SearchCacheCreator::Create(param, MemoryQuotaControllerPtr(), std::shared_ptr<TaskScheduler>(), NULL);
}
}} // namespace indexlib::util
