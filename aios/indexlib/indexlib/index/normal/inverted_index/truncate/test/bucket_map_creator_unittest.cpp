#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/truncate/bucket_map_creator.h"
#include "indexlib/util/resource_control_thread_pool.h"

using namespace std;
using namespace std::tr1;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(index);

class BucketMapCreatorTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(BucketMapCreatorTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForCreateBucketMapThreadPool()
    {
        BucketMapCreator creator;
        util::ResourceControlThreadPoolPtr threadPool = 
            creator.CreateBucketMapThreadPool(2, 100, 1000);
        INDEXLIB_TEST_TRUE(threadPool);
        EXPECT_EQ(1u, threadPool->GetThreadNum());
        
        threadPool = creator.CreateBucketMapThreadPool(20, 100, 28000);
        //28000 = bucketMapSize(8000) + 10 * memUsePerSortItem(2000)
        INDEXLIB_TEST_TRUE(threadPool);
        EXPECT_EQ(10u, threadPool->GetThreadNum());

        threadPool = creator.CreateBucketMapThreadPool(2, 100, 28000);
        //20800 = bucketMapSize(800) + 10 * memUsePerSortItem(2000)
        INDEXLIB_TEST_TRUE(threadPool);
        EXPECT_EQ(2u, threadPool->GetThreadNum());
    }

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, BucketMapCreatorTest);

INDEXLIB_UNIT_TEST_CASE(BucketMapCreatorTest, TestCaseForCreateBucketMapThreadPool);

IE_NAMESPACE_END(index);
