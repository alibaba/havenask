#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/truncate/bucket_map.h"
#include "indexlib/common/field_format/attribute/type_info.h"
#include "indexlib/index/normal/inverted_index/truncate/test/truncate_test_helper.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);

class BucketMapTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(BucketMapTest);
    void CaseSetUp() override
    {
        mRootDir = GET_TEST_DATA_PATH();
    }

    void CaseTearDown() override
    {
    }

    void TestCaseNormal()
    {
        InnerTestCase(5, "1;2;4;3;0", "0;0;1;1;0");
    }

    void TestCaseForInitError()
    {
        BucketMap bucketMap;
        ASSERT_FALSE(bucketMap.Init(0));
    }

    void TestCaseForSizeSmallerThanDocCount()
    {
        InnerTestCase(2, "1;0", "0;0");
    }

    void TestCaseForSizeEqualDocCount()
    {
        InnerTestCase(4, "2;1;0;3", "1;0;0;1");
    }

    void TestCaseWithMoreData()
    { 
        BucketMap bucketMap;
        bucketMap.Init(30);
        INDEXLIB_TEST_EQUAL((uint32_t)5, bucketMap.GetBucketCount());
        INDEXLIB_TEST_EQUAL((uint32_t)30, bucketMap.GetSize());
        for (int i = 0; i < 30; ++i)
        {
            bucketMap.SetSortValue(i, 29-i);
        }

        std::string str = "4;4;4;4;4;4;3;3;3;3;3;3;2;2;2;2;2;2;1;1;1;1;1;1;0;0;0;0;0;0";
        std::vector<int> bucketValueVec;
        autil::StringUtil::fromString(str, bucketValueVec, ";");
        for (size_t i = 0; i < bucketValueVec.size(); ++i)
        {
            uint32_t iSortValue = 29 - i;
            INDEXLIB_TEST_EQUAL(iSortValue, bucketMap.GetSortValue(i));
            INDEXLIB_TEST_EQUAL(bucketValueVec[i], (int)bucketMap.GetBucketValue(i));
        }
    }

    void TestCaseForBucketCount()
    {
        {
            BucketMap bucketMap;
            bucketMap.Init(25);        
            INDEXLIB_TEST_EQUAL((uint32_t)5, bucketMap.GetBucketCount());
            INDEXLIB_TEST_EQUAL((uint32_t)5, bucketMap.GetBucketSize());
        }
        {
            BucketMap bucketMap;
            bucketMap.Init(29);        
            INDEXLIB_TEST_EQUAL((uint32_t)5, bucketMap.GetBucketCount());
            INDEXLIB_TEST_EQUAL((uint32_t)6, bucketMap.GetBucketSize());
        }
        {
            BucketMap bucketMap;
            bucketMap.Init(1);        
            INDEXLIB_TEST_EQUAL((uint32_t)1, bucketMap.GetBucketCount());
            INDEXLIB_TEST_EQUAL((uint32_t)1, bucketMap.GetBucketSize());
        }
        {
            BucketMap bucketMap;
            bucketMap.Init(2);        
            INDEXLIB_TEST_EQUAL((uint32_t)1, bucketMap.GetBucketCount());
            INDEXLIB_TEST_EQUAL((uint32_t)2, bucketMap.GetBucketSize());
        }
        {
            BucketMap bucketMap;
            INDEXLIB_TEST_TRUE(!bucketMap.Init(0));        
        }
    }

    void TestCaseForStoreAndLoad()
    {
        uint32_t size = 100;
        BucketMapPtr bucketMap1 = TruncateTestHelper::CreateBucketMap(size);
        string filePath = mRootDir + "/bucket_map_file";
        bucketMap1->Store(filePath);
        BucketMapPtr bucketMap2(new BucketMap);
        bucketMap2->Load(filePath);
        INDEXLIB_TEST_EQUAL(bucketMap1->GetSize(), bucketMap2->GetSize());
        INDEXLIB_TEST_EQUAL(bucketMap1->GetBucketCount(), bucketMap2->GetBucketCount());
        INDEXLIB_TEST_EQUAL(bucketMap1->GetBucketSize(), bucketMap2->GetBucketSize());        
        for (uint32_t i = 0; i < size; ++i)
        {
            INDEXLIB_TEST_EQUAL(bucketMap1->GetSortValue(i), bucketMap2->GetSortValue(i));
            INDEXLIB_TEST_EQUAL(bucketMap1->GetBucketValue(i), bucketMap2->GetBucketValue(i));
        }        
    }


private:
    void InnerTestCase(int size, std::string sortValue, std::string bucketValue)
    {
        BucketMap bucketMap;
        bucketMap.Init(size);
        std::vector<int> sortValueVec;
        autil::StringUtil::fromString(sortValue, sortValueVec, ";");
        for (size_t i = 0; i < sortValueVec.size(); ++i)
        {
            bucketMap.SetSortValue(i, sortValueVec[i]);
        }
        std::vector<int> bucketValueVec;
        autil::StringUtil::fromString(bucketValue, bucketValueVec, ";");
        for (size_t i = 0; i < bucketValueVec.size(); ++i)
        {
            INDEXLIB_TEST_EQUAL(sortValueVec[i], (int)bucketMap.GetSortValue(i));
            INDEXLIB_TEST_EQUAL(bucketValueVec[i], (int)bucketMap.GetBucketValue(i));
        }
        INDEXLIB_TEST_EQUAL(size, (int)bucketMap.GetSize());

    }
private:
    std::string mRootDir;
private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, BucketMapTest);

INDEXLIB_UNIT_TEST_CASE(BucketMapTest, TestCaseNormal);
INDEXLIB_UNIT_TEST_CASE(BucketMapTest, TestCaseForInitError);
INDEXLIB_UNIT_TEST_CASE(BucketMapTest, TestCaseWithMoreData);
INDEXLIB_UNIT_TEST_CASE(BucketMapTest, TestCaseForSizeSmallerThanDocCount);
INDEXLIB_UNIT_TEST_CASE(BucketMapTest, TestCaseForSizeEqualDocCount);
INDEXLIB_UNIT_TEST_CASE(BucketMapTest, TestCaseForBucketCount);
INDEXLIB_UNIT_TEST_CASE(BucketMapTest, TestCaseForStoreAndLoad);

IE_NAMESPACE_END(index);
