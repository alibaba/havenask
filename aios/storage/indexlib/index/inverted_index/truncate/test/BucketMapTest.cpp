#include "indexlib/index/inverted_index/truncate/BucketMap.h"

#include "autil/StringUtil.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/inverted_index/truncate/test/TruncateTestHelper.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class BucketMapTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}

private:
    void InnerTestCase(int size, const std::string& sortValue, const std::string& bucketValue);
};

void BucketMapTest::InnerTestCase(int size, const std::string& sortValue, const std::string& bucketValue)
{
    BucketMap bucketMap("ut_bucket_map", "ut_bucket_map_type");
    bucketMap.Init(size);
    std::vector<int> sortValueVec;
    autil::StringUtil::fromString(sortValue, sortValueVec, ";");
    for (size_t i = 0; i < sortValueVec.size(); ++i) {
        bucketMap.SetSortValue(i, sortValueVec[i]);
    }
    std::vector<int> bucketValueVec;
    autil::StringUtil::fromString(bucketValue, bucketValueVec, ";");
    for (size_t i = 0; i < bucketValueVec.size(); ++i) {
        ASSERT_EQ(sortValueVec[i], (int)bucketMap.GetSortValue(i));
        ASSERT_EQ(bucketValueVec[i], (int)bucketMap.GetBucketValue(i));
    }
    ASSERT_EQ(size, (int)bucketMap.GetSize());
}

TEST_F(BucketMapTest, TestNormalCase1) { InnerTestCase(5, "1;2;4;3;0", "0;0;1;1;0"); }

TEST_F(BucketMapTest, TestNormalCase2) { InnerTestCase(4, "2;1;0;3", "1;0;0;1"); }

TEST_F(BucketMapTest, TestCaseWithMoreData)
{
    BucketMap bucketMap("ut_bucket_map", "ut_bucket_map_type");
    bucketMap.Init(30);
    ASSERT_EQ((uint32_t)5, bucketMap.GetBucketCount());
    ASSERT_EQ((uint32_t)30, bucketMap.GetSize());
    for (int i = 0; i < 30; ++i) {
        bucketMap.SetSortValue(i, 29 - i);
    }

    std::string str = "4;4;4;4;4;4;3;3;3;3;3;3;2;2;2;2;2;2;1;1;1;1;1;1;0;0;0;0;0;0";
    std::vector<int> bucketValueVec;
    autil::StringUtil::fromString(str, bucketValueVec, ";");
    for (size_t i = 0; i < bucketValueVec.size(); ++i) {
        uint32_t iSortValue = 29 - i;
        ASSERT_EQ(iSortValue, bucketMap.GetSortValue(i));
        ASSERT_EQ(bucketValueVec[i], (int)bucketMap.GetBucketValue(i));
    }
}

TEST_F(BucketMapTest, TestBucketCount)
{
    {
        BucketMap bucketMap("ut_bucket_map", "ut_bucket_map_type");
        ASSERT_FALSE(bucketMap.Init(0));
    }
    {
        BucketMap bucketMap("ut_bucket_map", "ut_bucket_map_type");
        bucketMap.Init(25);
        ASSERT_EQ((uint32_t)5, bucketMap.GetBucketCount());
        ASSERT_EQ((uint32_t)5, bucketMap.GetBucketSize());
    }
    {
        BucketMap bucketMap("ut_bucket_map", "ut_bucket_map_type");
        bucketMap.Init(29);
        ASSERT_EQ((uint32_t)5, bucketMap.GetBucketCount());
        ASSERT_EQ((uint32_t)6, bucketMap.GetBucketSize());
    }
    {
        BucketMap bucketMap("ut_bucket_map", "ut_bucket_map_type");
        bucketMap.Init(1);
        ASSERT_EQ((uint32_t)1, bucketMap.GetBucketCount());
        ASSERT_EQ((uint32_t)1, bucketMap.GetBucketSize());
    }
    {
        BucketMap bucketMap("ut_bucket_map", "ut_bucket_map_type");
        bucketMap.Init(2);
        ASSERT_EQ((uint32_t)1, bucketMap.GetBucketCount());
        ASSERT_EQ((uint32_t)2, bucketMap.GetBucketSize());
    }
}

TEST_F(BucketMapTest, TestStoreAndLoad)
{
    std::string indexRoot = GET_TEMP_DATA_PATH();
    auto indexDir = indexlib::file_system::Directory::GetPhysicalDirectory(indexRoot);
    uint32_t size = 100;
    std::shared_ptr<BucketMap> bucketMap1 = TruncateTestHelper::CreateBucketMap(size);
    auto st = bucketMap1->Store(indexDir);
    ASSERT_TRUE(st.IsOK());
    auto bucketMap2 = std::make_shared<BucketMap>("ut_bucket_map", "ut_bucket_map_type");
    st = bucketMap2->Load(indexDir);
    ASSERT_TRUE(st.IsOK());
    ASSERT_EQ(bucketMap1->GetSize(), bucketMap2->GetSize());
    ASSERT_EQ(bucketMap1->GetBucketCount(), bucketMap2->GetBucketCount());
    ASSERT_EQ(bucketMap1->GetBucketSize(), bucketMap2->GetBucketSize());
    for (uint32_t i = 0; i < size; ++i) {
        ASSERT_EQ(bucketMap1->GetSortValue(i), bucketMap2->GetSortValue(i));
        ASSERT_EQ(bucketMap1->GetBucketValue(i), bucketMap2->GetBucketValue(i));
    }
}

} // namespace indexlib::index
