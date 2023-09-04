#include "indexlib/index/ann/ANNPostingIterator.h"

#include "unittest/unittest.h"

using namespace std;
using namespace autil;
namespace indexlibv2::index::ann {

class ANNPostingIteratorTest : public ::testing::Test
{
public:
    void SetUp() override {}
    void TearDown() override {}

protected:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, ANNPostingIteratorTest);

TEST_F(ANNPostingIteratorTest, testEmptyIter)
{
    autil::mem_pool::Pool pool;
    ANNPostingIterator iter(&pool);
    auto curDocId = INVALID_DOCID;
    curDocId = iter.SeekDoc(curDocId);
    EXPECT_EQ(INVALID_DOCID, curDocId);
}

TEST_F(ANNPostingIteratorTest, testNormalIter)
{
    autil::mem_pool::Pool pool;
    size_t docCount = 3;
    vector<ANNMatchItem> annMatchItems;
    for (size_t i = 0; i != docCount; ++i) {
        annMatchItems.push_back({(docid_t)i, (i + 1) * 1.0f});
    }
    {
        // -1 -> 0; 0 -> 1; 1 -> 2;
        ANNPostingIterator iter(annMatchItems, &pool);
        auto curDocId = INVALID_DOCID;

        curDocId = iter.SeekDoc(curDocId);
        EXPECT_EQ(0, curDocId);
        EXPECT_FLOAT_EQ(1.0f, iter.GetMatchValue().GetFloat());

        curDocId = iter.SeekDoc(curDocId);
        EXPECT_EQ(1, curDocId);
        EXPECT_FLOAT_EQ(2.0f, iter.GetMatchValue().GetFloat());

        curDocId = iter.SeekDoc(curDocId);
        EXPECT_EQ(2, curDocId);
        EXPECT_FLOAT_EQ(3.0f, iter.GetMatchValue().GetFloat());
    }
    {
        // 0 -> 0; 0 -> 1; 1 -> 2;
        ANNPostingIterator iter(annMatchItems, &pool);
        auto curDocId = 0;

        curDocId = iter.SeekDoc(curDocId);
        EXPECT_EQ(0, curDocId);
        EXPECT_FLOAT_EQ(1.0f, iter.GetMatchValue().GetFloat());

        curDocId = iter.SeekDoc(curDocId);
        EXPECT_EQ(1, curDocId);
        EXPECT_FLOAT_EQ(2.0f, iter.GetMatchValue().GetFloat());

        curDocId = iter.SeekDoc(curDocId);
        EXPECT_EQ(2, curDocId);
        EXPECT_FLOAT_EQ(3.0f, iter.GetMatchValue().GetFloat());
    }
    {
        // -1 -> 0; 1 -> 1; 1 -> 2;
        ANNPostingIterator iter(annMatchItems, &pool);
        auto curDocId = -1;

        curDocId = iter.SeekDoc(curDocId);
        EXPECT_EQ(0, curDocId);
        EXPECT_FLOAT_EQ(1.0f, iter.GetMatchValue().GetFloat());

        curDocId = iter.SeekDoc(1);
        EXPECT_EQ(1, curDocId);
        EXPECT_FLOAT_EQ(2.0f, iter.GetMatchValue().GetFloat());

        curDocId = iter.SeekDoc(curDocId);
        EXPECT_EQ(2, curDocId);
        EXPECT_FLOAT_EQ(3.0f, iter.GetMatchValue().GetFloat());
    }
    {
        // 1 -> 1; 0 -> 0; 0 -> 1;
        ANNPostingIterator iter(annMatchItems, &pool);
        auto curDocId = -1;

        curDocId = iter.SeekDoc(1);
        EXPECT_EQ(1, curDocId);
        EXPECT_FLOAT_EQ(2.0f, iter.GetMatchValue().GetFloat());

        curDocId = iter.SeekDoc(0);
        EXPECT_EQ(0, curDocId);
        EXPECT_FLOAT_EQ(1.0f, iter.GetMatchValue().GetFloat());

        curDocId = iter.SeekDoc(curDocId);
        EXPECT_EQ(1, curDocId);
        EXPECT_FLOAT_EQ(2.0f, iter.GetMatchValue().GetFloat());
    }
    {
        // 1 -> 1; 1 -> 2;
        ANNPostingIterator iter(annMatchItems, &pool);
        auto curDocId = 1;

        curDocId = iter.SeekDoc(curDocId);
        EXPECT_EQ(1, curDocId);
        EXPECT_FLOAT_EQ(2.0f, iter.GetMatchValue().GetFloat());

        curDocId = iter.SeekDoc(1);
        EXPECT_EQ(2, curDocId);
        EXPECT_FLOAT_EQ(3.0f, iter.GetMatchValue().GetFloat());
    }
}

TEST_F(ANNPostingIteratorTest, testClone)
{
    autil::mem_pool::Pool pool;
    size_t docCount = 3;
    vector<ANNMatchItem> annMatchItems;
    for (size_t i = 0; i != docCount; ++i) {
        annMatchItems.push_back({(docid_t)i, (i + 1) * 1.0f});
    }

    auto iter = new ANNPostingIterator(annMatchItems, &pool);
    auto clonedIter = iter->Clone();
    delete iter;

    auto curDocId = INVALID_DOCID;
    curDocId = clonedIter->SeekDoc(curDocId);
    EXPECT_EQ(0, curDocId);
    EXPECT_FLOAT_EQ(1.0f, clonedIter->GetMatchValue().GetFloat());

    curDocId = clonedIter->SeekDoc(curDocId);
    EXPECT_EQ(1, curDocId);
    EXPECT_FLOAT_EQ(2.0f, clonedIter->GetMatchValue().GetFloat());

    curDocId = clonedIter->SeekDoc(curDocId);
    EXPECT_EQ(2, curDocId);
    EXPECT_FLOAT_EQ(3.0f, clonedIter->GetMatchValue().GetFloat());

    indexlib::IE_POOL_COMPATIBLE_DELETE_CLASS(&pool, clonedIter);
}

} // namespace indexlibv2::index::ann
