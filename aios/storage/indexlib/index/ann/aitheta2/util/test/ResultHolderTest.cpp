
#include "indexlib/index/ann/aitheta2/util/ResultHolder.h"

#include "autil/mem_pool/Pool.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;
namespace indexlibv2::index::ann {

class ResultHolderTest : public TESTBASE
{
public:
    void SetUp() override {}
    void TearDown() override {}

protected:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.index, ResultHolderTest);

TEST_F(ResultHolderTest, TestAppend)
{
    std::string distanceType = INNER_PRODUCT;
    ResultHolder resultHolder(distanceType);
    resultHolder.AppendResult(1, 1.0f);
    ASSERT_EQ(1, resultHolder._matchItems[0].docid);
    EXPECT_FLOAT_EQ(1.0f, resultHolder._matchItems[0].score);
    resultHolder.AppendResult(2, 2.0f);
    ASSERT_EQ(2, resultHolder._matchItems[1].docid);
    EXPECT_FLOAT_EQ(2.0f, resultHolder._matchItems[1].score);
    resultHolder.AppendResult(3, 3.0f);
    ASSERT_EQ(3, resultHolder._matchItems[2].docid);
    EXPECT_FLOAT_EQ(3.0f, resultHolder._matchItems[2].score);
}

TEST_F(ResultHolderTest, TestAscendingScoreSort)
{
    std::string distanceType = SQUARED_EUCLIDEAN;
    ResultHolder resultHolder(distanceType);
    resultHolder.AppendResult(1, 3.0f);
    resultHolder.AppendResult(2, 2.0f);
    resultHolder.AppendResult(3, 1.0f);
    resultHolder.OrderByScore();
    ASSERT_EQ(3, resultHolder._matchItems.size());
    ASSERT_EQ(3, resultHolder._matchItems[0].docid);
    ASSERT_EQ(2, resultHolder._matchItems[1].docid);
    ASSERT_EQ(1, resultHolder._matchItems[2].docid);
    EXPECT_FLOAT_EQ(1, resultHolder._matchItems[0].score);
    EXPECT_FLOAT_EQ(2, resultHolder._matchItems[1].score);
    EXPECT_FLOAT_EQ(3, resultHolder._matchItems[2].score);
}

TEST_F(ResultHolderTest, TestAscendingScoreSort_Empty)
{
    std::string distanceType = SQUARED_EUCLIDEAN;
    ResultHolder resultHolder(distanceType);
    resultHolder.OrderByScore();
    ASSERT_EQ(0, resultHolder._matchItems.size());
}

TEST_F(ResultHolderTest, TestDescendingScoreSort)
{
    std::string distanceType = INNER_PRODUCT;
    ResultHolder resultHolder(distanceType);
    resultHolder.AppendResult(1, 1.0f);
    resultHolder.AppendResult(2, 2.0f);
    resultHolder.AppendResult(3, 3.0f);
    resultHolder.OrderByScore();
    ASSERT_EQ(3, resultHolder._matchItems.size());
    ASSERT_EQ(3, resultHolder._matchItems[0].docid);
    ASSERT_EQ(2, resultHolder._matchItems[1].docid);
    ASSERT_EQ(1, resultHolder._matchItems[2].docid);
    EXPECT_FLOAT_EQ(3, resultHolder._matchItems[0].score);
    EXPECT_FLOAT_EQ(2, resultHolder._matchItems[1].score);
    EXPECT_FLOAT_EQ(1, resultHolder._matchItems[2].score);
}

TEST_F(ResultHolderTest, TestDescendingScoreSort_Empty)
{
    std::string distanceType = INNER_PRODUCT;
    ResultHolder resultHolder(distanceType);
    resultHolder.OrderByScore();
    ASSERT_EQ(0, resultHolder._matchItems.size());
}

TEST_F(ResultHolderTest, TestFormat)
{
    autil::mem_pool::Pool pool;

    std::string distanceType = INNER_PRODUCT;
    ResultHolder resultHolder(distanceType);
    resultHolder.AppendResult(1, 1.0f);
    resultHolder.AppendResult(2, 2.0f);
    resultHolder.AppendResult(3, 3.0f);
    resultHolder.AppendResult(4, 4.0f);
    resultHolder.AppendResult(5, 5.0f);
    resultHolder.AppendResult(6, 6.0f);
    //    bool FormatResult(size_t topk, indexlib::MatchInfoPtr &output, autil::mem_pool::Pool *pool);
    size_t topk = 3;

    resultHolder.TruncateResult(topk);
    auto matchItems = resultHolder.GetTopkMatchItems(topk);
    ASSERT_EQ(3, matchItems.size());
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_EQ((float)i + 1, matchItems[i].docid);
    }
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_FLOAT_EQ((float)i + 1, matchItems[i].score);
    }
}

TEST_F(ResultHolderTest, TestFormat_Topk)
{
    autil::mem_pool::Pool pool;

    std::string distanceType = INNER_PRODUCT;
    ResultHolder resultHolder(distanceType);
    resultHolder.AppendResult(1, 1.0f);
    resultHolder.AppendResult(2, 2.0f);
    resultHolder.AppendResult(3, 3.0f);
    resultHolder.AppendResult(4, 4.0f);
    resultHolder.AppendResult(5, 5.0f);
    resultHolder.AppendResult(6, 6.0f);
    // bool FormatResult(size_t topk, MatchInfoPtr &output, autil::mem_pool::Pool *pool);
    size_t topk = 10;

    auto matchItems = resultHolder.GetTopkMatchItems(topk);
    ASSERT_EQ(6, matchItems.size());
    for (size_t i = 0; i < 6; ++i) {
        EXPECT_EQ(i + 1, matchItems[i].docid);
    }
    for (size_t i = 0; i < 6; ++i) {
        EXPECT_FLOAT_EQ((float)i + 1, matchItems[i].score);
    }
}

TEST_F(ResultHolderTest, TestUniqByDocId_KeepMaxScore)
{
    std::string distanceType = INNER_PRODUCT;
    ResultHolder resultHolder(distanceType);
    resultHolder.AppendResult(1, 1.0f);
    resultHolder.AppendResult(2, 2.0f);
    resultHolder.AppendResult(3, 3.0f);
    resultHolder.AppendResult(4, 4.0f);
    resultHolder.AppendResult(5, 5.0f);
    resultHolder.AppendResult(2, 2.0f + 1);
    resultHolder.AppendResult(3, 3.0f + 1);
    resultHolder.AppendResult(4, 4.0f + 1);

    resultHolder.UniqAndOrderByDocId();
    ASSERT_EQ(5, resultHolder._matchItems.size());
    ASSERT_EQ(1, resultHolder._matchItems[0].docid);
    ASSERT_EQ(2, resultHolder._matchItems[1].docid);
    ASSERT_EQ(3, resultHolder._matchItems[2].docid);
    ASSERT_EQ(4, resultHolder._matchItems[3].docid);
    ASSERT_EQ(5, resultHolder._matchItems[4].docid);

    EXPECT_FLOAT_EQ(1, resultHolder._matchItems[0].score);
    EXPECT_FLOAT_EQ(2 + 1, resultHolder._matchItems[1].score);
    EXPECT_FLOAT_EQ(3 + 1, resultHolder._matchItems[2].score);
    EXPECT_FLOAT_EQ(4 + 1, resultHolder._matchItems[3].score);
    EXPECT_FLOAT_EQ(5, resultHolder._matchItems[4].score);
}

TEST_F(ResultHolderTest, TestUniqByDocId_KeepMinScore)
{
    std::string distanceType = SQUARED_EUCLIDEAN;
    ResultHolder resultHolder(distanceType);
    resultHolder.AppendResult(1, 1.0f);
    resultHolder.AppendResult(2, 2.0f);
    resultHolder.AppendResult(3, 3.0f);
    resultHolder.AppendResult(4, 4.0f);
    resultHolder.AppendResult(5, 5.0f);
    resultHolder.AppendResult(2, 2.0f + 1);
    resultHolder.AppendResult(3, 3.0f + 1);
    resultHolder.AppendResult(4, 4.0f + 1);

    resultHolder.UniqAndOrderByDocId();
    ASSERT_EQ(5, resultHolder._matchItems.size());
    ASSERT_EQ(1, resultHolder._matchItems[0].docid);
    ASSERT_EQ(2, resultHolder._matchItems[1].docid);
    ASSERT_EQ(3, resultHolder._matchItems[2].docid);
    ASSERT_EQ(4, resultHolder._matchItems[3].docid);
    ASSERT_EQ(5, resultHolder._matchItems[4].docid);

    EXPECT_FLOAT_EQ(1, resultHolder._matchItems[0].score);
    EXPECT_FLOAT_EQ(2, resultHolder._matchItems[1].score);
    EXPECT_FLOAT_EQ(3, resultHolder._matchItems[2].score);
    EXPECT_FLOAT_EQ(4, resultHolder._matchItems[3].score);
    EXPECT_FLOAT_EQ(5, resultHolder._matchItems[4].score);
}

TEST_F(ResultHolderTest, TestUniqByDocId_KeepMaxScore2)
{
    std::string distanceType = INNER_PRODUCT;
    ResultHolder resultHolder(distanceType);
    resultHolder.AppendResult(1, 1.0f);
    resultHolder.AppendResult(2, 2.0f);
    resultHolder.AppendResult(3, 3.0f);
    resultHolder.AppendResult(1, 1.0f + 1);
    resultHolder.AppendResult(2, 2.0f + 1);
    resultHolder.AppendResult(3, 3.0f + 1);

    resultHolder.UniqAndOrderByDocId();
    ASSERT_EQ(3, resultHolder._matchItems.size());
    ASSERT_EQ(1, resultHolder._matchItems[0].docid);
    ASSERT_EQ(2, resultHolder._matchItems[1].docid);
    ASSERT_EQ(3, resultHolder._matchItems[2].docid);
    EXPECT_FLOAT_EQ(2, resultHolder._matchItems[0].score);
    EXPECT_FLOAT_EQ(3, resultHolder._matchItems[1].score);
    EXPECT_FLOAT_EQ(4, resultHolder._matchItems[2].score);
}

TEST_F(ResultHolderTest, TestUniqByDocId_KeepMinScore2)
{
    std::string distanceType = SQUARED_EUCLIDEAN;
    ResultHolder resultHolder(distanceType);
    resultHolder.AppendResult(1, 1.0f);
    resultHolder.AppendResult(2, 2.0f);
    resultHolder.AppendResult(3, 3.0f);
    resultHolder.AppendResult(1, 1.0f + 1);
    resultHolder.AppendResult(2, 2.0f + 1);
    resultHolder.AppendResult(3, 3.0f + 1);

    resultHolder.UniqAndOrderByDocId();
    ASSERT_EQ(3, resultHolder._matchItems.size());
    ASSERT_EQ(1, resultHolder._matchItems[0].docid);
    ASSERT_EQ(2, resultHolder._matchItems[1].docid);
    ASSERT_EQ(3, resultHolder._matchItems[2].docid);
    EXPECT_FLOAT_EQ(1, resultHolder._matchItems[0].score);
    EXPECT_FLOAT_EQ(2, resultHolder._matchItems[1].score);
    EXPECT_FLOAT_EQ(3, resultHolder._matchItems[2].score);
}

TEST_F(ResultHolderTest, TestFormatResult)
{
    autil::mem_pool::Pool pool;
    {
        std::string distanceType = SQUARED_EUCLIDEAN;
        ResultHolder resultHolder(distanceType);
        resultHolder.AppendResult(1, 1.0f);
        resultHolder.AppendResult(2, 2.0f);
        resultHolder.AppendResult(3, 3.0f);
        resultHolder.AppendResult(1, 1.0f + 1);
        resultHolder.AppendResult(2, 2.0f + 1);
        resultHolder.AppendResult(3, 3.0f + 1);
        resultHolder.AppendResult(4, 4.0f);
        size_t topk = 10;

        auto matchItems = resultHolder.GetTopkMatchItems(topk);
        ASSERT_EQ(4, matchItems.size());
        for (size_t i = 0; i < 4; ++i) {
            EXPECT_EQ(i + 1, matchItems[i].docid);
        }
        for (size_t i = 0; i < 4; ++i) {
            EXPECT_FLOAT_EQ((float)i + 1, matchItems[i].score);
        }
    }
    {
        std::string distanceType = INNER_PRODUCT;
        ResultHolder resultHolder(distanceType);
        resultHolder.AppendResult(1, 1.0f);
        resultHolder.AppendResult(2, 2.0f);
        resultHolder.AppendResult(3, 3.0f);
        resultHolder.AppendResult(1, 1.0f + 1);
        resultHolder.AppendResult(2, 2.0f + 1);
        resultHolder.AppendResult(3, 3.0f + 1);
        resultHolder.AppendResult(4, 4.0f);

        size_t topk = 10;

        auto matchItems = resultHolder.GetTopkMatchItems(topk);
        ASSERT_EQ(4, matchItems.size());
        for (size_t i = 0; i < 4; ++i) {
            EXPECT_EQ(i + 1, matchItems[i].docid);
        }
        EXPECT_FLOAT_EQ(2.0f, matchItems[0].score);
        EXPECT_FLOAT_EQ(3.0f, matchItems[1].score);
        EXPECT_FLOAT_EQ(4.0f, matchItems[2].score);
        EXPECT_FLOAT_EQ(4.0f, matchItems[3].score);
    }
    {
        std::string distanceType = INNER_PRODUCT;
        ResultHolder resultHolder(distanceType);
        resultHolder.AppendResult(1, 1.0f);
        resultHolder.AppendResult(2, 2.0f);
        resultHolder.AppendResult(3, 3.0f);
        resultHolder.AppendResult(1, 1.0f + 1);
        resultHolder.AppendResult(2, 2.0f + 1);
        resultHolder.AppendResult(3, 3.0f + 1);
        resultHolder.AppendResult(4, 4.0f);

        size_t topk = 2;

        auto matchItems = resultHolder.GetTopkMatchItems(topk);
        ASSERT_EQ(2, matchItems.size());
        ASSERT_EQ(3, matchItems[0].docid);
        ASSERT_EQ(4, matchItems[1].docid);
        EXPECT_FLOAT_EQ(4.0f, matchItems[0].score);
        EXPECT_FLOAT_EQ(4.0f, matchItems[1].score);
    }
    {
        std::string distanceType = SQUARED_EUCLIDEAN;
        ResultHolder resultHolder(distanceType);
        resultHolder.AppendResult(1, 1.0f);
        resultHolder.AppendResult(2, 2.0f);
        resultHolder.AppendResult(3, 3.0f);
        resultHolder.AppendResult(1, 1.0f + 1);
        resultHolder.AppendResult(2, 2.0f + 1);
        resultHolder.AppendResult(3, 3.0f + 1);
        resultHolder.AppendResult(4, 4.0f);

        size_t topk = 2;

        auto matchItems = resultHolder.GetTopkMatchItems(topk);
        ASSERT_EQ(2, matchItems.size());

        ASSERT_EQ(1, matchItems[0].docid);
        ASSERT_EQ(2, matchItems[1].docid);
        EXPECT_FLOAT_EQ(1.0f, matchItems[0].score);
        EXPECT_FLOAT_EQ(2.0f, matchItems[1].score);
    }
    {
        std::string distanceType = SQUARED_EUCLIDEAN;
        ResultHolder resultHolder(distanceType);
        size_t topk = 2;

        auto matchItems = resultHolder.GetTopkMatchItems(topk);
        ASSERT_EQ(0, matchItems.size());
    }
}

TEST_F(ResultHolderTest, TestResetBaseDocId)
{
    autil::mem_pool::Pool pool;

    std::string distanceType = SQUARED_EUCLIDEAN;
    ResultHolder resultHolder(distanceType);
    resultHolder.AppendResult(1, 1.0f);
    resultHolder.AppendResult(2, 2.0f);
    resultHolder.ResetBaseDocId(10);
    resultHolder.AppendResult(1, 11.0f);
    resultHolder.AppendResult(2, 12.0f);
    size_t topk = 10;

    auto matchItems = resultHolder.GetTopkMatchItems(topk);
    ASSERT_EQ(4, matchItems.size());

    ASSERT_EQ(1, matchItems[0].docid);
    ASSERT_EQ(2, matchItems[1].docid);
    ASSERT_EQ(11, matchItems[2].docid);
    ASSERT_EQ(12, matchItems[3].docid);
    EXPECT_FLOAT_EQ(1.0f, matchItems[0].score);
    EXPECT_FLOAT_EQ(2.0f, matchItems[1].score);
    EXPECT_FLOAT_EQ(11.0f, matchItems[2].score);
    EXPECT_FLOAT_EQ(12.0f, matchItems[3].score);
}

TEST_F(ResultHolderTest, TestThreshold_InnerProduct)
{
    autil::mem_pool::Pool pool;

    std::string distanceType = INNER_PRODUCT;
    ResultHolder resultHolder(distanceType);
    resultHolder.AppendResult(1, 1.0f, 2.0f);
    resultHolder.AppendResult(2, 11.0f, 2.0f);
    size_t topk = 2;

    auto matchItems = resultHolder.GetTopkMatchItems(topk);
    ASSERT_EQ(1, matchItems.size());

    ASSERT_EQ(2, matchItems[0].docid);
    EXPECT_FLOAT_EQ(11.0f, matchItems[0].score);
}

TEST_F(ResultHolderTest, TestThreshold_l2)
{
    autil::mem_pool::Pool pool;

    std::string distanceType = SQUARED_EUCLIDEAN;
    ResultHolder resultHolder(distanceType);
    resultHolder.AppendResult(1, 1.0f, 2.0f);
    resultHolder.AppendResult(2, 11.0f, 2.0f);
    size_t topk = 2;

    auto matchItems = resultHolder.GetTopkMatchItems(topk);
    ASSERT_EQ(1, matchItems.size());
    ASSERT_EQ(1, matchItems[0].docid);
    EXPECT_FLOAT_EQ(1.0f, matchItems[0].score);
}

TEST_F(ResultHolderTest, TestCreateANNPostringIterator)
{
    autil::mem_pool::Pool pool;

    std::string distanceType = SQUARED_EUCLIDEAN;
    ResultHolder resultHolder(distanceType);
    resultHolder.AppendResult(3, 1.0f);
    resultHolder.AppendResult(1, 11.0f);
    resultHolder.AppendResult(2, 3.0f);

    auto matchItems = resultHolder.GetTopkMatchItems(10);

    EXPECT_EQ(1, matchItems[0].docid);
    EXPECT_FLOAT_EQ(11.0f, matchItems[0].score);

    EXPECT_EQ(2, matchItems[1].docid);
    EXPECT_FLOAT_EQ(3.0f, matchItems[1].score);

    EXPECT_EQ(3, matchItems[2].docid);
    EXPECT_FLOAT_EQ(1.0f, matchItems[2].score);
}

} // namespace indexlibv2::index::ann
