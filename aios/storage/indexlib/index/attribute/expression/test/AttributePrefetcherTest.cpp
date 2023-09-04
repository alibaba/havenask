#include "indexlib/index/attribute/expression/AttributePrefetcher.h"

#include "indexlib/index/attribute/expression/test/AttributeExpressionTestHelper.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class AttributePrefetcherTest : public TESTBASE
{
public:
    AttributePrefetcherTest() = default;
    ~AttributePrefetcherTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void AttributePrefetcherTest::setUp() {}

void AttributePrefetcherTest::tearDown() {}

TEST_F(AttributePrefetcherTest, TestSimple)
{
    AttributeExpressionTestHelper<ft_uint32, false> helper;
    ASSERT_TRUE(helper.Init());

    helper._memSegment->AddOneDoc(100);

    AttributePrefetcher<uint32_t> fetcher;
    ASSERT_TRUE(fetcher.Init(&(helper._pool), helper._config, helper._segments).IsOK());
    ASSERT_TRUE(fetcher.Prefetch(0).IsOK());
    ASSERT_EQ(100, fetcher.GetValue());
    ASSERT_TRUE(fetcher.Prefetch(0).IsOK());
    ASSERT_EQ(100, fetcher.GetValue());
    helper._memSegment->AddOneDoc(200);
    ASSERT_TRUE(fetcher.Prefetch(1).IsOK());
    ASSERT_EQ(200, fetcher.GetValue());
    ASSERT_EQ(1, fetcher.GetCurrentDocId());
}

TEST_F(AttributePrefetcherTest, TestPrefetchFailed)
{
    AttributeExpressionTestHelper<ft_uint32, false> helper;
    ASSERT_TRUE(helper.Init());

    AttributePrefetcher<uint32_t> fetcher;

    // invalid docid
    ASSERT_TRUE(fetcher.Init(&(helper._pool), helper._config, helper._segments).IsOK());
    ASSERT_FALSE(fetcher.Prefetch(0).IsOK());

    // prefetch rollback
    helper._memSegment->AddOneDoc(100);
    helper._memSegment->AddOneDoc(100);
    ASSERT_TRUE(fetcher.Prefetch(1).IsOK());
    ASSERT_FALSE(fetcher.Prefetch(0).IsOK());
}

} // namespace indexlibv2::index
