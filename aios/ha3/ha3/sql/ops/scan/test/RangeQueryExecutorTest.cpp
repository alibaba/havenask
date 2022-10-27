#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/sql/ops/scan/RangeQueryExecutor.h>
#include <ha3/search/LayerMetas.h>
#include <autil/mem_pool/Pool.h>

using namespace std;
using namespace testing;
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(sql);

class RangeQueryExecutorTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void RangeQueryExecutorTest::setUp() {
}

void RangeQueryExecutorTest::tearDown() {
}

TEST_F(RangeQueryExecutorTest, testSeekEmptyLayer) {
    autil::mem_pool::Pool pool;
    LayerMeta layerMeta(&pool);
    RangeQueryExecutor executor(&layerMeta);
    docid_t tmp;
    ASSERT_EQ(IE_NAMESPACE(common)::ErrorCode::OK, executor.seek(0, tmp));
    ASSERT_EQ(END_DOCID, tmp);
    ASSERT_EQ(END_DOCID, executor.getDocId());
}

TEST_F(RangeQueryExecutorTest, testSeek) {
    autil::mem_pool::Pool pool;
    LayerMeta layerMeta(&pool);
    layerMeta.push_back(DocIdRangeMeta(0, 0));
    layerMeta.push_back(DocIdRangeMeta(5, 7));
    layerMeta.push_back(DocIdRangeMeta(9, 10));
    RangeQueryExecutor executor(&layerMeta);
    ASSERT_EQ(6, executor._df);
    docid_t tmp;
    ASSERT_EQ(IE_NAMESPACE(common)::ErrorCode::OK, executor.seek(0, tmp));
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(0, executor.getDocId());

    ASSERT_EQ(IE_NAMESPACE(common)::ErrorCode::OK, executor.seek(1, tmp));
    ASSERT_EQ(5, tmp);
    ASSERT_EQ(5, executor.getDocId());

    ASSERT_EQ(IE_NAMESPACE(common)::ErrorCode::OK, executor.seek(4, tmp));
    ASSERT_EQ(5, tmp);
    ASSERT_EQ(5, executor.getDocId());

    ASSERT_EQ(IE_NAMESPACE(common)::ErrorCode::OK, executor.seek(5, tmp));
    ASSERT_EQ(5, tmp);
    ASSERT_EQ(5, executor.getDocId());

    ASSERT_EQ(IE_NAMESPACE(common)::ErrorCode::OK, executor.seek(6, tmp));
    ASSERT_EQ(6, tmp);
    ASSERT_EQ(6, executor.getDocId());

    ASSERT_EQ(IE_NAMESPACE(common)::ErrorCode::OK, executor.seek(10, tmp));
    ASSERT_EQ(10, tmp);
    ASSERT_EQ(10, executor.getDocId());

    ASSERT_EQ(IE_NAMESPACE(common)::ErrorCode::OK, executor.seek(11, tmp));
    ASSERT_EQ(END_DOCID, tmp);
    ASSERT_EQ(END_DOCID, executor.getDocId());
}

TEST_F(RangeQueryExecutorTest, testSeekSub) {
    autil::mem_pool::Pool pool;
    LayerMeta layerMeta(&pool);
    layerMeta.push_back(DocIdRangeMeta(0, 0));
    layerMeta.push_back(DocIdRangeMeta(5, 7));
    layerMeta.push_back(DocIdRangeMeta(9, 10));
    RangeQueryExecutor executor(&layerMeta);
    docid_t tmp;
    ASSERT_EQ(IE_NAMESPACE(common)::ErrorCode::OK, executor.seek(0, tmp));

    ASSERT_EQ(0, tmp);
    ASSERT_EQ(0, executor.getDocId());
    ASSERT_EQ(IE_NAMESPACE(common)::ErrorCode::OK, executor.seekSubDoc(0, 0, 1, false, tmp));
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(IE_NAMESPACE(common)::ErrorCode::OK, executor.seekSubDoc(0, 2, 1, false, tmp));
    ASSERT_EQ(END_DOCID, tmp);
}
END_HA3_NAMESPACE();
