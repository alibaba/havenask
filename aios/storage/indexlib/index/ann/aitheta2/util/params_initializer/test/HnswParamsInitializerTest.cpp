
#include "unittest/unittest.h"
#define protected public
#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/index/ann/aitheta2/util/params_initializer/HnswParamsInitializer.h"

using namespace std;
using namespace aitheta2;

namespace indexlibv2::index::ann {

class HnswParamsInitializerTest : public ::testing::Test
{
public:
    void SetUp() override {}
    void TearDown() override {}

protected:
    AUTIL_LOG_DECLARE();
};

TEST_F(HnswParamsInitializerTest, testInitSearcherIndexParams)
{
    indexlib::util::KeyValueMap parameters({{"min_scan_doc_cnt", "1200"}});
    string indexParamsStr = R"({
        "proxima.hnsw.searcher.max_scan_ratio" : 0.2
    })";
    parameters[INDEX_PARAMETERS] = indexParamsStr;
    AithetaIndexConfig indexConfig(parameters);
    AiThetaParams indexParams;
    HnswParamsInitializer initializer(2000);
    ASSERT_TRUE(initializer.InitSearchParams(indexConfig, indexParams));
    EXPECT_FLOAT_EQ(0.6, indexParams.get_as_float("proxima.hnsw.searcher.max_scan_ratio"));
}

TEST_F(HnswParamsInitializerTest, testInitRealtimeIndexParams)
{
    indexlib::util::KeyValueMap parameters;
    string rtIndexParams = R"({
        "proxima.hnsw.streamer.max_scan_ratio" : 0.6
    })";
    parameters[REALTIME_PARAMETERS] = rtIndexParams;
    AithetaIndexConfig indexConfig(parameters);
    AiThetaParams indexParams;
    HnswParamsInitializer initializer(2000);
    ASSERT_TRUE(initializer.InitRealtimeParams(indexConfig, indexParams));
    EXPECT_FLOAT_EQ(0.6, indexParams.get_as_float("proxima.hnsw.streamer.max_scan_ratio"));
    EXPECT_FLOAT_EQ(0.0, indexParams.get_as_float("proxima.mips.reformer.l2_norm"));
}

TEST_F(HnswParamsInitializerTest, testInitRealtimeIndexParams_WithNorm)
{
    indexlib::util::KeyValueMap parameters = {{DISTANCE_TYPE, INNER_PRODUCT}};
    string rtIndexParams =
        R"({
            "proxima.hnsw.streamer.max_scan_ratio" : 0.6,
            "proxima.mips.reformer.l2_norm" : 2.0
            })";
    parameters[REALTIME_PARAMETERS] = rtIndexParams;
    AithetaIndexConfig indexConfig(parameters);
    AiThetaParams indexParams;
    HnswParamsInitializer initializer(2000);
    ASSERT_TRUE(initializer.InitRealtimeParams(indexConfig, indexParams));
    EXPECT_FLOAT_EQ(0.6, indexParams.get_as_float("proxima.hnsw.streamer.max_scan_ratio"));
    EXPECT_FLOAT_EQ(2.0, indexParams.get_as_float("proxima.mips.reformer.l2_norm"));
}

AUTIL_LOG_SETUP(indexlib.index, HnswParamsInitializerTest);
} // namespace indexlibv2::index::ann
