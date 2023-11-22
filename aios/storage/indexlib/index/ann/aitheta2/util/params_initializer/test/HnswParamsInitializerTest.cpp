
#include "indexlib/index/ann/aitheta2/util/params_initializer/HnswParamsInitializer.h"

#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "unittest/unittest.h"

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
    ASSERT_TRUE(initializer.InitNormalSearchParams(indexConfig, indexParams));
    EXPECT_FLOAT_EQ(0.6, indexParams.get_as_float("proxima.hnsw.searcher.max_scan_ratio"));
}

TEST_F(HnswParamsInitializerTest, testInitRealtimeBuildParams)
{
    indexlib::util::KeyValueMap parameters;

    string rtIndexParams = R"({})";
    parameters[REALTIME_PARAMETERS] = rtIndexParams;

    string buildIndexParams = R"({
        "proxima.hnsw.builder.efconstruction" : 1024,
        "proxima.hnsw.builder.enable_adsampling": true,
        "proxima.hnsw.builder.slack_pruning_factor": 1.1
    })";
    parameters[INDEX_BUILD_PARAMETERS] = buildIndexParams;

    AithetaIndexConfig indexConfig(parameters);
    AiThetaParams indexParams;
    HnswParamsInitializer initializer(2000);
    ASSERT_TRUE(initializer.InitRealtimeBuildParams(indexConfig, indexParams, false));
    EXPECT_EQ(1024, indexParams.get_as_int32("proxima.hnsw.streamer.efconstruction"));
    EXPECT_EQ(true, indexParams.get_as_bool("proxima.hnsw.streamer.enable_adsampling"));
    EXPECT_FLOAT_EQ(1.1, indexParams.get_as_float("proxima.hnsw.streamer.slack_pruning_factor"));
}

TEST_F(HnswParamsInitializerTest, testInitRealtimeSearchParams)
{
    indexlib::util::KeyValueMap parameters;

    string rtIndexParams = R"({})";
    parameters[REALTIME_PARAMETERS] = rtIndexParams;

    string searchIndexParams = R"({
        "proxima.hnsw.searcher.ef" : 1024,
        "proxima.hnsw.searcher.max_scan_ratio": 0.1
    })";
    parameters[INDEX_SEARCH_PARAMETERS] = searchIndexParams;

    AithetaIndexConfig indexConfig(parameters);
    AiThetaParams indexParams;
    HnswParamsInitializer initializer(2000);
    ASSERT_TRUE(initializer.InitRealtimeSearchParams(indexConfig, indexParams));
    EXPECT_FLOAT_EQ(0.1, indexParams.get_as_float("proxima.hnsw.streamer.max_scan_ratio"));
    EXPECT_EQ(1024, indexParams.get_as_int32("proxima.hnsw.streamer.ef"));
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
    ASSERT_TRUE(initializer.InitRealtimeBuildParams(indexConfig, indexParams, false));
    EXPECT_FLOAT_EQ(0.6, indexParams.get_as_float("proxima.hnsw.streamer.max_scan_ratio"));
    EXPECT_FLOAT_EQ(2.0, indexParams.get_as_float("proxima.mips.reformer.l2_norm"));
}

TEST_F(HnswParamsInitializerTest, testInitSegmentSize)
{
    indexlib::util::KeyValueMap parameters;
    string indexParamsStr = R"({
        "proxima.hnsw.streamer.chunk_size" : 1024,
        "proxima.oswg.streamer.segment_size" : 1025,
    })";
    parameters[REALTIME_PARAMETERS] = indexParamsStr;
    AithetaIndexConfig indexConfig(parameters);
    AiThetaParams indexParams;
    HnswParamsInitializer initializer;
    ASSERT_TRUE(initializer.InitRealtimeBuildParams(indexConfig, indexParams, true));
    size_t segmentSize = 0;
    indexParams.get("proxima.hnsw.streamer.chunk_size", &segmentSize);
    ASSERT_EQ(1024, segmentSize);
    segmentSize = 0;
    indexParams.get("proxima.oswg.streamer.segment_size", &segmentSize);
    ASSERT_EQ(1025, segmentSize);
}

TEST_F(HnswParamsInitializerTest, testInitInitRealtimeParams2)
{
    indexlib::util::KeyValueMap parameters;
    AithetaIndexConfig indexConfig(parameters);
    AiThetaParams indexParams;
    HnswParamsInitializer initializer;
    ASSERT_TRUE(initializer.InitRealtimeBuildParams(indexConfig, indexParams, true));

    size_t segmentSize = 0;
    indexParams.get("proxima.hnsw.streamer.chunk_size", &segmentSize);
    ASSERT_EQ(16 * 1024, segmentSize);
    segmentSize = 0;
    indexParams.get("proxima.oswg.streamer.segment_size", &segmentSize);
    ASSERT_EQ(16 * 1024, segmentSize);
}

AUTIL_LOG_SETUP(indexlib.index, HnswParamsInitializerTest);
} // namespace indexlibv2::index::ann
