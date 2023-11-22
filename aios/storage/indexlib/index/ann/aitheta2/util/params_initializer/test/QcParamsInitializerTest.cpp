
#include "indexlib/index/ann/aitheta2/util/params_initializer/QcParamsInitializer.h"

#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "unittest/unittest.h"

using namespace std;
using namespace aitheta2;

namespace indexlibv2::index::ann {

class QcParamsInitializerTest : public ::testing::Test
{
public:
    void SetUp() override {}
    void TearDown() override {}

protected:
    AUTIL_LOG_DECLARE();
};

TEST_F(QcParamsInitializerTest, testInitInitRealtimeBuildParams)
{
    indexlib::util::KeyValueMap parameters;
    string indexParamsStr = R"({
        "proxima.qc.streamer.segment_size" : 1024,
    })";
    parameters[REALTIME_PARAMETERS] = indexParamsStr;
    AithetaIndexConfig indexConfig(parameters);
    AiThetaParams indexParams;
    QcParamsInitializer initializer;
    ASSERT_TRUE(initializer.InitRealtimeBuildParams(indexConfig, indexParams, true));
    size_t segmentSize = 0;
    indexParams.get("proxima.qc.streamer.segment_size", &segmentSize);
    ASSERT_EQ(1024, segmentSize);
}

TEST_F(QcParamsInitializerTest, testInitInitRealtimeSearchParams)
{
    indexlib::util::KeyValueMap parameters;
    // string rtParamsStr = R"({
    //     "proxima.qc.streamer.scan_ratio" : 0.1,
    // })";
    // parameters[REALTIME_PARAMETERS] = rtParamsStr;

    string searchParams = R"({
        "proxima.qc.searcher.optimizer_params": {
            "proxima.hc.searcher.scan_ratio": 0.02
        },
        "proxima.qc.searcher.scan_ratio": 0.01
    })";
    parameters[INDEX_SEARCH_PARAMETERS] = searchParams;

    AithetaIndexConfig indexConfig(parameters);
    AiThetaParams indexParams;
    QcParamsInitializer initializer;
    ASSERT_TRUE(initializer.InitRealtimeSearchParams(indexConfig, indexParams));
    float scanRatio = 0.0;
    indexParams.get("proxima.qc.streamer.scan_ratio", &scanRatio);
    ASSERT_FLOAT_EQ(0.01f, scanRatio);

    AiThetaParams optimizerIndexParams;
    indexParams.get("proxima.qc.searcher.optimizer_params", &optimizerIndexParams);
    float optimizerScanRatio = 0.0;
    optimizerIndexParams.get("proxima.hc.searcher.scan_ratio", &optimizerScanRatio);
    ASSERT_FLOAT_EQ(0.02f, optimizerScanRatio);
}

TEST_F(QcParamsInitializerTest, testInitInitRealtimeSearchParams2)
{
    indexlib::util::KeyValueMap parameters;
    string rtParamsStr = R"({
        "proxima.qc.streamer.scan_ratio" : 0.1,
    })";
    parameters[REALTIME_PARAMETERS] = rtParamsStr;

    string searchParams = R"({
        "proxima.qc.searcher.optimizer_params": {
            "proxima.hc.searcher.scan_ratio": 0.02
        },
        "proxima.qc.searcher.scan_ratio": 0.01
    })";
    parameters[INDEX_SEARCH_PARAMETERS] = searchParams;

    AithetaIndexConfig indexConfig(parameters);
    AiThetaParams indexParams;
    QcParamsInitializer initializer;
    ASSERT_TRUE(initializer.InitRealtimeSearchParams(indexConfig, indexParams));
    float scanRatio = 0.0;
    indexParams.get("proxima.qc.streamer.scan_ratio", &scanRatio);
    ASSERT_FLOAT_EQ(0.1f, scanRatio);

    AiThetaParams optimizerIndexParams;
    indexParams.get("proxima.qc.searcher.optimizer_params", &optimizerIndexParams);
    float optimizerScanRatio = 0.0;
    optimizerIndexParams.get("proxima.hc.searcher.scan_ratio", &optimizerScanRatio);
    ASSERT_FLOAT_EQ(0.02f, optimizerScanRatio);
}

TEST_F(QcParamsInitializerTest, testInitInitRealtimeParams2)
{
    indexlib::util::KeyValueMap parameters;
    AithetaIndexConfig indexConfig(parameters);
    AiThetaParams indexParams;
    QcParamsInitializer initializer;
    ASSERT_TRUE(initializer.InitRealtimeBuildParams(indexConfig, indexParams, true));
    size_t segmentSize = 0;
    indexParams.get("proxima.qc.streamer.segment_size", &segmentSize);
    ASSERT_EQ(16 * 1024, segmentSize);
}

TEST_F(QcParamsInitializerTest, testCalcCentriodCount_Default)
{
    size_t count = 0;
    ASSERT_EQ("0", QcParamsInitializer::CalcCentriodCount(AiThetaParams(), count));
    count = 200;
    ASSERT_EQ("1", QcParamsInitializer::CalcCentriodCount(AiThetaParams(), count));
    count = 200 * 2500;
    ASSERT_EQ("2500", QcParamsInitializer::CalcCentriodCount(AiThetaParams(), count));
    count = 200 * (16000);
    ASSERT_EQ("400*40", QcParamsInitializer::CalcCentriodCount(AiThetaParams(), count));
}

TEST_F(QcParamsInitializerTest, testCalcCentriodCount_LegacyUserDefineParams)
{
    string paramsContent = R"({
        "stratified.doc_count_threshold" : 100000,
        "proxima.general.cluster.per_centroid_doc_count" : 500,
        "stratified.centroid_count_scaling_factor" : 5
     })";

    AiThetaParams params;
    ASSERT_TRUE(AiThetaParams::ParseFromBuffer(paramsContent, &params));

    size_t count = 0;
    ASSERT_EQ("0", QcParamsInitializer::CalcCentriodCount(params, count));
    count = 100000;
    ASSERT_EQ("200", QcParamsInitializer::CalcCentriodCount(params, count));
    count = 100001;
    ASSERT_EQ("35*7", QcParamsInitializer::CalcCentriodCount(params, count));
}

TEST_F(QcParamsInitializerTest, testCalcCentriodCount_UserDefineParams)
{
    string paramsContent = R"({
        "stratified.doc_count_threshold" : 100000,
        "stratified.per_centroid_doc_count" : 500,
        "stratified.centroid_count_scaling_factor" : 5
     })";

    AiThetaParams params;
    ASSERT_TRUE(AiThetaParams::ParseFromBuffer(paramsContent, &params));

    size_t count = 0;
    ASSERT_EQ("0", QcParamsInitializer::CalcCentriodCount(params, count));
    count = 100000;
    ASSERT_EQ("200", QcParamsInitializer::CalcCentriodCount(params, count));
    count = 100001;
    ASSERT_EQ("35*7", QcParamsInitializer::CalcCentriodCount(params, count));
}

TEST_F(QcParamsInitializerTest, testCalcCentriodCount_UserDefineParams_InvalidValue)
{
    string paramsContent = R"({
        "stratified.doc_count_threshold" : -1,
        "proxima.general.cluster.per_centroid_doc_count" : -1,
        "stratified.centroid_count_scaling_factor" : -1,
     })";

    AiThetaParams params;
    ASSERT_TRUE(AiThetaParams::ParseFromBuffer(paramsContent, &params));

    size_t count = 0;
    ASSERT_EQ("0", QcParamsInitializer::CalcCentriodCount(AiThetaParams(), count));
    count = 200;
    ASSERT_EQ("1", QcParamsInitializer::CalcCentriodCount(AiThetaParams(), count));
    count = 200 * 2500;
    ASSERT_EQ("2500", QcParamsInitializer::CalcCentriodCount(AiThetaParams(), count));
    count = 200 * (16000);
    ASSERT_EQ("400*40", QcParamsInitializer::CalcCentriodCount(AiThetaParams(), count));
}

TEST_F(QcParamsInitializerTest, testInitBuilderIndexParams)
{
    indexlib::util::KeyValueMap parameters;
    string indexParamsStr = R"({
        "proxima.qc.builder.centroid_count" : "1024*1024",
        "proxima.qc.builder.quantize_by_centroid" : true
    })";
    parameters[INDEX_PARAMETERS] = indexParamsStr;
    AithetaIndexConfig indexConfig(parameters);
    AiThetaParams indexParams;

    QcParamsInitializer initializer(1024);
    ASSERT_TRUE(initializer.InitNormalBuildParams(indexConfig, indexParams));
    ASSERT_EQ("1024*1024", indexParams.get_as_string("proxima.qc.builder.centroid_count"));
    ASSERT_TRUE(indexParams.get_as_bool("proxima.qc.builder.quantize_by_centroid"));
}

TEST_F(QcParamsInitializerTest, testInitBuilderIndexParams_CalcCentriodCount)
{
    indexlib::util::KeyValueMap parameters;
    string indexParamsStr = R"({
        "proxima.qc.builder.quantize_by_centroid" : true
    })";
    parameters[INDEX_PARAMETERS] = indexParamsStr;
    AithetaIndexConfig indexConfig(parameters);
    AiThetaParams indexParams;

    QcParamsInitializer initializer(1024 * 100);
    ASSERT_TRUE(initializer.InitNormalBuildParams(indexConfig, indexParams));
    ASSERT_EQ("512", indexParams.get_as_string("proxima.qc.builder.centroid_count"));
    ASSERT_TRUE(indexParams.get_as_bool("proxima.qc.builder.quantize_by_centroid"));
}

TEST_F(QcParamsInitializerTest, testInitSearcherIndexParams)
{
    indexlib::util::KeyValueMap parameters = {{"min_scan_doc_cnt", "1200"}};
    string indexParamsStr = R"({
        "proxima.qc.searcher.scan_ratio" : 0.2,
    })";
    parameters[INDEX_PARAMETERS] = indexParamsStr;
    AithetaIndexConfig indexConfig(parameters);
    AiThetaParams indexParams;
    QcParamsInitializer initializer(2000);
    ASSERT_TRUE(initializer.InitNormalSearchParams(indexConfig, indexParams));
    EXPECT_FLOAT_EQ(0.6, indexParams.get_as_float("proxima.qc.searcher.scan_ratio"));
}

TEST_F(QcParamsInitializerTest, testRewriteGpuStreamCount)
{
    {
        indexlib::util::KeyValueMap parameters = {{"min_scan_doc_cnt", "1200"}};
        string indexParamsStr = R"({
            "proxima.qc.searcher.scan_ratio" : 0.2,
            "proxima.qc.searcher.gpu_stream_count": 10
        })";
        parameters[INDEX_PARAMETERS] = indexParamsStr;
        AithetaIndexConfig indexConfig(parameters);
        AiThetaParams indexParams;
        QcParamsInitializer initializer(2000);
        ASSERT_TRUE(initializer.InitNormalSearchParams(indexConfig, indexParams));
        EXPECT_FLOAT_EQ(0.6, indexParams.get_as_float("proxima.qc.searcher.scan_ratio"));
        EXPECT_EQ(2, indexParams.get_as_int32("proxima.qc.searcher.gpu_stream_count"));
    }
    {
        autil::EnvUtil::setEnv("GPU_QC_STREAM_COUNT", "16", false);
        indexlib::util::KeyValueMap parameters = {{"min_scan_doc_cnt", "1200"}};
        string indexParamsStr = R"({
            "proxima.qc.searcher.scan_ratio" : 0.2,
            "proxima.qc.searcher.gpu_stream_count": 10
        })";
        parameters[INDEX_PARAMETERS] = indexParamsStr;
        AithetaIndexConfig indexConfig(parameters);
        AiThetaParams indexParams;
        QcParamsInitializer initializer(2000);
        ASSERT_TRUE(initializer.InitNormalSearchParams(indexConfig, indexParams));
        EXPECT_FLOAT_EQ(0.6, indexParams.get_as_float("proxima.qc.searcher.scan_ratio"));
        EXPECT_EQ(16, indexParams.get_as_int32("proxima.qc.searcher.gpu_stream_count"));
        autil::EnvUtil::unsetEnv("GPU_QC_STREAM_COUNT");
    }
    {
        indexlib::util::KeyValueMap parameters = {{"min_scan_doc_cnt", "1200"}};
        string indexParamsStr = R"({
            "proxima.qc.searcher.scan_ratio" : 0.2,
            "proxima.qc.searcher.optimizer_params": "{\"proxima.qc.searcher.gpu_stream_count\":10}"
        })";
        parameters[INDEX_PARAMETERS] = indexParamsStr;
        AithetaIndexConfig indexConfig(parameters);
        AiThetaParams indexParams;
        QcParamsInitializer initializer(2000);
        ASSERT_TRUE(initializer.InitNormalSearchParams(indexConfig, indexParams));
        EXPECT_FLOAT_EQ(0.6, indexParams.get_as_float("proxima.qc.searcher.scan_ratio"));
        EXPECT_FLOAT_EQ(2, indexParams.get_as_int32("proxima.qc.searcher.gpu_stream_count"));
        AiThetaParams optimizerParams;
        indexParams.get("proxima.qc.searcher.optimizer_params", &optimizerParams);
        EXPECT_EQ(2, optimizerParams.get_as_int32("proxima.qc.searcher.gpu_stream_count"));
    }
    {
        autil::EnvUtil::setEnv("GPU_QC_STREAM_COUNT", "16", false);
        indexlib::util::KeyValueMap parameters = {{"min_scan_doc_cnt", "1200"}};
        string indexParamsStr = R"({
            "proxima.qc.searcher.scan_ratio" : 0.2,
            "proxima.qc.searcher.optimizer_params": "{\"proxima.qc.searcher.gpu_stream_count\":10}"
        })";
        parameters[INDEX_PARAMETERS] = indexParamsStr;
        AithetaIndexConfig indexConfig(parameters);
        AiThetaParams indexParams;
        QcParamsInitializer initializer(2000);
        ASSERT_TRUE(initializer.InitNormalSearchParams(indexConfig, indexParams));
        EXPECT_FLOAT_EQ(0.6, indexParams.get_as_float("proxima.qc.searcher.scan_ratio"));
        EXPECT_EQ(16, indexParams.get_as_int32("proxima.qc.searcher.gpu_stream_count"));
        AiThetaParams optimizerParams;
        indexParams.get("proxima.qc.searcher.optimizer_params", &optimizerParams);
        EXPECT_EQ(16, optimizerParams.get_as_int32("proxima.qc.searcher.gpu_stream_count"));
        autil::EnvUtil::unsetEnv("GPU_QC_STREAM_COUNT");
    }
}

AUTIL_LOG_SETUP(indexlib.index, QcParamsInitializerTest);
} // namespace indexlibv2::index::ann
