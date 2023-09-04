
#include "fslib/fslib.h"
#include "unittest/unittest.h"

#define protected public
#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/index/ann/aitheta2/util/params_initializer/ParamsInitializer.h"

using namespace std;
using namespace aitheta2;

namespace indexlibv2::index::ann {

class ParamsInitializerTest : public ::testing::Test
{
public:
    void SetUp() override {}
    void TearDown() override {}

protected:
    AUTIL_LOG_DECLARE();
};

TEST_F(ParamsInitializerTest, testInitBuilderIndexParams)
{
    indexlib::util::KeyValueMap parameters;
    parameters[INDEX_PARAMETERS] = R"({
        "bool" : true,
        "integer" : 1024,
        "float" : 102.4,
        "string" : "1024",
        "object" : {
            "bool_1" : true,
            "integer_1" : 1024,
            "float_1" : 102.4,
            "string_1" : "1024"
        }
     })";
    AithetaIndexConfig indexConfig(parameters);
    ParamsInitializer initializer;
    AiThetaParams indexParams;
    ASSERT_TRUE(initializer.InitBuildParams(indexConfig, indexParams));
    ASSERT_TRUE(indexParams.get_as_bool("bool"));
    ASSERT_EQ(1024, indexParams.get_as_int32("integer"));
    EXPECT_FLOAT_EQ(102.4, indexParams.get_as_float("float"));
    ASSERT_EQ("1024", indexParams.get_as_string("string"));
    AiThetaParams subIndexParams;
    ASSERT_TRUE(indexParams.get("object", &subIndexParams));
    ASSERT_TRUE(subIndexParams.get_as_bool("bool_1"));
    ASSERT_EQ(1024, subIndexParams.get_as_int32("integer_1"));
    EXPECT_FLOAT_EQ(102.4, subIndexParams.get_as_float("float_1"));
    ASSERT_EQ("1024", subIndexParams.get_as_string("string_1"));
}

TEST_F(ParamsInitializerTest, testInitBuilderIndexParams_parseFailed)
{
    indexlib::util::KeyValueMap parameters;
    parameters[INDEX_PARAMETERS] = R"({
        "bool" : INVALID
    })";
    AithetaIndexConfig indexConfig(parameters);
    ParamsInitializer initializer;
    AiThetaParams indexParams;
    ASSERT_FALSE(initializer.InitBuildParams(indexConfig, indexParams));
}

TEST_F(ParamsInitializerTest, testInitSearcherIndexParams)
{
    indexlib::util::KeyValueMap parameters;
    parameters[INDEX_PARAMETERS] = R"({
        "bool" : true,
        "integer" : 1024,
        "float" : 102.4,
        "string" : "1024",
        "object" : {
            "bool_1" : true,
            "integer_1" : 1024,
            "float_1" : 102.4,
            "string_1" : "1024"
        }
     })";
    AithetaIndexConfig indexConfig(parameters);
    ParamsInitializer initializer;
    AiThetaParams indexParams;
    ASSERT_TRUE(initializer.InitBuildParams(indexConfig, indexParams));
    ASSERT_TRUE(indexParams.get_as_bool("bool"));
    ASSERT_EQ(1024, indexParams.get_as_int32("integer"));
    EXPECT_FLOAT_EQ(102.4, indexParams.get_as_float("float"));
    ASSERT_EQ("1024", indexParams.get_as_string("string"));
    AiThetaParams subIndexParams;
    ASSERT_TRUE(indexParams.get("object", &subIndexParams));
    ASSERT_TRUE(subIndexParams.get_as_bool("bool_1"));
    ASSERT_EQ(1024, subIndexParams.get_as_int32("integer_1"));
    EXPECT_FLOAT_EQ(102.4, subIndexParams.get_as_float("float_1"));
    ASSERT_EQ("1024", subIndexParams.get_as_string("string_1"));
}

TEST_F(ParamsInitializerTest, testInitSearcherIndexParams_parseFailed)
{
    indexlib::util::KeyValueMap parameters;
    parameters[INDEX_PARAMETERS] = R"({
        "bool" : INVALID
    })";
    AithetaIndexConfig indexConfig(parameters);
    ParamsInitializer initializer;
    AiThetaParams indexParams;
    ASSERT_FALSE(initializer.InitBuildParams(indexConfig, indexParams));
}

TEST_F(ParamsInitializerTest, testInitMeta)
{
    indexlib::util::KeyValueMap parameters = {
        {DISTANCE_TYPE, INNER_PRODUCT}, {DIMENSION, "128"}, {INDEX_ORDER_TYPE, ORDER_TYPE_COL}};
    AithetaIndexConfig indexConfig(parameters);
    ParamsInitializer initializer;
    AiThetaMeta aiTheta2IndexMeta;
    ASSERT_TRUE(initializer.InitAiThetaMeta(indexConfig, aiTheta2IndexMeta));
    ASSERT_EQ(128, aiTheta2IndexMeta.dimension());
    ASSERT_EQ(MajorOrder::MO_UNDEFINED, aiTheta2IndexMeta.major_order());
    ASSERT_EQ(FeatureType::FT_FP32, aiTheta2IndexMeta.type());
    ASSERT_EQ(INNER_PRODUCT, aiTheta2IndexMeta.measure_name());
}

TEST_F(ParamsInitializerTest, testInitMeta2)
{
    indexlib::util::KeyValueMap parameters = {{"streamer_name", "QcStreamer"},
                                              {DISTANCE_TYPE, SQUARED_EUCLIDEAN},
                                              {DIMENSION, "128"},
                                              {INDEX_ORDER_TYPE, ORDER_TYPE_ROW}};
    AithetaIndexConfig indexConfig(parameters);
    ParamsInitializer initializer;
    AiThetaMeta aiTheta2IndexMeta;
    ASSERT_TRUE(initializer.InitAiThetaMeta(indexConfig, aiTheta2IndexMeta));
    ASSERT_EQ(128, aiTheta2IndexMeta.dimension());
    ASSERT_EQ(MajorOrder::MO_ROW, aiTheta2IndexMeta.major_order());
    ASSERT_EQ(FeatureType::FT_FP32, aiTheta2IndexMeta.type());
    ASSERT_EQ(SQUARED_EUCLIDEAN, aiTheta2IndexMeta.measure_name());
}

TEST_F(ParamsInitializerTest, testInitMeta_InitDimensionFailded)
{
    indexlib::util::KeyValueMap parameters = {
        {DISTANCE_TYPE, INNER_PRODUCT}, {INDEX_ORDER_TYPE, ORDER_TYPE_COL}, {FEATURE_TYPE, FEATURE_TYPE_FP16}};
    AithetaIndexConfig indexConfig(parameters);
    ParamsInitializer initializer;
    AiThetaMeta aiTheta2IndexMeta;
    ASSERT_FALSE(initializer.InitAiThetaMeta(indexConfig, aiTheta2IndexMeta));
}

TEST_F(ParamsInitializerTest, testInitMeta_InitDistanceTypeFailed)
{
    indexlib::util::KeyValueMap parameters = {{DISTANCE_TYPE, "UNKNOWN_DISTANCE_TYPE"},
                                              {DIMENSION, "128"},
                                              {INDEX_ORDER_TYPE, ORDER_TYPE_COL},
                                              {FEATURE_TYPE, FEATURE_TYPE_FP16}};
    AithetaIndexConfig indexConfig(parameters);
    ParamsInitializer initializer;
    AiThetaMeta aiTheta2IndexMeta;
    ASSERT_FALSE(initializer.InitAiThetaMeta(indexConfig, aiTheta2IndexMeta));
}

TEST_F(ParamsInitializerTest, testCalcCentriodCount_Default)
{
    size_t count = 0;
    ASSERT_EQ("0", ParamsInitializer::CalcCentriodCount(AiThetaParams(), count));
    count = 200;
    ASSERT_EQ("1", ParamsInitializer::CalcCentriodCount(AiThetaParams(), count));
    count = 200 * 2500;
    ASSERT_EQ("2500", ParamsInitializer::CalcCentriodCount(AiThetaParams(), count));
    count = 200 * (16000);
    ASSERT_EQ("400*40", ParamsInitializer::CalcCentriodCount(AiThetaParams(), count));
}

TEST_F(ParamsInitializerTest, testCalcCentriodCount_LegacyUserDefineParams)
{
    string paramsContent = R"({
        "stratified.doc_count_threshold" : 100000,
        "proxima.general.cluster.per_centroid_doc_count" : 500,
        "stratified.centroid_count_scaling_factor" : 5
     })";

    AiThetaParams params;
    ASSERT_TRUE(AiThetaParams::ParseFromBuffer(paramsContent, &params));

    size_t count = 0;
    ASSERT_EQ("0", ParamsInitializer::CalcCentriodCount(params, count));
    count = 100000;
    ASSERT_EQ("200", ParamsInitializer::CalcCentriodCount(params, count));
    count = 100001;
    ASSERT_EQ("35*7", ParamsInitializer::CalcCentriodCount(params, count));
}

TEST_F(ParamsInitializerTest, testCalcCentriodCount_UserDefineParams)
{
    string paramsContent = R"({
        "stratified.doc_count_threshold" : 100000,
        "stratified.per_centroid_doc_count" : 500,
        "stratified.centroid_count_scaling_factor" : 5
     })";

    AiThetaParams params;
    ASSERT_TRUE(AiThetaParams::ParseFromBuffer(paramsContent, &params));

    size_t count = 0;
    ASSERT_EQ("0", ParamsInitializer::CalcCentriodCount(params, count));
    count = 100000;
    ASSERT_EQ("200", ParamsInitializer::CalcCentriodCount(params, count));
    count = 100001;
    ASSERT_EQ("35*7", ParamsInitializer::CalcCentriodCount(params, count));
}

TEST_F(ParamsInitializerTest, testCalcCentriodCount_UserDefineParams_InvalidValue)
{
    string paramsContent = R"({
        "stratified.doc_count_threshold" : -1,
        "proxima.general.cluster.per_centroid_doc_count" : -1,
        "stratified.centroid_count_scaling_factor" : -1,
     })";

    AiThetaParams params;
    ASSERT_TRUE(AiThetaParams::ParseFromBuffer(paramsContent, &params));

    size_t count = 0;
    ASSERT_EQ("0", ParamsInitializer::CalcCentriodCount(AiThetaParams(), count));
    count = 200;
    ASSERT_EQ("1", ParamsInitializer::CalcCentriodCount(AiThetaParams(), count));
    count = 200 * 2500;
    ASSERT_EQ("2500", ParamsInitializer::CalcCentriodCount(AiThetaParams(), count));
    count = 200 * (16000);
    ASSERT_EQ("400*40", ParamsInitializer::CalcCentriodCount(AiThetaParams(), count));
}

TEST_F(ParamsInitializerTest, testUpdateScanRatio)
{
    {
        string paramsContent = R"({})";
        AiThetaParams params;
        ASSERT_TRUE(AiThetaParams::ParseFromBuffer(paramsContent, &params));
        ParamsInitializer::UpdateScanRatio(2000, "proxima.qc.searcher.scan_ratio", params, 1200);
        EXPECT_FLOAT_EQ(0.6f, params.get_as_float("proxima.qc.searcher.scan_ratio"));
    }
    {
        string paramsContent = R"({})";
        AiThetaParams params;
        ASSERT_TRUE(AiThetaParams::ParseFromBuffer(paramsContent, &params));
        ParamsInitializer::UpdateScanRatio(2000, "proxima.qc.searcher.scan_ratio", params, 100);
        EXPECT_FLOAT_EQ(0.5f, params.get_as_float("proxima.qc.searcher.scan_ratio"));
    }
    {
        string paramsContent = R"({})";
        AiThetaParams params;
        ASSERT_TRUE(AiThetaParams::ParseFromBuffer(paramsContent, &params));
        ParamsInitializer::UpdateScanRatio(0, "proxima.qc.searcher.scan_ratio", params, 0);
        EXPECT_FLOAT_EQ(0.0f, params.get_as_float("proxima.qc.searcher.scan_ratio"));
    }
    {
        string paramsContent = R"({
        "proxima.qc.searcher.scan_ratio": 0.3
     })";
        AiThetaParams params;
        ASSERT_TRUE(AiThetaParams::ParseFromBuffer(paramsContent, &params));
        ParamsInitializer::UpdateScanRatio(1000, "proxima.qc.searcher.scan_ratio", params, 10);
        EXPECT_FLOAT_EQ(1.0f, params.get_as_float("proxima.qc.searcher.scan_ratio"));
    }
    {
        string paramsContent = R"({
        "proxima.qc.searcher.scan_ratio": 0.3
     })";
        AiThetaParams params;
        ASSERT_TRUE(AiThetaParams::ParseFromBuffer(paramsContent, &params));
        ParamsInitializer::UpdateScanRatio(1000, "proxima.qc.searcher.scan_ratio", params, 0);
        EXPECT_FLOAT_EQ(1.0f, params.get_as_float("proxima.qc.searcher.scan_ratio"));
    }
    {
        string paramsContent = R"({
        "proxima.qc.searcher.scan_ratio": 0.3
     })";
        AiThetaParams params;
        ASSERT_TRUE(AiThetaParams::ParseFromBuffer(paramsContent, &params));
        ParamsInitializer::UpdateScanRatio(100, "proxima.qc.searcher.scan_ratio", params, 200);
        EXPECT_FLOAT_EQ(1.0f, params.get_as_float("proxima.qc.searcher.scan_ratio"));
    }
}

TEST_F(ParamsInitializerTest, UpdateAiThetaParamsKey)
{
    string newParamsStr = R"({
            "qc.searcher.scan_ratio" : 0.2,
            "qc.searcher.optimizer_params": {"qc.searcher.gpu_stream_count":10}
    })";
    AiThetaParams newParams;
    ParamsInitializer::ParseValue(newParamsStr, newParams, false);
    EXPECT_FALSE(newParams.empty());

    string oldParamsStr = R"({
            "proxima.qc.searcher.scan_ratio" : 0.2,
            "proxima.qc.searcher.optimizer_params": {"proxima.qc.searcher.gpu_stream_count":10}
    })";
    AiThetaParams oldParams;
    ParamsInitializer::ParseValue(oldParamsStr, oldParams, false);
    EXPECT_EQ(newParams.debug_string(), oldParams.debug_string());
}

AUTIL_LOG_SETUP(indexlib.index, ParamsInitializerTest);
} // namespace indexlibv2::index::ann
