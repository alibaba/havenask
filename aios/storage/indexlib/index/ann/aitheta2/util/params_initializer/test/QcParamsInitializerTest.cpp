
#include "unittest/unittest.h"
#define protected public
#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/index/ann/aitheta2/util/params_initializer/QcParamsInitializer.h"

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
    ASSERT_TRUE(initializer.InitBuildParams(indexConfig, indexParams));
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
    ASSERT_TRUE(initializer.InitBuildParams(indexConfig, indexParams));
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
    ASSERT_TRUE(initializer.InitSearchParams(indexConfig, indexParams));
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
        ASSERT_TRUE(initializer.InitSearchParams(indexConfig, indexParams));
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
        ASSERT_TRUE(initializer.InitSearchParams(indexConfig, indexParams));
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
        ASSERT_TRUE(initializer.InitSearchParams(indexConfig, indexParams));
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
        ASSERT_TRUE(initializer.InitSearchParams(indexConfig, indexParams));
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
