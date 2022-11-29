/**
 * @file   knn_params_selector.cpp
 * @author luoli.hn <luoli.hn@taobao.com>
 * @date   Wed Jan 16 17:44:37 2019
 *
 * @brief
 *
 *
 */
#include "aitheta_indexer/plugins/aitheta/util/knn_params_selector/pq_knn_params_selector.h"
#include <aitheta/algorithm/mips_reformer.h>
#include "aitheta_indexer/plugins/aitheta/util/param_util.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace aitheta;
using namespace proxima;
IE_NAMESPACE_BEGIN(aitheta_plugin);
IE_LOG_SETUP(aitheta_plugin, PQKnnParamsSelector);
PQKnnParamsSelector::PQKnnParamsSelector(const KnnStrategies &strategies) : NoneLinearKnnParamsSelector(strategies) {}

PQKnnParamsSelector::~PQKnnParamsSelector() {}

bool PQKnnParamsSelector::InitKnnBuilderParams(const IE_NAMESPACE(util)::KeyValueMap &parameters,
                                               aitheta::IndexParams &indexParams, bool printParam) {
    int32_t docSize = 0;
    ParamUtil::ExtractValue(parameters, DOCUMENT_NUM, &docSize);
    IE_NAMESPACE(util)::KeyValueMap dynamicParams;
    if (!GetDynamicParams(parameters, docSize, dynamicParams)) {
        IE_LOG(DEBUG, "donnot need use dynamic params");
    }

    // 将parameters 与 动态参数合并
    ParamUtil::MergeParams(parameters, dynamicParams, true);
    // TODO: memory quota need to be flexible
    int32_t productCentriodNum = 256;
    int32_t fragmentNum = 13;
    int32_t coarseCentroidNum = 8192;
    int64_t memoryQuota = 4LL * 1024LL * 1024LL * 1024LL;
    // TODO: has to be int32_t, weird
    int32_t trainSampleCnt = 0;
    float trainSampleRatio = 0.0f;
    string buildTempStorageDir = "pq_builder_temp";

    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_PQ_BUILDER_TRAIN_PRODUCT_CENTROID_NUM, &productCentriodNum);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_PQ_BUILDER_TRAIN_FRAGMENT_NUM, &fragmentNum);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_PQ_BUILDER_TRAIN_COARSE_CENTROID_NUM, &coarseCentroidNum);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_GENERAL_BUILDER_MEMORY_QUOTA, &memoryQuota);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_COUNT, &trainSampleCnt);
    ParamUtil::ExtractValue(dynamicParams, PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_RATIO, &trainSampleRatio);
    if (productCentriodNum > docSize) {
        IE_LOG(DEBUG,
               "product centroid number [%d] is bigger than document "
               "size[%d], use [%d] size as product centroid number",
               productCentriodNum, docSize, docSize);
        productCentriodNum = docSize;
    }
    indexParams.set(proxima::PARAM_PQ_BUILDER_TRAIN_PRODUCT_CENTROID_NUM, productCentriodNum);

    if (EnableMipsParams(parameters)) {
        MipsParams params;
        if (!InitMipsParams(parameters, params)) {
            return false;
        }
        int dim = -1;
        ParamUtil::ExtractValue(dynamicParams, DIMENSION, &dim);
        int mipsDim = params.mval + dim;
        if (mipsDim % fragmentNum != 0) {
            IE_LOG(ERROR, "mips is enabled, failed to divide Dimension[%d] by fragment[%d]", mipsDim, fragmentNum);
            return false;
        }
    }
    indexParams.set(proxima::PARAM_PQ_BUILDER_TRAIN_FRAGMENT_NUM, fragmentNum);

    if (coarseCentroidNum > docSize) {
        IE_LOG(DEBUG,
               "rough centroid number [%d] is bigger than document "
               "size[%d], use [%d] as rough centroid number",
               coarseCentroidNum, docSize, docSize);
        coarseCentroidNum = docSize;
    }
    indexParams.set(proxima::PARAM_PQ_BUILDER_TRAIN_COARSE_CENTROID_NUM, coarseCentroidNum);

    if (memoryQuota) {
        indexParams.set(proxima::PARAM_GENERAL_BUILDER_MEMORY_QUOTA, memoryQuota);
    }
    trainSampleCnt = std::max(static_cast<int32_t>(trainSampleRatio * docSize), trainSampleCnt);
    if (!trainSampleCnt || trainSampleCnt > docSize) {
        trainSampleCnt = docSize;
    }
    indexParams.set(proxima::PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_COUNT, trainSampleCnt);

    auto iterator = parameters.find(BUILD_TEMP_DUMP_DIR);
    if (parameters.end() != iterator) {
        buildTempStorageDir = iterator->second;
    } else {
        IE_LOG(WARN, "build temp storage dir is not defined");
    }
    indexParams.set(proxima::PARAM_PQ_BUILDER_INTERMEDIATE_PATH, buildTempStorageDir);
    if (printParam) {
        IE_LOG(DEBUG,
               "pq builder index param init: [%s = %d], [%s = %d], [%s] = %d], "
               "[%s = %ld], [%s = %d], [%s = %d], [%s] = [%s]",
               proxima::PARAM_PQ_BUILDER_TRAIN_PRODUCT_CENTROID_NUM.data(), productCentriodNum,
               proxima::PARAM_PQ_BUILDER_TRAIN_FRAGMENT_NUM.data(), fragmentNum,
               proxima::PARAM_PQ_BUILDER_TRAIN_COARSE_CENTROID_NUM.data(), coarseCentroidNum,
               proxima::PARAM_GENERAL_BUILDER_MEMORY_QUOTA.data(), memoryQuota,
               proxima::PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_COUNT.data(), trainSampleCnt, DOCUMENT_NUM.data(), docSize,
               BUILD_TEMP_DUMP_DIR.data(), buildTempStorageDir.data());
    }
    return true;
}

bool PQKnnParamsSelector::InitKnnSearcherParams(const IE_NAMESPACE(util)::KeyValueMap &parameters,
                                                aitheta::IndexParams &indexParams, bool printParam) {
    int32_t docSize = 0;
    ParamUtil::ExtractValue(parameters, DOCUMENT_NUM, &docSize);
    IE_NAMESPACE(util)::KeyValueMap dynamicParams;
    if (!GetDynamicParams(parameters, docSize, dynamicParams)) {
        IE_LOG(DEBUG, "get dynamic params failed");
    }

    // 将parameters 与 动态参数合并
    ParamUtil::MergeParams(parameters, dynamicParams, true);
    float coarseScanRatio = 0.01f;
    int32_t productScanNum = 5000;
    int32_t useDynamicParams = 0;
    IndexDistance::Methods searchMetricType = GetMethodType(parameters, SEARCH_METRIC_TYPE);

    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_PQ_SEARCHER_COARSE_SCAN_RATIO, &coarseScanRatio);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_PQ_SEARCHER_PRODUCT_SCAN_NUM, &productScanNum);

    indexParams.set(proxima::PARAM_PQ_SEARCHER_COARSE_SCAN_RATIO, coarseScanRatio);
    indexParams.set(proxima::PARAM_PQ_SEARCHER_PRODUCT_SCAN_NUM, productScanNum);
    indexParams.erase(proxima::PARAM_GENERAL_SEARCHER_SEARCH_METHOD);
    indexParams.set(proxima::PARAM_GENERAL_SEARCHER_SEARCH_METHOD, searchMetricType);
    if (printParam) {
        IE_LOG(DEBUG, "pq searcher param init: [%s = %d], [%s = %f], [%s = %d]", SEARCH_METRIC_TYPE.data(),
               searchMetricType, proxima::PARAM_PQ_SEARCHER_COARSE_SCAN_RATIO.data(), coarseScanRatio,
               proxima::PARAM_PQ_SEARCHER_PRODUCT_SCAN_NUM.data(), productScanNum);
    }

    return true;
}
IE_NAMESPACE_END(aitheta_plugin);
