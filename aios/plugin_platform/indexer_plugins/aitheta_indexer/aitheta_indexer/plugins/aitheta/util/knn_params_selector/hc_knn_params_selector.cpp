/**
 * @file   knn_params_selector.cpp
 * @author luoli.hn <luoli.hn@taobao.com>
 * @date   Wed Jan 16 17:44:37 2019
 *
 * @brief
 *
 *
 */

#include "aitheta_indexer/plugins/aitheta/util/knn_params_selector/hc_knn_params_selector.h"
#include "aitheta_indexer/plugins/aitheta/util/param_util.h"
#include <aitheta/algorithm/mips_reformer.h>

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace aitheta;
using namespace proxima;
IE_NAMESPACE_BEGIN(aitheta_plugin);

IE_LOG_SETUP(aitheta_plugin, HCKnnParamsSelector);
HCKnnParamsSelector::HCKnnParamsSelector(const KnnStrategies &strategies) : NoneLinearKnnParamsSelector(strategies) {}
HCKnnParamsSelector::~HCKnnParamsSelector() {}

bool HCKnnParamsSelector::InitKnnBuilderParams(const IE_NAMESPACE(util)::KeyValueMap &parameters,
                                               aitheta::IndexParams &indexParams, bool printParam) {
    int32_t docSize = 0;
    ParamUtil::ExtractValue(parameters, DOCUMENT_NUM, &docSize);
    IE_NAMESPACE(util)::KeyValueMap dynamicParams;
    if (!GetDynamicParams(parameters, docSize, dynamicParams)) {
        IE_LOG(DEBUG, "donnot need use dynamic params");
    }

    // 将parameters 与 动态参数合并
    bool allowCoverDynamicParams = false;
    ParamUtil::MergeParams(parameters, dynamicParams, allowCoverDynamicParams);
    int32_t basicBlockSize = 16;
    int32_t centroidNumInLevel1 = 1000;
    int32_t centroidNumInLevel2 = 100;
    int32_t leafCentriodNum = 100000;
    int32_t levelCnt = 2;
    int32_t maxDocCnt = 50000000;
    int32_t trainSampleCnt = 0;
    int64_t memoryQuota = 128LL * 1024LL * 1024LL * 1024LL;
    float trainSampleRatio = 0.0f;
    string buildTempStorageDir = "hc_builder_temp";
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_COMMON_BASIC_BLOCK_SIZE, &basicBlockSize);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "1",
                            &centroidNumInLevel1);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "2",
                            &centroidNumInLevel2);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_COMMON_LEVEL_CNT, &levelCnt);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_COMMON_MAX_DOC_CNT, &maxDocCnt);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_COUNT, &trainSampleCnt);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_BUILDER_MEMORY_QUOTA, &memoryQuota);
    ParamUtil::ExtractValue(dynamicParams, PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_RATIO, &trainSampleRatio);
    indexParams.set(proxima::PARAM_HC_COMMON_BASIC_BLOCK_SIZE, basicBlockSize);

    indexParams.set(proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "1", centroidNumInLevel1);
    indexParams.set(proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "2", centroidNumInLevel2);

    leafCentriodNum = centroidNumInLevel1 * centroidNumInLevel2;
    indexParams.set(proxima::PARAM_HC_COMMON_LEAF_CENTROID_NUM, leafCentriodNum);
    indexParams.set(proxima::PARAM_HC_COMMON_LEVEL_CNT, levelCnt);

    // TODO: equals to docSize + 1, tmp solution ,当后续向量支持增量之后此处会有问题!!!
    maxDocCnt = docSize + 1;
    indexParams.set(proxima::PARAM_HC_COMMON_MAX_DOC_CNT, maxDocCnt);

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
    indexParams.set(proxima::PARAM_HC_BUILDER_BASIC_PATH, buildTempStorageDir);
    indexParams.set(proxima::PARAM_HC_BUILDER_MEMORY_QUOTA, memoryQuota);
    indexParams.set(proxima::PARAM_HC_COMMON_COMBO_FILE, true);
    if (printParam) {
        IE_LOG(DEBUG,
               "hc builder param init: [%s = %d], [%s = %d], [%s] = %d], [%s = %d], [%s = %d], [%s = %d], [%s = %d], "
               "[%s = %f], [%s = %ld], [%s = %d], [%s] = [%s]",
               proxima::PARAM_HC_COMMON_BASIC_BLOCK_SIZE.data(), basicBlockSize,
               (proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "1").data(), centroidNumInLevel1,
               (proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "2").data(), centroidNumInLevel2,
               proxima::PARAM_HC_COMMON_LEAF_CENTROID_NUM.data(), leafCentriodNum,
               proxima::PARAM_HC_COMMON_LEVEL_CNT.data(), levelCnt, proxima::PARAM_HC_COMMON_MAX_DOC_CNT.data(),
               maxDocCnt, proxima::PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_COUNT.data(), trainSampleCnt,
               PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_RATIO.data(), trainSampleRatio,
               proxima::PARAM_HC_BUILDER_MEMORY_QUOTA.data(), memoryQuota, DOCUMENT_NUM.data(), docSize,
               BUILD_TEMP_DUMP_DIR.data(), buildTempStorageDir.data());
    }

    return true;
}

bool HCKnnParamsSelector::InitKnnSearcherParams(const IE_NAMESPACE(util)::KeyValueMap &parameters,
                                                aitheta::IndexParams &indexParams, bool printParam) {
    int32_t docSize = 0;
    ParamUtil::ExtractValue(parameters, DOCUMENT_NUM, &docSize);
    IE_NAMESPACE(util)::KeyValueMap dynamicParams;
    if (!GetDynamicParams(parameters, docSize, dynamicParams)) {
        IE_LOG(DEBUG, "donnot need use dynamic params");
    }
    bool allowCoverDynamicParams = false;
    ParamUtil::MergeParams(parameters, dynamicParams, allowCoverDynamicParams);

    int32_t basicBlockSize = 16;
    int32_t centroidNumInLevel1 = 1000;
    int32_t centroidNumInLevel2 = 100;
    int32_t leafCentriodNum = 100000;
    int32_t levelCnt = 2;
    int32_t maxDocCnt = 10000000;
    int32_t scanNumInLevel1 = 60;
    int32_t scanNumInLevel2 = 6000;
    int32_t maxScanNum = 50000;
    int32_t topK = 500;
    IndexDistance::Methods searchMetricType = GetMethodType(parameters, SEARCH_METRIC_TYPE);
    int32_t useDynamicParams = 0;

    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_COMMON_BASIC_BLOCK_SIZE, &basicBlockSize);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "1",
                            &centroidNumInLevel1);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "2",
                            &centroidNumInLevel2);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_COMMON_LEVEL_CNT, &levelCnt);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_COMMON_MAX_DOC_CNT, &maxDocCnt);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_SEARCHER_SCAN_NUM_IN_LEVEL_PREFIX + "1", &scanNumInLevel1);
    if (!ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_SEARCHER_SCAN_NUM_IN_LEVEL_PREFIX + "2",
                                 &scanNumInLevel2)) {
        scanNumInLevel2 = scanNumInLevel1 * centroidNumInLevel2;
    }
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_SEARCHER_MAX_SCAN_NUM, &maxScanNum);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_SEARCHER_TOPK, &topK);

    auto iterator = parameters.find(SEARCH_METRIC_TYPE);
    if (iterator != parameters.end()) {
        if (iterator->second == L2) {
            searchMetricType = IndexDistance::kMethodFloatSquaredEuclidean;
        } else if (iterator->second != INNER_PRODUCT) {
            IE_LOG(WARN, "unknown metric type [%s]", iterator->second.data());
        }
    }

    indexParams.set(proxima::PARAM_HC_COMMON_BASIC_BLOCK_SIZE, basicBlockSize);

    // TODO(zongyuan.wuzy): proxima 1.3+ will not need set these params
    leafCentriodNum = centroidNumInLevel1 * centroidNumInLevel2;
    indexParams.set(proxima::PARAM_HC_COMMON_LEAF_CENTROID_NUM, leafCentriodNum);
    indexParams.set(proxima::PARAM_HC_COMMON_LEVEL_CNT, levelCnt);
    maxDocCnt = docSize + 1;
    indexParams.set(proxima::PARAM_HC_COMMON_MAX_DOC_CNT, maxDocCnt);
    // TODO(zongyuan.wuzy): end

    indexParams.set(proxima::PARAM_HC_SEARCHER_SCAN_NUM_IN_LEVEL_PREFIX + "1", scanNumInLevel1);
    indexParams.set(proxima::PARAM_HC_SEARCHER_SCAN_NUM_IN_LEVEL_PREFIX + "2", scanNumInLevel2);
    indexParams.set(proxima::PARAM_HC_SEARCHER_MAX_SCAN_NUM, maxScanNum);
    indexParams.set(proxima::PARAM_HC_SEARCHER_TOPK, topK);
    indexParams.erase(proxima::PARAM_GENERAL_SEARCHER_SEARCH_METHOD);
    indexParams.set(proxima::PARAM_GENERAL_SEARCHER_SEARCH_METHOD, searchMetricType);
    indexParams.set(proxima::PARAM_HC_COMMON_COMBO_FILE, true);
    if (printParam) {
        IE_LOG(DEBUG,
               "hc searcher param init: [%s = %d], [%s = %d], [%s] = %d], [%s = "
               "%d], [%s = %d], [%s = %d], [%s = %d], [%s] = %d], [%s = %d]",
               proxima::PARAM_HC_COMMON_BASIC_BLOCK_SIZE.data(), basicBlockSize,
               proxima::PARAM_HC_COMMON_LEAF_CENTROID_NUM.data(), leafCentriodNum,
               proxima::PARAM_HC_COMMON_LEVEL_CNT.data(), levelCnt, proxima::PARAM_HC_SEARCHER_MAX_SCAN_NUM.data(),
               maxScanNum, (proxima::PARAM_HC_SEARCHER_SCAN_NUM_IN_LEVEL_PREFIX + "1").data(), scanNumInLevel1,
               (proxima::PARAM_HC_SEARCHER_SCAN_NUM_IN_LEVEL_PREFIX + "2").data(), scanNumInLevel2,
               proxima::PARAM_HC_COMMON_MAX_DOC_CNT.data(), maxDocCnt, proxima::PARAM_HC_SEARCHER_TOPK.data(), topK,
               proxima::PARAM_GENERAL_SEARCHER_SEARCH_METHOD.data(), searchMetricType);
    }
    return true;
}

IE_NAMESPACE_END(aitheta_plugin);
