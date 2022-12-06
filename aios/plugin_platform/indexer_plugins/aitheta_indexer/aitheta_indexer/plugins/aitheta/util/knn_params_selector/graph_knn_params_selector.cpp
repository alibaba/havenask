/**
 * @file   knn_params_selector.cpp
 * @author luoli.hn <luoli.hn@taobao.com>
 * @date   Wed Jan 16 17:44:37 2019
 *
 * @brief
 *
 *
 */

#include "aitheta_indexer/plugins/aitheta/util/knn_params_selector/graph_knn_params_selector.h"
#include "aitheta_indexer/plugins/aitheta/util/param_util.h"
#include <aitheta/algorithm/mips_reformer.h>

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace aitheta;
using namespace proxima;
IE_NAMESPACE_BEGIN(aitheta_plugin);

IE_LOG_SETUP(aitheta_plugin, GraphKnnParamsSelector);
GraphKnnParamsSelector::GraphKnnParamsSelector(const KnnStrategies &strategies)
    : NoneLinearKnnParamsSelector(strategies) {}

GraphKnnParamsSelector::~GraphKnnParamsSelector() {}

bool GraphKnnParamsSelector::InitKnnBuilderParams(const IE_NAMESPACE(util)::KeyValueMap &parameters,
                                                  aitheta::IndexParams &indexParams, bool printParam) {
    int32_t docSize = 0;
    ParamUtil::ExtractValue(parameters, DOCUMENT_NUM, &docSize);
    IE_NAMESPACE(util)::KeyValueMap dynamicParams;
    if (!GetDynamicParams(parameters, docSize, dynamicParams)) {
        IE_LOG(DEBUG, "donnot need use dynamic params");
    }

    // 将parameters 与 动态参数合并
    bool enbleCover = false;
    ParamUtil::MergeParams(parameters, dynamicParams, enbleCover);

    string graphType = "hnsw";
    uint64_t graphMaxScanNum = 25000;
    uint64_t graphMaxDocCnt = 1000 * 1000 * 50;
    uint64_t graphNeighborCnt = 100;
    uint64_t memoryQuota = 1024L * 1024L * 1024L;
    uint64_t hnswEfconstruction = 400;
    uint64_t hnswMaxLevel = 6;
    uint64_t hnswScalingFactor = 50;
    uint64_t hnswUpperNeighborCnt = 50;
    bool graphWithQuantizer = false;
    uint64_t hnswEf = 200;
    bool graphComboFile = true;

    // TODO(luoli.hn) 临时方案,有增量的时候会有问题
    graphMaxDocCnt = docSize + 100;
    auto iterator = parameters.find(PARAM_GRAPH_COMMON_GRAPH_TYPE);
    if (iterator != parameters.end()) {
        graphType = iterator->second;
    }

    iterator = parameters.find(PARAM_GRAPH_COMMON_WITH_QUANTIZER);
    if (iterator != parameters.end() && iterator->second == "true") {
        graphWithQuantizer = true;
    }

    iterator = parameters.find(PARAM_GRAPH_COMMON_COMBO_FILE);
    if (iterator != parameters.end() && iterator->second == "false") {
        graphComboFile = false;
    }
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM, &graphMaxScanNum);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_GRAPH_COMMON_NEIGHBOR_CNT, &graphNeighborCnt);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_GRAPH_BUILDER_MEMORY_QUOTA, &memoryQuota);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HNSW_BUILDER_EFCONSTRUCTION, &hnswEfconstruction);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HNSW_BUILDER_MAX_LEVEL, &hnswMaxLevel);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HNSW_BUILDER_SCALING_FACTOR, &hnswScalingFactor);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HNSW_BUILDER_UPPER_NEIGHBOR_CNT, &hnswUpperNeighborCnt);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HNSW_SEARCHER_EF, &hnswEf);

    // 检查以及纠正参数
    if (graphMaxScanNum > (uint64_t)docSize) {
        IE_LOG(DEBUG, "params[%s]'s value[%ld] > doc size[%d], use[%d] as its value",
               proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM.c_str(), graphMaxScanNum, docSize, docSize);
        graphMaxScanNum = docSize;
    }
    if (graphNeighborCnt > (uint64_t)docSize) {
        IE_LOG(DEBUG, "params[%s]'s value[%ld] > doc size[%d], use[%d] as its value",
               proxima::PARAM_GRAPH_COMMON_NEIGHBOR_CNT.c_str(), graphNeighborCnt, docSize, docSize);
        graphNeighborCnt = docSize;
    }
    if (hnswEf > (uint64_t)docSize) {
        IE_LOG(DEBUG, "params[%s]'s value[%ld] > doc size[%d], use[%d] as its value",
               proxima::PARAM_HNSW_SEARCHER_EF.c_str(), hnswEf, docSize, docSize);
        hnswEf = docSize;
    }
    if (hnswEfconstruction > (uint64_t)docSize) {
        IE_LOG(DEBUG, "params[%s]'s value[%ld] > doc size[%d], use[%d] as its value",
               proxima::PARAM_HNSW_BUILDER_EFCONSTRUCTION.c_str(), hnswEfconstruction, docSize, docSize);
        hnswEfconstruction = docSize;
    }
    if (hnswUpperNeighborCnt > graphNeighborCnt) {
        IE_LOG(DEBUG, "params[%s]'s value[%ld] > params[%s]'s value[%ld], use[%ld] as its value",
               proxima::PARAM_GRAPH_COMMON_NEIGHBOR_CNT.c_str(), hnswUpperNeighborCnt,
               proxima::PARAM_GRAPH_COMMON_NEIGHBOR_CNT.c_str(), graphNeighborCnt, graphNeighborCnt);
        hnswUpperNeighborCnt = graphNeighborCnt;
    }

    // 设置index parama参数
    indexParams.set(proxima::PARAM_GRAPH_COMMON_MAX_DOC_CNT, graphMaxDocCnt);
    indexParams.set(proxima::PARAM_GRAPH_COMMON_GRAPH_TYPE, graphType);
    indexParams.set(proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM, graphMaxScanNum);
    indexParams.set(proxima::PARAM_GRAPH_COMMON_NEIGHBOR_CNT, graphNeighborCnt);
    indexParams.set(proxima::PARAM_GRAPH_BUILDER_MEMORY_QUOTA, memoryQuota);
    indexParams.set(proxima::PARAM_GRAPH_COMMON_WITH_QUANTIZER, graphWithQuantizer);
    indexParams.set(proxima::PARAM_HNSW_BUILDER_EFCONSTRUCTION, hnswEfconstruction);
    indexParams.set(proxima::PARAM_HNSW_BUILDER_MAX_LEVEL, hnswMaxLevel);
    indexParams.set(proxima::PARAM_HNSW_BUILDER_SCALING_FACTOR, hnswScalingFactor);
    indexParams.set(proxima::PARAM_HNSW_BUILDER_UPPER_NEIGHBOR_CNT, hnswUpperNeighborCnt);
    indexParams.set(proxima::PARAM_HNSW_SEARCHER_EF, hnswEf);
    indexParams.set(proxima::PARAM_GRAPH_COMMON_COMBO_FILE, graphComboFile);

    if (printParam) {
        IE_LOG(DEBUG,
               "graph build param init:[%s = %ld], [%s = %s], [%s = %ld], [%s = %ld], [%s = %ld], [%s = %d], [%s "
               "= %ld], [%s = %ld], [%s = %ld], [%s = %ld], [%s = %d]",
               proxima::PARAM_GRAPH_COMMON_MAX_DOC_CNT.data(), graphMaxDocCnt,
               proxima::PARAM_GRAPH_COMMON_GRAPH_TYPE.data(), graphType.c_str(),
               proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM.data(), graphMaxScanNum,
               proxima::PARAM_GRAPH_COMMON_NEIGHBOR_CNT.data(), graphNeighborCnt,
               proxima::PARAM_GRAPH_BUILDER_MEMORY_QUOTA.data(), memoryQuota,
               proxima::PARAM_GRAPH_COMMON_WITH_QUANTIZER.data(), graphWithQuantizer,
               proxima::PARAM_HNSW_BUILDER_EFCONSTRUCTION.data(), hnswEfconstruction,
               proxima::PARAM_HNSW_BUILDER_MAX_LEVEL.data(), hnswMaxLevel,
               proxima::PARAM_HNSW_BUILDER_SCALING_FACTOR.data(), hnswScalingFactor,
               proxima::PARAM_HNSW_SEARCHER_EF.data(), hnswEf, proxima::PARAM_GRAPH_COMMON_COMBO_FILE.data(),
               graphComboFile);
    }

    return true;
}

bool GraphKnnParamsSelector::InitKnnSearcherParams(const IE_NAMESPACE(util)::KeyValueMap &parameters,
                                                   aitheta::IndexParams &indexParams, bool printParam) {
    int32_t docSize = 0;
    ParamUtil::ExtractValue(parameters, DOCUMENT_NUM, &docSize);
    IE_NAMESPACE(util)::KeyValueMap dynamicParams;
    if (!GetDynamicParams(parameters, docSize, dynamicParams)) {
        IE_LOG(DEBUG, "donnot need use dynamic params");
    }
    bool enbleCover = false;
    ParamUtil::MergeParams(parameters, dynamicParams, enbleCover);
    string graphType = "hnsw";
    uint64_t hnswEf = 100;
    uint64_t graphMaxScanNum = 15000;
    bool graphComboFile = true;

    auto iterator = parameters.find(PARAM_GRAPH_COMMON_GRAPH_TYPE);
    if (iterator != parameters.end()) {
        graphType = iterator->second;
    }

    iterator = parameters.find(PARAM_GRAPH_COMMON_COMBO_FILE);
    if (iterator != parameters.end() && iterator->second == "false") {
        graphComboFile = false;
    }

    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HNSW_SEARCHER_EF, &hnswEf);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM, &graphMaxScanNum);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_GRAPH_COMMON_COMBO_FILE, &graphComboFile);
    // 检查以及纠正参数
    if (graphMaxScanNum > (uint64_t)docSize) {
        IE_LOG(DEBUG, "params[%s]'s value[%ld] > doc size[%d], use[%d] as its value",
               proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM.c_str(), graphMaxScanNum, docSize, docSize);
        graphMaxScanNum = docSize;
    }

    if (hnswEf > (uint64_t)docSize) {
        IE_LOG(DEBUG, "params[%s]'s value[%ld] > doc size[%d], use[%d] as its value",
               proxima::PARAM_HNSW_SEARCHER_EF.c_str(), hnswEf, docSize, docSize);
        hnswEf = docSize;
    }

    // 设置index parama参数
    indexParams.set(proxima::PARAM_GRAPH_COMMON_GRAPH_TYPE, graphType);
    indexParams.set(proxima::PARAM_HNSW_SEARCHER_EF, hnswEf);
    indexParams.set(proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM, graphMaxScanNum);
    indexParams.set(proxima::PARAM_GRAPH_COMMON_COMBO_FILE, graphComboFile);

    if (printParam) {
        IE_LOG(DEBUG, "graph build param init:[%s = %s], [%s = %ld], [%s = %ld]",
               proxima::PARAM_GRAPH_COMMON_GRAPH_TYPE.data(), graphType.c_str(), proxima::PARAM_HNSW_SEARCHER_EF.data(),
               hnswEf, proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM.data(), graphMaxScanNum);
    }

    return true;
}

IE_NAMESPACE_END(aitheta_plugin);
