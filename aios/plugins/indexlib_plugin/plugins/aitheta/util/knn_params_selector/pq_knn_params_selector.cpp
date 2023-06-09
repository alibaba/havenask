/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * @file   knn_params_selector.cpp
 * @author luoli.hn <luoli.hn@taobao.com>
 * @date   Wed Jan 16 17:44:37 2019
 *
 * @brief
 *
 *
 */
#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/pq_knn_params_selector.h"
#include <aitheta/algorithm/mips_reformer.h>
#include "indexlib_plugin/plugins/aitheta/util/param_util.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace aitheta;
using namespace proxima;
namespace indexlib {
namespace aitheta_plugin {
IE_LOG_SETUP(aitheta_plugin, PQKnnParamsSelector);
PQKnnParamsSelector::PQKnnParamsSelector(const KnnStrategies &strategies)
    : NoneLinearKnnParamsSelector(strategies), mSearcherName(KNN_PQ_SEARCHER) {}

PQKnnParamsSelector::~PQKnnParamsSelector() {}

bool PQKnnParamsSelector::InitBuilderParams(const indexlib::util::KeyValueMap &parameters,
                                               aitheta::IndexParams &indexParams, bool printParam) {
    int32_t docNum = 0;
    ParamUtil::ExtractValue(parameters, DOC_NUM, &docNum);
    indexlib::util::KeyValueMap dynamicParams;
    if (!GetDynamicParams(parameters, docNum, dynamicParams)) {
        IE_LOG(INFO, "donnot need use dynamic params");
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
    if (productCentriodNum > docNum) {
        IE_LOG(INFO,
               "product centroid number [%d] is bigger than document "
               "size[%d], use [%d] size as product centroid number",
               productCentriodNum, docNum, docNum);
        productCentriodNum = docNum;
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

    if (coarseCentroidNum > docNum) {
        IE_LOG(INFO,
               "rough centroid number [%d] is bigger than document "
               "size[%d], use [%d] as rough centroid number",
               coarseCentroidNum, docNum, docNum);
        coarseCentroidNum = docNum;
    }
    indexParams.set(proxima::PARAM_PQ_BUILDER_TRAIN_COARSE_CENTROID_NUM, coarseCentroidNum);

    if (memoryQuota) {
        indexParams.set(proxima::PARAM_GENERAL_BUILDER_MEMORY_QUOTA, memoryQuota);
    }
    trainSampleCnt = std::max(static_cast<int32_t>(trainSampleRatio * docNum), trainSampleCnt);
    if (!trainSampleCnt || trainSampleCnt > docNum) {
        trainSampleCnt = docNum;
    }
    indexParams.set(proxima::PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_COUNT, trainSampleCnt);

    auto iterator = parameters.find(TMP_DUMP_DIR);
    if (parameters.end() != iterator) {
        buildTempStorageDir = iterator->second;
    } else {
        IE_LOG(WARN, "build temp storage dir is not defined");
    }
    indexParams.set(proxima::PARAM_PQ_BUILDER_INTERMEDIATE_PATH, buildTempStorageDir);
    if (printParam) {
        IE_LOG(INFO,
               "pq builder index param init: [%s = %d], [%s = %d], [%s] = %d], "
               "[%s = %ld], [%s = %d], [%s = %d], [%s] = [%s]",
               proxima::PARAM_PQ_BUILDER_TRAIN_PRODUCT_CENTROID_NUM.data(), productCentriodNum,
               proxima::PARAM_PQ_BUILDER_TRAIN_FRAGMENT_NUM.data(), fragmentNum,
               proxima::PARAM_PQ_BUILDER_TRAIN_COARSE_CENTROID_NUM.data(), coarseCentroidNum,
               proxima::PARAM_GENERAL_BUILDER_MEMORY_QUOTA.data(), memoryQuota,
               proxima::PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_COUNT.data(), trainSampleCnt, DOC_NUM.data(), docNum,
               TMP_DUMP_DIR.data(), buildTempStorageDir.data());
    }
    return true;
}

bool PQKnnParamsSelector::InitSearcherParams(const indexlib::util::KeyValueMap &parameters,
                                                aitheta::IndexParams &indexParams, bool printParam) {
    auto itr = parameters.find(SEARCH_DEVICE_TYPE);
    if (itr != parameters.end() && 0 == strcasecmp(itr->second.c_str(), DEVICE_TYPE_GPU.c_str())) {
        mSearcherName = GPU_KNN_PQ_SEARCHER;
        AUTIL_LOG(INFO, "searcher name with gpu is [%s]", mSearcherName.c_str());
    }

    int32_t docNum = 0;
    ParamUtil::ExtractValue(parameters, DOC_NUM, &docNum);
    indexlib::util::KeyValueMap dynamicParams;
    if (!GetDynamicParams(parameters, docNum, dynamicParams)) {
        IE_LOG(INFO, "get dynamic params failed");
    }

    // 将parameters 与 动态参数合并
    ParamUtil::MergeParams(parameters, dynamicParams, true);
    float coarseScanRatio = 0.01f;
    int32_t productScanNum = 5000;
    IndexDistance::Methods searchMetricType = GetMethodType(parameters, SEARCH_METRIC_TYPE);

    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_PQ_SEARCHER_COARSE_SCAN_RATIO, &coarseScanRatio);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_PQ_SEARCHER_PRODUCT_SCAN_NUM, &productScanNum);

    if (GPU_KNN_PQ_SEARCHER == mSearcherName && productScanNum > 1024) {  // GPU QP searcher productScanNum<=1024
        productScanNum = 1024;
    }

    indexParams.set(proxima::PARAM_PQ_SEARCHER_COARSE_SCAN_RATIO, coarseScanRatio);
    indexParams.set(proxima::PARAM_PQ_SEARCHER_PRODUCT_SCAN_NUM, productScanNum);
    indexParams.erase(proxima::PARAM_GENERAL_SEARCHER_SEARCH_METHOD);
    indexParams.set(proxima::PARAM_GENERAL_SEARCHER_SEARCH_METHOD, searchMetricType);
    if (printParam) {
        IE_LOG(INFO, "pq searcher param init: [%s = %d], [%s = %f], [%s = %d]", SEARCH_METRIC_TYPE.data(),
               searchMetricType, proxima::PARAM_PQ_SEARCHER_COARSE_SCAN_RATIO.data(), coarseScanRatio,
               proxima::PARAM_PQ_SEARCHER_PRODUCT_SCAN_NUM.data(), productScanNum);
    }

    return true;
}
}
}
