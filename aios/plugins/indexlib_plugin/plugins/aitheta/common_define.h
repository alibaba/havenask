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
#ifndef __INDEXLIB_PLUGIN_COMMON_DEFINE_H
#define __INDEXLIB_PLUGIN_COMMON_DEFINE_H

#include <typeinfo>
#include <aitheta/index_framework.h>
#include <aitheta/algorithm/mips_reformer.h>
#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/util/KeyValueMap.h"
#include "indexlib/misc/log.h"
#include "indexlib/misc/common.h"
#include "indexlib/indexlib.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/indexlib.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_define.h"

namespace indexlib {
namespace aitheta_plugin {

typedef aitheta::IndexBuilder::Pointer IndexBuilderPtr;
typedef aitheta::IndexSearcher::Pointer IndexSearcherPtr;
typedef aitheta::IndexStreamer::Pointer IndexStreamerPtr;
typedef aitheta::IndexStorage::Pointer IndexStoragePtr;
typedef aitheta::IndexSearcher::Context::Pointer ContextPtr;
typedef aitheta::IndexParams IndexParams;
typedef aitheta::IndexMeta IndexMeta;
typedef std::shared_ptr<float> EmbeddingPtr;
typedef aitheta::IndexSearcher::Document Document;
typedef std::shared_ptr<DocIdMap> DocIdMapPtr;
typedef int64_t catid_t;
typedef EmbeddingPtr embedding_t;

const std::string DIMENSION = "dimension";

const std::string SEGMENT_META_FILE = "aitheta.segment.meta";
const std::string EMBEDDING_FILE = "aitheta.embedding.info";
const std::string INDEX_ATTR_FILE = "aitheta.index.attr.json";
const std::string INDEX_FILE = "aitheta.index";

const std::string USE_LINEAR_THRESHOLD = "use_linear_threshold";
const std::string TOPK_KEY = "topk_flag";
const std::string SCORE_FILTER_KEY = "score_filter_flag";
const std::string EMBEDDING_SEPARATOR = "embedding_separator";
const std::string CATEGORY_EMBEDDING_SEPARATOR = "category_embedding_separator";
const std::string CATEGORY_SEPARATOR = "category_separator";
const std::string EXTEND_SEPARATOR = "extend_separator";
const std::string QUERY_SEPARATOR = "query_separator";

const std::string INNER_PRODUCT = "ip";
const std::string IP = INNER_PRODUCT;
const std::string L2 = "l2";
const std::string INDEX_TYPE = "index_type";
const std::string BUILD_METRIC_TYPE = "build_metric_type";
const std::string SEARCH_METRIC_TYPE = "search_metric_type";
const std::string SEARCH_DEVICE_TYPE = "search_device_type";
const std::string DEVICE_TYPE_GPU = "gpu";
const std::string DEVICE_TYPE_CPU = "cpu";
const std::string GPU_SEARCH_DOC_NUM_LOW_BOUND = "gpu_search.doc_num_low_bound";

const std::string INDEX_TYPE_LR = "linear";
const std::string KNN_LR_BUILDER = "KnnLinearBuilder";
const std::string KNN_LR_SEARCHER = "KnnLinearSearcher";
const std::string GPU_KNN_LR_SEARCHER = "GpuKnnLinearSearcher";
const std::string INDEX_TYPE_PQ = "pq";
const std::string KNN_PQ_BUILDER = "KnnPQBuilder";
const std::string KNN_PQ_SEARCHER = "KnnPQSearcher";
const std::string GPU_KNN_PQ_SEARCHER = "GpuKnnPQSearcher";
const std::string INDEX_TYPE_HC = "hc";
const std::string KNN_HC_BUILDER = "ClusteringBuilder";
const std::string KNN_HC_SEARCHER = "ClusteringSearcher";
const std::string GPU_KNN_HC_SEARCHER = "GpuKnnHCSearcher";
const std::string INDEX_TYPE_GRAPH = "graph";
const std::string KNN_GRAPH_BUILDER = "GraphBuilder";
const std::string KNN_GRAPH_SEARCHER = "GraphSearcher";

const std::string INDEX_TYPE_HNSW = "hnsw";
const std::string KNN_HNSW_SEARCHER = "HnswSearcher";
const std::string KNN_HNSW_STREAMER = "HnswStreamer";

const std::string INVALID_PQ_STREAMER = "UnknownPqStreamer";
const std::string INVALID_LR_STREAMER = "UnknownLrStreamer";
const std::string INVALID_HC_STREAMER = "UnknownHcStreamer";
const std::string INVALID_GRAPH_STREAMER = "UnknownGraphStreamer";

const std::string DOC_NUM = "document_num";
const std::string TMP_DUMP_DIR = "build_temp_dump_dir";
const std::string PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_RATIO = "proxima.general.builder.train_sample_ratio";

const std::string USE_DYNAMIC_PARAMS = "use_dynamic_params";

const std::string DEFAULT_STRATEGY = "default";
const std::string STRATEGY_NAME = "strategy_name";
const std::string DYNAMIC_STRATEGY = "dynamic_strategy";

const std::string KNN_GLOBAL_CONFIG = "knn_global_config.json";

const std::string MIPS_ENABLE = "mips.enable";
const std::string MIPS_PARAM_MVAL = "mips.param.mval";
const std::string MIPS_PARAM_UVAL = "mips.param.uval";
const std::string MIPS_PARAM_NORM = "mips.param.norm";

const std::string CONCURRENT_SEARCH_QUEUE_SIZE = "concurrent_search.queue_size";
const std::string CONCURRENTL_SEARCH_THREAD_NUM = "concurrent_search.thread_num";
const std::string CONCURRENT_SEARCH_ENABLE = "concurrent_search.enable";

const std::string METRIC_RECALL_ENABLE = "metric.recall.enable";
const std::string METRIC_RECALL_SAMPLE_INTERVAL = "metric.recall.sample_interval";

const std::string TOPK_ADJUST_ECONOMIC = "topk_adjust.economic";
const std::string TOPK_ADJUST_ZOOMOUT = "topk_adjust.zoomout";

const std::string RT_ENABLE = "rt.enable";
const std::string RT_SKIP_DOC = "rt.skip_doc";
const std::string RT_QUEUE_SIZE = "rt.queue_size";
const std::string RT_MIPS_NORM = "rt.mips.norm";
const std::string RT_DOC_NUM_PREDICT = "rt.doc_num_predict";

const std::string INC_ENABLE = "inc.enable";

const std::string FP16_QUANTIZATION_ENABLE = "fp16_quantization.enable";

const std::string FORCE_BUILD_INDEX = "force_build_index";

const uint32_t INVALID_DIMENSION = 0u;
const catid_t DEFAULT_CATEGORY_ID = INT64_MAX;
const catid_t INVALID_CATEGORY_ID = -1l;
const double REDUCE_MEM_USE_MAGIC_NUM = 2.5f;
const double RT_BUILD_MEM_USE_MAGIC_NUM = 1.2f;
const double IDXSEG_LOAD_MEM_USE_MAGIC_NUM = 1.1f;
const int32_t INVALID_SGT_NO = -1;

enum class DistType : int16_t { kUnknown = 0, kL2, kIP };
// kPMIndex -> Parallel Merge Index
// kRtIndex -> RealTime Index
enum class SegmentType : int16_t { kUnknown = 0, kRaw, kIndex, kPMIndex, kRtIndex };

struct MipsParams : public autil::legacy::Jsonizable {
    MipsParams(bool en = false, uint32_t mv = 0u, float uv = 0.0f, float nm = 0.0f)
        : enable(en), mval(mv), uval(uv), norm(nm) {}
    void Jsonize(JsonWrapper &json) override;
    bool enable;
    uint32_t mval;
    float uval;
    float norm;
};

struct GpuSearchOption {
    bool enable = false;
    // enable Gpu when doc num >= docNumLowBound
    size_t docNumLowBound = 0u;
};

struct TopkOption {
    // kEconomic: each segment's adjusted topk
    //            = topk * zoomout * (segment docNum) / (total docNum)
    // kHighRec : each segment's adjusted topk = topk
    enum Plan { kEconomic, kHighRec };
    Plan plan = Plan::kHighRec;
    float zoomout = 1.2f;
};

struct RtOption {
    bool enable = false;
    bool skipDoc = true;
    uint32_t queueSize = 1024u;
    // predicted real-time docNum before inc-segment is loaded
    uint32_t docNumPredict = 0u;
    float mipsNorm = 0.0f;
};

struct IncOption {
    bool enable = false;
};

struct MetricOption {
    // latency, return num, etc.
    bool enableRecReport = true;
    uint32_t recReportSampleInterval = 1024u;
};

struct SchemaParameter {
    SchemaParameter(const util::KeyValueMap &parameters);
    SchemaParameter(const SchemaParameter &) = default;
    ~SchemaParameter() = default;

    std::string indexType;
    uint32_t dimension;
    // when doc size is smaller than this, use linear method instead
    size_t knnLrThold;

    DistType searchDistType;
    DistType buildDistType;

    RtOption rtOption;
    IncOption incOption;
    TopkOption topkOption;
    MetricOption metricOption;
    GpuSearchOption gpuSearchOption;
    bool forceBuildIndex;
    std::string topkKey;
    // score filter key
    std::string sfKey;
    std::string catidEmbedSeparator;
    std::string catidSeparator;
    std::string embedSeparator;
    std::string querySeparator;

    util::KeyValueMap keyValMap;

 private:
    IE_LOG_DECLARE();
};

struct EmbedInfo {
    EmbedInfo(docid_t docid, const EmbeddingPtr &embedding) : docid(docid), embedding(embedding) {}
    EmbedInfo() : docid(INVALID_DOCID), embedding(nullptr) {}
    docid_t docid;
    EmbeddingPtr embedding;
};

struct Query {
    Query(int64_t id, uint32_t k, bool ef, float thresh, int32_t d, EmbeddingPtr &emb, uint32_t num = 1u,
          std::shared_ptr<float> osqn = nullptr, bool ils = false)
        : catid(id),
          topk(k),
          enableScoreFilter(ef),
          threshold(thresh),
          dimension(d),
          embedding(emb),
          queryNum(num),
          originSqNorms(osqn),
          isLrSearch(ils) {}

    Query(const Query &) = default;
    Query &operator=(const Query &) = default;

    int64_t catid;
    uint32_t topk;
    bool enableScoreFilter;
    float threshold;
    int32_t dimension;
    EmbeddingPtr embedding;
    // used for gpu batch search
    uint32_t queryNum;
    // used for mips
    std::shared_ptr<float> originSqNorms;
    bool isLrSearch;
    std::string toString() const;
};

// Richard.sy(TODO):  vector<Query> -> QueryHolder
struct QueryHolder {
    std::vector<Query> ques;
    bool enableDistFilter = false;
    float distLowBound = 0.0f;
    bool isLrSearch = false;
};

#define MALLOC(smartPtr, n, T)                                                                      \
    do {                                                                                            \
        smartPtr.reset(new (std::nothrow) T[n], [](T *p) { delete[] p; });                          \
        if (nullptr == smartPtr) {                                                                  \
            IE_LOG(ERROR, "failed to allocate size[%lu] in type[%s]", (size_t)n, typeid(T).name()); \
        }                                                                                           \
    } while (0)

#define MALLOC_CHECK(smartPtr, n, T)                                                                \
    do {                                                                                            \
        smartPtr.reset(new (std::nothrow) T[n], [](T *p) { delete[] p; });                          \
        if (nullptr == smartPtr) {                                                                  \
            IE_LOG(ERROR, "failed to allocate size[%lu] in type[%s]", (size_t)n, typeid(T).name()); \
        }                                                                                           \
    } while (0);                                                                                    \
    if (!smartPtr) {                                                                                \
        return false;                                                                               \
    }

bool Str2Type(const std::string &str, DistType &type);
std::string Type2Str(const SegmentType &type);

template <typename KEY, typename VAL>
std::string HashMap2Str(const std::unordered_map<KEY, VAL> &map_) {
    std::string str;
    for (const auto &keyValue : map_) {
        str.append(autil::StringUtil::toString(keyValue.first))
            .append(":")
            .append(autil::StringUtil::toString(keyValue.second))
            .append(",");
    }
    return str;
}

}
}

#endif  //__INDEXLIB_PLUGIN_COMMON_DEFINE_H
