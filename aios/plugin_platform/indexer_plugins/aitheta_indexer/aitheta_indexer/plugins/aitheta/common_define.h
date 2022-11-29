#ifndef __INDEXLIB_PLUGIN_COMMON_DEFINE_H
#define __INDEXLIB_PLUGIN_COMMON_DEFINE_H

#include <memory>
#include <sstream>
#include <typeinfo>
#include <aitheta/index_framework.h>
#include <autil/StringUtil.h>
#include <autil/legacy/jsonizable.h>
#include <indexlib/common_define.h>
#include <indexlib/util/key_value_map.h>
#include <indexlib/misc/log.h>
#include <indexlib/indexlib.h>

IE_NAMESPACE_BEGIN(aitheta_plugin);

typedef aitheta::IndexBuilder::Pointer IndexBuilderPtr;
typedef aitheta::IndexSearcher::Pointer IndexSearcherPtr;
typedef aitheta::IndexStorage::Pointer IndexStoragePtr;
typedef aitheta::IndexSearcher::Context::Pointer ContextPtr;
typedef aitheta::IndexParams IndexParams;
typedef std::shared_ptr<float> EmbeddingPtr;
typedef aitheta::IndexSearcher::Document Document;
typedef std::map<int64_t, size_t> ReduceTaskInput;

const std::string DIMENSION = "dimension";

const std::string SEGMENT_META_FILE_NAME = "aitheta.segment.meta";
const std::string CATEGORYID_FILE_NAME = "aitheta.category.id";
const std::string DOCID_FILE_NAME = "aitheta.doc.id";
const std::string PKEY_FILE_NAME = "aitheta.pkey.id";
const std::string EMBEDDING_FILE_NAME = "aitheta.embedding";
const std::string INDEX_INFO_FILE_NAME = "aitheta.index.info";
const std::string JSONIZABLE_INDEX_INFO_FILE_NAME = "aitheta.index.info.json";
const std::string INDEX_FILE_NAME = "aitheta.index";
const std::string DOCID_REMAP_FILE_NAME = "aitheta.docid.remap";
const std::string KV_PARAMS_FILE_NAME = "aitheta.kvparams";

const std::string USE_LINEAR_THRESHOLD = "use_linear_threshold";
const std::string TOPK_FLAG = "topk_flag";
const std::string SCORE_FILTER_FLAG = "score_filter_flag";
const std::string EMBEDDING_SEPARATOR = "embedding_separator";
const std::string CATEGORY_EMBEDDING_SEPARATOR = "category_embedding_separator";
const std::string CATEGORY_SEPARATOR = "category_separator";
const std::string EXTEND_SEPARATOR = "extend_separator";
const std::string QUERY_SEPARATOR = "query_separator";

const std::string MAX_SIZE_PER_CAT = "max_size_per_category";
const std::string DOCID_REMAP_FILE_SIZE = "docid_remap_file_size";
const std::string DOCID_PKEY_FILE_SIZE = "docid_pkey_file_size";

const int32_t INVALID_DIMENSION = -1;
const int64_t DEFAULT_CATEGORY_ID = INT64_MAX;
const int64_t INVALID_CATEGORY_ID = -1l;

const std::string INNER_PRODUCT = "ip";
const std::string L2 = "l2";
const std::string INDEX_TYPE = "index_type";
const std::string BUILD_METRIC_TYPE = "build_metric_type";
const std::string SEARCH_METRIC_TYPE = "search_metric_type";

const std::string INDEX_TYPE_LR = "linear";
const std::string KNN_LR_BUILDER = "KnnLinearBuilder";
const std::string KNN_LR_SEARCHER = "KnnLinearSearcher";
const std::string INDEX_TYPE_PQ = "pq";
const std::string KNN_PQ_BUILDER = "KnnPQBuilder";
const std::string KNN_PQ_SEARCHER = "KnnPQSearcher";
const std::string INDEX_TYPE_HC = "hc";
const std::string KNN_HC_BUILDER = "ClusteringBuilder";
const std::string KNN_HC_SEARCHER = "ClusteringSearcher";
const std::string INDEX_TYPE_GRAPH = "graph";
const std::string KNN_GRAPH_BUILDER = "GraphBuilder";
const std::string KNN_GRAPH_SEARCHER = "GraphSearcher";

const std::string DOCUMENT_NUM = "document_num";
const std::string BUILD_TEMP_DUMP_DIR = "build_temp_dump_dir";
const std::string PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_RATIO = "proxima.general.builder.train_sample_ratio";

const std::string USE_DYNAMIC_PARAMS = "use_dynamic_params";
const std::string DYNAMIC_PARAMS_LIST = "dynamic_params_list";

const std::string DEFAULT_STRATEGY = "default";
const std::string STRATEGY_NAME = "strategy_name";
const std::string DYNAMIC_STRATEGY = "dynamic_strategy";

const std::string KNN_GLOBAL_CONFIG = "knn_global_config.json";

const double RAW_SEG_FULL_REDUCE_MEM_USE_MAGIC_NUM = 2.5f;
const double IDX_SEG_LOAD_4_RETRIEVE_MEM_USE_MAGIC_NUM = 1.1f;
const double PKEY_2_DOCID_MAP_MAGIC_NUM = 2.5f;

enum class SegmentType { kUnknown = 0, kRaw, kIndex, kMultiIndex };
enum class DistanceType { kUnknown, kL2, kInnerProduct };

const std::string MIPS_ENABLE = "mips.enable";
const std::string MIPS_PARAM_MVAL = "mips.param.mval";
const std::string MIPS_PARAM_UVAL = "mips.param.uval";
const std::string MIPS_PARAM_NORM = "mips.param.norm";

const std::string PARALLEL_SEARCH_QUEUE_SIZE = "parallel_search_queue_size";
const std::string PARALLEL_SEARCH_THREAD_NUM = "parallel_search_thread_num";
const std::string ENABLE_PARALLEL_SEARCH = "enable_parallel_search";

const std::string ENABLE_REPORT_METRIC = "enable_report_metric";
const std::string INDEX_NAME = "index_name";

struct MipsParams : public autil::legacy::Jsonizable {
    MipsParams() : enable(false), mval(0U), uval(0.0), norm(0.0) {}
    MipsParams(bool e, uint32_t m, float u, float n) : enable(e), mval(m), uval(u), norm(n) {}
    void Jsonize(JsonWrapper &json) override;
    bool enable;
    uint32_t mval;
    float uval;
    float norm;
};

struct CommonParam {
    CommonParam(const util::KeyValueMap &parameters);
    CommonParam(const CommonParam &) = default;
    CommonParam();

    std::string mIndexType;
    int32_t mDim;
    // when doc size is smaller than this, use linear method instead
    size_t mKnnLrThold;
    std::string mTopKFlag;
    std::string mScoreFilterFlag;
    char mCategoryEmbeddingSeparator;
    char mCategorySeparator;
    char mEmbeddingSeparator;
    char mExtendSeparator;
    char mQuerySeparator;
    bool mEnableReportMetric;

 private:
    IE_LOG_DECLARE();
};

struct EmbeddingInfo {
    EmbeddingInfo(docid_t docid, const EmbeddingPtr &embedding) : mDocId(docid), mEmbedding(embedding) {}
    EmbeddingInfo() : mDocId(INVALID_DOCID), mEmbedding(nullptr) {}
    docid_t mDocId;
    EmbeddingPtr mEmbedding;
};

struct IndexInfo {
    IndexInfo(int64_t catId, size_t docSize) : mCatId(catId), mDocNum(docSize) {}
    IndexInfo() : mCatId(-1LL), mDocNum(0LL) {}
    int64_t mCatId;
    size_t mDocNum;
};

struct JsonizableIndexInfo : public autil::legacy::Jsonizable {
    JsonizableIndexInfo(int64_t c, size_t d, const MipsParams &m) : mCatId(c), mDocNum(d), mMipsParams(m) {}
    JsonizableIndexInfo() : mCatId(-1LL), mDocNum(0LL) {}
    void Jsonize(JsonWrapper &json) override;
    int64_t mCatId;
    size_t mDocNum;
    MipsParams mMipsParams;
};

struct QueryInfo {
    QueryInfo(uint64_t id, EmbeddingPtr &embedding, int dim) : mCatId(id), mEmbedding(embedding), mDim(dim) {}
    uint64_t mCatId;
    EmbeddingPtr mEmbedding;
    int mDim;
    std::string toString() const {
        std::stringstream ss;
        ss << "category id: [" << mCatId << "], embedding : [";
        for (int i = 0; i < mDim; ++i) {
            ss << mEmbedding.get()[i];
            if (i + 1 == mDim) {
                ss << "]";
            } else {
                ss << ",";
            }
        }
        return ss.str();
    }
};

#define SHARED_PTR_MALLOC(ptr, n, T)                                                                \
    do {                                                                                            \
        assert(n > 0);                                                                              \
        ptr.reset(new (std::nothrow) T[n], [](T *p) { delete[] p; });                               \
        if (nullptr == ptr) {                                                                       \
            IE_LOG(ERROR, "failed to allocate size[%lu] in type[%s]", (size_t)n, typeid(T).name()); \
        }                                                                                           \
    } while (0)

const std::string EMPTY_STRING = "";
inline const std::string &GetValueFromKeyValueMap(const std::map<std::string, std::string> &keyValueMap,
                                                  const std::string &key,
                                                  const std::string &defaultValue = EMPTY_STRING) {
    const auto iter = keyValueMap.find(key);
    if (iter != keyValueMap.end()) {
        return iter->second;
    }
    return defaultValue;
}

IE_NAMESPACE_END(aitheta_plugin);

#endif  //__INDEXLIB_PLUGIN_COMMON_DEFINE_H
