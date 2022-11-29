#ifndef __INDEXLIB_AITHETA_INDEXER_H
#define __INDEXLIB_AITHETA_INDEXER_H

#include <map>
#include <memory>
#include <unordered_set>
#include <vector>

#include <aitheta/index_params.h>
#include <indexlib/common_define.h>
#include <indexlib/misc/log.h>
#include <indexlib/index/normal/inverted_index/customized_index/indexer.h>
#include <indexlib/indexlib.h>

#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/raw_segment_builder.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class AithetaIndexer : public IE_NAMESPACE(index)::Indexer {
 public:
    AithetaIndexer(const util::KeyValueMap &parameters)
        : Indexer(parameters),
          mComParam(parameters),
          mBuilder(new RawSegmentBuilder(mComParam)),
          mCheckPkeyOnly(false),
          mCastEmbeddingFailureNum(0u) {}
    ~AithetaIndexer() {}

 public:
    bool Build(const std::vector<const document::Field *> &fieldVec, docid_t docId) override;
    bool Dump(const IE_NAMESPACE(file_system)::DirectoryPtr &indexDir) override;
    int64_t EstimateTempMemoryUseInDump() const override { return mBuilder->EstimateBuildMemoryUse(); }
    int64_t EstimateDumpFileSize() const override { return mBuilder->EstimateBuildMemoryUse(); }
    int64_t EstimateExpandMemoryUseInDump() const override { return mBuilder->EstimateBuildMemoryUse(); }
    uint32_t GetDistinctTermCount() const override { return 0U; }
    size_t EstimateInitMemoryUse(size_t lastSegmentDistinctTermCount) const override { return 65536U; }
    int64_t EstimateCurrentMemoryUse() const override { return mBuilder->EstimateBuildMemoryUse(); }
    bool DoInit(const IndexerResourcePtr &resource) override;

 private:
    bool CastCategoryFieldToVector(const std::string &catIdsStr, std::vector<int64_t> &categoryIdVec);
    bool CastEmbeddingFieldToVector(const std::string &embeddingStr, EmbeddingPtr &embedding);

 private:
    CommonParam mComParam;
    RawSegmentBuilderPtr mBuilder;
    bool mCheckPkeyOnly;
    size_t mCastEmbeddingFailureNum;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AithetaIndexer);

IE_NAMESPACE_END(aitheta_plugin);

#endif  //__INDEXLIB_AITHETA_INDEXER_H
