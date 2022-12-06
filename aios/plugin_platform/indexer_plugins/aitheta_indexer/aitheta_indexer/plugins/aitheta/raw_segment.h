#ifndef __INDEXLIB_AITHETA_RAW_SEGMENT_H
#define __INDEXLIB_AITHETA_RAW_SEGMENT_H

#include <tuple>
#include <unordered_map>

#include <indexlib/common_define.h>
#include <indexlib/misc/log.h>
#include <indexlib/file_system/directory.h>
#include <indexlib/indexlib.h>

#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/segment.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

typedef std::tuple<std::vector<int64_t>, std::vector<docid_t>, std::vector<EmbeddingPtr>> CategoryRecords;

class RawSegment : public Segment {
 public:
    RawSegment(const SegmentMeta &meta = SegmentMeta(SegmentType::kRaw, 0u, 0u))
        : Segment(meta), mDimension(0), mBuildMemoryUse(0u) {}
    ~RawSegment() {}

 public:
    bool PrepareReduceTaskInput(const IE_NAMESPACE(file_system)::DirectoryPtr &dir, ReduceTaskInput &input);
    bool Load(const IE_NAMESPACE(file_system)::DirectoryPtr &dir) override;
    bool UpdateDocId(const DocIdMap &docIdMap) override;
    bool EstimateLoad4ReduceMemoryUse(const IE_NAMESPACE(file_system)::DirectoryPtr &dir, size_t &size) const override;
    bool EstimateLoad4RetrieveMemoryUse(const IE_NAMESPACE(file_system)::DirectoryPtr &dir,
                                        size_t &size) const override;
    bool Dump(const IE_NAMESPACE(file_system)::DirectoryPtr &dir) override;

 public:
    size_t GetMetaSize() const;
    IE_NAMESPACE(file_system)::FileReaderPtr GetEmbeddingReader() { return mEmbeddingReader; }
    bool HasEmbedding() { return mEmbeddingReader && mEmbeddingReader->GetLength() > 0u; }
    void SetDimension(int32_t d) { mDimension = d; }
    int32_t GetDimension() const { return mDimension; }
    std::map<int64_t, CategoryRecords> &GetCatId2Records() { return mCatId2Records; }
    bool Add(int64_t catId, int64_t pkey, docid_t docId, const EmbeddingPtr &embedding);
    size_t EstimateBuildMemoryUse() const { return mBuildMemoryUse; }

 private:
    int32_t mDimension;
    std::map<int64_t, CategoryRecords> mCatId2Records;
    IE_NAMESPACE(file_system)::FileReaderPtr mEmbeddingReader;
    size_t mBuildMemoryUse;

 private:
    IE_LOG_DECLARE();
    friend class AithetaIndexerTest;
};

DEFINE_SHARED_PTR(RawSegment);

IE_NAMESPACE_END(aitheta_plugin);

#endif  //__INDEXLIB_AITHETA_RAW_SEGMENT_H
