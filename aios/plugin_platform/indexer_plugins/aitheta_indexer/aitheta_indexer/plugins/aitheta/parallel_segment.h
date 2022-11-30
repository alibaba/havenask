#ifndef __INDEXLIB_AITHETA_PARALLEL_SEGMENT_H
#define __INDEXLIB_AITHETA_PARALLEL_SEGMENT_H

#include <indexlib/common_define.h>
#include <indexlib/misc/log.h>
#include <indexlib/file_system/directory.h>
#include <indexlib/indexlib.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_reducer.h>

#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/segment.h"
#include "aitheta_indexer/plugins/aitheta/raw_segment.h"
#include "aitheta_indexer/plugins/aitheta/index_segment.h"
#include "aitheta_indexer/plugins/aitheta/util/flexible_float_index_holder.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class ParallelSegment : public Segment {
 public:
    ParallelSegment(const SegmentMeta &meta = SegmentMeta(SegmentType::kMultiIndex, 0U, 0U))
        : Segment(meta), mDumpSegId(std::numeric_limits<uint32_t>::max()) {}
    ~ParallelSegment() {}

 public:
    bool PrepareReduceTaskInput(const IE_NAMESPACE(file_system)::DirectoryPtr &dir, ReduceTaskInput &input) override;
    bool Load(const IE_NAMESPACE(file_system)::DirectoryPtr &dir) override;
    bool UpdateDocId(const DocIdMap &docIdMap) override;
    bool Dump(const IE_NAMESPACE(file_system)::DirectoryPtr &directory) override;
    bool EstimateLoad4ReduceMemoryUse(const IE_NAMESPACE(file_system)::DirectoryPtr &dir, size_t &size) const override;
    bool EstimateLoad4RetrieveMemoryUse(const IE_NAMESPACE(file_system)::DirectoryPtr &dir,
                                        size_t &size) const override;

 public:
    void SetDumpSegId(uint32_t id) { mDumpSegId = id; }
    bool Merge(const std::vector<RawSegmentPtr> &segVector, const std::vector<docid_t> &segBaseDocIds = {},
               const docid_t baseDocId = 0);

    IndexSegmentPtr GetSegment(const uint32_t taskId);
    const std::vector<IndexSegmentPtr> GetSegmentVector() const { return mSubSegVector; }

    static bool GetSubDir(const IE_NAMESPACE(file_system)::DirectoryPtr &dir, uint32_t taskId,
                          IE_NAMESPACE(index)::ParallelReduceMeta &parallelMeta,
                          IE_NAMESPACE(file_system)::DirectoryPtr &subDir);

    static bool GetSubDir(const IE_NAMESPACE(file_system)::DirectoryPtr &dir, uint32_t taskId,
                          IE_NAMESPACE(file_system)::DirectoryPtr &subDir);

    static bool MergeSegMeta(const IE_NAMESPACE(file_system)::DirectoryPtr &dir,
                          IE_NAMESPACE(index)::ParallelReduceMeta &parallelMeta, SegmentMeta &mergedSegMeta);

    static bool MergeDocIdRemap(const IE_NAMESPACE(file_system)::DirectoryPtr &dir,
                                IE_NAMESPACE(index)::ParallelReduceMeta &parallelMeta, size_t &docIdRemapFileSize);

 private:
    std::vector<IndexSegmentPtr> mSubSegVector;
    IE_NAMESPACE(file_system)::DirectoryPtr mDirectory;
    IE_NAMESPACE(index)::ParallelReduceMeta mReduceMeta;
    uint32_t mDumpSegId;

    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ParallelSegment);

IE_NAMESPACE_END(aitheta_plugin);
#endif  //__INDEXLIB_AITHETA_PARALLEL_SEGMENT_H
