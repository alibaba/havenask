#ifndef __INDEXLIB_AITHETA_PARALLEL_SEGMENT_MERGER_H
#define __INDEXLIB_AITHETA_PARALLEL_SEGMENT_MERGER_H

#include <unordered_map>

#include <indexlib/common_define.h>
#include <indexlib/misc/log.h>
#include <indexlib/file_system/directory.h>
#include <indexlib/indexlib.h>

#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/raw_segment.h"
#include "aitheta_indexer/plugins/aitheta/index_segment.h"
#include "aitheta_indexer/plugins/aitheta/parallel_segment.h"
#include "aitheta_indexer/plugins/aitheta/util/flexible_float_index_holder.h"
#include "aitheta_indexer/plugins/aitheta/util/knn_config.h"
#include "aitheta_indexer/plugins/aitheta/util/reduce_task/reduce_task_creater.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class ParallelSegmentMerger {
 public:
    ParallelSegmentMerger(const ParallelSegmentPtr &parallelSegment, const std::vector<RawSegmentPtr> &rawSegmentVec,
                          const KnnConfig &knnConfig)
        : mParallelSegment(parallelSegment), mRawSegVector(rawSegmentVec), mKnnConfig(knnConfig) {}
    ~ParallelSegmentMerger() {}

 public:
    bool Merge(const IE_NAMESPACE(file_system)::DirectoryPtr &directory, const CustomReduceTaskPtr &task,
               docid_t baseDocId = 0);

 private:
    ParallelSegmentPtr mParallelSegment;
    std::vector<RawSegmentPtr> mRawSegVector;
    KnnConfig mKnnConfig;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ParallelSegmentMerger);

IE_NAMESPACE_END(aitheta_plugin);

#endif  //__INDEXLIB_AITHETA_PARALLEL_SEGMENT_MERGER_H
