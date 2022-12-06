#ifndef __INDEXLIB_AITHETA_INDEX_SEGMENT_MERGER_H
#define __INDEXLIB_AITHETA_INDEX_SEGMENT_MERGER_H

#include <unordered_map>

#include <indexlib/common_define.h>
#include <indexlib/misc/log.h>
#include <indexlib/file_system/directory.h>
#include <indexlib/indexlib.h>

#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/raw_segment.h"
#include "aitheta_indexer/plugins/aitheta/index_segment.h"
#include "aitheta_indexer/plugins/aitheta/util/flexible_float_index_holder.h"
#include "aitheta_indexer/plugins/aitheta/util/knn_config.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class IndexSegmentMerger {
 public:
    IndexSegmentMerger(const IndexSegmentPtr &indexSegment, const std::vector<RawSegmentPtr> &rawSegmentVec,
                       const KnnConfig &knnConfig)
        : mIndexSegment(indexSegment), mRawSegmentVec(rawSegmentVec), mKnnConfig(knnConfig) {}
    ~IndexSegmentMerger() {}

 public:
    bool Merge(const IE_NAMESPACE(file_system)::DirectoryPtr &directory, docid_t baseDocId = 0);

 private:
    IndexSegmentPtr mIndexSegment;
    std::vector<RawSegmentPtr> mRawSegmentVec;
    KnnConfig mKnnConfig;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexSegmentMerger);

IE_NAMESPACE_END(aitheta_plugin);

#endif  //__INDEXLIB_AITHETA_INDEX_SEGMENT_MERGER_H
