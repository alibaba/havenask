#ifndef __INDEXLIB_AITHETA_RAW_SEGMENT_MERGER_H
#define __INDEXLIB_AITHETA_RAW_SEGMENT_MERGER_H

#include <tuple>
#include <unordered_map>

#include <indexlib/common_define.h>
#include <indexlib/misc/log.h>
#include <indexlib/file_system/directory.h>
#include <indexlib/indexlib.h>

#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/raw_segment.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class RawSegmentMerger {
 public:
    RawSegmentMerger(const std::vector<RawSegmentPtr> &segmentVec) : mSegmentVec(segmentVec) {}
    ~RawSegmentMerger() {}

 public:
    bool Merge(const IE_NAMESPACE(file_system)::DirectoryPtr &directory);

 private:
    IE_LOG_DECLARE();
    std::vector<RawSegmentPtr> mSegmentVec;
};

DEFINE_SHARED_PTR(RawSegmentMerger);

IE_NAMESPACE_END(aitheta_plugin);

#endif  //__INDEXLIB_AITHETA_RAW_SEGMENT_MERGER_H
