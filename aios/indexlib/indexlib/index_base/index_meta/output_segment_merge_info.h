#ifndef __INDEXLIB_OUTPUT_SEGMENT_MERGE_INFO_H
#define __INDEXLIB_OUTPUT_SEGMENT_MERGE_INFO_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(index_base);

struct OutputSegmentMergeInfo
{
public:
    OutputSegmentMergeInfo() = default;
    ~OutputSegmentMergeInfo() {}

    segmentid_t targetSegmentId = INVALID_SEGMENTID;
    segmentindex_t targetSegmentIndex = 0;
    std::string path;
    file_system::DirectoryPtr directory;
    size_t docCount = 0;
};

DEFINE_SHARED_PTR(OutputSegmentMergeInfo);

typedef std::vector<OutputSegmentMergeInfo> OutputSegmentMergeInfos;

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_OUTPUT_SEGMENT_MERGE_INFO_H
