#ifndef __INDEXLIB_SEGMENT_DATA_CREATOR_H
#define __INDEXLIB_SEGMENT_DATA_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/segment/building_segment_data.h"

DECLARE_REFERENCE_CLASS(index_base, SegmentDirectory);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(config, BuildConfig);

IE_NAMESPACE_BEGIN(partition);

class SegmentDataCreator
{
public:
    SegmentDataCreator();
    ~SegmentDataCreator();
    
public:
    static index_base::BuildingSegmentData CreateNewSegmentData(
        const index_base::SegmentDirectoryPtr& segDir,
        const index_base::InMemorySegmentPtr& lastDumpingSegment,
        const config::BuildConfig& buildConfig);
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentDataCreator);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_SEGMENT_DATA_CREATOR_H
