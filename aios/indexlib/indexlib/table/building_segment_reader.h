#ifndef __INDEXLIB_BUILDING_SEGMENT_READER_H
#define __INDEXLIB_BUILDING_SEGMENT_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(table);

class BuildingSegmentReader
{
public:
    BuildingSegmentReader();
    virtual ~BuildingSegmentReader();

public:
    const std::set<segmentid_t>& GetDependentSegmentIds() const { return mDependentSegIds; }
    void AddDependentSegmentId(segmentid_t segId)
    {
        mDependentSegIds.insert(segId);
    } 

protected:
    std::set<segmentid_t> mDependentSegIds;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuildingSegmentReader);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_BUILDING_SEGMENT_READER_H
