#ifndef __INDEXLIB_MERGER_RESOURCE_H
#define __INDEXLIB_MERGER_RESOURCE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"

IE_NAMESPACE_BEGIN(index);

struct MergerResource
{
    MergerResource()
        : targetSegmentCount(0)
        , isEntireDataSet(false)
    {
    }

    ReclaimMapPtr reclaimMap;
    index_base::Version targetVersion;
    size_t targetSegmentCount;
    bool isEntireDataSet;
    std::vector<docid_t> mainBaseDocIds;

    segmentindex_t GetOutputSegmentIndex(docid_t newDocId) const
    {
        assert(reclaimMap.get());
        assert(newDocId < static_cast<int64_t>(reclaimMap->GetNewDocCount()));
        return reclaimMap->GetTargetSegmentIndex(newDocId);        
    }    
};

DEFINE_SHARED_PTR(MergerResource);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MERGER_RESOURCE_H
