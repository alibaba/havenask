#ifndef __INDEXLIB_MERGE_PLAN_RESOURCE_H
#define __INDEXLIB_MERGE_PLAN_RESOURCE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"
#include "indexlib/index/normal/inverted_index/truncate/bucket_map.h"

IE_NAMESPACE_BEGIN(merger);

struct MergePlanResource
{
    MergePlanResource(const index::ReclaimMapPtr &reclaimMap = index::ReclaimMapPtr(),
                      const index::ReclaimMapPtr &subReclaimMap = index::ReclaimMapPtr(),
                      const index::BucketMaps &bucketMaps = index::BucketMaps())
    {
        this->reclaimMap = reclaimMap;
        this->subReclaimMap = subReclaimMap;
        this->bucketMaps = bucketMaps;
    }
    ~MergePlanResource()
    {}
    index::ReclaimMapPtr reclaimMap;
    index::ReclaimMapPtr subReclaimMap;
    index::BucketMaps bucketMaps;
};

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MERGE_PLAN_RESOURCE_H
