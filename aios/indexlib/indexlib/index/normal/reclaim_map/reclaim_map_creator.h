#ifndef __INDEXLIB_RECLAIM_MAP_CREATOR_H
#define __INDEXLIB_RECLAIM_MAP_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"

DECLARE_REFERENCE_CLASS(index, DeletionMapReader);
DECLARE_REFERENCE_CLASS(index, ReclaimMap);
DECLARE_REFERENCE_CLASS(index, OfflineAttributeSegmentReaderContainer);

IE_NAMESPACE_BEGIN(index);

class ReclaimMapCreator
{
public:
    ReclaimMapCreator(const config::MergeConfig& mergeConfig,
        const OfflineAttributeSegmentReaderContainerPtr& attrReaderContainer);
    virtual ~ReclaimMapCreator();
public:
    virtual ReclaimMapPtr Create(const DeletionMapReaderPtr& delMapReader,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const std::function<segmentindex_t(segmentid_t, docid_t)>& segmentSplitHandler);

protected:
    const config::MergeConfig mMergeConfig;
    OfflineAttributeSegmentReaderContainerPtr mAttrReaderContainer;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ReclaimMapCreator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_RECLAIM_MAP_CREATOR_H
