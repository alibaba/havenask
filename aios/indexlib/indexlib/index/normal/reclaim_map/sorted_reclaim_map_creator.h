#ifndef __INDEXLIB_SORTED_RECLAIM_MAP_CREATOR_H
#define __INDEXLIB_SORTED_RECLAIM_MAP_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/sort_description.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map_creator.h"
#include "indexlib/index/segment_directory_base.h"

DECLARE_REFERENCE_CLASS(index, OfflineAttributeSegmentReaderContainer);
DECLARE_REFERENCE_CLASS(index, ReclaimMap);

IE_NAMESPACE_BEGIN(index);

class SortedReclaimMapCreator : public ReclaimMapCreator
{
public:
    SortedReclaimMapCreator(const config::MergeConfig& mergeConfig,
        const OfflineAttributeSegmentReaderContainerPtr& attrReaderContainer,
        const config::IndexPartitionSchemaPtr& schema, const common::SortDescriptions& sortDescs,
        const index::SegmentDirectoryBasePtr& segmentDirectory);
    ~SortedReclaimMapCreator();

public:
    ReclaimMapPtr Create(const DeletionMapReaderPtr& delMapReader,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const std::function<segmentindex_t(segmentid_t, docid_t)>& segmentSplitHandler) override;

private:
    const config::IndexPartitionSchemaPtr mSchema;
    const common::SortDescriptions mSortDescs;
    const index::SegmentDirectoryBasePtr mSegmentDirectory;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SortedReclaimMapCreator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SORTED_RECLAIM_MAP_CREATOR_H
