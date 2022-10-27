#include "indexlib/index/normal/reclaim_map/reclaim_map_creator.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"
#include "indexlib/index/normal/attribute/accessor/offline_attribute_segment_reader_container.h"
#include "indexlib/config/merge_config.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, ReclaimMapCreator);

ReclaimMapCreator::ReclaimMapCreator(const config::MergeConfig& mergeConfig,
        const OfflineAttributeSegmentReaderContainerPtr& attrReaderContainer)
    : mMergeConfig(mergeConfig)
    , mAttrReaderContainer(attrReaderContainer)
{
}

ReclaimMapCreator::~ReclaimMapCreator()
{
}

ReclaimMapPtr ReclaimMapCreator::Create(const DeletionMapReaderPtr& delMapReader,
    const index_base::SegmentMergeInfos& segMergeInfos,
    const std::function<segmentindex_t(segmentid_t, docid_t)>& segmentSplitHandler)
{
    ReclaimMapPtr reclaimMap(new ReclaimMap);
    if (!reclaimMap)
    {
        INDEXLIB_FATAL_ERROR(OutOfMemory, "Allocate memory FAILED.");
    }
    if (segmentSplitHandler)
    {
        reclaimMap->SetSegmentSplitHandler(segmentSplitHandler);
    }
    reclaimMap->Init(segMergeInfos, delMapReader, mAttrReaderContainer,
        mMergeConfig.truncateOptionConfig != NULL);
    return reclaimMap;
}

IE_NAMESPACE_END(index);

