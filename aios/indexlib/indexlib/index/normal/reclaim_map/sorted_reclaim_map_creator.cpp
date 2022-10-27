#include "indexlib/index/normal/reclaim_map/sorted_reclaim_map_creator.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/merger/split_strategy/split_segment_strategy.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SortedReclaimMapCreator);

SortedReclaimMapCreator::SortedReclaimMapCreator(
        const MergeConfig& mergeConfig,
        const OfflineAttributeSegmentReaderContainerPtr& attrReaderContainer,        
        const IndexPartitionSchemaPtr& schema,
        const SortDescriptions& sortDescs,
        const SegmentDirectoryBasePtr& segmentDirectory)
    : ReclaimMapCreator(mergeConfig, attrReaderContainer)
    , mSchema(schema)
    , mSortDescs(sortDescs)
    , mSegmentDirectory(segmentDirectory)
{

}


SortedReclaimMapCreator::~SortedReclaimMapCreator()
{
}

ReclaimMapPtr SortedReclaimMapCreator::Create(const DeletionMapReaderPtr& delMapReader,
    const SegmentMergeInfos& segMergeInfos,
    const std::function<segmentindex_t(segmentid_t, docid_t)>& segmentSplitHandler)
{
    ReclaimMapPtr reclaimMap(new (nothrow) ReclaimMap());
    if (!reclaimMap)
    {
        INDEXLIB_FATAL_ERROR(OutOfMemory, "Allocate memory FAILED.");
    }
    if (segmentSplitHandler)
    {
        reclaimMap->SetSegmentSplitHandler(segmentSplitHandler);
    }

    AttributeSchemaPtr attrSchema = mSchema->GetAttributeSchema();

    ReclaimMap::AttributeConfigVector attrConfigs;
    vector<SortPattern> sortPatterns;
    for(size_t i = 0; i < mSortDescs.size(); ++i)
    {
        const string &fieldName = mSortDescs[i].sortFieldName;
        AttributeConfigPtr attrConfig = attrSchema->GetAttributeConfig(fieldName);
        if (!attrConfig)
        {
            INDEXLIB_FATAL_ERROR(NonExist, "Sort Field [%s] not exist in attribute!",
                    fieldName.c_str());
        }

        if (attrConfig->IsMultiValue())
        {
            INDEXLIB_FATAL_ERROR(UnImplement, "Sort Field [%s] should not be multi value!",
                    fieldName.c_str());
        }
        attrConfigs.push_back(attrConfig);
        sortPatterns.push_back(mSortDescs[i].sortPattern);
    }

    reclaimMap->Init(segMergeInfos, delMapReader, mAttrReaderContainer,
                     attrConfigs, sortPatterns, mSegmentDirectory,
                     mMergeConfig.truncateOptionConfig != NULL);
    return reclaimMap;
}

IE_NAMESPACE_END(index);

