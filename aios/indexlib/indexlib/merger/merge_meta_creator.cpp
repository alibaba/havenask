#include "indexlib/merger/merge_meta_creator.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/index/calculator/on_disk_segment_size_calculator.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/merge_config.h"

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, MergeMetaCreator);

MergeMetaCreator::MergeMetaCreator() 
{
}

MergeMetaCreator::~MergeMetaCreator() 
{
}

SegmentMergeInfos MergeMetaCreator::CreateSegmentMergeInfos(
        const IndexPartitionSchemaPtr& schema,
        const MergeConfig& mergeConfig, const merger::SegmentDirectoryPtr& segDir)
{
    IE_LOG(INFO, "create segmentMergeInfos begin");
    SegmentMergeInfos segMergeInfos;
    bool needSegmentSize = false;
    if (mergeConfig.mergeStrategyStr == REALTIME_MERGE_STRATEGY_STR ||
        mergeConfig.mergeStrategyStr == SHARD_BASED_MERGE_STRATEGY_STR ||
        mergeConfig.mergeStrategyStr == PRIORITY_QUEUE_MERGE_STRATEGY_STR || 
        mergeConfig.mergeStrategyStr == TIMESERIES_MERGE_STRATEGY_STR)
    {
        needSegmentSize = true;
    }
    Version version = segDir->GetVersion();
    const LevelInfo& levelInfo = version.GetLevelInfo();
    Version::Iterator vIt = version.CreateIterator();
    merger::SegmentDirectory::Iterator it = segDir->CreateIterator();
    exdocid_t baseDocId = 0;
    OnDiskSegmentSizeCalculator segCalculator;

    DeletionMapReaderPtr deletionReader = segDir->GetDeletionMapReader();
    index_base::PartitionDataPtr partData = segDir->GetPartitionData();
    assert(partData);
    while (it.HasNext())
    {
        it.Next();
        assert(vIt.HasNext());
        segmentid_t segId = vIt.Next();
        SegmentInfo segInfo = partData->GetSegmentData(segId).GetSegmentInfo();

        int64_t segSize = 0;
        if (needSegmentSize)
        {
            segSize = segCalculator.GetSegmentSize(partData->GetSegmentData(segId), schema);
        }

        uint32_t deleteDocCount = deletionReader ? deletionReader->GetDeletedDocCount(segId) : 0;
        uint32_t levelIdx = 0;
        uint32_t inLevelIdx = 0;
        bool ret = levelInfo.FindPosition(segId, levelIdx, inLevelIdx);
        assert(ret); (void)ret;

        if (segInfo.shardingColumnId == SegmentInfo::INVALID_SHARDING_ID)
        {
            segInfo.docCount = segInfo.docCount / segInfo.shardingColumnCount;
        }

        SegmentMergeInfo segMergeInfo(
            segId, segInfo, deleteDocCount, baseDocId, segSize, levelIdx, inLevelIdx);
        segMergeInfo.segmentMetrics = partData->GetSegmentData(segId).GetSegmentMetrics();
        segMergeInfos.push_back(segMergeInfo);

        baseDocId += segInfo.docCount;
    }
    if (schema->GetTableType() == tt_index && baseDocId >= MAX_DOCID)
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "partition doc count [%ld] exceed limit [%d]",
                             baseDocId, MAX_DOCID);
    }
    IE_LOG(INFO, "create segmentMergeInfos done");
    return segMergeInfos;
}

IE_NAMESPACE_END(merger);

