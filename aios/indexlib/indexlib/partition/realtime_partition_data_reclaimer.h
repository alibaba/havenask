#ifndef __INDEXLIB_REALTIME_PARTITION_DATA_RECLAIMER_H
#define __INDEXLIB_REALTIME_PARTITION_DATA_RECLAIMER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index, DeletionMapReader);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

IE_NAMESPACE_BEGIN(partition);

class RealtimePartitionDataReclaimer
{
private:
    struct SimpleSegmentMeta
    {
        segmentid_t segmentId;
        uint32_t docCount;
        int64_t timestamp;
        bool hasOperation;
    };

    typedef std::vector<SimpleSegmentMeta> SimpleSegmentMetas;

public:
    RealtimePartitionDataReclaimer(const config::IndexPartitionSchemaPtr& schema,
                                   const config::IndexPartitionOptions& options);
    ~RealtimePartitionDataReclaimer();

public:
    void Reclaim(int64_t timestamp, const index_base::PartitionDataPtr& buildingPartData);

    static void ExtractSegmentsToReclaimFromPartitionData(
        const index_base::PartitionDataPtr& partData, int64_t reclaimTimestamp,
            std::vector<segmentid_t>& outputSegIds);

    static void TrimObsoleteAndEmptyRtSegments(int64_t reclaimTimestamp, 
                                               const index_base::PartitionDataPtr& partitionData);

    static void TrimBuildingSegment(int64_t reclaimTimestamp,
                                    const index_base::PartitionDataPtr& partData);

private:
    void RemoveObsoleteRtDocs(int64_t reclaimTimestamp,
                              const index_base::PartitionDataPtr& partitionData);

    static void ExtractRtSegMetasFromPartitionData(
        const index_base::PartitionDataPtr& partData,
            SimpleSegmentMetas& segMetas);

    static void ExtractSegmentsToReclaim(
            const SimpleSegmentMetas& segMetas,
            int64_t reclaimTimestamp,
            const index::DeletionMapReaderPtr& deletionMap, 
            std::vector<segmentid_t>& outputSegIds);

    static bool NeedReclaimSegment(
            const SimpleSegmentMeta& segMeta, int64_t timestamp, 
            const index::DeletionMapReaderPtr& deletionMap);

    static void RemoveSegmentIds(const index_base::PartitionDataPtr& partData,
                                 const std::vector<segmentid_t>& segIdsToRemove);

private:
    friend class RealtimePartitionDataReclaimerTest;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RealtimePartitionDataReclaimer);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_REALTIME_PARTITION_DATA_RECLAIMER_H
