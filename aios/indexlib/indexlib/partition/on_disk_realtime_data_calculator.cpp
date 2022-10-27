#include "indexlib/partition/on_disk_realtime_data_calculator.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/file_system/link_directory.h"
#include "indexlib/index/calculator/segment_lock_size_calculator.h"
#include "indexlib/index_base/index_meta/version.h"

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OnDiskRealtimeDataCalculator);

OnDiskRealtimeDataCalculator::OnDiskRealtimeDataCalculator()
{
}

OnDiskRealtimeDataCalculator::~OnDiskRealtimeDataCalculator() 
{
}

void OnDiskRealtimeDataCalculator::Init(const IndexPartitionSchemaPtr& schema,
                                        const PartitionDataPtr& partData,
                                        const plugin::PluginManagerPtr& pluginManager)
{
    mPartitionData = partData;
    mCalculator.reset(new SegmentLockSizeCalculator(schema, pluginManager));
}

size_t OnDiskRealtimeDataCalculator::CalculateExpandSize(
        segmentid_t lastRtSegmentId, segmentid_t currentRtSegId)
{
    size_t ret = 0;
    Version version = mPartitionData->GetVersion();
    segmentid_t firstId = INVALID_SEGMENTID;
    segmentid_t lastId = INVALID_SEGMENTID;
    for (size_t i = 0; i < version.GetSegmentCount(); i++)
    {
        if (!RealtimeSegmentDirectory::IsRtSegmentId(version[i]))
        {
            continue;
        }

        if (version[i] > lastRtSegmentId && version[i] <= currentRtSegId)
        {
            if (firstId == INVALID_SEGMENTID)
            {
                firstId = version[i];
            }
            lastId = version[i];
            ret += CalculateLockSize(version[i]);
        }
    }

    if (currentRtSegId > lastRtSegmentId)
    {
        IE_LOG(INFO, "calculate expand size [%lu] for rt segments [%d,%d]",
               ret, firstId, lastId);
    }
    return ret;
}

size_t OnDiskRealtimeDataCalculator::CalculateLockSize(segmentid_t rtSegId)
{
    Version version = mPartitionData->GetVersion();
    if (!RealtimeSegmentDirectory::IsRtSegmentId(rtSegId))
    {
        IE_LOG(WARN, "segment [%d] not is not rt segment", rtSegId);
        return 0;
    }
    
    if (!version.HasSegment(rtSegId))
    {
        IE_LOG(WARN, "segment [%d] not is version [%s]",
               rtSegId, version.ToString().c_str());
        return 0;
    }

    SegmentData segData = mPartitionData->GetSegmentData(rtSegId);
    DirectoryPtr rtDir = segData.GetDirectory();
    if (!rtDir)
    {
        IE_LOG(ERROR, "GetDirectory() for segment [%d] failed!", rtSegId);
        return 0;
    }

    LinkDirectoryPtr rtLinkDir = rtDir->CreateLinkDirectory();
    assert(rtLinkDir);
    return mCalculator->CalculateSize(rtLinkDir);
}

IE_NAMESPACE_END(partition);

