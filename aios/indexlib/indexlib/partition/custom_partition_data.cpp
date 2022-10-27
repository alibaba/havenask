#include "indexlib/partition/custom_partition_data.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/util/memory_control/partition_memory_quota_controller.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/index_base/segment/segment_data_base.h"
#include "indexlib/partition/segment/dump_segment_queue.h"
#include "indexlib/partition/segment/custom_segment_dump_item.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, CustomPartitionData);

CustomPartitionData::CustomPartitionData(const config::IndexPartitionOptions& options,
                                         const config::IndexPartitionSchemaPtr& schema,
                                         const util::PartitionMemoryQuotaControllerPtr& memController,
                                         const util::BlockMemoryQuotaControllerPtr& tableWriterMemController,
                                         misc::MetricProviderPtr metricProvider,
                                         const util::CounterMapPtr& counterMap,
                                         const plugin::PluginManagerPtr& pluginManager,
                                         const DumpSegmentQueuePtr& dumpSegmentQueue,
                                         int64_t reclaimTimestamp)
    : mOptions(options)
    , mSchema(schema)
    , mMemController(memController)
    , mTableWriterMemController(tableWriterMemController)
    , mMetricProvider(metricProvider)
    , mCounterMap(counterMap)
    , mPluginManager(pluginManager)
    , mDumpSegmentQueue(dumpSegmentQueue)
    , mReclaimTimestamp(reclaimTimestamp)
{
    
}

CustomPartitionData::CustomPartitionData(const CustomPartitionData& other)
    : mOptions(other.mOptions)
    , mSchema(other.mSchema)
    , mMemController(other.mMemController)
    , mTableWriterMemController(other.mTableWriterMemController)
    , mMetricProvider(other.mMetricProvider)
    , mCounterMap(other.mCounterMap)
    , mPluginManager(other.mPluginManager)
    , mReclaimTimestamp(other.mReclaimTimestamp)
    , mPartitionMeta(other.mPartitionMeta)
    , mSegmentDirectory(other.mSegmentDirectory->Clone())
{
    if (other.mDumpSegmentQueue)
    {
        mDumpSegmentQueue.reset(other.mDumpSegmentQueue->Clone());
    }
}

CustomPartitionData::~CustomPartitionData() 
{
}

bool CustomPartitionData::Open(const SegmentDirectoryPtr& segDir)
{
    mSegmentDirectory = segDir;
    // init parttion meta

    if (mReclaimTimestamp >= 0)
    {
        for (auto it = Begin(); it != End(); ++it)
        {
            const SegmentData& segData = *it;
            segmentid_t segId = segData.GetSegmentId();
            if (!RealtimeSegmentDirectory::IsRtSegmentId(segId))
            {
                continue;
            }
            if (segData.GetSegmentInfo().timestamp < mReclaimTimestamp)
            {
                mToReclaimRtSegIds.emplace_back(segId);
            }
        }
    }
    return true;
}

void CustomPartitionData::RemoveObsoleteSegments()
{
    autil::ScopedLock lock(mLock);
    if (mToReclaimRtSegIds.size() >= 0)
    {
        string segListStr = StringUtil::toString(mToReclaimRtSegIds, ",");
        IE_LOG(INFO, "rtSegments: %s will be reclaimed with relcaimTs[%ld]",
               segListStr.c_str(), mReclaimTimestamp);
        RemoveSegments(mToReclaimRtSegIds);
        if (mDumpSegmentQueue)
        {
            mDumpSegmentQueue->ReclaimSegments(mReclaimTimestamp);
        }
    }
    CommitVersion();
    mToReclaimRtSegIds.clear();
}

index_base::Version CustomPartitionData::GetVersion() const
{
    auto baseVersion = mSegmentDirectory->GetVersion(); 
    if (mToReclaimRtSegIds.empty())
    {
        return baseVersion;
    }
    for (const auto& segId : mToReclaimRtSegIds)
    {
        baseVersion.RemoveSegment(segId);
    }
    return baseVersion;
}

index_base::Version CustomPartitionData::GetOnDiskVersion() const
{
    return mSegmentDirectory->GetOnDiskVersion();
}

index_base::PartitionMeta CustomPartitionData::GetPartitionMeta() const
{
    return mPartitionMeta;
}

const file_system::DirectoryPtr& CustomPartitionData::GetRootDirectory() const
{
    return mSegmentDirectory->GetRootDirectory();
}

const IndexFormatVersion& CustomPartitionData::GetIndexFormatVersion() const
{
    return mSegmentDirectory->GetIndexFormatVersion();
}

PartitionData::Iterator CustomPartitionData::Begin() const
{
    return mSegmentDirectory->GetSegmentDatas().begin();    
}

PartitionData::Iterator CustomPartitionData::End() const
{ 
    return mSegmentDirectory->GetSegmentDatas().end(); 
}

SegmentData CustomPartitionData::GetSegmentData(segmentid_t segId) const
{
    return mSegmentDirectory->GetSegmentData(segId);
}

SegmentDataVector CustomPartitionData::GetSegmentDatas() const
{
    SegmentDataVector segDataVec = mSegmentDirectory->GetSegmentDatas();
    if (mReclaimTimestamp >= 0)
    {
        vector<segmentid_t> toReclaimRtSegIds;
        for (auto it = segDataVec.begin(); it != segDataVec.end(); )
        {
            const SegmentData& segData = *it;
            segmentid_t segId = segData.GetSegmentId();
            if (!RealtimeSegmentDirectory::IsRtSegmentId(segId))
            {
                ++it;
                continue;
            }
            if (segData.GetSegmentInfo().timestamp < mReclaimTimestamp)
            {
                toReclaimRtSegIds.push_back(segId);
                it = segDataVec.erase(it);
                continue;
            }
            ++it;
        }
        if (!toReclaimRtSegIds.empty())
        {
            string segListStr = StringUtil::toString(toReclaimRtSegIds, ",");
            IE_LOG(INFO, "rtSegments: %s will be skipped with relcaimTs[%ld]",
                   segListStr.c_str(), mReclaimTimestamp);
        }
    }
    return segDataVec;
}

CustomPartitionData* CustomPartitionData::Clone()
{
    autil::ScopedLock lock(mLock);
    return new CustomPartitionData(*this);
}

const SegmentDirectoryPtr& CustomPartitionData::GetSegmentDirectory() const
{
    return mSegmentDirectory;
}

PartitionSegmentIteratorPtr CustomPartitionData::CreateSegmentIterator()
{
    return PartitionSegmentIteratorPtr();
}

NewSegmentMetaPtr CustomPartitionData::CreateNewSegmentData()
{
    autil::ScopedLock lock(mLock);
    SegmentDataBasePtr newSegData(new SegmentDataBase());
    SegmentInfo newSegmentInfo;    

    CustomSegmentDumpItemPtr lastDumpingSeg;
    if (mDumpSegmentQueue)
    {
        lastDumpingSeg = mDumpSegmentQueue->GetLastSegment();
    } 
    if (lastDumpingSeg)
    {
        NewSegmentMetaPtr buildingSegMeta = lastDumpingSeg->GetSegmentMeta();
        segmentid_t newSegId = buildingSegMeta->segmentDataBase->GetSegmentId() + 1;
        SegmentInfo lastSegmentInfo = buildingSegMeta->segmentInfo;
        
        newSegData->SetSegmentId(newSegId);
        const Version& version = mSegmentDirectory->GetVersion();
        newSegData->SetSegmentDirName(
            version.GetNewSegmentDirName(newSegId));
        newSegData->SetBaseDocId(buildingSegMeta->segmentDataBase->GetBaseDocId() + lastSegmentInfo.docCount);
        newSegmentInfo.locator = lastSegmentInfo.locator;
        newSegmentInfo.timestamp = lastSegmentInfo.timestamp;
    }
    else
    {
        newSegData->SetSegmentId(mSegmentDirectory->CreateNewSegmentId());
        const Version& version = mSegmentDirectory->GetVersion();
        newSegData->SetSegmentDirName(
            version.GetNewSegmentDirName(mSegmentDirectory->CreateNewSegmentId()));

        const SegmentDataVector& segmentDatas = mSegmentDirectory->GetSegmentDatas();
        auto it = segmentDatas.rbegin();
        if (it != segmentDatas.rend())
        {
            SegmentInfo lastSegmentInfo = it->GetSegmentInfo();
            newSegmentInfo.locator = lastSegmentInfo.locator;
            newSegmentInfo.timestamp = lastSegmentInfo.timestamp;
            newSegData->SetBaseDocId(it->GetBaseDocId() + lastSegmentInfo.docCount);
        }
        else
        {
            newSegmentInfo.locator = version.GetLocator();
            newSegmentInfo.timestamp = version.GetTimestamp();
            newSegData->SetBaseDocId(0);
        }
    }
    return NewSegmentMetaPtr(new NewSegmentMeta(newSegData, newSegmentInfo));
}

void CustomPartitionData::AddBuiltSegment(segmentid_t segId, const SegmentInfoPtr& segInfo)
{
    autil::ScopedLock lock(mLock);    
    AddBuiltSegment(segId, *segInfo);
}

void CustomPartitionData::AddBuiltSegment(segmentid_t segId, const SegmentInfo& segInfo)
{
    autil::ScopedLock lock(mLock);    
    mSegmentDirectory->AddSegment(segId, segInfo.timestamp); 
}

void CustomPartitionData::RemoveSegments(const vector<segmentid_t>& segIds)
{
    autil::ScopedLock lock(mLock);
    mSegmentDirectory->RemoveSegments(segIds);
}

void CustomPartitionData::CommitVersion()
{
    autil::ScopedLock lock(mLock);
    if (mDumpSegmentQueue)
    {
        vector<CustomSegmentDumpItemPtr> dumpItems;
        mDumpSegmentQueue->GetDumpedItems(dumpItems);
        for (const auto& dumpItem : dumpItems)
        {
            if (dumpItem)
            {
                AddBuiltSegment(dumpItem->GetSegmentId(), dumpItem->GetSegmentInfo());
            }
        }
        mSegmentDirectory->CommitVersion();
    }
}

PartitionMemoryQuotaControllerPtr
CustomPartitionData::GetPartitionMemoryQuotaController() const
{
    return mMemController;
}

BlockMemoryQuotaControllerPtr
CustomPartitionData::GetTableWriterMemoryQuotaController() const
{
    return mTableWriterMemController;
}

string CustomPartitionData::GetLastLocator() const
{
    autil::ScopedLock lock(mLock);
    return mSegmentDirectory->GetLastLocator();
}

std::vector<table::BuildingSegmentReaderPtr> CustomPartitionData::GetDumpingSegmentReaders()
{
    autil::ScopedLock lock(mLock);
    std::vector<table::BuildingSegmentReaderPtr> buildingSegReaders;
    if (mDumpSegmentQueue)
    {
        buildingSegReaders.reserve(mDumpSegmentQueue->Size());
        mDumpSegmentQueue->GetBuildingSegmentReaders(buildingSegReaders, mReclaimTimestamp);
    }
    return buildingSegReaders;
}

IE_NAMESPACE_END(partition);

