#include "indexlib/partition/building_partition_data.h"
#include "indexlib/partition/segment/in_memory_segment_creator.h"
#include "indexlib/partition/segment/dump_segment_container.h"
#include "indexlib/partition/segment/in_memory_segment_container.h"
#include "indexlib/partition/in_memory_partition_data.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/segment_directory_creator.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index_base/offline_recover_strategy.h"
#include "indexlib/file_system/local_directory.h"
#include "indexlib/file_system/in_mem_directory.h"
#include "indexlib/config/load_config_list.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index_base/index_meta/segment_metrics.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/online_segment_directory.h"
#include "indexlib/index_base/segment/offline_segment_directory.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/util/counter/state_counter.h"
#include "indexlib/util/memory_control/block_memory_quota_controller.h"
#include "indexlib/index/calculator/on_disk_segment_size_calculator.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, BuildingPartitionData);

BuildingPartitionData::BuildingPartitionData(
        const IndexPartitionOptions& options,
        const IndexPartitionSchemaPtr& schema,
        const util::PartitionMemoryQuotaControllerPtr& memController,
        const DumpSegmentContainerPtr& container,
        misc::MetricProviderPtr metricProvider,
        const CounterMapPtr& counterMap,
        const plugin::PluginManagerPtr& pluginManager)
    : mOptions(options)
    , mSchema(schema)
    , mDumpSegmentContainer(container)
    , mMemController(memController)
{
    bool needDeletionMap = schema->GetTableType() != tt_kkv &&
        schema->GetTableType() != tt_kv &&
        schema->GetTableType() != tt_customized;
    mInMemPartitionData.reset(new InMemoryPartitionData(
                    container, metricProvider, needDeletionMap,
                    counterMap, options, pluginManager));
}

BuildingPartitionData::BuildingPartitionData(const BuildingPartitionData& other)
    : mOptions(other.mOptions)
    , mSchema(other.mSchema)
    , mDumpSegmentContainer(other.mDumpSegmentContainer->Clone())
    , mInMemPartitionData(other.mInMemPartitionData->Clone())
    , mInMemSegment(other.mInMemSegment)
    , mJoinInMemSegment(other.mJoinInMemSegment)
    , mInMemorySegmentCreator(other.mInMemorySegmentCreator)
    , mMemController(other.mMemController)
{
    mInMemPartitionData->SetDumpSegmentContainer(mDumpSegmentContainer);
}

BuildingPartitionData::~BuildingPartitionData() 
{
}

BuildingPartitionData* BuildingPartitionData::Clone()
{
    ScopedLock lock(mLock);
    return new BuildingPartitionData(*this);
}

void BuildingPartitionData::Open(const index_base::SegmentDirectoryPtr& segDir)
{
    ScopedLock lock(mLock);
    CheckSegmentDirectory(segDir, mOptions);
    mInMemPartitionData->Open(segDir);
    if (mOptions.IsOnline())
    {
        FillIndexSizeCounters(mInMemPartitionData->GetCounterMap(), segDir, mSchema);
    }
    mInMemorySegmentCreator.reset(new InMemorySegmentCreator(GetShardingColumnNum()));
    mInMemorySegmentCreator->Init(mSchema, mOptions);
}

void BuildingPartitionData::FillIndexSizeCounters(
        const CounterMapPtr& counterMap,
        const index_base::SegmentDirectoryPtr& segDir,
        const IndexPartitionSchemaPtr& schema)
{
    if (!counterMap || !schema)
    {
        return;
    }
    
    OnDiskSegmentSizeCalculator::SizeInfoMap sizeInfoMap;
    OnDiskSegmentSizeCalculator calculator;
    
    const SegmentDataVector& segDataVec = segDir->GetSegmentDatas();
    for (auto it = segDataVec.begin(); it != segDataVec.end(); ++it)
    {
        if (OnlineSegmentDirectory::IsIncSegmentId(it->GetSegmentId()))
        {
            calculator.CollectSegmentSizeInfo(*it, schema, sizeInfoMap);
        }
    }

    string nodePrefix = "offline.indexSize.";
    for (auto& kv : sizeInfoMap)
    {
        string counterNode = nodePrefix + kv.first;
        auto counter = counterMap->GetStateCounter(counterNode);
        if (!counter)
        {
            INDEXLIB_FATAL_ERROR(InconsistentState, "init state counter [%s] failed",
                    counterNode.c_str());
        }
        counter->Set(kv.second);
    }
}

InMemorySegmentPtr BuildingPartitionData::GetInMemorySegment() const
{
    ScopedLock lock(mLock);
    return mInMemSegment;
}

void BuildingPartitionData::ResetInMemorySegment()
{
    ScopedLock lock(mLock);
    if (mDumpSegmentContainer)
    {
        // push old InMemorySegment to container
        // current using InMemorySemgent is not in container
        mDumpSegmentContainer->GetInMemSegmentContainer()->PushBack(mInMemSegment);
    }
    mInMemSegment.reset();
    mInMemPartitionData->ResetInMemorySegment();
}

PartitionInfoPtr BuildingPartitionData::GetPartitionInfo() const
{
    ScopedLock lock(mLock);
    return mInMemPartitionData->GetPartitionInfo();
}

Version BuildingPartitionData::GetVersion() const
{
    ScopedLock lock(mLock);
    return mInMemPartitionData->GetVersion();
}

Version BuildingPartitionData::GetOnDiskVersion() const
{
    ScopedLock lock(mLock);
    return mInMemPartitionData->GetOnDiskVersion();
}

void BuildingPartitionData::UpdatePartitionInfo()
{
    ScopedLock lock(mLock);
    mInMemPartitionData->UpdatePartitionInfo();
}

void BuildingPartitionData::RemoveSegments(const vector<segmentid_t>& segmentIds) 
{
    ScopedLock lock(mLock);
    mInMemPartitionData->RemoveSegments(segmentIds);
}

InMemorySegmentPtr BuildingPartitionData::CreateNewSegment()
{
    ScopedLock lock(mLock);

    if (mInMemSegment && mInMemSegment->GetStatus() == InMemorySegment::BUILDING)
    {
        return mInMemSegment;
    }

    if (mDumpSegmentContainer)
    {
        // push old InMemorySegment to container
        // current using InMemorySemgent is not in container
        mDumpSegmentContainer->GetInMemSegmentContainer()->PushBack(mInMemSegment);
    }
    mInMemSegment = mInMemorySegmentCreator->Create(mInMemPartitionData, mMemController);
    return mInMemSegment;
}

InMemorySegmentPtr BuildingPartitionData::CreateJoinSegment()
{
    if (!mOptions.IsOnline())
    {
        return InMemorySegmentPtr();
    }
    
    ScopedLock lock(mLock);
    if (mJoinInMemSegment && !mJoinInMemSegment->IsDirectoryDumped())
    {
        return mJoinInMemSegment;
    }

    if (mJoinInMemSegment)
    {
        mInMemPartitionData->AddSegmentAndUpdateData(
                mJoinInMemSegment->GetSegmentId(), 
                mJoinInMemSegment->GetTimestamp());
    }

    //TODO: now we use in mem segment, but not set base docid and segment writer
    //TODO: update in memory partition data
    mJoinInMemSegment = CreateJoinSegment(mInMemPartitionData);
    PartitionDataPtr subPartitionData = 
        mInMemPartitionData->GetSubPartitionData();
    if (subPartitionData)
    {
        InMemoryPartitionDataPtr subInMemPartitionData = DYNAMIC_POINTER_CAST(
                InMemoryPartitionData, subPartitionData);
        assert(subInMemPartitionData);
        InMemorySegmentPtr subJoinSegment = CreateJoinSegment(subInMemPartitionData);
        mJoinInMemSegment->SetSubInMemorySegment(subJoinSegment);
    }

    return mJoinInMemSegment;
}

InMemorySegmentPtr BuildingPartitionData::CreateJoinSegment(
        const InMemoryPartitionDataPtr& partitionData) const
{
    ScopedLock lock(mLock);
    const index_base::SegmentDirectoryPtr& segDir = partitionData->GetSegmentDirectory();
    OnlineSegmentDirectoryPtr onlineSegDir = 
        DYNAMIC_POINTER_CAST(OnlineSegmentDirectory, segDir);
    assert(onlineSegDir);
    segmentid_t segId = onlineSegDir->CreateJoinSegmentId();
    DirectoryPtr parentDirectory = onlineSegDir->GetSegmentParentDirectory(segId);
    BuildingSegmentData segmentData(mOptions.GetBuildConfig());
    segmentData.SetSegmentId(segId);
    segmentData.SetSegmentDirName(onlineSegDir->GetVersion().GetNewSegmentDirName(segId));
    segmentData.Init(parentDirectory, onlineSegDir->IsSubSegmentDirectory());

    util::BlockMemoryQuotaControllerPtr segmentMemController(
            new util::BlockMemoryQuotaController(mMemController, "in_memory_segment"));
    InMemorySegmentPtr joinInMemSegment(new InMemorySegment(mOptions.GetBuildConfig(), segmentMemController,
                    partitionData->GetCounterMap()));
    joinInMemSegment->Init(segmentData, onlineSegDir->IsSubSegmentDirectory());
    return joinInMemSegment;
}

const file_system::DirectoryPtr& BuildingPartitionData::GetRootDirectory() const 
{
    ScopedLock lock(mLock);
    return mInMemPartitionData->GetRootDirectory(); 
}

SegmentData BuildingPartitionData::GetSegmentData(segmentid_t segId) const
{
    ScopedLock lock(mLock);
    return mInMemPartitionData->GetSegmentData(segId);
}

void BuildingPartitionData::CommitVersion()
{
    ScopedLock lock(mLock);
    vector<InMemorySegmentPtr> dumpedSegments;
    mDumpSegmentContainer->GetDumpedSegments(dumpedSegments);
    for (size_t i = 0; i < dumpedSegments.size(); i++)
    {
        AddBuiltSegment(
                    dumpedSegments[i]->GetSegmentId(), dumpedSegments[i]->GetSegmentInfo());
    }
    CreateJoinSegment();
    mInMemPartitionData->CommitVersion();
    mDumpSegmentContainer->ClearDumpedSegment();
}

void BuildingPartitionData::AddBuiltSegment(segmentid_t segId, const SegmentInfoPtr& segInfo)
{
    ScopedLock lock(mLock);
    mInMemPartitionData->AddSegment(segId, segInfo->timestamp);
}

void BuildingPartitionData::InheritInMemorySegment(
        const InMemorySegmentPtr& inMemSegment)
{
    ScopedLock lock(mLock);
    assert(inMemSegment);
    assert(!mInMemSegment);
    mInMemSegment = mInMemorySegmentCreator->Create(mInMemPartitionData, inMemSegment);
}

void BuildingPartitionData::CheckSegmentDirectory(
        const index_base::SegmentDirectoryPtr& segDir,
        const IndexPartitionOptions& options)
{
#define CHECK_SEGMENT_DIRECTORY(segDir, SegmentDirectoryType)           \
    SegmentDirectoryType* typedSegDir = dynamic_cast<SegmentDirectoryType*>(segDir.get()); \
    if (!typedSegDir)                                                   \
    {                                                                   \
        INDEXLIB_FATAL_ERROR(BadParameter, "SegmentDirectory should be [%s] type", #SegmentDirectoryType); \
    }                                                                   \
    
    if (options.IsOnline())
    {
        CHECK_SEGMENT_DIRECTORY(segDir, OnlineSegmentDirectory);
    }
    else
    {
        CHECK_SEGMENT_DIRECTORY(segDir, OfflineSegmentDirectory);
    }

    bool hasSubSchema = mSchema->GetSubIndexPartitionSchema() ? true : false;
    bool hasSubSegDir = segDir->GetSubSegmentDirectory() ? true : false;

    if ((hasSubSchema && !hasSubSegDir)
        || (!hasSubSchema && hasSubSegDir))
    {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "SegmentDirectory about subDoc should be consistent with schema");
    }
}

segmentid_t BuildingPartitionData::GetLastValidRtSegmentInLinkDirectory() const
{
    // segment_info file dump sequence : sub segmentinfo -> segmentinfo
    // so: if main rt segment is valid in link, sub must be valid
    return mInMemPartitionData->GetLastValidRtSegmentInLinkDirectory();
}

bool BuildingPartitionData::SwitchToLinkDirectoryForRtSegments(
        segmentid_t lastLinkRtSegId)
{
    if (!mInMemPartitionData->SwitchToLinkDirectoryForRtSegments(lastLinkRtSegId))
    {
        return false;
    }
    PartitionDataPtr subPartData = GetSubPartitionData();
    if (subPartData)
    {
        return subPartData->SwitchToLinkDirectoryForRtSegments(lastLinkRtSegId);
    }
    return true;
}

string BuildingPartitionData::GetLastLocator() const
{
    ScopedLock lock(mLock);
    if (mInMemSegment)
    {
        SegmentInfoPtr segInfo = mInMemSegment->GetSegmentInfo();
        return segInfo ? segInfo->locator.ToString() : string("");
    }
    return mInMemPartitionData->GetLastLocator();
}

PartitionSegmentIteratorPtr BuildingPartitionData::CreateSegmentIterator()
{
    return mInMemPartitionData->CreateSegmentIterator();
}

SegmentDataVector::const_iterator BuildingPartitionData::Begin() const 
{
    return mInMemPartitionData->Begin();
}


SegmentDataVector::const_iterator BuildingPartitionData::End() const 
{
    return mInMemPartitionData->End();
}

PartitionMeta BuildingPartitionData::GetPartitionMeta() const 
{
    return mInMemPartitionData->GetPartitionMeta();
}
const IndexFormatVersion& BuildingPartitionData::GetIndexFormatVersion() const 
{
    return mInMemPartitionData->GetIndexFormatVersion();
}

index::DeletionMapReaderPtr BuildingPartitionData::GetDeletionMapReader() const 
{ return mInMemPartitionData->GetDeletionMapReader(); }

// only used for reader
index_base::PartitionDataPtr BuildingPartitionData::GetSubPartitionData() const 
{ return mInMemPartitionData->GetSubPartitionData(); }    

const index_base::SegmentDirectoryPtr& BuildingPartitionData::GetSegmentDirectory() const 
{ return mInMemPartitionData->GetSegmentDirectory(); }

uint32_t BuildingPartitionData::GetIndexShardingColumnNum(
    const config::IndexPartitionOptions& options) const 
{ return mInMemPartitionData->GetIndexShardingColumnNum(options); }

const util::CounterMapPtr& BuildingPartitionData::GetCounterMap() const 
{ return mInMemPartitionData->GetCounterMap(); }

const plugin::PluginManagerPtr& BuildingPartitionData::GetPluginManager() const 
{ return mInMemPartitionData->GetPluginManager(); }

IE_NAMESPACE_END(partition);

