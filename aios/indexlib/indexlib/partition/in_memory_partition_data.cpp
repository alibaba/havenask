#include <beeper/beeper.h>
#include "indexlib/partition/in_memory_partition_data.h"
#include "indexlib/partition/partition_info_holder.h"
#include "indexlib/partition/segment/dump_segment_container.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/building_segment_data.h"
#include "indexlib/index_base/segment/online_segment_directory.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/partition/segment/segment_data_creator.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/util/counter/counter_base.h"
#include "indexlib/util/counter/state_counter.h"
#include "indexlib/plugin/plugin_manager.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, InMemoryPartitionData);

InMemoryPartitionData::InMemoryPartitionData(
    const DumpSegmentContainerPtr& container,
    MetricProviderPtr metricProvider,
    bool needDeletionMap, const util::CounterMapPtr& counterMap,
    const IndexPartitionOptions& options,
    const plugin::PluginManagerPtr& pluginManager)
    : OnDiskPartitionData(pluginManager)
    , mMetricProvider(metricProvider)
    , mNeedDeletionMap(needDeletionMap)
    , mCounterMap(counterMap)
    , mOptions(options)
    , mDumpSegmentContainer(container)
{
}

InMemoryPartitionData::~InMemoryPartitionData() 
{
}

void InMemoryPartitionData::RegisterMetrics()
{
    assert(mSegmentDirectory);
    bool isSub = mSegmentDirectory->IsSubSegmentDirectory();
    if (isSub)
    {
        IE_INIT_METRIC_GROUP(mMetricProvider, partitionDocCount,
                             "partition/subPartitionDocCount", kmonitor::STATUS, "count");
        IE_INIT_METRIC_GROUP(mMetricProvider, segmentCount,
                             "partition/subSegmentCount", kmonitor::STATUS, "count");
        IE_INIT_METRIC_GROUP(mMetricProvider, incSegmentCount,
                             "partition/subIncSegmentCount", kmonitor::STATUS, "count");
        IE_INIT_METRIC_GROUP(mMetricProvider, deletedDocCount,
                             "partition/subDeletedDocCount", kmonitor::STATUS, "count");
    }
    else
    {
        IE_INIT_METRIC_GROUP(mMetricProvider, partitionDocCount,
                             "partition/partitionDocCount", kmonitor::STATUS, "count");
        IE_INIT_METRIC_GROUP(mMetricProvider, segmentCount,
                             "partition/segmentCount", kmonitor::STATUS, "count");
        IE_INIT_METRIC_GROUP(mMetricProvider, incSegmentCount,
                             "partition/incSegmentCount", kmonitor::STATUS, "count");
        if (mNeedDeletionMap)
        {
            IE_INIT_METRIC_GROUP(mMetricProvider, deletedDocCount,
                    "partition/deletedDocCount", kmonitor::STATUS, "count");
        }
    }
}

void InMemoryPartitionData::Open(const SegmentDirectoryPtr& segDir,
                                 bool needDeletionMap)
{
    SegmentDirectoryPtr subSegDir = segDir->GetSubSegmentDirectory();
    if (subSegDir)
    {
        InMemoryPartitionDataPtr partitionData(
            new InMemoryPartitionData(
                mDumpSegmentContainer, mMetricProvider,
                mNeedDeletionMap, mCounterMap, mOptions));
        partitionData->Open(subSegDir);
        mSubPartitionData = partitionData;
    }
    mSegmentDirectory = segDir;
    MergeCounterMap(segDir);
    InitCounters(mCounterMap);
    RegisterMetrics();
    InitPartitionMeta(segDir->GetRootDirectory());
    UpdateData();
}


InMemoryPartitionData* InMemoryPartitionData::Clone()
{
    return new InMemoryPartitionData(*this);
}

void InMemoryPartitionData::MergeCounterMap(const SegmentDirectoryPtr& segDir)
{
    if (!mCounterMap)
    {
        return;
    }
    assert(segDir);
    segDir->UpdateCounterMap(mCounterMap);
}

void InMemoryPartitionData::InitCounters(const util::CounterMapPtr& counterMap)
{
    if (mOptions.IsOnline() || !counterMap)
    {
        return;
    }
    assert(mSegmentDirectory);
    bool isSub = mSegmentDirectory->IsSubSegmentDirectory();

    auto InitCounter = [&counterMap, isSub] (const string& counterName) {
        string prefix = isSub ? "offline.sub_" : "offline.";
        string counterPath = prefix + counterName;
        auto stateCounter = counterMap->GetStateCounter(counterPath);
        if (!stateCounter)
        {
            INDEXLIB_FATAL_ERROR(InconsistentState, "init counter[%s] failed", counterPath.c_str());
        }
        return stateCounter;
    };
    mPartitionDocCounter = InitCounter("partitionDocCount");
    mDeletedDocCounter = InitCounter("deletedDocCount");
}


void InMemoryPartitionData::SetInMemorySegment(const InMemorySegmentPtr& inMemSegment)
{
    if (mSubPartitionData)
    {
        InMemorySegmentPtr subInMemSegment = inMemSegment->GetSubInMemorySegment();
        GetSubInMemoryPartitionData()->SetInMemorySegment(subInMemSegment);
    }
    mInMemSegment = inMemSegment;
    UpdateData();
}

void InMemoryPartitionData::UpdatePartitionInfo()
{
    if (mPartitionInfoHolder)
    {
        mPartitionInfoHolder->UpdatePartitionInfo(mInMemSegment);
    }
    ReportPartitionDocCount();
    ReportDelDocCount();
    if (mSubPartitionData)
    {
        GetSubInMemoryPartitionData()->ReportPartitionDocCount();
        GetSubInMemoryPartitionData()->ReportDelDocCount();
    }
}

void InMemoryPartitionData::AddSegment(segmentid_t segId, timestamp_t ts)
{
    mSegmentDirectory->AddSegment(segId, ts);
    
    if (mSubPartitionData)
    {
        GetSubInMemoryPartitionData()->AddSegment(segId, ts);
    }
}

void InMemoryPartitionData::AddSegmentAndUpdateData(segmentid_t segId, timestamp_t ts)
{
    if (mSubPartitionData)
    {
        GetSubInMemoryPartitionData()->AddSegmentAndUpdateData(segId, ts);
    }
    mSegmentDirectory->AddSegment(segId, ts);
    UpdateData();
}

void InMemoryPartitionData::RemoveSegments(const vector<segmentid_t>& segIds)
{
    // TODO: not delete phsical, only clean version can delete index
    if (mSubPartitionData)
    {
        GetSubInMemoryPartitionData()->RemoveSegments(segIds);
    }
    mSegmentDirectory->RemoveSegments(segIds);
    UpdateData();

    IE_LOG(INFO, "End remove segment [%s]", 
           StringUtil::toString(segIds, ",").c_str());
}

BuildingSegmentData InMemoryPartitionData::CreateNewSegmentData()
{
    //new segment data has not directory, it'll be created after segment dumped
    //TODO: no on disk segment data, set base docid = 0, timestamp? locator?
    InMemorySegmentPtr lastDumpingSegment = mDumpSegmentContainer->GetLastSegment();
    if (IsSubPartitionData() && lastDumpingSegment)
    {
        lastDumpingSegment = lastDumpingSegment->GetSubInMemorySegment();
    }
    BuildingSegmentData newSegmentData =
        SegmentDataCreator::CreateNewSegmentData(mSegmentDirectory, lastDumpingSegment,
                                                 mOptions.GetBuildConfig());
    if (mSubPartitionData)
    {
        BuildingSegmentData subSegmentData = GetSubInMemoryPartitionData()->CreateNewSegmentData();
        newSegmentData.SetSubSegmentData(
                SegmentDataPtr(new BuildingSegmentData(subSegmentData)));
    }
    return newSegmentData;
}

void InMemoryPartitionData::CommitVersion()
{
    if (!mSegmentDirectory->IsVersionChanged())
    {
        return;
    }
    mSegmentDirectory->CommitVersion();
}

void InMemoryPartitionData::UpdateData()
{
    if (mNeedDeletionMap)
    {
        mDeletionMapReader = CreateDeletionMapReader();
    }
    mPartitionInfoHolder = CreatePartitionInfoHolder();
    if (mInMemSegment)
    {
        mPartitionInfoHolder->AddInMemorySegment(mInMemSegment);
    }
    // mSubPartitionData must update first
    if (mSubPartitionData)
    {
        mPartitionInfoHolder->SetSubPartitionInfoHolder(
            GetSubInMemoryPartitionData()->GetPartitionInfoHolder());
    }
    ReportSegmentCount();
    ReportPartitionDocCount();
    ReportDelDocCount();
}

void InMemoryPartitionData::ReportPartitionDocCount()
{
    double docCount = 0;

    const SegmentDataVector& segmentDatas = mSegmentDirectory->GetSegmentDatas();
    SegmentDataVector::const_reverse_iterator it = segmentDatas.rbegin();
    if (mInMemSegment)
    {
         docCount = mInMemSegment->GetBaseDocId()
                    + mInMemSegment->GetSegmentInfo()->docCount;
    }
    else if (it != segmentDatas.rend())
    {
        docCount = it->GetBaseDocId() + it->GetSegmentInfo().docCount;
    }
    IE_REPORT_METRIC(partitionDocCount, docCount);
    if (mPartitionDocCounter)
    {
        mPartitionDocCounter->Set(docCount);
    }
}

void InMemoryPartitionData::ReportDelDocCount()
{
    double delDocCount = 0;
    if (mDeletionMapReader)
    {
        delDocCount = mDeletionMapReader->GetDeletedDocCount();
    }
    IE_REPORT_METRIC(deletedDocCount, delDocCount);
    if (mDeletedDocCounter)
    {
        mDeletedDocCounter->Set(delDocCount);
    }
}

void InMemoryPartitionData::ReportSegmentCount()
{
    const SegmentDataVector& segmentDatas = mSegmentDirectory->GetSegmentDatas();
    int64_t incSegCount = 0;
    for (const auto& segData : segmentDatas)
    {
        if (OnlineSegmentDirectory::IsIncSegmentId(segData.GetSegmentId()))
        {
            ++incSegCount;
        }
    }

    int64_t totalSegCount = (int64_t)segmentDatas.size();
    if (mInMemSegment)
    {
        ++totalSegCount;
    }

    IE_REPORT_METRIC(segmentCount, totalSegCount);
    IE_REPORT_METRIC(incSegmentCount, incSegCount);

    if (mOptions.IsOffline() && totalSegCount >= 40)
    {
        beeper::EventTags tags;
        BEEPER_FORMAT_INTERVAL_REPORT(120, INDEXLIB_BUILD_INFO_COLLECTOR_NAME, tags,
                "offline build segment count [%ld] over 40", totalSegCount);
        BEEPER_FORMAT_INTERVAL_REPORT(120, "bs_worker_error", tags,
                "offline build segment count [%ld] over 40", totalSegCount);
    }
}

InMemoryPartitionData* InMemoryPartitionData::GetSubInMemoryPartitionData() const
{
    assert(mSubPartitionData);
    InMemoryPartitionData* partData = dynamic_cast<InMemoryPartitionData*>(mSubPartitionData.get());
    assert(partData);
    return partData;
}

PartitionSegmentIteratorPtr InMemoryPartitionData::CreateSegmentIterator()
{
    std::vector<InMemorySegmentPtr> dumpingSegments;
    if (!IsSubPartitionData())
    {
        mDumpSegmentContainer->GetDumpingSegments(dumpingSegments);        
    }
    else{
        mDumpSegmentContainer->GetSubDumpingSegments(dumpingSegments);
    }
    PartitionSegmentIteratorPtr segIter(new PartitionSegmentIterator(mOptions.IsOnline()));
    segmentid_t nextBuildingSegmentId = INVALID_SEGMENTID;
    if (mInMemSegment)
    {
        nextBuildingSegmentId = mInMemSegment->GetSegmentId() + 1;
    }
    else
    {
        BuildingSegmentData newSegData = CreateNewSegmentData();
        nextBuildingSegmentId = newSegData.GetSegmentId();
    }
    segIter->Init(mSegmentDirectory->GetSegmentDatas(), dumpingSegments,
                  mInMemSegment, nextBuildingSegmentId);
    return segIter;
}

bool InMemoryPartitionData::IsSubPartitionData() const
{
    return mSegmentDirectory->IsSubSegmentDirectory();    
}

PartitionInfoHolderPtr InMemoryPartitionData::CreatePartitionInfoHolder() const
{
    std::vector<InMemorySegmentPtr> dumpingSegments;
    if (IsSubPartitionData())
    {
        mDumpSegmentContainer->GetSubDumpingSegments(dumpingSegments);
    }
    else
    {
        mDumpSegmentContainer->GetDumpingSegments(dumpingSegments);   
    }
    PartitionInfoHolderPtr partitionInfoHolder(new PartitionInfoHolder);
    partitionInfoHolder->Init(mSegmentDirectory->GetVersion(),
                              mPartitionMeta,
                              mSegmentDirectory->GetSegmentDatas(),
                              dumpingSegments,
                              mDeletionMapReader);
    return partitionInfoHolder;
}

void InMemoryPartitionData::SetDumpSegmentContainer(const DumpSegmentContainerPtr& container)
{
    mDumpSegmentContainer = container;   
    if (mSubPartitionData)
    {
        GetSubInMemoryPartitionData()->SetDumpSegmentContainer(container);
    }
}

IE_NAMESPACE_END(partition);

