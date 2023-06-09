/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/partition/in_memory_partition_data.h"

#include "beeper/beeper.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index_base/segment/building_segment_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/online_segment_directory.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/partition/partition_info_holder.h"
#include "indexlib/partition/segment/dump_segment_container.h"
#include "indexlib/partition/segment/segment_data_creator.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/util/counter/CounterBase.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/counter/StateCounter.h"

using namespace std;
using namespace autil;
using namespace indexlib::index;
using namespace indexlib::file_system;
using namespace indexlib::config;
using namespace indexlib::index_base;

using namespace indexlib::util;
namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, InMemoryPartitionData);

InMemoryPartitionData::InMemoryPartitionData(const DumpSegmentContainerPtr& container, MetricProviderPtr metricProvider,
                                             const util::CounterMapPtr& counterMap,
                                             const IndexPartitionOptions& options,
                                             const plugin::PluginManagerPtr& pluginManager)
    : OnDiskPartitionData(pluginManager)
    , mMetricProvider(metricProvider)
    , mCounterMap(counterMap)
    , mOptions(options)
    , mDumpSegmentContainer(container)
{
}

InMemoryPartitionData::~InMemoryPartitionData() {}

void InMemoryPartitionData::RegisterMetrics()
{
    assert(mSegmentDirectory);
    bool isSub = mSegmentDirectory->IsSubSegmentDirectory();
    if (isSub) {
        IE_INIT_METRIC_GROUP(mMetricProvider, partitionDocCount, "partition/subPartitionDocCount", kmonitor::STATUS,
                             "count");
        IE_INIT_METRIC_GROUP(mMetricProvider, segmentCount, "partition/subSegmentCount", kmonitor::STATUS, "count");
        IE_INIT_METRIC_GROUP(mMetricProvider, incSegmentCount, "partition/subIncSegmentCount", kmonitor::STATUS,
                             "count");
        IE_INIT_METRIC_GROUP(mMetricProvider, deletedDocCount, "partition/subDeletedDocCount", kmonitor::STATUS,
                             "count");
    } else {
        IE_INIT_METRIC_GROUP(mMetricProvider, partitionDocCount, "partition/partitionDocCount", kmonitor::STATUS,
                             "count");
        IE_INIT_METRIC_GROUP(mMetricProvider, segmentCount, "partition/segmentCount", kmonitor::STATUS, "count");
        IE_INIT_METRIC_GROUP(mMetricProvider, incSegmentCount, "partition/incSegmentCount", kmonitor::STATUS, "count");
        if (mDeletionMapOption != OnDiskPartitionData::DMO_NO_NEED) {
            IE_INIT_METRIC_GROUP(mMetricProvider, deletedDocCount, "partition/deletedDocCount", kmonitor::STATUS,
                                 "count");
        }
    }
}

void InMemoryPartitionData::Open(const SegmentDirectoryPtr& segDir,
                                 OnDiskPartitionData::DeletionMapOption deletionMapOption)
{
    mDeletionMapOption = deletionMapOption;
    SegmentDirectoryPtr subSegDir = segDir->GetSubSegmentDirectory();
    if (subSegDir) {
        InMemoryPartitionDataPtr partitionData(
            new InMemoryPartitionData(mDumpSegmentContainer, mMetricProvider, mCounterMap, mOptions));
        partitionData->Open(subSegDir, mDeletionMapOption);
        mSubPartitionData = partitionData;
    }
    mSegmentDirectory = segDir;
    if (!mOptions.IsOnline()) {
        MergeCounterMap(segDir);
        InitCounters(mCounterMap);
    }
    RegisterMetrics();
    InitPartitionMeta(segDir->GetRootDirectory());
    UpdateData();
}

InMemoryPartitionData* InMemoryPartitionData::Clone() { return new InMemoryPartitionData(*this); }

InMemoryPartitionData* InMemoryPartitionData::Snapshot(autil::ThreadMutex* dataLock)
{
    ScopedLock lk(*dataLock);
    InMemoryPartitionData* inMemoryPartitionData = new InMemoryPartitionData(*this);
    if (mInMemSegment) {
        inMemoryPartitionData->ResetInMemorySegment();
        inMemoryPartitionData->SetInMemorySegment(mInMemSegment->Snapshot());
    }
    DumpSegmentContainerPtr snapshotContainer(mDumpSegmentContainer->Clone());
    inMemoryPartitionData->SetDumpSegmentContainer(snapshotContainer);
    return inMemoryPartitionData;
}

void InMemoryPartitionData::MergeCounterMap(const SegmentDirectoryPtr& segDir)
{
    if (!mCounterMap) {
        return;
    }
    assert(segDir);
    segDir->UpdateCounterMap(mCounterMap);
}

void InMemoryPartitionData::InitCounters(const util::CounterMapPtr& counterMap)
{
    if (mOptions.IsOnline() || !counterMap) {
        return;
    }
    assert(mSegmentDirectory);
    bool isSub = mSegmentDirectory->IsSubSegmentDirectory();

    auto InitCounter = [&counterMap, isSub](const string& counterName) {
        string prefix = isSub ? "offline.sub_" : "offline.";
        string counterPath = prefix + counterName;
        auto stateCounter = counterMap->GetStateCounter(counterPath);
        if (!stateCounter) {
            INDEXLIB_FATAL_ERROR(InconsistentState, "init counter[%s] failed", counterPath.c_str());
        }
        return stateCounter;
    };
    mPartitionDocCounter = InitCounter("partitionDocCount");
    mDeletedDocCounter = InitCounter("deletedDocCount");
}

void InMemoryPartitionData::SetInMemorySegment(const InMemorySegmentPtr& inMemSegment)
{
    if (mSubPartitionData) {
        InMemorySegmentPtr subInMemSegment = inMemSegment->GetSubInMemorySegment();
        GetSubInMemoryPartitionData()->SetInMemorySegment(subInMemSegment);
    }
    mInMemSegment = inMemSegment;
    UpdateData();
}

void InMemoryPartitionData::UpdatePartitionInfo()
{
    auto partitionInfoHolder = GetPartitionInfoHolder();
    if (partitionInfoHolder) {
        partitionInfoHolder->UpdatePartitionInfo(mInMemSegment);
    }
    ReportPartitionDocCount();
    ReportDelDocCount();
    if (mSubPartitionData) {
        GetSubInMemoryPartitionData()->ReportPartitionDocCount();
        GetSubInMemoryPartitionData()->ReportDelDocCount();
    }
}

void InMemoryPartitionData::AddSegmentTemperatureMeta(const SegmentTemperatureMeta& temperatureMeta)
{
    mSegmentDirectory->AddSegmentTemperatureMeta(temperatureMeta);
    // todo for sub
}

void InMemoryPartitionData::AddSegment(segmentid_t segId, timestamp_t ts)
{
    mSegmentDirectory->AddSegment(segId, ts);

    if (mSubPartitionData) {
        GetSubInMemoryPartitionData()->AddSegment(segId, ts);
    }
}

void InMemoryPartitionData::AddSegmentAndUpdateData(segmentid_t segId, timestamp_t ts)
{
    if (mSubPartitionData) {
        GetSubInMemoryPartitionData()->AddSegmentAndUpdateData(segId, ts);
    }
    mSegmentDirectory->AddSegment(segId, ts);
    UpdateData();
}

void InMemoryPartitionData::RemoveSegments(const vector<segmentid_t>& segIds)
{
    // TODO: not delete phsical, only clean version can delete index
    if (mSubPartitionData) {
        GetSubInMemoryPartitionData()->RemoveSegments(segIds);
    }
    mSegmentDirectory->RemoveSegments(segIds);
    UpdateData();

    IE_LOG(INFO, "End remove segment [%s]", StringUtil::toString(segIds, ",").c_str());
}

BuildingSegmentData InMemoryPartitionData::CreateNewSegmentData()
{
    // new segment data has not directory, it'll be created after segment dumped
    // TODO: no on disk segment data, set base docid = 0, timestamp? locator?
    InMemorySegmentPtr lastDumpingSegment = mDumpSegmentContainer->GetLastSegment();
    if (IsSubPartitionData() && lastDumpingSegment) {
        lastDumpingSegment = lastDumpingSegment->GetSubInMemorySegment();
    }
    BuildingSegmentData newSegmentData =
        SegmentDataCreator::CreateNewSegmentData(mSegmentDirectory, lastDumpingSegment, mOptions.GetBuildConfig());
    if (mSubPartitionData) {
        BuildingSegmentData subSegmentData = GetSubInMemoryPartitionData()->CreateNewSegmentData();
        newSegmentData.SetSubSegmentData(SegmentDataPtr(new BuildingSegmentData(subSegmentData)));
    }
    return newSegmentData;
}

void InMemoryPartitionData::CommitVersion()
{
    if (!mSegmentDirectory->IsVersionChanged()) {
        return;
    }
    mSegmentDirectory->CommitVersion(mOptions);
}

void InMemoryPartitionData::UpdateData()
{
    if (mDeletionMapOption != DeletionMapOption::DMO_NO_NEED) {
        mDeletionMapReader = CreateDeletionMapReader(mDeletionMapOption);
    }
    mPartitionInfoHolder = CreatePartitionInfoHolder();
    if (mInMemSegment) {
        mPartitionInfoHolder->AddInMemorySegment(mInMemSegment);
    }
    // mSubPartitionData must update first
    if (mSubPartitionData) {
        mPartitionInfoHolder->SetSubPartitionInfoHolder(GetSubInMemoryPartitionData()->GetPartitionInfoHolder());
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
    if (mInMemSegment) {
        docCount = mInMemSegment->GetBaseDocId() + mInMemSegment->GetSegmentInfo()->docCount;
    } else if (it != segmentDatas.rend()) {
        docCount = it->GetBaseDocId() + it->GetSegmentInfo()->docCount;
    }
    IE_REPORT_METRIC(partitionDocCount, docCount);
    if (mPartitionDocCounter) {
        mPartitionDocCounter->Set(docCount);
    }
}

void InMemoryPartitionData::ClearDumpedSegment() { mDumpSegmentContainer->ClearDumpedSegment(); }

void InMemoryPartitionData::ReportDelDocCount()
{
    double delDocCount = 0;
    if (mDeletionMapReader) {
        delDocCount = mDeletionMapReader->GetDeletedDocCount();
    }
    IE_REPORT_METRIC(deletedDocCount, delDocCount);
    if (mDeletedDocCounter) {
        mDeletedDocCounter->Set(delDocCount);
    }
}

void InMemoryPartitionData::ReportSegmentCount()
{
    const SegmentDataVector& segmentDatas = mSegmentDirectory->GetSegmentDatas();
    int64_t incSegCount = 0;
    for (const auto& segData : segmentDatas) {
        if (OnlineSegmentDirectory::IsIncSegmentId(segData.GetSegmentId())) {
            ++incSegCount;
        }
    }

    int64_t totalSegCount = (int64_t)segmentDatas.size();
    if (mInMemSegment) {
        ++totalSegCount;
    }

    IE_REPORT_METRIC(segmentCount, totalSegCount);
    IE_REPORT_METRIC(incSegmentCount, incSegCount);

    if (mOptions.IsOffline() && totalSegCount >= 40) {
        beeper::EventTags tags;
        BEEPER_FORMAT_INTERVAL_REPORT(120, INDEXLIB_BUILD_INFO_COLLECTOR_NAME, tags,
                                      "offline build segment count [%ld] over 40", totalSegCount);
        BEEPER_FORMAT_INTERVAL_REPORT(120, "bs_worker_error", tags, "offline build segment count [%ld] over 40",
                                      totalSegCount);
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
    if (!IsSubPartitionData()) {
        mDumpSegmentContainer->GetDumpingSegments(dumpingSegments);
    } else {
        mDumpSegmentContainer->GetSubDumpingSegments(dumpingSegments);
    }
    PartitionSegmentIteratorPtr segIter(new PartitionSegmentIterator(mOptions.IsOnline()));
    segmentid_t nextBuildingSegmentId = INVALID_SEGMENTID;
    if (mInMemSegment) {
        nextBuildingSegmentId = mInMemSegment->GetSegmentId() + 1;
    } else {
        BuildingSegmentData newSegData = CreateNewSegmentData();
        nextBuildingSegmentId = newSegData.GetSegmentId();
    }
    segIter->Init(mSegmentDirectory->GetSegmentDatas(), dumpingSegments, mInMemSegment, nextBuildingSegmentId);
    return segIter;
}

bool InMemoryPartitionData::IsSubPartitionData() const { return mSegmentDirectory->IsSubSegmentDirectory(); }

PartitionInfoHolderPtr InMemoryPartitionData::CreatePartitionInfoHolder()
{
    std::vector<InMemorySegmentPtr> dumpingSegments;
    if (IsSubPartitionData()) {
        mDumpSegmentContainer->GetSubDumpingSegments(dumpingSegments);
    } else {
        mDumpSegmentContainer->GetDumpingSegments(dumpingSegments);
    }
    PartitionInfoHolderPtr partitionInfoHolder(new PartitionInfoHolder);
    partitionInfoHolder->Init(mSegmentDirectory->GetVersion(), mPartitionMeta, mSegmentDirectory->GetSegmentDatas(),
                              dumpingSegments, GetDeletionMapReader());
    return partitionInfoHolder;
}

void InMemoryPartitionData::SetDumpSegmentContainer(const DumpSegmentContainerPtr& container)
{
    mDumpSegmentContainer = container;
    if (mSubPartitionData) {
        GetSubInMemoryPartitionData()->SetDumpSegmentContainer(container);
    }
}
}} // namespace indexlib::partition
