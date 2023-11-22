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
#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/on_disk_partition_data.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "indexlib/util/metrics/Monitor.h"

DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(util, StateCounter);
DECLARE_REFERENCE_CLASS(partition, DumpSegmentContainer);
DECLARE_REFERENCE_CLASS(index_base, BuildingSegmentData);

namespace indexlib { namespace partition {

class InMemoryPartitionData;
DEFINE_SHARED_PTR(InMemoryPartitionData);

class InMemoryPartitionData : public OnDiskPartitionData
{
public:
    InMemoryPartitionData(const DumpSegmentContainerPtr& container,
                          util::MetricProviderPtr metricProvider = util::MetricProviderPtr(),
                          const util::CounterMapPtr& counterMap = util::CounterMapPtr(),
                          const config::IndexPartitionOptions& options = config::IndexPartitionOptions(),
                          const plugin::PluginManagerPtr& pluginManager = plugin::PluginManagerPtr());

    ~InMemoryPartitionData();

public:
    void Open(const index_base::SegmentDirectoryPtr& segDir,
              OnDiskPartitionData::DeletionMapOption deletionMapOption = DMO_NO_NEED) override;

    void RemoveSegments(const std::vector<segmentid_t>& segIds) override;
    InMemoryPartitionData* Clone() override;
    InMemoryPartitionData* Snapshot(autil::ThreadMutex* dataLock) override;
    void UpdatePartitionInfo() override;
    index_base::InMemorySegmentPtr GetInMemorySegment() const override { return mInMemSegment; }
    void CommitVersion() override;
    index_base::PartitionSegmentIteratorPtr CreateSegmentIterator() override;
    void ResetInMemorySegment() override { mInMemSegment.reset(); }

    void AddSegmentTemperatureMeta(const index_base::SegmentTemperatureMeta& temperatureMeta) override;
    void AddSegment(segmentid_t segId, timestamp_t ts);
    void AddSegmentAndUpdateData(segmentid_t segId, timestamp_t ts);
    index_base::BuildingSegmentData CreateNewSegmentData();
    void SetInMemorySegment(const index_base::InMemorySegmentPtr& inMemSegment);
    const util::CounterMapPtr& GetCounterMap() const override { return mCounterMap; }
    void SetDumpSegmentContainer(const DumpSegmentContainerPtr& container);
    void InitCounters(const util::CounterMapPtr& counterMap);
    void ClearDumpedSegment();

private:
    void MergeCounterMap(const index_base::SegmentDirectoryPtr& segDir);
    void UpdateData();
    void ReportPartitionDocCount();
    void ReportDelDocCount();
    void ReportSegmentCount();
    void RegisterMetrics();

    InMemoryPartitionData* GetSubInMemoryPartitionData() const;
    bool IsSubPartitionData() const;
    PartitionInfoHolderPtr CreatePartitionInfoHolder() override;

private:
    index_base::InMemorySegmentPtr mInMemSegment;
    util::MetricProviderPtr mMetricProvider;
    util::CounterMapPtr mCounterMap;
    config::IndexPartitionOptions mOptions;

    IE_DECLARE_METRIC(partitionDocCount);
    IE_DECLARE_METRIC(segmentCount);
    IE_DECLARE_METRIC(incSegmentCount);
    IE_DECLARE_METRIC(deletedDocCount);

    util::StateCounterPtr mPartitionDocCounter;
    util::StateCounterPtr mDeletedDocCounter;
    DumpSegmentContainerPtr mDumpSegmentContainer;

private:
    friend class PartitionCounterTest;
    friend class BuildingPartitionDataTest;
    friend class InMemoryPartitionDataTest;
    IE_LOG_DECLARE();
};
}} // namespace indexlib::partition
