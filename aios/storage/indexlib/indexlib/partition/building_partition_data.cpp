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
#include "indexlib/partition/building_partition_data.h"

#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/load_config_list.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/LocalDirectory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/calculator/on_disk_segment_size_calculator.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/offline_recover_strategy.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/offline_segment_directory.h"
#include "indexlib/index_base/segment/online_segment_directory.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/segment/segment_directory_creator.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/partition/in_memory_partition_data.h"
#include "indexlib/partition/segment/dump_segment_container.h"
#include "indexlib/partition/segment/in_memory_segment_container.h"
#include "indexlib/partition/segment/in_memory_segment_creator.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/counter/StateCounter.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"

using namespace std;
using namespace autil;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::file_system;
using namespace indexlib::common;
using namespace indexlib::index_base;

using namespace indexlib::util;
namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, BuildingPartitionData);

BuildingPartitionData::BuildingPartitionData(const IndexPartitionOptions& options,
                                             const IndexPartitionSchemaPtr& schema,
                                             const util::PartitionMemoryQuotaControllerPtr& memController,
                                             const DumpSegmentContainerPtr& container,
                                             util::MetricProviderPtr metricProvider, const CounterMapPtr& counterMap,
                                             const plugin::PluginManagerPtr& pluginManager,
                                             const document::SrcSignature& srcSignature)
    : mOptions(options)
    , mSchema(schema)
    , mDumpSegmentContainer(container)
    , mMemController(memController)
    , mSrcSignature(srcSignature)
{
    mInMemPartitionData.reset(new InMemoryPartitionData(container, metricProvider, counterMap, options, pluginManager));
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
    , mSrcSignature(other.mSrcSignature)
{
    mInMemPartitionData->SetDumpSegmentContainer(mDumpSegmentContainer);
}

BuildingPartitionData::~BuildingPartitionData() {}

BuildingPartitionData* BuildingPartitionData::Clone()
{
    ScopedLock lock(mLock);
    return new BuildingPartitionData(*this);
}

BuildingPartitionData* BuildingPartitionData::Snapshot(autil::ThreadMutex* dataLock)
{
    ScopedLock dLock(*dataLock);
    ScopedLock lock(mLock);
    BuildingPartitionData* snapshotData = new BuildingPartitionData(*this);
    InMemoryPartitionData* snapshotInMemData = mInMemPartitionData->Snapshot(dataLock);
    snapshotData->mInMemPartitionData.reset(snapshotInMemData);
    snapshotData->mInMemSegment = snapshotInMemData->GetInMemorySegment();
    return snapshotData;
}

void BuildingPartitionData::Open(const index_base::SegmentDirectoryPtr& segDir, bool preferMemoryShared)
{
    ScopedLock lock(mLock);
    CheckSegmentDirectory(segDir, mOptions);

    OnDiskPartitionData::DeletionMapOption deletionMapOption = OnDiskPartitionData::DMO_NO_NEED;
    if (mSchema->GetTableType() != tt_kkv && mSchema->GetTableType() != tt_kv &&
        mSchema->GetTableType() != tt_customized) {
        deletionMapOption =
            preferMemoryShared ? OnDiskPartitionData::DMO_SHARED_NEED : OnDiskPartitionData::DMO_PRIVATE_NEED;
    }
    mInMemPartitionData->Open(segDir, deletionMapOption);
    mInMemorySegmentCreator.reset(new InMemorySegmentCreator(GetShardingColumnNum()));
    mInMemorySegmentCreator->Init(mSchema, mOptions);
}

void BuildingPartitionData::ResetCounters()
{
    const CounterMapPtr& counterMap = GetCounterMap();
    const index_base::SegmentDirectoryPtr segDir = mInMemPartitionData->GetSegmentDirectory();
    const IndexPartitionSchemaPtr schema = mSchema;
    if (!counterMap || !schema || !segDir) {
        return;
    }

    OnDiskSegmentSizeCalculator::SizeInfoMap sizeInfoMap;
    OnDiskSegmentSizeCalculator calculator;

    const SegmentDataVector segDataVec = segDir->GetSegmentDatas();
    for (auto it = segDataVec.begin(); it != segDataVec.end(); ++it) {
        if (OnlineSegmentDirectory::IsIncSegmentId(it->GetSegmentId())) {
            calculator.CollectSegmentSizeInfo(*it, schema, sizeInfoMap);
        }
    }

    segDir->UpdateCounterMap(counterMap);
    mInMemPartitionData->InitCounters(counterMap);

    string nodePrefix = "offline.indexSize.";
    for (auto& kv : sizeInfoMap) {
        string counterNode = nodePrefix + kv.first;
        auto counter = counterMap->GetStateCounter(counterNode);
        if (!counter) {
            INDEXLIB_FATAL_ERROR(InconsistentState, "init state counter [%s] failed", counterNode.c_str());
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
    if (mDumpSegmentContainer) {
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

const TemperatureDocInfoPtr BuildingPartitionData::GetTemperatureDocInfo() const
{
    ScopedLock lock(mLock);
    auto partitionInfo = GetPartitionInfo();
    if (partitionInfo) {
        return partitionInfo->GetTemperatureDocInfo();
    }
    return TemperatureDocInfoPtr();
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

    if (mInMemSegment && mInMemSegment->GetStatus() == InMemorySegment::BUILDING) {
        return mInMemSegment;
    }

    if (mDumpSegmentContainer) {
        // push old InMemorySegment to container
        // current using InMemorySemgent is not in container
        mDumpSegmentContainer->GetInMemSegmentContainer()->PushBack(mInMemSegment);
    }
    mInMemSegment = mInMemorySegmentCreator->Create(mInMemPartitionData, mMemController);
    return mInMemSegment;
}

InMemorySegmentPtr BuildingPartitionData::CreateJoinSegment()
{
    if (!mOptions.IsOnline()) {
        return InMemorySegmentPtr();
    }

    ScopedLock lock(mLock);
    if (mJoinInMemSegment && !mJoinInMemSegment->IsDirectoryDumped()) {
        return mJoinInMemSegment;
    }

    if (mJoinInMemSegment) {
        mInMemPartitionData->AddSegmentAndUpdateData(mJoinInMemSegment->GetSegmentId(),
                                                     mJoinInMemSegment->GetTimestamp());
    }

    // TODO: now we use in mem segment, but not set base docid and segment writer
    // TODO: update in memory partition data
    mJoinInMemSegment = CreateJoinSegment(mInMemPartitionData);
    PartitionDataPtr subPartitionData = mInMemPartitionData->GetSubPartitionData();
    if (subPartitionData) {
        InMemoryPartitionDataPtr subInMemPartitionData = DYNAMIC_POINTER_CAST(InMemoryPartitionData, subPartitionData);
        assert(subInMemPartitionData);
        InMemorySegmentPtr subJoinSegment = CreateJoinSegment(subInMemPartitionData);
        mJoinInMemSegment->SetSubInMemorySegment(subJoinSegment);
    }

    return mJoinInMemSegment;
}

InMemorySegmentPtr BuildingPartitionData::CreateJoinSegment(const InMemoryPartitionDataPtr& partitionData) const
{
    ScopedLock lock(mLock);
    const index_base::SegmentDirectoryPtr& segDir = partitionData->GetSegmentDirectory();
    OnlineSegmentDirectoryPtr onlineSegDir = DYNAMIC_POINTER_CAST(OnlineSegmentDirectory, segDir);
    assert(onlineSegDir);
    segmentid_t segId = onlineSegDir->CreateJoinSegmentId();
    DirectoryPtr parentDirectory = onlineSegDir->GetSegmentParentDirectory(segId);
    BuildingSegmentData segmentData(mOptions.GetBuildConfig());
    segmentData.SetSegmentId(segId);
    segmentData.SetSegmentDirName(onlineSegDir->GetVersion().GetNewSegmentDirName(segId));
    segmentData.Init(parentDirectory, onlineSegDir->IsSubSegmentDirectory());

    util::BlockMemoryQuotaControllerPtr segmentMemController(
        new util::BlockMemoryQuotaController(mMemController, "in_memory_segment"));
    InMemorySegmentPtr joinInMemSegment(
        new InMemorySegment(mOptions.GetBuildConfig(), segmentMemController, partitionData->GetCounterMap()));
    joinInMemSegment->Init(segmentData, onlineSegDir->IsSubSegmentDirectory());
    return joinInMemSegment;
}

const file_system::DirectoryPtr& BuildingPartitionData::GetRootDirectory() const
{
    ScopedLock lock(mLock);
    return mInMemPartitionData->GetRootDirectory();
}

const DumpSegmentContainerPtr& BuildingPartitionData::GetDumpSegmentContainer() const
{
    ScopedLock lock(mLock);
    return mDumpSegmentContainer;
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
    for (size_t i = 0; i < dumpedSegments.size(); i++) {
        AddBuiltSegment(dumpedSegments[i]->GetSegmentId(), dumpedSegments[i]->GetSegmentInfo());
    }
    CreateJoinSegment();
    mInMemPartitionData->CommitVersion();
    mDumpSegmentContainer->ClearDumpedSegment();
}

void BuildingPartitionData::UpdateDumppedSegment()
{
    ScopedLock lock(mLock);
    vector<InMemorySegmentPtr> dumpedSegments;
    mDumpSegmentContainer->GetDumpedSegments(dumpedSegments);
    for (size_t i = 0; i < dumpedSegments.size(); ++i) {
        AddBuiltSegment(dumpedSegments[i]->GetSegmentId(), dumpedSegments[i]->GetSegmentInfo());
    }
    mDumpSegmentContainer->ClearDumpedSegment();
    mInMemPartitionData->ClearDumpedSegment();
}

void BuildingPartitionData::AddBuiltSegment(segmentid_t segId, const SegmentInfoPtr& segInfo)
{
    ScopedLock lock(mLock);
    mInMemPartitionData->AddSegment(segId, segInfo->timestamp);
}

void BuildingPartitionData::AddSegmentTemperatureMeta(const SegmentTemperatureMeta& meta)
{
    ScopedLock lock(mLock);
    mInMemPartitionData->AddSegmentTemperatureMeta(meta);
}

void BuildingPartitionData::InheritInMemorySegment(const InMemorySegmentPtr& inMemSegment)
{
    ScopedLock lock(mLock);
    assert(inMemSegment);
    assert(!mInMemSegment);
    mInMemSegment = mInMemorySegmentCreator->Create(mInMemPartitionData, inMemSegment, InMemorySegmentCreator::SHARED);
}

void BuildingPartitionData::SwitchPrivateInMemSegment(const InMemorySegmentPtr& inMemSegment)
{
    ScopedLock lock(mLock);
    assert(inMemSegment);
    assert(!mInMemSegment);
    mInMemSegment = mInMemorySegmentCreator->Create(mInMemPartitionData, inMemSegment, InMemorySegmentCreator::PRIVATE);
}

void BuildingPartitionData::CheckSegmentDirectory(const index_base::SegmentDirectoryPtr& segDir,
                                                  const IndexPartitionOptions& options)
{
#define CHECK_SEGMENT_DIRECTORY(segDir, SegmentDirectoryType)                                                          \
    SegmentDirectoryType* typedSegDir = dynamic_cast<SegmentDirectoryType*>(segDir.get());                             \
    if (!typedSegDir) {                                                                                                \
        INDEXLIB_FATAL_ERROR(BadParameter, "SegmentDirectory should be [%s] type", #SegmentDirectoryType);             \
    }

    if (options.IsOnline()) {
        CHECK_SEGMENT_DIRECTORY(segDir, OnlineSegmentDirectory);
    } else {
        CHECK_SEGMENT_DIRECTORY(segDir, OfflineSegmentDirectory);
    }

    bool hasSubSchema = mSchema->GetSubIndexPartitionSchema() ? true : false;
    bool hasSubSegDir = segDir->GetSubSegmentDirectory() ? true : false;

    if ((hasSubSchema && !hasSubSegDir) || (!hasSubSchema && hasSubSegDir)) {
        INDEXLIB_FATAL_ERROR(BadParameter, "SegmentDirectory about subDoc should be consistent with schema");
    }
}

segmentid_t BuildingPartitionData::GetLastValidRtSegmentInLinkDirectory() const
{
    // segment_info file dump sequence : sub segmentinfo -> segmentinfo
    // so: if main rt segment is valid in link, sub must be valid
    return mInMemPartitionData->GetLastValidRtSegmentInLinkDirectory();
}

bool BuildingPartitionData::SwitchToLinkDirectoryForRtSegments(segmentid_t lastLinkRtSegId)
{
    if (!mInMemPartitionData->SwitchToLinkDirectoryForRtSegments(lastLinkRtSegId)) {
        return false;
    }
    PartitionDataPtr subPartData = GetSubPartitionData();
    if (subPartData) {
        return subPartData->SwitchToLinkDirectoryForRtSegments(lastLinkRtSegId);
    }
    return true;
}

string BuildingPartitionData::GetLastLocator() const
{
    ScopedLock lock(mLock);
    if (mInMemSegment) {
        SegmentInfoPtr segInfo = mInMemSegment->GetSegmentInfo();
        if (!segInfo) {
            return string("");
        }
        if (!segInfo->GetLocator().IsValid()) {
            return string("");
        }
        return segInfo->GetLocator().Serialize();
    }
    return mInMemPartitionData->GetLastLocator();
}

PartitionSegmentIteratorPtr BuildingPartitionData::CreateSegmentIterator()
{
    return mInMemPartitionData->CreateSegmentIterator();
}

SegmentDataVector::const_iterator BuildingPartitionData::Begin() const { return mInMemPartitionData->Begin(); }

SegmentDataVector::const_iterator BuildingPartitionData::End() const { return mInMemPartitionData->End(); }

segmentid_t BuildingPartitionData::GetLastSegmentId() const { return mInMemPartitionData->GetLastSegmentId(); }

PartitionMeta BuildingPartitionData::GetPartitionMeta() const { return mInMemPartitionData->GetPartitionMeta(); }
const IndexFormatVersion& BuildingPartitionData::GetIndexFormatVersion() const
{
    return mInMemPartitionData->GetIndexFormatVersion();
}

index::DeletionMapReaderPtr BuildingPartitionData::GetDeletionMapReader() const
{
    ScopedLock lock(mLock);
    return mInMemPartitionData->GetDeletionMapReader();
}

// only used for reader
index_base::PartitionDataPtr BuildingPartitionData::GetSubPartitionData() const
{
    return mInMemPartitionData->GetSubPartitionData();
}

const index_base::SegmentDirectoryPtr& BuildingPartitionData::GetSegmentDirectory() const
{
    return mInMemPartitionData->GetSegmentDirectory();
}

uint32_t BuildingPartitionData::GetIndexShardingColumnNum(const config::IndexPartitionOptions& options) const
{
    return mInMemPartitionData->GetIndexShardingColumnNum(options);
}

const util::CounterMapPtr& BuildingPartitionData::GetCounterMap() const { return mInMemPartitionData->GetCounterMap(); }

const plugin::PluginManagerPtr& BuildingPartitionData::GetPluginManager() const
{
    return mInMemPartitionData->GetPluginManager();
}
}} // namespace indexlib::partition
