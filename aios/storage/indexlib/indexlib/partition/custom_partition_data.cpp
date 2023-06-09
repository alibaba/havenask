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
#include "indexlib/partition/custom_partition_data.h"

#include "autil/StringUtil.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/framework/SegmentStatistics.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/lifecycle_strategy.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/segment_data_base.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/partition/segment/custom_segment_dump_item.h"
#include "indexlib/partition/segment/dump_segment_queue.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/memory_control/PartitionMemoryQuotaController.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::index;
using namespace indexlib::plugin;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

using namespace indexlib::util;
namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, CustomPartitionData);

CustomPartitionData::CustomPartitionData(const config::IndexPartitionOptions& options,
                                         const config::IndexPartitionSchemaPtr& schema,
                                         const util::PartitionMemoryQuotaControllerPtr& memController,
                                         const util::BlockMemoryQuotaControllerPtr& tableWriterMemController,
                                         util::MetricProviderPtr metricProvider, const util::CounterMapPtr& counterMap,
                                         const plugin::PluginManagerPtr& pluginManager,
                                         const DumpSegmentQueuePtr& dumpSegmentQueue, int64_t reclaimTimestamp)
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
    , mToReclaimRtSegIds(other.mToReclaimRtSegIds)
{
    if (other.mDumpSegmentQueue) {
        mDumpSegmentQueue.reset(other.mDumpSegmentQueue->Clone());
    }
}

CustomPartitionData::~CustomPartitionData() {}

bool CustomPartitionData::Open(const SegmentDirectoryPtr& segDir)
{
    mSegmentDirectory = segDir;
    // init parttion meta

    if (mReclaimTimestamp >= 0) {
        for (auto it = Begin(); it != End(); ++it) {
            const SegmentData& segData = *it;
            segmentid_t segId = segData.GetSegmentId();
            if (!RealtimeSegmentDirectory::IsRtSegmentId(segId)) {
                continue;
            }
            if (segData.GetSegmentInfo()->timestamp < mReclaimTimestamp) {
                mToReclaimRtSegIds.emplace_back(segId);
            }
        }
    }
    return true;
}

void CustomPartitionData::RemoveObsoleteSegments()
{
    autil::ScopedLock lock(mLock);
    if (mToReclaimRtSegIds.size() >= 0) {
        string segListStr = StringUtil::toString(mToReclaimRtSegIds, ",");
        IE_LOG(INFO, "rtSegments: %s will be reclaimed with relcaimTs[%ld]", segListStr.c_str(), mReclaimTimestamp);
        RemoveSegments(mToReclaimRtSegIds);
        if (mDumpSegmentQueue) {
            mDumpSegmentQueue->ReclaimSegments(mReclaimTimestamp);
        }
    }
    CommitVersion();
    mToReclaimRtSegIds.clear();
}

index_base::Version CustomPartitionData::GetVersion() const
{
    auto baseVersion = mSegmentDirectory->GetVersion();
    if (mToReclaimRtSegIds.empty()) {
        return baseVersion;
    }
    for (const auto& segId : mToReclaimRtSegIds) {
        baseVersion.RemoveSegment(segId);
    }
    return baseVersion;
}

index_base::Version CustomPartitionData::GetOnDiskVersion() const { return mSegmentDirectory->GetOnDiskVersion(); }

index_base::PartitionMeta CustomPartitionData::GetPartitionMeta() const { return mPartitionMeta; }

const file_system::DirectoryPtr& CustomPartitionData::GetRootDirectory() const
{
    return mSegmentDirectory->GetRootDirectory();
}

const IndexFormatVersion& CustomPartitionData::GetIndexFormatVersion() const
{
    return mSegmentDirectory->GetIndexFormatVersion();
}

PartitionData::Iterator CustomPartitionData::Begin() const { return mSegmentDirectory->GetSegmentDatas().begin(); }

PartitionData::Iterator CustomPartitionData::End() const { return mSegmentDirectory->GetSegmentDatas().end(); }

segmentid_t CustomPartitionData::GetLastSegmentId() const
{
    return (*(mSegmentDirectory->GetSegmentDatas().rbegin())).GetSegmentId();
}

SegmentData CustomPartitionData::GetSegmentData(segmentid_t segId) const
{
    return mSegmentDirectory->GetSegmentData(segId);
}

SegmentDataVector CustomPartitionData::GetSegmentDatas() const
{
    SegmentDataVector segDataVec = mSegmentDirectory->GetSegmentDatas();
    if (mReclaimTimestamp >= 0) {
        vector<segmentid_t> toReclaimRtSegIds;
        for (auto it = segDataVec.begin(); it != segDataVec.end();) {
            const SegmentData& segData = *it;
            segmentid_t segId = segData.GetSegmentId();
            if (!RealtimeSegmentDirectory::IsRtSegmentId(segId)) {
                ++it;
                continue;
            }
            if (segData.GetSegmentInfo()->timestamp < mReclaimTimestamp) {
                toReclaimRtSegIds.push_back(segId);
                it = segDataVec.erase(it);
                continue;
            }
            ++it;
        }
        if (!toReclaimRtSegIds.empty()) {
            string segListStr = StringUtil::toString(toReclaimRtSegIds, ",");
            IE_LOG(INFO, "rtSegments: %s will be skipped with relcaimTs[%ld]", segListStr.c_str(), mReclaimTimestamp);
        }
    }
    return segDataVec;
}

CustomPartitionData* CustomPartitionData::Clone()
{
    autil::ScopedLock lock(mLock);
    return new CustomPartitionData(*this);
}

const SegmentDirectoryPtr& CustomPartitionData::GetSegmentDirectory() const { return mSegmentDirectory; }

bool CustomPartitionData::SwitchToLinkDirectoryForRtSegments(segmentid_t lastLinkRtSegId)
{
    return mSegmentDirectory->SwitchToLinkDirectoryForRtSegments(lastLinkRtSegId);
}

PartitionSegmentIteratorPtr CustomPartitionData::CreateSegmentIterator() { return PartitionSegmentIteratorPtr(); }

NewSegmentMetaPtr CustomPartitionData::CreateNewSegmentData()
{
    autil::ScopedLock lock(mLock);
    SegmentDataBasePtr newSegData(new SegmentDataBase());
    SegmentInfo newSegmentInfo;

    CustomSegmentDumpItemPtr lastDumpingSeg;
    if (mDumpSegmentQueue) {
        lastDumpingSeg = mDumpSegmentQueue->GetLastSegment();
    }
    if (lastDumpingSeg) {
        NewSegmentMetaPtr buildingSegMeta = lastDumpingSeg->GetSegmentMeta();
        segmentid_t newSegId = mSegmentDirectory->GetNextSegmentId(buildingSegMeta->segmentDataBase->GetSegmentId());
        SegmentInfo lastSegmentInfo = buildingSegMeta->segmentInfo;

        newSegData->SetSegmentId(newSegId);
        const Version& version = mSegmentDirectory->GetVersion();
        newSegData->SetSegmentDirName(version.GetNewSegmentDirName(newSegId));
        newSegData->SetBaseDocId(buildingSegMeta->segmentDataBase->GetBaseDocId() + lastSegmentInfo.docCount);
        newSegmentInfo.SetLocator(lastSegmentInfo.GetLocator());
        newSegmentInfo.timestamp = lastSegmentInfo.timestamp;
    } else {
        newSegData->SetSegmentId(mSegmentDirectory->CreateNewSegmentId());
        const Version& version = mSegmentDirectory->GetVersion();
        newSegData->SetSegmentDirName(version.GetNewSegmentDirName(mSegmentDirectory->CreateNewSegmentId()));

        const SegmentDataVector& segmentDatas = mSegmentDirectory->GetSegmentDatas();
        auto it = segmentDatas.rbegin();
        if (it != segmentDatas.rend()) {
            const std::shared_ptr<const SegmentInfo> lastSegmentInfo = it->GetSegmentInfo();
            newSegmentInfo.SetLocator(lastSegmentInfo->GetLocator());
            newSegmentInfo.timestamp = lastSegmentInfo->timestamp;
            newSegData->SetBaseDocId(it->GetBaseDocId() + lastSegmentInfo->docCount);
        } else {
            indexlibv2::framework::Locator locator;
            locator.Deserialize(version.GetLocator().ToString());
            newSegmentInfo.SetLocator(locator);
            newSegmentInfo.timestamp = version.GetTimestamp();
            newSegData->SetBaseDocId(0);
        }
    }
    return NewSegmentMetaPtr(new NewSegmentMeta(newSegData, newSegmentInfo));
}

void CustomPartitionData::AddDumpedSegment(segmentid_t segId, const SegmentInfo& segInfo,
                                           const indexlibv2::framework::SegmentStatistics& segmentStats)
{
    auto segmentStatsPtr = std::make_shared<indexlibv2::framework::SegmentStatistics>(segmentStats);
    segmentStatsPtr->SetSegmentId(segId);
    if (segmentStats.empty()) {
        segmentStatsPtr.reset();
    }
    autil::ScopedLock lock(mLock);
    mSegmentDirectory->AddSegment(segId, segInfo.timestamp, segmentStatsPtr);
}

void CustomPartitionData::RemoveSegments(const vector<segmentid_t>& segIds)
{
    autil::ScopedLock lock(mLock);
    mSegmentDirectory->RemoveSegments(segIds);
}

void CustomPartitionData::CommitVersion()
{
    autil::ScopedLock lock(mLock);
    if (mDumpSegmentQueue) {
        vector<CustomSegmentDumpItemPtr> dumpItems;
        mDumpSegmentQueue->GetDumpedItems(dumpItems);
        for (const auto& dumpItem : dumpItems) {
            if (dumpItem) {
                AddDumpedSegment(dumpItem->GetSegmentId(), *dumpItem->GetSegmentInfo(),
                                 dumpItem->GetSegmentDescription().segmentStats);
            }
        }
        mSegmentDirectory->CommitVersion(mOptions);
    }
}

PartitionMemoryQuotaControllerPtr CustomPartitionData::GetPartitionMemoryQuotaController() const
{
    return mMemController;
}

BlockMemoryQuotaControllerPtr CustomPartitionData::GetTableWriterMemoryQuotaController() const
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
    if (mDumpSegmentQueue) {
        buildingSegReaders.reserve(mDumpSegmentQueue->Size());
        mDumpSegmentQueue->GetBuildingSegmentReaders(buildingSegReaders, mReclaimTimestamp);
    }
    return buildingSegReaders;
}

segmentid_t CustomPartitionData::GetLastValidRtSegmentInLinkDirectory() const
{
    assert(mSegmentDirectory);
    return mSegmentDirectory->GetLastValidRtSegmentInLinkDirectory();
}

const TemperatureDocInfoPtr CustomPartitionData::GetTemperatureDocInfo() const
{
    assert(false);
    return TemperatureDocInfoPtr();
}

}} // namespace indexlib::partition
