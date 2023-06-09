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
#include "indexlib/partition/custom_online_partition_reader.h"

#include "autil/StringUtil.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_container.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_reader.h"
#include "indexlib/index/normal/source/source_reader.h"
#include "indexlib/index/normal/summary/summary_reader.h"
#include "indexlib/index/normal/util/index_metrics_base.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_data_base.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/partition/custom_partition_data.h"
#include "indexlib/table/executor_manager.h"
#include "indexlib/table/executor_provider.h"
#include "indexlib/table/normal_table/DimensionDescription.h"
#include "indexlib/table/table_factory_wrapper.h"
#include "indexlib/table/table_reader.h"
#include "indexlib/table/table_resource.h"
#include "indexlib/table/table_writer.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::table;
using namespace indexlib::common;
using namespace indexlib::index_base;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, CustomOnlinePartitionReader);

CustomOnlinePartitionReader::CustomOnlinePartitionReader(
    const IndexPartitionSchemaPtr& schema, const IndexPartitionOptions& options, const string& partitionName,
    const TableFactoryWrapperPtr& tableFactory, const TableWriterPtr& tableWriter, segmentid_t latestValidRtLinkSegId)
    : IndexPartitionReader(schema)
    , mOptions(options)
    , mPartitionName(partitionName)
    , mTableFactory(tableFactory)
    , mTableWriter(tableWriter)
    , mLatestValidRtLinkSegId(latestValidRtLinkSegId)
    , mInMemRtSegmentCount(0)
    , mOnDiskRtSegmentCount(0)
    , mUsedOnDiskRtSegmentCount(0)
{
}

CustomOnlinePartitionReader::~CustomOnlinePartitionReader()
{
    auto onDiskVersionId = INVALID_VERSION;
    if (mPartitionData) {
        onDiskVersionId = mPartitionData->GetOnDiskVersion().GetVersionId();
    }
    mTableReader.reset();
    mTableWriter.reset();
    mPartitionData.reset();
    mFutureExecutor.reset();

    stringstream dss;
    if (mDependentSegIds.size() > 0) {
        for (const auto& segId : mDependentSegIds) {
            dss << segId << " ";
        }
    }
    stringstream rss;
    if (mRtSegmentIds.size() > 0) {
        for (const auto& segId : mRtSegmentIds) {
            rss << segId << " ";
        }
    }
    IE_LOG(INFO, "PartitionReader(OnDiskVersion=[%d]) Destructed. With Dependent Segments[%s], RtSegments[%s]",
           onDiskVersionId, dss.str().c_str(), rss.str().c_str());
}

void CustomOnlinePartitionReader::Open(const PartitionDataPtr& partitionData)
{
    mPartitionData = DYNAMIC_POINTER_CAST(CustomPartitionData, partitionData);
    SwitchToLinkDirectoryForRtSegments(mPartitionData);

    PartitionMemoryQuotaControllerPtr partitionMemController = mPartitionData->GetPartitionMemoryQuotaController();
    index_base::Version incVersion = partitionData->GetOnDiskVersion();
    string controllerName = "readerMemory_version_" + StringUtil::toString(incVersion.GetVersionId());
    mReaderMemController.reset(new BlockMemoryQuotaController(partitionMemController, controllerName));

    TableReaderPtr tableReader = mTableFactory->CreateTableReader();
    if (!tableReader) {
        INDEXLIB_FATAL_ERROR(Runtime, "TableReader is not defined in TableFactory");
    }
    if (!mTableWriter) {
        IE_LOG(WARN, "TableWriter is null");
    }
    mFutureExecutor = CreateExecutor(mTableFactory, mSchema, mOptions);
    vector<BuildingSegmentReaderPtr> dumpingSegReaders = mPartitionData->GetDumpingSegmentReaders();
    BuildingSegmentReaderPtr buildingSegReader;
    if (mTableWriter) {
        buildingSegReader = mTableWriter->GetBuildingSegmentReader();
    }
    if (!tableReader->Init(mSchema, mOptions, mFutureExecutor.get(), mPartitionData->GetMetricProvider())) {
        INDEXLIB_FATAL_ERROR(Runtime, "TableReader Init failed");
    }

    vector<SegmentMetaPtr> segMetas;
    if (!CreateBuiltSegmentMetas(mPartitionData, segMetas)) {
        INDEXLIB_FATAL_ERROR(Runtime,
                             "Prepare built segment meta"
                             " failed for partition[%s]",
                             mPartitionName.c_str());
    }
    size_t estimateInitMemUse =
        tableReader->EstimateMemoryUse(segMetas, dumpingSegReaders, buildingSegReader, incVersion.GetTimestamp());
    if (!mReaderMemController->Reserve(estimateInitMemUse)) {
        INDEXLIB_FATAL_ERROR(OutOfMemory,
                             "Reserve memory[%zu Bytes] from "
                             "partition[%s] failed when open inc version[%d]",
                             estimateInitMemUse, mPartitionName.c_str(), incVersion.GetVersionId());
    }

    int64_t tsOnDiskVersion = incVersion.GetTimestamp();
    std::pair<int64_t, int64_t> incompletedTsRange(mForceSeekInfo.first, mForceSeekInfo.second);
    if (tsOnDiskVersion >= mForceSeekInfo.second) {
        incompletedTsRange.first = -1;
        incompletedTsRange.second = -1;
    } else if (tsOnDiskVersion > mForceSeekInfo.first) {
        incompletedTsRange.first = tsOnDiskVersion;
    }
    tableReader->SetForceSeekInfo(incompletedTsRange);
    if (!tableReader->Open(segMetas, dumpingSegReaders, buildingSegReader, incVersion.GetTimestamp())) {
        INDEXLIB_FATAL_ERROR(Runtime, "TableReader open failed");
    }
    for (const auto& segMeta : segMetas) {
        mSegmentLifecycles.insert({segMeta->segmentDataBase.GetSegmentId(), segMeta->segmentDataBase.GetLifecycle()});
    }
    if (buildingSegReader) {
        dumpingSegReaders.push_back(buildingSegReader);
    }

    const auto& buildingStateSegments = dumpingSegReaders;
    tableReader->SetSegmentDependency(buildingStateSegments);

    int64_t freeQuota = partitionMemController->GetFreeQuota();
    if (freeQuota < 0) {
        INDEXLIB_FATAL_ERROR(OutOfMemory,
                             "init reader out of memory for partition[%s] after load version[%d], "
                             "reader estimate memory is [%ld], current free quota is [%ld]",
                             mPartitionName.c_str(), incVersion.GetVersionId(), mReaderMemController->GetUsedQuota(),
                             freeQuota);
    }
    auto buildingSegId = (mTableWriter == nullptr) ? INVALID_SEGMENTID : mTableWriter->GetBuildingSegmentId();
    mTableReader = tableReader;
    InitDependentSegments(mPartitionData, segMetas, buildingStateSegments, buildingSegId);
}

void CustomOnlinePartitionReader::OpenWithHeritage(const index_base::PartitionDataPtr& partitionData,
                                                   const table::TableReaderPtr& lastTableReader,
                                                   const std::set<segmentid_t>& excludeSet)
{
    table::TableReaderPtr nullPreloadTableReader;
    DoOpenWithHeritage(partitionData, lastTableReader, nullPreloadTableReader, excludeSet);
}

void CustomOnlinePartitionReader::OpenWithHeritage(const index_base::PartitionDataPtr& partitionData,
                                                   const table::TableReaderPtr& lastTableReader,
                                                   const table::TableReaderPtr& preloadTableReader)
{
    DoOpenWithHeritage(partitionData, lastTableReader, preloadTableReader, {});
}

void CustomOnlinePartitionReader::DoOpenWithHeritage(const index_base::PartitionDataPtr& partitionData,
                                                     const table::TableReaderPtr& lastTableReader,
                                                     const table::TableReaderPtr& preloadTableReader,
                                                     const std::set<segmentid_t>& excludeSet)
{
    auto customPartData = DYNAMIC_POINTER_CAST(CustomPartitionData, partitionData);
    TableReaderPtr tableReader = mTableFactory->CreateTableReader();
    if (!tableReader) {
        INDEXLIB_FATAL_ERROR(Runtime, "TableReader is not defined in TableFactory");
    }
    mFutureExecutor = CreateExecutor(mTableFactory, mSchema, mOptions);
    if (!tableReader->Init(mSchema, mOptions, mFutureExecutor.get(), customPartData->GetMetricProvider())) {
        INDEXLIB_FATAL_ERROR(Runtime, "TableReader Init failed");
    }
    if (!tableReader->SupportSegmentLevelReader()) {
        AUTIL_LOG(WARN, "TableReader do not support SegmentLevelReader, use Open interface instead");
        Open(partitionData);
        return;
    }
    mPartitionData = DYNAMIC_POINTER_CAST(CustomPartitionData, partitionData);
    SwitchToLinkDirectoryForRtSegments(mPartitionData);
    PartitionMemoryQuotaControllerPtr partitionMemController = mPartitionData->GetPartitionMemoryQuotaController();
    index_base::Version incVersion = partitionData->GetOnDiskVersion();
    string controllerName = "readerMemory_version_" + StringUtil::toString(incVersion.GetVersionId());
    mReaderMemController.reset(new BlockMemoryQuotaController(partitionMemController, controllerName));

    if (!mTableWriter) {
        IE_LOG(WARN, "TableWriter is null");
    }
    vector<BuildingSegmentReaderPtr> dumpingSegReaders = mPartitionData->GetDumpingSegmentReaders();
    BuildingSegmentReaderPtr buildingSegReader;
    if (mTableWriter) {
        buildingSegReader = mTableWriter->GetBuildingSegmentReader();
    }

    vector<SegmentMetaPtr> segMetas;
    if (!CreateBuiltSegmentMetas(mPartitionData, segMetas)) {
        INDEXLIB_FATAL_ERROR(Runtime,
                             "Prepare built segment meta"
                             " failed for partition[%s]",
                             mPartitionName.c_str());
    }
    vector<BuiltSegmentReaderPtr> builtSegmentReaders;
    if (!CreateBuiltSegmentReaders(tableReader, lastTableReader, preloadTableReader, excludeSet, segMetas,
                                   builtSegmentReaders)) {
        INDEXLIB_FATAL_ERROR(Runtime,
                             "Prepare built segment reader"
                             " failed for partition[%s]",
                             mPartitionName.c_str());
    }

    size_t estimateInitMemUse =
        tableReader->EstimateMemoryUse(segMetas, dumpingSegReaders, buildingSegReader, incVersion.GetTimestamp());
    if (!mReaderMemController->Reserve(estimateInitMemUse)) {
        INDEXLIB_FATAL_ERROR(OutOfMemory,
                             "Reserve memory[%zu Bytes] from "
                             "partition[%s] failed when open inc version[%d]",
                             estimateInitMemUse, mPartitionName.c_str(), incVersion.GetVersionId());
    }

    int64_t tsOnDiskVersion = incVersion.GetTimestamp();
    std::pair<int64_t, int64_t> incompletedTsRange(mForceSeekInfo.first, mForceSeekInfo.second);
    if (tsOnDiskVersion >= mForceSeekInfo.second) {
        incompletedTsRange.first = -1;
        incompletedTsRange.second = -1;
    } else if (tsOnDiskVersion > mForceSeekInfo.first) {
        incompletedTsRange.first = tsOnDiskVersion;
    }
    tableReader->SetForceSeekInfo(incompletedTsRange);
    if (!tableReader->Open(builtSegmentReaders, dumpingSegReaders, buildingSegReader, incVersion.GetTimestamp())) {
        INDEXLIB_FATAL_ERROR(Runtime, "TableReader open failed");
    }

    for (const auto& builtSegReader : builtSegmentReaders) {
        const auto& segMeta = builtSegReader->GetSegmentMeta();
        mSegmentLifecycles.insert({segMeta->segmentDataBase.GetSegmentId(), segMeta->segmentDataBase.GetLifecycle()});
    }

    if (buildingSegReader) {
        dumpingSegReaders.push_back(buildingSegReader);
    }

    const auto& buildingStateSegments = dumpingSegReaders;
    tableReader->SetSegmentDependency(buildingStateSegments);
    int64_t freeQuota = partitionMemController->GetFreeQuota();
    if (freeQuota < 0) {
        INDEXLIB_FATAL_ERROR(OutOfMemory,
                             "init reader out of memory for partition[%s] after load version[%d], "
                             "reader estimate memory is [%ld], current free quota is [%ld]",
                             mPartitionName.c_str(), incVersion.GetVersionId(), mReaderMemController->GetUsedQuota(),
                             freeQuota);
    }
    mTableReader = tableReader;
    auto buildingSegId = (mTableWriter == nullptr) ? INVALID_SEGMENTID : mTableWriter->GetBuildingSegmentId();
    InitDependentSegments(mPartitionData, segMetas, buildingStateSegments, buildingSegId);
}

void CustomOnlinePartitionReader::InitDependentSegments(const index_base::PartitionDataPtr& partitionData,
                                                        const vector<SegmentMetaPtr>& segMetas,
                                                        const vector<BuildingSegmentReaderPtr>& buildingStateSegments,
                                                        segmentid_t buildingSegId)
{
    for (const auto& segMeta : segMetas) {
        auto segId = segMeta->segmentDataBase.GetSegmentId();
        if (RealtimeSegmentDirectory::IsRtSegmentId(segId)) {
            mRtSegmentIds.push_back(segId);
        }
    }
    for (const auto& buildingSegment : buildingStateSegments) {
        if (!buildingSegment) {
            continue;
        }
        const auto& singleSegIdSet = buildingSegment->GetDependentSegmentIds();
        mDependentSegIds.insert(singleSegIdSet.begin(), singleSegIdSet.end());
        mRtSegmentIds.push_back(buildingSegment->GetSegmentId());
    }
    InitPartitionVersion(partitionData, buildingSegId, mLatestValidRtLinkSegId);
}

void CustomOnlinePartitionReader::InitPartitionVersion(const index_base::PartitionDataPtr& partitionData,
                                                       segmentid_t buildingSegId, segmentid_t lastLinkSegId)
{
    assert(partitionData);
    mPartitionVersion = std::make_unique<PartitionVersion>(partitionData->GetVersion(), buildingSegId, lastLinkSegId);
    mPartitionVersionHashId = mPartitionVersion->GetHashId();
}

bool CustomOnlinePartitionReader::IsUsingSegment(segmentid_t segmentId) const
{
    auto it = mDependentSegIds.find(segmentId);
    if (it != mDependentSegIds.end()) {
        return true;
    }
    return IndexPartitionReader::IsUsingSegment(segmentId);
}

void CustomOnlinePartitionReader::SetForceSeekInfo(const std::pair<int64_t, int64_t>& forceSeekInfo)
{
    mForceSeekInfo = forceSeekInfo;
    if (mTableReader != NULL) {
        mTableReader->SetForceSeekInfo(forceSeekInfo);
    }
}

bool CustomOnlinePartitionReader::CreateBuiltSegmentMetas(const CustomPartitionDataPtr& partitionData,
                                                          vector<SegmentMetaPtr>& segMetas)
{
    segmentid_t lastReaderLinkRtSegId = mLatestValidRtLinkSegId;
    segmentid_t lastValidLinkRtSegId = partitionData->GetLastValidRtSegmentInLinkDirectory();
    SegmentDataVector segDataVec = partitionData->GetSegmentDatas();
    segMetas.reserve(segDataVec.size());
    for (const auto& segData : segDataVec) {
        SegmentMetaPtr segMeta(new SegmentMeta);
        segMeta->segmentDataBase = SegmentDataBase(segData);
        segMeta->segmentInfo = *segData.GetSegmentInfo();

        DirectoryPtr segDir = segData.GetDirectory();
        assert(segDir);

        DirectoryPtr dataDir = segDir->GetDirectory(CUSTOM_DATA_DIR_NAME, false);
        if (!dataDir) {
            IE_LOG(ERROR, "init TableResource[%s] failed: data dir [%s] not found in segment[%s]",
                   mSchema->GetSchemaName().c_str(), CUSTOM_DATA_DIR_NAME.c_str(), segDir->DebugString().c_str());
            return false;
        }
        segMeta->segmentDataDirectory = dataDir;
        segmentid_t curSegId = segMeta->segmentDataBase.GetSegmentId();
        if (RealtimeSegmentDirectory::IsRtSegmentId(curSegId)) {
            segMeta->type = SegmentMeta::SegmentSourceType::RT_BUILD;
            if (curSegId <= lastValidLinkRtSegId) {
                ++mOnDiskRtSegmentCount;
            }
            if (curSegId <= lastReaderLinkRtSegId) {
                ++mUsedOnDiskRtSegmentCount;
            } else {
                ++mInMemRtSegmentCount;
            }
        } else {
            segMeta->type = SegmentMeta::SegmentSourceType::INC_BUILD;
        }
        segMetas.push_back(segMeta);
    }
    return true;
}

bool CustomOnlinePartitionReader::CreateBuiltSegmentReaders(const table::TableReaderPtr& currentTableReader,
                                                            const table::TableReaderPtr& lastTableReader,
                                                            const table::TableReaderPtr& preloadTableReader,
                                                            const std::set<segmentid_t>& excludeSet,
                                                            const vector<SegmentMetaPtr>& segMetas,
                                                            vector<BuiltSegmentReaderPtr>& builtSegmentReaders)
{
    builtSegmentReaders.reserve(segMetas.size());
    for (const auto& segMeta : segMetas) {
        BuiltSegmentReaderPtr segReader;
        segmentid_t segId = segMeta->segmentDataBase.GetSegmentId();

        if (excludeSet.find(segId) != excludeSet.end()) {
            segReader = currentTableReader->CreateBuiltSegmentReader();
            if (!segReader->Init(segMeta)) {
                IE_LOG(ERROR, "failed to open segment[%d]", segId);
                return false;
            }
            builtSegmentReaders.emplace_back(segReader);
            continue;
        }

        if (preloadTableReader) {
            segReader = preloadTableReader->GetSegmentReader(segId);
            if (segReader) {
                IE_LOG(INFO, "segment reader[%d] inherit from preload table reader, lifecycle[%s]", segId,
                       segMeta->segmentDataBase.GetLifecycle().c_str());
                builtSegmentReaders.emplace_back(segReader);
                continue;
            }
        }
        if (lastTableReader) {
            segReader = lastTableReader->GetSegmentReader(segId);
            if (segReader) {
                IE_LOG(INFO, "segment reader[%d] inherit from last table reader, lifecycle[%s]", segId,
                       segMeta->segmentDataBase.GetLifecycle().c_str());
                builtSegmentReaders.emplace_back(segReader);
                continue;
            }
        }
        segReader = currentTableReader->CreateBuiltSegmentReader();
        if (!segReader->Init(segMeta)) {
            IE_LOG(ERROR, "failed to open segment[%d]", segId);
            return false;
        }
        builtSegmentReaders.emplace_back(segReader);
    }
    return true;
}

Version CustomOnlinePartitionReader::GetVersion() const { return mPartitionData->GetVersion(); }

PartitionInfoPtr CustomOnlinePartitionReader::GetPartitionInfo() const
{
    assert(false);
    return PartitionInfoPtr();
}

PartitionDataPtr CustomOnlinePartitionReader::GetPartitionData() const { return mPartitionData; }

const TableReaderPtr& CustomOnlinePartitionReader::GetTableReader() const { return mTableReader; }

index_base::Version CustomOnlinePartitionReader::GetOnDiskVersion() const
{
    assert(mPartitionData);
    return mPartitionData->GetOnDiskVersion();
}

const SummaryReaderPtr& CustomOnlinePartitionReader::GetSummaryReader() const
{
    assert(false);
    static SummaryReaderPtr emptyReader;
    return emptyReader;
}

const SourceReaderPtr& CustomOnlinePartitionReader::GetSourceReader() const
{
    assert(false);
    static SourceReaderPtr emptyReader;
    return emptyReader;
}

const AttributeReaderPtr& CustomOnlinePartitionReader::GetAttributeReader(const std::string& field) const
{
    assert(false);
    static AttributeReaderPtr emptyReader;
    return emptyReader;
}
const PackAttributeReaderPtr& CustomOnlinePartitionReader::GetPackAttributeReader(const std::string& packAttrName) const
{
    assert(false);
    static PackAttributeReaderPtr emptyReader;
    return emptyReader;
}
const PackAttributeReaderPtr& CustomOnlinePartitionReader::GetPackAttributeReader(packattrid_t packAttrId) const
{
    assert(false);
    static PackAttributeReaderPtr emptyReader;
    return emptyReader;
}

std::shared_ptr<InvertedIndexReader> CustomOnlinePartitionReader::GetInvertedIndexReader() const
{
    assert(false);
    return std::shared_ptr<InvertedIndexReader>();
}
const InvertedIndexReaderPtr& CustomOnlinePartitionReader::GetInvertedIndexReader(const std::string& indexName) const
{
    assert(false);
    static std::shared_ptr<InvertedIndexReader> emptyReader;
    return emptyReader;
}

const InvertedIndexReaderPtr& CustomOnlinePartitionReader::GetInvertedIndexReader(indexid_t indexId) const
{
    assert(false);
    static std::shared_ptr<InvertedIndexReader> emptyReader;
    return emptyReader;
}

const PrimaryKeyIndexReaderPtr& CustomOnlinePartitionReader::GetPrimaryKeyReader() const
{
    assert(false);
    static PrimaryKeyIndexReaderPtr emptyReader;
    return emptyReader;
}

const DeletionMapReaderPtr& CustomOnlinePartitionReader::GetDeletionMapReader() const
{
    assert(false);
    static DeletionMapReaderPtr emptyReader;
    return emptyReader;
}

IndexPartitionReaderPtr CustomOnlinePartitionReader::GetSubPartitionReader() const { return IndexPartitionReaderPtr(); }
bool CustomOnlinePartitionReader::GetSortedDocIdRanges(const DimensionDescriptionVector& dimensions,
                                                       const DocIdRange& rangeLimits,
                                                       DocIdRangeVector& resultRanges) const
{
    return false;
}

const IndexMetricsBase::AccessCounterMap& CustomOnlinePartitionReader::GetIndexAccessCounters() const
{
    static IndexMetricsBase::AccessCounterMap emptyIndexCounters;
    return emptyIndexCounters;
}

const IndexMetricsBase::AccessCounterMap& CustomOnlinePartitionReader::GetAttributeAccessCounters() const
{
    static IndexMetricsBase::AccessCounterMap emptyAttrCounters;
    return emptyAttrCounters;
}

const AttributeReaderContainerPtr& CustomOnlinePartitionReader::GetAttributeReaderContainer() const
{
    assert(false);
    return AttributeReaderContainer::RET_EMPTY_ATTR_READER_CONTAINER;
}

void CustomOnlinePartitionReader::SwitchToLinkDirectoryForRtSegments(const CustomPartitionDataPtr& partitionData)
{
    assert(partitionData);
    if (mLatestValidRtLinkSegId == INVALID_SEGMENTID) {
        return;
    }

    if (!partitionData->SwitchToLinkDirectoryForRtSegments(mLatestValidRtLinkSegId)) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "SwitchToLinkDirectoryForRtSegments fail!");
    }

    const auto& segDataVec = partitionData->GetSegmentDatas();
    for (const auto& segData : segDataVec) {
        segmentid_t segId = segData.GetSegmentId();
        if (RealtimeSegmentDirectory::IsRtSegmentId(segId) && segId <= mLatestValidRtLinkSegId) {
            mSwitchRtSegIds.push_back(segId);
        }
    }
}

segmentid_t CustomOnlinePartitionReader::GetNextValidSegIdToLink()
{
    segmentid_t nextValidLinkRtSegId = INVALID_SEGMENTID;
    const auto& segDataVec = mPartitionData->GetSegmentDatas();
    for (const auto& segData : segDataVec) {
        segmentid_t segId = segData.GetSegmentId();
        if (RealtimeSegmentDirectory::IsRtSegmentId(segId) && segId > mLatestValidRtLinkSegId &&
            (nextValidLinkRtSegId == INVALID_SEGMENTID || nextValidLinkRtSegId > segId)) {
            nextValidLinkRtSegId = segId;
        }
    }
    return nextValidLinkRtSegId == INVALID_SEGMENTID ? mLatestValidRtLinkSegId : nextValidLinkRtSegId;
}

std::shared_ptr<future_lite::Executor>
CustomOnlinePartitionReader::CreateExecutor(const table::TableFactoryWrapperPtr& factoryWrapper,
                                            const config::IndexPartitionSchemaPtr& schema,
                                            const config::IndexPartitionOptions& options) const
{
    ExecutorManager* executorManager = indexlib::util::Singleton<ExecutorManager>::GetInstance();
    ExecutorProviderPtr executorProvider = mTableFactory->CreateExecutorProvider();

    if (executorProvider != nullptr) {
        if (!executorProvider->Init(schema, options)) {
            return nullptr;
        }
        auto executor = executorManager->RegisterExecutor(executorProvider);
        return executor;
    }
    return nullptr;
}

}} // namespace indexlib::partition
