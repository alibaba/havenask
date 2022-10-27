#include <autil/StringUtil.h>
#include "indexlib/partition/custom_online_partition_reader.h"
#include "indexlib/partition/custom_partition_data.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_data_base.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/dimension_description.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/table/table_resource.h"
#include "indexlib/table/table_writer.h"
#include "indexlib/table/table_reader.h"
#include "indexlib/table/table_factory_wrapper.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_container.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_reader.h"
#include "indexlib/index/normal/summary/summary_reader.h"
#include "indexlib/index/normal/index_metrics_base.h"
#include "indexlib/file_system/directory.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(table);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, CustomOnlinePartitionReader);

CustomOnlinePartitionReader::CustomOnlinePartitionReader(
    const IndexPartitionSchemaPtr& schema,
    const IndexPartitionOptions& options,
    const string& partitionName,
    const TableFactoryWrapperPtr& tableFactory,
    const TableWriterPtr& tableWriter)
    : IndexPartitionReader(schema)
    , mOptions(options)
    , mPartitionName(partitionName)
    , mTableFactory(tableFactory)
    , mTableWriter(tableWriter)
{
}

CustomOnlinePartitionReader::~CustomOnlinePartitionReader() 
{
    mTableReader.reset();
    mTableWriter.reset();
    mPartitionData.reset();
}

void CustomOnlinePartitionReader::Open(const PartitionDataPtr& partitionData)
{
    mPartitionData = DYNAMIC_POINTER_CAST(CustomPartitionData, partitionData);
    PartitionMemoryQuotaControllerPtr partitionMemController = mPartitionData->GetPartitionMemoryQuotaController();
    index_base::Version incVersion = partitionData->GetOnDiskVersion();    
    string controllerName = "readerMemory_version_" + StringUtil::toString(incVersion.GetVersionId());
    mReaderMemController.reset(new BlockMemoryQuotaController(partitionMemController, controllerName));
    
    TableReaderPtr tableReader = mTableFactory->CreateTableReader();
    if (!tableReader)
    {
        INDEXLIB_FATAL_ERROR(Runtime, "TableReader is not defined in TableFactory");
    }
    if (!mTableWriter)
    {
        IE_LOG(WARN, "TableWriter is null"); 
    }
    vector<BuildingSegmentReaderPtr> dumpingSegReaders = mPartitionData->GetDumpingSegmentReaders();
    BuildingSegmentReaderPtr buildingSegReader;
    if (mTableWriter)
    {
        buildingSegReader = mTableWriter->GetBuildingSegmentReader();
    }
    if (!tableReader->Init(mSchema, mOptions))
    {
        INDEXLIB_FATAL_ERROR(Runtime, "TableReader Init failed"); 
    }

    vector<SegmentMetaPtr> segMetas;
    if (!CreateBuiltSegmentMetas(mPartitionData, segMetas))
    {
        INDEXLIB_FATAL_ERROR(Runtime, "Prepare built segment meta"
                             " failed for partition[%s]",
                             mPartitionName.c_str());
    }
    size_t estimateInitMemUse = tableReader->EstimateMemoryUse(
        segMetas, dumpingSegReaders, buildingSegReader, incVersion.GetTimestamp());
    if (!mReaderMemController->Reserve(estimateInitMemUse))
    {
        INDEXLIB_FATAL_ERROR(OutOfMemory, "Reserve memory[%zu Bytes] from "
                             "partition[%s] failed when open inc version[%d]",
                             estimateInitMemUse, mPartitionName.c_str(),
                             incVersion.GetVersionId());
    }
    
    int64_t tsOnDiskVersion = incVersion.GetTimestamp();
    std::pair<int64_t, int64_t> incompletedTsRange(mForceSeekInfo.first, mForceSeekInfo.second);
    if (tsOnDiskVersion >= mForceSeekInfo.second)
    {
        incompletedTsRange.first = -1;
        incompletedTsRange.second = -1;
    }
    else if (tsOnDiskVersion > mForceSeekInfo.first)
    {
        incompletedTsRange.first = tsOnDiskVersion;
    }
    tableReader->SetForceSeekInfo(incompletedTsRange);
    if (!tableReader->Open(segMetas, dumpingSegReaders, buildingSegReader, incVersion.GetTimestamp()))
    {
        INDEXLIB_FATAL_ERROR(Runtime, "TableReader open failed");        
    }
    if (buildingSegReader)
    {
        dumpingSegReaders.push_back(buildingSegReader);
    }
    tableReader->SetSegmentDependency(dumpingSegReaders);
    
    int64_t freeQuota = partitionMemController->GetFreeQuota();
    if (freeQuota < 0)
    {
        INDEXLIB_FATAL_ERROR(
            OutOfMemory, "init reader out of memory for partition[%s] after load version[%d], "
            "reader estimate memory is [%ld], current free quota is [%ld]",
            mPartitionName.c_str(), incVersion.GetVersionId(),
            mReaderMemController->GetUsedQuota(), freeQuota);
    }
    mTableReader = tableReader;
    InitDependentSegments(mPartitionData, dumpingSegReaders, mTableWriter);
}

void CustomOnlinePartitionReader::OpenWithHeritage(
    const index_base::PartitionDataPtr& partitionData,
    const table::TableReaderPtr& lastTableReader,
    const table::TableReaderPtr& preloadTableReader)
{
    TableReaderPtr tableReader = mTableFactory->CreateTableReader();
    if (!tableReader)
    {
        INDEXLIB_FATAL_ERROR(Runtime, "TableReader is not defined in TableFactory");
    }
    if (!tableReader->SupportSegmentLevelReader())
    {
        AUTIL_LOG(WARN, "TableReader do not support SegmentLevelReader, use Open interface instead");
        Open(partitionData);
        return;
    }
    mPartitionData = DYNAMIC_POINTER_CAST(CustomPartitionData, partitionData);
    PartitionMemoryQuotaControllerPtr partitionMemController = mPartitionData->GetPartitionMemoryQuotaController();
    index_base::Version incVersion = partitionData->GetOnDiskVersion();    
    string controllerName = "readerMemory_version_" + StringUtil::toString(incVersion.GetVersionId());
    mReaderMemController.reset(new BlockMemoryQuotaController(partitionMemController, controllerName));
    
    if (!mTableWriter)
    {
        IE_LOG(WARN, "TableWriter is null"); 
    }
    vector<BuildingSegmentReaderPtr> dumpingSegReaders = mPartitionData->GetDumpingSegmentReaders();
    BuildingSegmentReaderPtr buildingSegReader;
    if (mTableWriter)
    {
        buildingSegReader = mTableWriter->GetBuildingSegmentReader();
    }
    if (!tableReader->Init(mSchema, mOptions))
    {
        INDEXLIB_FATAL_ERROR(Runtime, "TableReader Init failed"); 
    }

    vector<SegmentMetaPtr> segMetas;
    if (!CreateBuiltSegmentMetas(mPartitionData, segMetas))
    {
        INDEXLIB_FATAL_ERROR(Runtime, "Prepare built segment meta"
                             " failed for partition[%s]",
                             mPartitionName.c_str());
    }
    vector<BuiltSegmentReaderPtr> builtSegmentReaders;
    if (!CreateBuiltSegmentReaders(tableReader, lastTableReader, preloadTableReader, segMetas, builtSegmentReaders))
    {
        INDEXLIB_FATAL_ERROR(Runtime, "Prepare built segment meta"
                             " failed for partition[%s]", mPartitionName.c_str());        
    }

    size_t estimateInitMemUse = tableReader->EstimateMemoryUse(
        segMetas, dumpingSegReaders, buildingSegReader, incVersion.GetTimestamp());
    if (!mReaderMemController->Reserve(estimateInitMemUse))
    {
        INDEXLIB_FATAL_ERROR(OutOfMemory, "Reserve memory[%zu Bytes] from "
                             "partition[%s] failed when open inc version[%d]",
                             estimateInitMemUse, mPartitionName.c_str(),
                             incVersion.GetVersionId());
    }
    
    int64_t tsOnDiskVersion = incVersion.GetTimestamp();
    std::pair<int64_t, int64_t> incompletedTsRange(mForceSeekInfo.first, mForceSeekInfo.second);
    if (tsOnDiskVersion >= mForceSeekInfo.second)
    {
        incompletedTsRange.first = -1;
        incompletedTsRange.second = -1;
    }
    else if (tsOnDiskVersion > mForceSeekInfo.first)
    {
        incompletedTsRange.first = tsOnDiskVersion;
    }
    tableReader->SetForceSeekInfo(incompletedTsRange);
    if (!tableReader->Open(builtSegmentReaders, dumpingSegReaders, buildingSegReader, incVersion.GetTimestamp()))
    {
        INDEXLIB_FATAL_ERROR(Runtime, "TableReader open failed");        
    }

    if (buildingSegReader)
    {
        dumpingSegReaders.push_back(buildingSegReader);        
    }
    tableReader->SetSegmentDependency(dumpingSegReaders);
    int64_t freeQuota = partitionMemController->GetFreeQuota();
    if (freeQuota < 0)
    {
        INDEXLIB_FATAL_ERROR(
            OutOfMemory, "init reader out of memory for partition[%s] after load version[%d], "
            "reader estimate memory is [%ld], current free quota is [%ld]",
            mPartitionName.c_str(), incVersion.GetVersionId(),
            mReaderMemController->GetUsedQuota(), freeQuota);
    }
    mTableReader = tableReader;
    InitDependentSegments(mPartitionData, dumpingSegReaders, mTableWriter);
}

void CustomOnlinePartitionReader::InitDependentSegments(
    const index_base::PartitionDataPtr& partitionData,
    const vector<BuildingSegmentReaderPtr>& dumpingSegReaders,
    const TableWriterPtr& tableWriter)
{
    segmentid_t buildingSegId = INVALID_SEGMENTID;
    if (tableWriter)
    {
        buildingSegId = tableWriter->GetBuildingSegmentId();
        auto buildingSegReader = tableWriter->GetBuildingSegmentReader();
        if (buildingSegReader)
        {
            auto singleDepSegIds = buildingSegReader->GetDependentSegmentIds();
            mDependentSegIds.insert(singleDepSegIds.begin(), singleDepSegIds.end());
        }
    }
    for (const auto& dumpingSegReader : dumpingSegReaders)
    {
        if (!dumpingSegReader)
        {
            continue;
        }
        const auto& singleSegIdSet = dumpingSegReader->GetDependentSegmentIds();
        mDependentSegIds.insert(singleSegIdSet.begin(), singleSegIdSet.end());
    }
    InitPartitionVersion(partitionData, buildingSegId);
}


void CustomOnlinePartitionReader::InitPartitionVersion(
    const index_base::PartitionDataPtr& partitionData,
    segmentid_t buildingSegId)
{
    assert(partitionData);
    mPartitionVersion = PartitionVersion(
        partitionData->GetVersion(), buildingSegId, INVALID_SEGMENTID);
    mPartitionVersionHashId = mPartitionVersion.GetHashId();
}

bool CustomOnlinePartitionReader::IsUsingSegment(segmentid_t segmentId) const
{
    auto it = mDependentSegIds.find(segmentId);
    if (it != mDependentSegIds.end())
    {
        return true;
    }
    return IndexPartitionReader::IsUsingSegment(segmentId);
}

void CustomOnlinePartitionReader::SetForceSeekInfo(const std::pair<int64_t, int64_t>& forceSeekInfo)
{
    mForceSeekInfo = forceSeekInfo;
    if (mTableReader != NULL)
    {
        mTableReader->SetForceSeekInfo(forceSeekInfo);
    }
}

bool CustomOnlinePartitionReader::CreateBuiltSegmentMetas(
    const CustomPartitionDataPtr& partitionData, vector<SegmentMetaPtr>& segMetas)
{
    SegmentDataVector segDataVec = partitionData->GetSegmentDatas();
    segMetas.reserve(segDataVec.size());
    for (const auto& segData : segDataVec)
    {
        SegmentMetaPtr segMeta(new SegmentMeta);
        segMeta->segmentDataBase = SegmentDataBase(segData);
        segMeta->segmentInfo = segData.GetSegmentInfo();

        DirectoryPtr segDir = segData.GetDirectory();
        assert(segDir);

        DirectoryPtr dataDir = segDir->GetDirectory(CUSTOM_DATA_DIR_NAME, false);
        if (!dataDir)
        {
            IE_LOG(ERROR, "init TableResource[%s] failed: data dir [%s] not found in segment[%s]",
                   mSchema->GetSchemaName().c_str(),
                   CUSTOM_DATA_DIR_NAME.c_str(), segDir->GetPath().c_str());
            return false;
        }
        segMeta->segmentDataDirectory = dataDir;
        if (RealtimeSegmentDirectory::IsRtSegmentId(segMeta->segmentDataBase.GetSegmentId()))
        {
            segMeta->type = SegmentMeta::SegmentSourceType::INC_BUILD;
        }
        else
        {
            segMeta->type = SegmentMeta::SegmentSourceType::RT_BUILD;
        }
        segMetas.push_back(segMeta);
    }
    return true;
}

bool CustomOnlinePartitionReader::CreateBuiltSegmentReaders(
    const table::TableReaderPtr& currentTableReader,
    const table::TableReaderPtr& lastTableReader,
    const table::TableReaderPtr& preloadTableReader,
    const vector<SegmentMetaPtr>& segMetas,
    vector<BuiltSegmentReaderPtr>& builtSegmentReaders)
{
    builtSegmentReaders.reserve(segMetas.size());
    for (const auto& segMeta: segMetas)
    {
        BuiltSegmentReaderPtr segReader;
        segmentid_t segId = segMeta->segmentDataBase.GetSegmentId();

        if (lastTableReader) {
            segReader = lastTableReader->GetSegmentReader(segId);
            if (segReader)
            {
                builtSegmentReaders.emplace_back(segReader);
                continue;
            }
        }
        if (preloadTableReader)
        {
            segReader = lastTableReader->GetSegmentReader(segId);
            if (segReader)
            {
                builtSegmentReaders.emplace_back(segReader);
                continue;
            }
        }

        segReader = currentTableReader->CreateBuiltSegmentReader();
        if (!segReader->Open(segMeta))
        {
            IE_LOG(ERROR, "failed to open segment[%d]", segId);
            return false;
        }
        builtSegmentReaders.emplace_back(segReader);
    }
    return true;
}

Version CustomOnlinePartitionReader::GetVersion() const
{
    return mPartitionData->GetVersion();
}

PartitionInfoPtr CustomOnlinePartitionReader::GetPartitionInfo() const
{
    assert(false);
    return PartitionInfoPtr();
}

const TableReaderPtr& CustomOnlinePartitionReader::GetTableReader() const
{
    return mTableReader;
}

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

const AttributeReaderPtr& CustomOnlinePartitionReader::GetAttributeReader(
        const std::string& field) const
{
    assert(false);
    static AttributeReaderPtr emptyReader;
    return emptyReader;
}
const PackAttributeReaderPtr& CustomOnlinePartitionReader::GetPackAttributeReader(
        const std::string& packAttrName) const 
{
    assert(false);
    static PackAttributeReaderPtr emptyReader;
    return emptyReader;
}
const PackAttributeReaderPtr& CustomOnlinePartitionReader::GetPackAttributeReader(
        packattrid_t packAttrId) const 
{
    assert(false);
    static PackAttributeReaderPtr emptyReader;
    return emptyReader;
}

IndexReaderPtr CustomOnlinePartitionReader::GetIndexReader() const 
{
    assert(false);
    return IndexReaderPtr();
}
const IndexReaderPtr& CustomOnlinePartitionReader::GetIndexReader(
        const std::string& indexName) const 
{
    assert(false);
    static IndexReaderPtr emptyReader;
    return emptyReader;
}

const IndexReaderPtr& CustomOnlinePartitionReader::GetIndexReader(indexid_t indexId) const 
{
    assert(false);
    static IndexReaderPtr emptyReader;
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

IndexPartitionReaderPtr CustomOnlinePartitionReader::GetSubPartitionReader() const
{
    return IndexPartitionReaderPtr();
}

bool CustomOnlinePartitionReader::GetSortedDocIdRanges(
            const DimensionDescriptionVector& dimensions,
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

IE_NAMESPACE_END(partition);

