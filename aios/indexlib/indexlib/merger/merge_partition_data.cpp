#include "indexlib/merger/merge_partition_data.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/segment/offline_segment_directory.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/segment_directory_creator.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/file_system/local_directory.h"
#include "indexlib/file_system/in_mem_directory.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_options.h"

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, MergePartitionData);

MergePartitionData::MergePartitionData(const plugin::PluginManagerPtr& pluginManager)
    : mPluginManager(pluginManager)
{
}

MergePartitionData::~MergePartitionData() 
{
}

MergePartitionData::MergePartitionData(const MergePartitionData& other)
    : mPartitionMeta(other.mPartitionMeta)
    , mPluginManager(other.mPluginManager)
{
    mSegmentDirectory.reset(other.mSegmentDirectory->Clone());
    if (other.mSubPartitionData)
    {
        mSubPartitionData.reset(other.mSubPartitionData->Clone());
    }
}

void MergePartitionData::Open(const SegmentDirectoryPtr& segDir)
{
    mSegmentDirectory = segDir;
    InitPartitionMeta(segDir->GetRootDirectory());
    SegmentDirectoryPtr subSegDir = segDir->GetSubSegmentDirectory();
    if (subSegDir)
    {
        mSubPartitionData.reset(new MergePartitionData(mPluginManager));
        mSubPartitionData->Open(subSegDir);
    }
}

void MergePartitionData::InitPartitionMeta(const DirectoryPtr& directory)
{
    assert(directory);
    if (directory->IsExist(INDEX_PARTITION_META_FILE_NAME))
    {
        mPartitionMeta.Load(directory);
    }
}

index_base::Version MergePartitionData::GetVersion() const
{ 
    return mSegmentDirectory->GetVersion();
}

index_base::Version MergePartitionData::GetOnDiskVersion() const
{ 
    return mSegmentDirectory->GetOnDiskVersion();
}

const DirectoryPtr& MergePartitionData::GetRootDirectory() const
{
    return mSegmentDirectory->GetRootDirectory(); 
}

SegmentData MergePartitionData::GetSegmentData(segmentid_t segId) const
{
    return mSegmentDirectory->GetSegmentData(segId);
}

MergePartitionData* MergePartitionData::Clone()
{
    return new MergePartitionData(*this);
}

PartitionMeta MergePartitionData::GetPartitionMeta() const
{
    if (mSegmentDirectory->IsSubSegmentDirectory())
    {
        return PartitionMeta();
    }
    return mPartitionMeta;
}

const IndexFormatVersion& MergePartitionData::GetIndexFormatVersion() const
{
    assert(mSegmentDirectory);
    return mSegmentDirectory->GetIndexFormatVersion();
}

uint32_t MergePartitionData::GetIndexShardingColumnNum(
        const IndexPartitionOptions& options) const
{
    // TODO: online build support sharding column
    if (options.IsOnline())
    {
        return 1;
    }
    
    uint32_t inSegColumnNum = SegmentInfo::INVALID_COLUMN_COUNT;
    for (Iterator iter = Begin(); iter != End(); iter++)
    {
        const SegmentInfo& segInfo = (*iter).GetSegmentInfo();
        if (inSegColumnNum != SegmentInfo::INVALID_COLUMN_COUNT &&
            inSegColumnNum != segInfo.shardingColumnCount)
        {
            INDEXLIB_FATAL_ERROR(InconsistentState,
                    "segments with different shardingColumnCount for version[%d]!",
                    GetOnDiskVersion().GetVersionId());
        }
        inSegColumnNum = segInfo.shardingColumnCount;
    }

    uint32_t inVersionColumnNum = GetOnDiskVersion().GetLevelInfo().GetColumnCount();
    if (inSegColumnNum != SegmentInfo::INVALID_COLUMN_COUNT &&
        inSegColumnNum != inVersionColumnNum)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "inSegColumnNum [%u] not equal with inVersionColumnNum [%u]!",
                             inSegColumnNum, inVersionColumnNum);
    }
    return inVersionColumnNum;
}


string MergePartitionData::GetLastLocator() const
{
    assert(mSegmentDirectory);
    return mSegmentDirectory->GetLastLocator();
}

PartitionSegmentIteratorPtr MergePartitionData::CreateSegmentIterator()
{
    bool isOnline = true;
    if (DYNAMIC_POINTER_CAST(OfflineSegmentDirectory, mSegmentDirectory))
    {
        isOnline = false;
    }
    PartitionSegmentIteratorPtr segIter(new PartitionSegmentIterator(isOnline));
    segIter->Init(mSegmentDirectory->GetSegmentDatas(),
                  std::vector<InMemorySegmentPtr>(), InMemorySegmentPtr());
    return segIter;
}

segmentid_t MergePartitionData::GetLastValidRtSegmentInLinkDirectory() const
{
    INDEXLIB_FATAL_ERROR(UnSupported,
                         "not support call GetLastValidRtSegmentInLinkDirectory!");
    return INVALID_SEGMENTID;
}

bool MergePartitionData::SwitchToLinkDirectoryForRtSegments(
        segmentid_t lastLinkRtSegId)
{
    INDEXLIB_FATAL_ERROR(UnSupported,
                         "not support call SwitchToLinkDirectoryForRtSegments!");
    return false;
}

DeletionMapReaderPtr MergePartitionData::GetDeletionMapReader() const
{
    INDEXLIB_FATAL_ERROR(UnSupported,
                         "not support get deletionmap reader!");
    return DeletionMapReaderPtr();
}

PartitionInfoPtr MergePartitionData::GetPartitionInfo() const
{
    INDEXLIB_FATAL_ERROR(UnSupported,
                         "not support call GetPartitionInfo!");
    return PartitionInfoPtr();
}

IE_NAMESPACE_END(merger);

