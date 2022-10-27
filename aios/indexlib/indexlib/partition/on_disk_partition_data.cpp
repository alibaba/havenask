#include "indexlib/partition/on_disk_partition_data.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/segment/offline_segment_directory.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/segment_directory_creator.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/file_system/local_directory.h"
#include "indexlib/file_system/in_mem_directory.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/patch/patch_file_finder_creator.h"
#include "indexlib/partition/partition_info_holder.h"

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);

IE_LOG_SETUP(partition, OnDiskPartitionData);

OnDiskPartitionData::OnDiskPartitionData(const plugin::PluginManagerPtr& pluginManager)
    : mPluginManager(pluginManager)
{
}

OnDiskPartitionData::~OnDiskPartitionData() 
{
}

OnDiskPartitionData::OnDiskPartitionData(const OnDiskPartitionData& other)
    : mPartitionMeta(other.mPartitionMeta)
    , mDeletionMapReader(other.mDeletionMapReader)
    , mPartitionInfoHolder(other.mPartitionInfoHolder)
    , mPluginManager(other.mPluginManager)
{
    mSegmentDirectory.reset(other.mSegmentDirectory->Clone());
    if (other.mSubPartitionData)
    {
        mSubPartitionData.reset(other.mSubPartitionData->Clone());
    }
}

void OnDiskPartitionData::Open(const SegmentDirectoryPtr& segDir,
                               bool needDeletionMap)
{
    mSegmentDirectory = segDir;
    InitPartitionMeta(segDir->GetRootDirectory());
    if (needDeletionMap)
    {
        mDeletionMapReader = CreateDeletionMapReader();
        mPartitionInfoHolder = CreatePartitionInfoHolder();
    }

    SegmentDirectoryPtr subSegDir = segDir->GetSubSegmentDirectory();
    if (subSegDir)
    {
        mSubPartitionData.reset(new OnDiskPartitionData(mPluginManager));
        mSubPartitionData->Open(subSegDir, needDeletionMap);
        if (mPartitionInfoHolder)
        {
            mPartitionInfoHolder->SetSubPartitionInfoHolder(
                    mSubPartitionData->GetPartitionInfoHolder());
        }
    }
}

void OnDiskPartitionData::InitPartitionMeta(const DirectoryPtr& directory)
{
    assert(directory);
    if (directory->IsExist(INDEX_PARTITION_META_FILE_NAME))
    {
        mPartitionMeta.Load(directory);
    }
}

index_base::Version OnDiskPartitionData::GetVersion() const
{ 
    return mSegmentDirectory->GetVersion();
}

index_base::Version OnDiskPartitionData::GetOnDiskVersion() const
{ 
    return mSegmentDirectory->GetOnDiskVersion();
}

const DirectoryPtr& OnDiskPartitionData::GetRootDirectory() const
{
    return mSegmentDirectory->GetRootDirectory(); 
}

SegmentData OnDiskPartitionData::GetSegmentData(segmentid_t segId) const
{
    return mSegmentDirectory->GetSegmentData(segId);
}

OnDiskPartitionData* OnDiskPartitionData::Clone()
{
    return new OnDiskPartitionData(*this);
}

DeletionMapReaderPtr OnDiskPartitionData::CreateDeletionMapReader() 
{
    DeletionMapReaderPtr deletionMapReader(new DeletionMapReader);
    deletionMapReader->Open(this);
    return deletionMapReader;
}

PartitionInfoHolderPtr OnDiskPartitionData::CreatePartitionInfoHolder() const
{
    PartitionInfoHolderPtr partitionInfoHolder(new PartitionInfoHolder);
    partitionInfoHolder->Init(mSegmentDirectory->GetVersion(),
                              mPartitionMeta,
                              mSegmentDirectory->GetSegmentDatas(),
                              std::vector<InMemorySegmentPtr>(),
                              mDeletionMapReader);
    return partitionInfoHolder;
}

PartitionInfoPtr OnDiskPartitionData::GetPartitionInfo() const
{
    if (mPartitionInfoHolder)
    {
        return mPartitionInfoHolder->GetPartitionInfo();
    }
    return PartitionInfoPtr();
}

PartitionMeta OnDiskPartitionData::GetPartitionMeta() const
{
    if (mSegmentDirectory->IsSubSegmentDirectory())
    {
        return PartitionMeta();
    }
    return mPartitionMeta;
}

const IndexFormatVersion& OnDiskPartitionData::GetIndexFormatVersion() const
{
    assert(mSegmentDirectory);
    return mSegmentDirectory->GetIndexFormatVersion();
}

uint32_t OnDiskPartitionData::GetIndexShardingColumnNum(
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

segmentid_t OnDiskPartitionData::GetLastValidRtSegmentInLinkDirectory() const
{
    assert(mSegmentDirectory);
    return mSegmentDirectory->GetLastValidRtSegmentInLinkDirectory();
}

bool OnDiskPartitionData::SwitchToLinkDirectoryForRtSegments(
        segmentid_t lastLinkRtSegId)
{
    assert(mSegmentDirectory);
    return mSegmentDirectory->SwitchToLinkDirectoryForRtSegments(lastLinkRtSegId);
}

string OnDiskPartitionData::GetLastLocator() const
{
    assert(mSegmentDirectory);
    return mSegmentDirectory->GetLastLocator();
}

PartitionSegmentIteratorPtr OnDiskPartitionData::CreateSegmentIterator()
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

SegmentDataVector::const_iterator OnDiskPartitionData::Begin() const
{
    return mSegmentDirectory->GetSegmentDatas().begin();
}

SegmentDataVector::const_iterator OnDiskPartitionData::End() const
{
    return mSegmentDirectory->GetSegmentDatas().end();
}


IE_NAMESPACE_END(partition);

