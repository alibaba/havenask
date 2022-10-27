#include "indexlib/partition/partition_resource_calculator.h"
#include "indexlib/index/calculator/partition_size_calculator.h"
#include "indexlib/index/normal/attribute/accessor/pack_attr_update_doc_size_calculator.h"
#include "indexlib/index/online_join_policy.h"
#include "indexlib/partition/online_partition_metrics.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/operation_queue/operation_iterator.h"
#include "indexlib/partition/patch_loader.h"
#include "indexlib/index/calculator/on_disk_segment_size_calculator.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/common/index_locator.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/util/counter/multi_counter.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, PartitionResourceCalculator);

void PartitionResourceCalculator::Init(const DirectoryPtr& rootDirectory,
                                       const PartitionWriterPtr& writer,
                                       const InMemorySegmentContainerPtr& container,
                                       const PluginManagerPtr& pluginManager)
{
    autil::ScopedLock lock(mLock);
    mRootDirectory = rootDirectory;
    mWriter = writer;
    mInMemSegContainer = container;
    mPluginManager = pluginManager;
}

void PartitionResourceCalculator::CalculateCurrentIndexSize(
        const PartitionDataPtr& partitionData,
        const IndexPartitionSchemaPtr& schema) const
{
    if (!partitionData)
    {
        return;
    }
    size_t indexSize = 0;
    OnDiskSegmentSizeCalculator calculator;
    index_base::SegmentDirectoryPtr segmentDirector(
            partitionData->GetSegmentDirectory()->Clone());
    for (auto& segmentData : segmentDirector->GetSegmentDatas())
    {
        indexSize += calculator.GetSegmentSize(segmentData, schema);
    }
    
    mCurrentIndexSize = indexSize;
}

size_t PartitionResourceCalculator::GetCurrentMemoryUse() const
{
    return GetRtIndexMemoryUse() + GetIncIndexMemoryUse() +
        GetWriterMemoryUse() + GetOldInMemorySegmentMemoryUse();
}

size_t PartitionResourceCalculator::GetRtIndexMemoryUse() const
{
    assert(mRootDirectory);
    const IndexlibFileSystemPtr& fileSystem = mRootDirectory->GetFileSystem();

    assert(fileSystem);
    IndexlibFileSystemMetrics fsMetrics = fileSystem->GetFileSystemMetrics();
    const StorageMetrics& inMemMetrics = fsMetrics.GetInMemStorageMetrics();
    return inMemMetrics.GetInMemFileLength() + inMemMetrics.GetMmapLockFileLength() +
        inMemMetrics.GetFlushMemoryUse() + mSwitchRtSegmentLockSize;
}

size_t PartitionResourceCalculator::GetBuildingSegmentDumpExpandSize() const
{
    ScopedLock lock(mLock);
    return mWriter ? mWriter->GetBuildingSegmentDumpExpandSize() : 0;
}

size_t PartitionResourceCalculator::GetWriterMemoryUse() const
{
    ScopedLock lock(mLock);
    return mWriter ? mWriter->GetTotalMemoryUse() : 0;
}

size_t PartitionResourceCalculator::GetIncIndexMemoryUse() const
{
    assert(mRootDirectory);
    const IndexlibFileSystemPtr& fileSystem = mRootDirectory->GetFileSystem();

    assert(fileSystem);
    IndexlibFileSystemMetrics fsMetrics = fileSystem->GetFileSystemMetrics();
    const StorageMetrics& localMetrics = fsMetrics.GetLocalStorageMetrics();

    // TODO: actual used cache size
    size_t totalIncSize = localMetrics.GetMmapLockFileLength()
                          + localMetrics.GetFileLength(FSFT_SLICE)
                          + localMetrics.GetFileLength(FSFT_IN_MEM)
                          + mOnlineConfig.loadConfigList.GetTotalCacheSize();
    if (totalIncSize >= mSwitchRtSegmentLockSize)
    {
        return totalIncSize - mSwitchRtSegmentLockSize;
    }
    IE_LOG(ERROR, "mSwitchRtSegmentLockSize [%lu] is bigger than totalIncSize [%lu], "
           "rootPath[%s]", mSwitchRtSegmentLockSize,
           totalIncSize, mRootDirectory->GetPath().c_str());
    return totalIncSize;
}
size_t PartitionResourceCalculator::GetOldInMemorySegmentMemoryUse() const
{
    if (!mInMemSegContainer)
    {
        return 0u;
    }
    return mInMemSegContainer->GetTotalMemoryUse();
}

size_t PartitionResourceCalculator::EstimateDiffVersionLockSizeWithoutPatch(
        const config::IndexPartitionSchemaPtr& schema,
        const file_system::DirectoryPtr& directory,
        const index_base::Version& version,
        const index_base::Version& lastLoadedVersion,
        const util::MultiCounterPtr& counter,
        bool throwExceptionIfNotExist) const
{
    PartitionSizeCalculator calculator(
            directory, schema, throwExceptionIfNotExist, mPluginManager);
    PartitionDataPtr partitionData;
    if (schema->GetTableType() == tt_index)
    {
        partitionData = 
            PartitionDataCreator::CreateOnDiskPartitionData(
                directory->GetFileSystem(), version, directory->GetPath(), 
                schema->GetSubIndexPartitionSchema() != NULL, false);
    }

    size_t size = calculator.CalculateDiffVersionLockSizeWithoutPatch(
        version, lastLoadedVersion, partitionData, counter);
    assert(!counter || size == counter->Sum());
    return size;
}

size_t PartitionResourceCalculator::EstimateRedoOperationExpandSize (
        const IndexPartitionSchemaPtr& schema,
        PartitionDataPtr& partitionData,
        int64_t timestamp) const
{
    size_t redoOperationExpandSizeInBytes = 0;
    PartitionData::Iterator iter = partitionData->Begin();
    for(; iter != partitionData->End(); iter++)
    {
        SegmentData segData = *iter;
        DirectoryPtr opDir = segData.GetOperationDirectory(false);
        if (!opDir)
        {
            continue;
        }

        if (opDir->IsExist(OPERATION_DATA_FILE_NAME))
        {
            redoOperationExpandSizeInBytes += opDir->GetFileLength(OPERATION_DATA_FILE_NAME);
        }
    }

    if (mOnlineConfig.onDiskFlushRealtimeIndex)
    {
        // TODO: make sure only one segment operation load to in memory file
        redoOperationExpandSizeInBytes +=
            OperationIterator::GetMaxUnObsoleteSegmentOperationSize(
                    partitionData, timestamp);
    }
    PackAttrUpdateDocSizeCalculator packUpdateCalculator(
            partitionData, schema);
    redoOperationExpandSizeInBytes +=
        packUpdateCalculator.EstimateUpdateDocSizeInUnobseleteRtSeg(timestamp);
    return redoOperationExpandSizeInBytes;
}

void PartitionResourceCalculator::UpdateSwitchRtSegments(
        const config::IndexPartitionSchemaPtr& schema,
        const vector<segmentid_t>& segIds)
{
    assert(mRootDirectory);
    if (!mRootDirectory->GetFileSystem()->UseRootLink())
    {
        return;
    }
    
    ScopedLock lock(mLock);
    if (mSwitchRtSegIds == segIds)
    {
        return;
    }

    string linkName = mRootDirectory->GetRootLinkName();
    DirectoryPtr linkDir = mRootDirectory->GetDirectory(linkName, false);
    if (!linkDir)
    {
        IE_LOG(ERROR, "%s not exist in [%s]!",
               linkName.c_str(), mRootDirectory->GetPath().c_str());
        return;
    }

    DirectoryPtr rtPartitionDir = linkDir->GetDirectory(
            RT_INDEX_PARTITION_DIR_NAME, false);
    if (!rtPartitionDir)
    {
        IE_LOG(ERROR, "%s not exist in [%s]!",
               RT_INDEX_PARTITION_DIR_NAME, linkDir->GetPath().c_str());
        return;
    }

    string segStr;
    Version version;
    for (size_t i = 0; i < segIds.size(); i++)
    {
        version.AddSegment(segIds[i]);
        segStr += StringUtil::toString(segIds[i]) + ";";
    }

    mSwitchRtSegmentLockSize = EstimateDiffVersionLockSizeWithoutPatch(
        schema, rtPartitionDir, version, INVALID_VERSION, MultiCounterPtr(), false);
    IE_LOG(INFO, "switched rt segment lock size [%lu] for segments[%s]",
           mSwitchRtSegmentLockSize, segStr.c_str());
    mSwitchRtSegIds.assign(segIds.begin(), segIds.end());
}

size_t PartitionResourceCalculator::EstimateLoadPatchMemoryUse(
    const IndexPartitionSchemaPtr& schema, const DirectoryPtr& directory,
    const Version& version, const Version& lastLoadedVersion) const
{
    PartitionDataPtr partitionData = PartitionDataCreator::CreateOnDiskPartitionData(
        directory->GetFileSystem(), version, directory->GetPath(), 
        schema->GetSubIndexPartitionSchema() != NULL, false);
    
    PatchLoaderPtr patchLoader(new PatchLoader(schema, mOnlineConfig));
    segmentid_t startLoadSegment = (lastLoadedVersion == INVALID_VERSION) ?
        0 : lastLoadedVersion.GetLastSegment() + 1;
    
    string locatorStr = partitionData->GetLastLocator();
    bool isIncConsistentWithRt = (lastLoadedVersion != INVALID_VERSION)
        && mOnlineConfig.isIncConsistentWithRealtime;
    IndexLocator indexLocator;
    if (!indexLocator.fromString(locatorStr))
    {
        isIncConsistentWithRt = false;
    }
    else
    {
        bool fromInc = false;
        OnlineJoinPolicy joinPolicy(version, schema->GetTableType());
        joinPolicy.GetRtSeekTimestamp(indexLocator, fromInc);
        if (fromInc)
        {
            isIncConsistentWithRt =  false;
        }
    }
    IE_LOG(INFO, "init patchLoader with isIncConsistentWithRt[%d]", isIncConsistentWithRt);
    patchLoader->Init(partitionData, isIncConsistentWithRt,
                      lastLoadedVersion, startLoadSegment, false);
    return patchLoader->CalculatePatchLoadExpandSize();
}

IE_NAMESPACE_END(partition);

