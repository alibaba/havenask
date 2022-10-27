#include <autil/TimeUtility.h>
#include "indexlib/partition/partition_reader_cleaner.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/partition/reader_container.h"
#include "indexlib/index_base/segment/segment_directory_creator.h"
#include "indexlib/index_base/segment/online_segment_directory.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/index_meta/version.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, PartitionReaderCleaner);

PartitionReaderCleaner::PartitionReaderCleaner(
        const ReaderContainerPtr& readerContainer,
        const IndexlibFileSystemPtr& fileSystem,
        RecursiveThreadMutex& dataLock,
        const string& partitionName)
    : mReaderContainer(readerContainer)
    , mFileSystem(fileSystem)
    , mDataLock(dataLock)
    , mPartitionName(partitionName)
    , mLastTs(0)
{
    assert(readerContainer);
    mLastTs = TimeUtility::currentTime();
}

PartitionReaderCleaner::~PartitionReaderCleaner() 
{
}

void PartitionReaderCleaner::Execute()
{
    int64_t currentTime = TimeUtility::currentTime();
    if (currentTime - mLastTs > 10 * 1000 * 1000) // 10s
    {
        IE_LOG(INFO, "[%s] Execute() timestamp gap over 10 seconds [%ld]",
               mPartitionName.c_str(), currentTime - mLastTs);
    }
    
    IndexPartitionReaderPtr reader = mReaderContainer->GetOldestReader();
    while (reader && reader.use_count() == 2)
    {
        IE_LOG(INFO, "[%s] Find target reader to clean", mPartitionName.c_str());
        mReaderContainer->EvictOldestReader();
        ScopedLock readerLock(mDataLock);
        IE_LOG(INFO, "[%s] Begin clean reader", mPartitionName.c_str());
        versionid_t versionId = reader->GetVersion().GetVersionId();
        reader = mReaderContainer->GetOldestReader();
        IE_LOG(INFO, "[%s] End clean reader[%d]", mPartitionName.c_str(), versionId);
    }
    mFileSystem->CleanCache();

    size_t readerCount = mReaderContainer->Size();
    if (readerCount > 10)
    {
        IE_LOG(INFO, "[%s] readerContainer size[%lu] over 10!", mPartitionName.c_str(), readerCount);
    }
    
    mLastTs = TimeUtility::currentTime();
    if (mLastTs - currentTime > 10 * 1000 * 1000) // 10s
    {
        IE_LOG(INFO, "[%s] run clean logic over 10 seconds [%ld]",
               mPartitionName.c_str(), mLastTs - currentTime);
    }
}

IE_NAMESPACE_END(partition);
