#include "indexlib/partition/in_memory_index_cleaner.h"
#include "indexlib/index_define.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/partition/reader_container.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/partition/unused_segment_collector.h"

using namespace std;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, InMemoryIndexCleaner);

InMemoryIndexCleaner::InMemoryIndexCleaner(
        const ReaderContainerPtr& readerContainer,
        const DirectoryPtr& directory)
    : mReaderContainer(readerContainer)
    , mDirectory(directory)
    , mLocalOnly(true)
{
}

InMemoryIndexCleaner::~InMemoryIndexCleaner() 
{
}

void InMemoryIndexCleaner::Execute()
{
    CleanUnusedIndex(mDirectory->GetDirectory(RT_INDEX_PARTITION_DIR_NAME, false));
    CleanUnusedIndex(mDirectory->GetDirectory(JOIN_INDEX_PARTITION_DIR_NAME, false));
}

void InMemoryIndexCleaner::CleanUnusedIndex(const DirectoryPtr& directory)
{
    if (!directory)
    {
        return;
    }
    CleanUnusedSegment(directory);
    CleanUnusedVersion(directory);
}

void InMemoryIndexCleaner::CleanUnusedSegment(const DirectoryPtr& directory)
{
    fslib::FileList segments;
    UnusedSegmentCollector::Collect(mReaderContainer, directory, segments, mLocalOnly);
    for (size_t i = 0; i < segments.size(); ++i)
    {
        directory->RemoveDirectory(segments[i]);
    }
}

void InMemoryIndexCleaner::CleanUnusedVersion(const DirectoryPtr& directory)
{
    fslib::FileList fileList;
    VersionLoader::ListVersion(directory, fileList);
    if (fileList.size() <= 1)
    {
        return;
    }
    for (size_t i = 0; i < fileList.size() - 1; ++i)
    {
        directory->RemoveFile(fileList[i]);
    }
}


IE_NAMESPACE_END(partition);

