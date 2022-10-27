#ifndef __INDEXLIB_IN_MEMORY_INDEX_CLEANER_H
#define __INDEXLIB_IN_MEMORY_INDEX_CLEANER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/executor.h"

DECLARE_REFERENCE_CLASS(partition, ReaderContainer);
DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(partition);

class InMemoryIndexCleaner : public common::Executor
{
public:
    InMemoryIndexCleaner(const ReaderContainerPtr& readerContainer,
                         const file_system::DirectoryPtr& directory);
    ~InMemoryIndexCleaner();

public:
    void Execute() override;

private:
    void CleanUnusedIndex(const file_system::DirectoryPtr& directory);
    void CleanUnusedSegment(const file_system::DirectoryPtr& directory);
    void CleanUnusedVersion(const file_system::DirectoryPtr& directory);
    
private:
    ReaderContainerPtr mReaderContainer;
    file_system::DirectoryPtr mDirectory;
    bool mLocalOnly;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemoryIndexCleaner);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_IN_MEMORY_INDEX_CLEANER_H
