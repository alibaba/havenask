#ifndef __INDEXLIB_PARTITION_READER_CLEANER_H
#define __INDEXLIB_PARTITION_READER_CLEANER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/executor.h"
#include <autil/Lock.h>

DECLARE_REFERENCE_CLASS(partition, ReaderContainer);
DECLARE_REFERENCE_CLASS(file_system, IndexlibFileSystem);

IE_NAMESPACE_BEGIN(partition);

class PartitionReaderCleaner : public common::Executor
{
public:
    PartitionReaderCleaner(const ReaderContainerPtr& readerContainer,
                           const file_system::IndexlibFileSystemPtr& fileSystem,
                           autil::RecursiveThreadMutex& dataLock,
                           const std::string& partitionName = "");
    ~PartitionReaderCleaner();

public:
    void Execute() override;

private:
    ReaderContainerPtr mReaderContainer;
    file_system::IndexlibFileSystemPtr mFileSystem;
    autil::RecursiveThreadMutex& mDataLock;
    std::string mPartitionName;
    int64_t mLastTs;

private:
    friend class PartitionReaderCleanerTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionReaderCleaner);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_PARTITION_READER_CLEANER_H
