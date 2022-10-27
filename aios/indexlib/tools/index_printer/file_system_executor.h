#ifndef __INDEXLIB_FILE_SYSTEM_EXECUTOR_H
#define __INDEXLIB_FILE_SYSTEM_EXECUTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/file_system/storage_metrics.h"

IE_NAMESPACE_BEGIN(tools);

class FileSystemExecutor
{
public:
    FileSystemExecutor();
    ~FileSystemExecutor();

public:
    static bool PrintFileSystemMetrics(partition::IndexPartition* partition,
                                       const std::string& line);

    static bool PrintCacheHit(partition::IndexPartition* partition,
                              const std::string& name);

private:
    static void DoPrintStorageMetrics(const std::string& name,
                                      const file_system::StorageMetrics& metrics);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FileSystemExecutor);

IE_NAMESPACE_END(tools);

#endif //__INDEXLIB_FILE_SYSTEM_EXECUTOR_H
