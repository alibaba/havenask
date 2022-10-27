#ifndef __INDEXLIB_FILE_SYSTEM_CREATOR_H
#define __INDEXLIB_FILE_SYSTEM_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/memory_control/partition_memory_quota_controller.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/file_system/load_config/load_config_list.h"

IE_NAMESPACE_BEGIN(file_system);

class FileSystemCreator
{
public:
    FileSystemCreator();
    ~FileSystemCreator();
public:
    static IndexlibFileSystemPtr CreateFileSystem(
            const std::string& rootPath,
            const std::string& secondaryRootPath = "",
            const LoadConfigList& loadConfigList = LoadConfigList(),
            bool needFlush = true, bool asyncFlush = false, bool useInMemLink = false,
            const util::PartitionMemoryQuotaControllerPtr& quotaController =
            util::PartitionMemoryQuotaControllerPtr());

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FileSystemCreator);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FILE_SYSTEM_CREATOR_H
