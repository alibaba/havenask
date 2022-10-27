#ifndef __INDEXLIB_INDEXLIB_FILE_SYSTEM_CREATOR_H
#define __INDEXLIB_INDEXLIB_FILE_SYSTEM_CREATOR_H

#include <tr1/memory>
#include "indexlib/misc/metric_provider.h"
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/indexlib_file_system.h"

IE_NAMESPACE_BEGIN(file_system);

class IndexlibFileSystemCreator
{
public:
    static IndexlibFileSystemPtr Create(const std::string& rootDir,
            const std::string& secondaryRootDir,
            misc::MetricProviderPtr metricProvider,
            const FileSystemOptions& fsOptions);
    
    static IndexlibFileSystemPtr Create(const std::string& rootDir,
                                        const std::string& secondaryRootDir = "",
                                        bool isOffline = false);

private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_INDEXLIB_FILE_SYSTEM_CREATOR_H
