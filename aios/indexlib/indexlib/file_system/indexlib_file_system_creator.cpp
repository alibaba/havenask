#include "indexlib/file_system/indexlib_file_system_creator.h"
#include "indexlib/file_system/indexlib_file_system_impl.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, IndexlibFileSystemCreator);

IndexlibFileSystemPtr IndexlibFileSystemCreator::Create(
        const string& rootDir,
        const string& secondaryRootDir,
        MetricProviderPtr metricProvider,
        const FileSystemOptions& fsOptions)
{
    IndexlibFileSystemPtr fileSystem(new IndexlibFileSystemImpl(
                    rootDir, secondaryRootDir, metricProvider));
    fileSystem->Init(fsOptions);
    return fileSystem;
}

IndexlibFileSystemPtr IndexlibFileSystemCreator::Create(
    const string& rootDir, const string& secondaryRootDir, bool isOffline)
{
    IndexlibFileSystemPtr fileSystem(new IndexlibFileSystemImpl(rootDir, secondaryRootDir, NULL));
    FileSystemOptions fsOptions;
    fsOptions.memoryQuotaController =
        util::MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    fsOptions.isOffline = isOffline;
    fileSystem->Init(fsOptions);
    return fileSystem;
}

IE_NAMESPACE_END(file_system);

