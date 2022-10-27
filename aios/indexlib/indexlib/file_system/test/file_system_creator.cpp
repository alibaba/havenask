#include "indexlib/file_system/test/file_system_creator.h"
#include "indexlib/file_system/indexlib_file_system_impl.h"

using namespace std;

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, FileSystemCreator);

FileSystemCreator::FileSystemCreator() 
{
}

FileSystemCreator::~FileSystemCreator() 
{
}

IndexlibFileSystemPtr FileSystemCreator::CreateFileSystem(
        const string& rootPath,
        const std::string& secondaryRootPath,
        const LoadConfigList& loadConfigList,
        bool needFlush, bool asyncFlush, bool useRootLink,
        const util::PartitionMemoryQuotaControllerPtr& quotaController)
{
    IndexlibFileSystemPtr fileSystem(new IndexlibFileSystemImpl(rootPath, secondaryRootPath));
    FileSystemOptions options;
    options.loadConfigList = loadConfigList;
    options.needFlush = needFlush;
    options.enableAsyncFlush = asyncFlush;
    options.useRootLink = useRootLink;

    util::PartitionMemoryQuotaControllerPtr controller = quotaController;
    if (!controller)
    {
        util::MemoryQuotaControllerPtr mqc(
                new util::MemoryQuotaController(std::numeric_limits<int64_t>::max()));
        controller.reset(new util::PartitionMemoryQuotaController(mqc));
    }
    options.memoryQuotaController = controller;
    fileSystem->Init(options);
    return fileSystem;
}

IE_NAMESPACE_END(file_system);

