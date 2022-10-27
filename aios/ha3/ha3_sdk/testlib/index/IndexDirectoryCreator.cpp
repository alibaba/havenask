#include <ha3_sdk/testlib/index/IndexDirectoryCreator.h>
#include <indexlib/file_system/directory_creator.h>
#include <indexlib/file_system/indexlib_file_system_impl.h>
#include <indexlib/util/memory_control/memory_quota_controller_creator.h>

using namespace std;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);

IndexDirectoryCreator::IndexDirectoryCreator() { 
}

IndexDirectoryCreator::~IndexDirectoryCreator() { 
}

DirectoryPtr IndexDirectoryCreator::Create(const std::string& path)
{
    FileSystemOptions options;
    LoadConfigList loadConfigList;
    options.loadConfigList = loadConfigList;
    options.enableAsyncFlush = false;
    options.needFlush = true;
    options.useCache = true;
    options.memoryQuotaController = MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    IndexlibFileSystemPtr fileSystem(
            new IndexlibFileSystemImpl(path));
    fileSystem->Init(options);
    return DirectoryCreator::Get(fileSystem, path, true);
}

IE_NAMESPACE_END(index);

