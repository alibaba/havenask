#include <fslib/fs/FileSystem.h>
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/in_mem_directory.h"
#include "indexlib/file_system/local_directory.h"
#include "indexlib/misc/exception.h"
#include "indexlib/util/path_util.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/file_system/indexlib_file_system_impl.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, DirectoryCreator);

DirectoryCreator::DirectoryCreator() 
{
}

DirectoryCreator::~DirectoryCreator() 
{
}

DirectoryPtr DirectoryCreator::Create(const IndexlibFileSystemPtr& fileSystem,
                                      const string& absPath,
                                      FSStorageType storageType)
{
    if (fileSystem->IsExist(absPath))
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, "Failed to create directory [%s]",
                             absPath.c_str());
        return DirectoryPtr();
    }
    if (storageType == FSST_IN_MEM)
    {
        fileSystem->MountInMemStorage(absPath);
    }
    else
    {
        fileSystem->MakeDirectory(absPath);
    }

    return CreateDirectory(fileSystem, absPath, storageType);
}

DirectoryPtr DirectoryCreator::Get(
        const IndexlibFileSystemPtr& fileSystem,
        const string& absPath, bool throwExceptionIfNotExist)
{
    FSStorageType storageType = FSST_UNKNOWN;
    storageType = fileSystem->GetStorageType(absPath, throwExceptionIfNotExist);
    if (storageType == FSST_UNKNOWN)
    {
        assert(throwExceptionIfNotExist == false);
        return DirectoryPtr();
    }

    return CreateDirectory(fileSystem, absPath, storageType);
}

DirectoryPtr DirectoryCreator::Create(const std::string& absPath, bool useCache,
                                      bool enablePathMetaContainer, bool isOffline)
{
    if (!storage::FileSystemWrapper::IsExist(absPath))
    {
        storage::FileSystemWrapper::MkDir(absPath);
    }
    IndexlibFileSystemPtr fileSystem(new file_system::IndexlibFileSystemImpl(absPath));
    FileSystemOptions options;
    options.enableAsyncFlush = false;
    options.useCache = useCache;
    options.memoryQuotaController =
        MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    options.enablePathMetaContainer = enablePathMetaContainer;
    options.isOffline = isOffline;
    fileSystem->Init(options);
    return CreateDirectory(fileSystem, absPath, FSST_LOCAL);
}

DirectoryPtr DirectoryCreator::Create(const IndexlibFileSystemPtr& fileSystem,
                                      const std::string& absPath)
{
    return CreateDirectory(fileSystem, absPath, FSST_LOCAL);
}

DirectoryPtr DirectoryCreator::CreateDirectory(
        const IndexlibFileSystemPtr& fileSystem,
        const string& absPath, FSStorageType storageType)
{
    switch (storageType) 
    {
    case FSST_IN_MEM:
    {
        return DirectoryPtr(new InMemDirectory(absPath, fileSystem));
    }
    case FSST_PACKAGE:
    case FSST_LOCAL:
    {
        return DirectoryPtr(new LocalDirectory(absPath, fileSystem));
    }
    case FSST_UNKNOWN:
    {
        assert(false);
    }
    default:
        assert(false);
        return DirectoryPtr();
    }    
    assert(false);
    return DirectoryPtr();
}

IE_NAMESPACE_END(fileSystem);

