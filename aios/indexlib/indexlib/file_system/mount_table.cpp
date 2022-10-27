#include "indexlib/file_system/mount_table.h"
#include "indexlib/file_system/directory_file_node_creator.h"
#include "indexlib/file_system/load_config/load_config.h"
#include "indexlib/file_system/hybrid_storage.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, MountTable);

MountTable::MountTable()
{
}

MountTable::~MountTable() 
{
}

void MountTable::Init(const string& rootPath,
                      const string& secondaryRootPath,
                      const FileSystemOptions& options,
                      const util::BlockMemoryQuotaControllerPtr& memController)
{
    mRootPath = PathUtil::NormalizePath(rootPath);
    mOptions = options;
    mMemController = memController;
    bool needFlush = mOptions.needFlush;
    bool asyncFlush = mOptions.enableAsyncFlush;

    mDirectoryFileNodeCreator.reset(new DirectoryFileNodeCreator());
    mDirectoryFileNodeCreator->Init(LoadConfig(), mMemController);

    mInMemStorage.reset(new InMemStorage(mRootPath, mMemController, needFlush, asyncFlush));
    mInMemStorage->Init(mOptions);

    if (secondaryRootPath.empty())
    {
        mDiskStorage.reset(new DiskStorage(mRootPath, mMemController));
    }
    else
    {
        string normSecondaryPath = PathUtil::NormalizePath(secondaryRootPath);
        mDiskStorage.reset(new HybridStorage(mRootPath, normSecondaryPath, memController));
    }
    mDiskStorage->Init(mOptions);
    
    mMultiStorage.reset(new MultiStorage(mInMemStorage, mDiskStorage, mPackageStorage, mMemController));
    mMultiStorage->Init();
}

void MountTable::Mount(const string& path, FSStorageType storageType, const string& description)
{
    assert(util::PathUtil::IsInRootPath(path, mRootPath));
    assert(path == PathUtil::NormalizePath(path));
    if (unlikely(storageType != FSST_IN_MEM && storageType != FSST_PACKAGE))
    {
        INDEXLIB_FATAL_ERROR(UnSupported,
                             "Unsupport mount path [%s] with storage type [%d]",
                             path.c_str(), storageType);
    }
    if (unlikely(path.size() == mRootPath.size()))
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "Root path [%s] can not be mounted",
                             mRootPath.c_str());
    }
    
    string matchPath;
    FSStorageType matchStorageType;
    if (GetFirstMatchPath(path, matchPath, matchStorageType))
    {
        if (matchStorageType == FSST_IN_MEM)
        {
            assert(mInMemStorage);
            mInMemStorage->MakeDirectory(path, true);
            return;
        }
        else if (matchStorageType == FSST_PACKAGE)
        {
            // do nothing
        }
        else
        {
            INDEXLIB_FATAL_ERROR(UnSupported,
                "Can not mount path [%s] with storage type [%d] when found "
                "match path [%s] with storage type [%d]", 
                path.c_str(), storageType, matchPath.c_str(), matchStorageType);
        }
    }
    
    string parentPath = PathUtil::GetParentDirPath(path);
    assert(mDiskStorage);
    if (unlikely(!mDiskStorage->IsExist(parentPath)))
    {
        INDEXLIB_FATAL_ERROR(NonExist, "Parent path [%s] is not exist",
                             parentPath.c_str());
    }
    if (storageType == FSST_IN_MEM)
    {
        assert(mInMemStorage);
        mInMemStorage->MakeRoot(path);
    }
    else if (storageType == FSST_PACKAGE)
    {
        PreparePackgeStorage(description);
        mPackageStorage->MakeRoot(path);
    }
}

FSStorageType MountTable::GetStorageType(const std::string& path) const
{
    string matchPath;
    FSStorageType storageType;
    GetFirstMatchPath(path, matchPath, storageType);
    return storageType;
}

Storage* MountTable::GetStorage(const std::string& path) const
{
    string matchPath;
    FSStorageType storageType;
    if (GetFirstMatchPath(path, matchPath, storageType))
    {
        if (storageType == FSST_IN_MEM)
        {
            assert(mInMemStorage);
            return mInMemStorage.get();
        }
        else if (storageType == FSST_PACKAGE)
        {
            assert(mPackageStorage);
            return mPackageStorage.get();
        }
        else
        {
            assert(mMultiStorage);
            return mMultiStorage.get();
        }
    }

    assert(mDiskStorage);
    return mDiskStorage.get();
}

// void MountTable::TEST_Insert(const std::string& rootPath, )
// {
//     assert(mDirectoryFileNodeCreator);
//     assert(mTable);
//     FileNodePtr dirNode =
//         mDirectoryFileNodeCreator->CreateFileNode(rootPath, FSOT_UNKNOWN, true);
//     mTable->Insert(dirNode);
// }

Storage* MountTable::GetStorage(FSStorageType type) const
{
    if (type == FSST_IN_MEM)
    {
        return mInMemStorage.get();
    }    
    if (type == FSST_LOCAL)
    {
        return mDiskStorage.get();
    }
    if (type == FSST_PACKAGE)
    {
        return mPackageStorage.get();
    }
    
    INDEXLIB_FATAL_ERROR(BadParameter, "Unknown Storage type [%d]", type);
    return NULL;
}

void MountTable::PreparePackgeStorage(const string& description)
{
    if (!mPackageStorage)
    {
        mPackageStorage.reset(new PackageStorage(mRootPath, description, mMemController));
        mPackageStorage->Init(mOptions);
    }
    else
    {
        mPackageStorage->AssertDescription(description);
    }
}

IE_NAMESPACE_END(file_system);

