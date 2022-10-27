#ifndef __INDEXLIB_MOUNT_TABLE_H
#define __INDEXLIB_MOUNT_TABLE_H

#include <tr1/memory>
#include <autil/Lock.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/path_util.h"
#include "indexlib/file_system/in_mem_storage.h"
#include "indexlib/file_system/disk_storage.h"
#include "indexlib/file_system/multi_storage.h"
#include "indexlib/file_system/package_storage.h"
#include "indexlib/file_system/file_system_options.h"

IE_NAMESPACE_BEGIN(file_system);

// NOTE: All input path must be normalized outside
class MountTable
{
public:
    MountTable();
    ~MountTable();

public:
    void Init(const std::string& rootPath,
              const std::string& secondaryRootPath,
              const FileSystemOptions& fileSystemOptions,
              const util::BlockMemoryQuotaControllerPtr& memController);

    void Mount(const std::string& path, FSStorageType storageType,
               const std::string& description = "");

    FSStorageType GetStorageType(const std::string& path) const;
    Storage* GetStorage(const std::string& path) const;
    Storage* GetStorage(FSStorageType type) const;
    
    const InMemStoragePtr& GetInMemStorage() const 
    { assert(mInMemStorage); return mInMemStorage; }
    const DiskStoragePtr& GetDiskStorage() const 
    { assert(mDiskStorage); return mDiskStorage; }
    const MultiStoragePtr& GetMultiStorage() const 
    { assert(mMultiStorage); return mMultiStorage; }
    const PackageStoragePtr& GetPackageStorage() const
    { return mPackageStorage; }

private:
    bool GetFirstMatchPath(const std::string& path, std::string& matchPath,
                           FSStorageType& storageType) const;
    void PreparePackgeStorage(const std::string& description);

/* public: */
/*     void TEST_Insert(const std::string& rootPath); */

private:
    std::string mRootPath;
    FileSystemOptions mOptions;
    util::BlockMemoryQuotaControllerPtr mMemController;
    FileNodeCreatorPtr mDirectoryFileNodeCreator;
    InMemStoragePtr mInMemStorage;
    DiskStoragePtr mDiskStorage;
    MultiStoragePtr mMultiStorage;
    PackageStoragePtr mPackageStorage;

private:
    IE_LOG_DECLARE();
    friend class MountTableTest;
};

DEFINE_SHARED_PTR(MountTable);
//////////////////////////////////////////////////////////////////
inline bool MountTable::GetFirstMatchPath(const std::string& path, std::string& matchPath,
                                          FSStorageType& storageType) const
{
    assert(util::PathUtil::IsInRootPath(path, mRootPath));
    
    if (mPackageStorage && mPackageStorage->MatchRoot(path, matchPath))
    {
        storageType = FSST_PACKAGE;
        return true;
    }
    storageType = FSST_LOCAL;
    if (mInMemStorage->GetFirstMatchRoot(path, matchPath))
    {
        if (path.size() >= matchPath.size())
        {
            storageType = FSST_IN_MEM;
        }
        // else is multi storage
        return true;
    }
    return false;
}


IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_MOUNT_TABLE_H
