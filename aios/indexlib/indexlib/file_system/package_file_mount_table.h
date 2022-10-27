#ifndef __INDEXLIB_PACKAGE_FILE_MOUNT_TABLE_H
#define __INDEXLIB_PACKAGE_FILE_MOUNT_TABLE_H

#include <tr1/memory>
#include <unordered_map>
#include <autil/Lock.h>
#include <fslib/fslib.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/package_file_meta.h"
#include "indexlib/file_system/directory_map_iterator.h"
#include "indexlib/file_system/storage_metrics.h"

DECLARE_REFERENCE_CLASS(file_system, Storage);
IE_NAMESPACE_BEGIN(file_system);

// logical file always at local
// physical file may at local or remote
class PackageFileMountTable
{
public:
    PackageFileMountTable(StorageMetrics* storageMetrics = NULL);
    ~PackageFileMountTable();

public:
    bool MountPackageFile(const std::string& primaryPackageFilePath, Storage* storage = NULL);

    // public for in_mem_package_writer close
    void MountPackageFile(const std::string& primaryPackageFilePath,
                          const PackageFileMetaPtr& fileMeta,
                          bool forDump = false);

    bool GetMountMeta(const std::string& innerFilePath,
                      PackageFileMeta::InnerFileMeta& meta) const;

    bool IsExist(const std::string& absPath) const;

    void ListFile(const std::string& dirPath, fslib::FileList& fileList, 
                  bool recursive) const;
    
    bool RemoveFile(const std::string& filePath, bool readOnlySecondaryRoot = false);
    bool RemoveDirectory(const std::string& dirPath, bool readOnlySecondaryRoot = false);
    bool MakeDirectory(const std::string& dirPath);
    
    // this is used for getting package data file length or package meta file length
    bool GetPackageFileLength(const std::string& filePath, size_t& fileLen) const;

    // for hybrid storage
    typedef std::function<std::string(const std::string&)> GetPhysicalPathFunc;
    void SetGetPhysicalPathFunc(const GetPhysicalPathFunc& func)
    { mGetPhysicalPathFunc = func; }

private:
    void ValidatePackageDataFiles(
        const std::string& primaryPackageFilePath,
        const PackageFileMetaPtr& fileMeta,
        Storage* storage) const;
    
    bool IsMounted(const std::string& primaryPackageFilePath) const;
    PackageFileMetaPtr LoadPackageFileMeta(const std::string& packageFileMetaPath, Storage* storage);

    void DecreaseMultiNodeCount(const std::string& packageFileDataPaths, bool readOnlySecondaryRoot);
    void DecreaseNodeCount(const std::string& packageFileDataPath, bool readOnlySecondaryRoot);

    void InsertMountItem(const std::string& itemPath, 
                         const PackageFileMeta::InnerFileMeta& mountMeta);

    void RemoveMountedPackageFile(const std::string& dirPath);

    void IncreaseInnerNodeMetrics(bool isDir);
    void DecreaseInnerNodeMetrics(bool isDir);

    const std::string& GetPackageFileDataPath(const std::string& primaryPackageFilePath,
            const std::string& description, uint32_t dataFileIdx, bool forDump = false) const;
    const std::string& GetPackageFileMetaPath(const std::string& primaryPackageFilePath) const;
    const std::string& GetPackageFilePath(const std::string& primaryPath, bool forDump) const;
    void CleanPhysicalPathCache() const;

private:
    typedef std::map<std::string, size_t> MountPackageFileMap;
    typedef std::unordered_map<std::string, std::string> PhysicalFileMap;
    
    struct PhysicalFileInfo
    {
        std::string primaryPath;
        std::string packageFileMetaPath;
        uint32_t refCount;
    };
    typedef std::unordered_map<std::string, PhysicalFileInfo> PhysicalFileInfoMap;

    typedef DirectoryMapIterator<PackageFileMeta::InnerFileMeta> DirectoryIterator;
    typedef DirectoryIterator::DirectoryMap MountMetaMap;

private:
    mutable autil::RecursiveThreadMutex mLock;
    mutable MountMetaMap mTable;           // primaryLogicalPath -> InnerFileMeta
    MountPackageFileMap mMountedMap;       // primaryPathPrefix -> refCount
    MountPackageFileMap mPhysicalFileSize; // primaryFilePath -> fileSize
    PhysicalFileInfoMap mPhysicalFileInfoMap;   // actualPhysicalFilePath -> physicalFileInfo
    mutable PhysicalFileMap mPhysicalPathCache; // primaryFilePath -> actualPath
    StorageMetrics* mStorageMetrics;
    GetPhysicalPathFunc mGetPhysicalPathFunc;

private:
    friend class FileNodeCreatorTest;
    friend class PackageFileMountTableTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackageFileMountTable);

///////////////////////////////////////////////////////////////
inline void PackageFileMountTable::IncreaseInnerNodeMetrics(bool isDir)
{
    if (!mStorageMetrics)
    { 
        return;
    }

    if (isDir)
    {
        mStorageMetrics->IncreasePackageInnerDirCount();
    }
    else
    {
        mStorageMetrics->IncreasePackageInnerFileCount();
    }
}

inline void PackageFileMountTable::DecreaseInnerNodeMetrics(bool isDir)
{
    if (!mStorageMetrics)
    { 
        return;
    }

    if (isDir)
    {
        mStorageMetrics->DecreasePackageInnerDirCount();
    }
    else
    {
        mStorageMetrics->DecreasePackageInnerFileCount();
    }
}

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_PACKAGE_FILE_MOUNT_TABLE_H
