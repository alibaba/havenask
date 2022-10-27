#ifndef __INDEXLIB_STORAGE_H
#define __INDEXLIB_STORAGE_H

#include <tr1/memory>
#include <fslib/fslib.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/file_node.h"
#include "indexlib/file_system/slice_file_writer.h"
#include "indexlib/file_system/slice_file_reader.h"
#include "indexlib/file_system/swap_mmap_file_writer.h"
#include "indexlib/file_system/swap_mmap_file_reader.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/file_system/package_file_writer.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/file_system/slice_file.h"
#include "indexlib/file_system/in_memory_file.h"
#include "indexlib/file_system/storage_metrics.h"
#include "indexlib/file_system/file_node_cache.h"
#include "indexlib/file_system/package_file_mount_table.h"
#include "indexlib/file_system/block_cache_metrics.h"
#include "indexlib/file_system/path_meta_container.h"
#include "indexlib/file_system/lifecycle_table.h"
#include "indexlib/file_system/file_system_options.h"

IE_NAMESPACE_BEGIN(file_system);

class Storage
{
public:
    Storage(const std::string& rootPath, FSStorageType storageType,
            const util::BlockMemoryQuotaControllerPtr& mMemController);
    virtual ~Storage();

public:
    virtual FileWriterPtr CreateFileWriter(
            const std::string& filePath,
            const FSWriterParam& param = FSWriterParam());
    virtual FileReaderPtr CreateFileReader(const std::string& filePath,
            FSOpenType type) = 0;
    virtual FileReaderPtr CreateWritableFileReader(const std::string& filePath,
            FSOpenType type) = 0;
    virtual FileReaderPtr CreateFileReaderWithoutCache(const std::string& filePath,
            FSOpenType type) = 0;
    virtual PackageFileWriterPtr CreatePackageFileWriter(
            const std::string& filePath) = 0;
    virtual PackageFileWriterPtr GetPackageFileWriter(
            const std::string& filePath) = 0;
    virtual void MakeDirectory(const std::string& dirPath,
                               bool recursive = true) = 0;
    virtual void RemoveDirectory(const std::string& dirPath, bool mayNonExist = false) = 0;
    virtual size_t EstimateFileLockMemoryUse(const std::string& filePath,
            FSOpenType type) = 0;
    virtual bool IsExist(const std::string& path) const = 0;
    virtual void ListFile(const std::string& dirPath, fslib::FileList& fileList, 
                          bool recursive = false, bool physicalPath = false,
                          bool skipSecondaryRoot = false) const = 0;
    virtual void Close() = 0;
    virtual void StoreFile(const FileNodePtr& fileNode,
        const FSDumpParam& param = FSDumpParam()) = 0;

    virtual void RemoveFile(const std::string& filePath, bool mayNonExist = false);
    virtual bool IsExistOnDisk(const std::string& path) const;
    virtual void Validate(const std::string& path, int64_t expectLength) const;
    virtual void CleanCache();

public:
    bool MountPackageFile(const std::string& filePath);
    
    SliceFilePtr CreateSliceFile(const std::string& path, 
                                 uint64_t sliceLen, int32_t sliceNum);
    SliceFilePtr GetSliceFile(const std::string& path);
    SwapMmapFileReaderPtr CreateSwapMmapFileReader(const std::string& path);
    SwapMmapFileWriterPtr CreateSwapMmapFileWriter(const std::string& path, size_t fileSize);
    InMemoryFilePtr CreateInMemoryFile(const std::string& path, uint64_t fileLength);
    InMemoryFilePtr GetInMemoryFile(const std::string& path);    
    const StorageMetrics& GetMetrics() const
    { return mStorageMetrics; }
    size_t GetFileLength(const std::string& filePath) const;
    FSFileType GetFileType(const std::string& filePath) const;
    void GetFileStat(const std::string& filePath, FileStat& fileStat) const;
    void GetFileMeta(const std::string& filePath, fslib::FileMeta& fileMeta) const;
    FSStorageType GetStorageType() const { return mStorageType; }
    void Truncate(const std::string& filePath, size_t newLength);
    bool IsDir(const std::string& path) const;
    bool IsExistInCache(const std::string& path) const
    { return mFileNodeCache->IsExist(path); }
    void StoreWhenNonExist(const FileNodePtr& fileNode);

    bool SetDirLifecycle(const std::string& path, const std::string& lifecycle)
    {
        return mLifecycleTable.AddDirectory(path, lifecycle);
    }

    bool IsExistInPackageFile(const std::string& path) const
    { return mPackageFileMountTable.IsExist(path); }
    inline bool IsExistInPackageMountTable(const std::string& path) const
    {
        size_t fileLen = 0;
        return mPackageFileMountTable.GetPackageFileLength(path, fileLen);
    }
    
    virtual bool AddPathFileInfo(const std::string& path, const FileInfo& fileInfo)
    {
        return mPathMetaContainer && mPathMetaContainer->AddFileInfo(
            util::PathUtil::JoinPath(path, fileInfo.filePath),
            fileInfo.fileLength, fileInfo.modifyTime, fileInfo.isDirectory());
    }

    virtual bool MatchSolidPath(const std::string& solidPath) const
    {
        return mPathMetaContainer && mPathMetaContainer->MatchSolidPath(solidPath);
    }
    
    virtual bool AddSolidPathFileInfos(
        const std::string& solidPath,
        const std::vector<FileInfo>::const_iterator& firstFileInfo,
        const std::vector<FileInfo>::const_iterator& lastFileInfo)
    {
        return mPathMetaContainer && mPathMetaContainer->AddFileInfoForOneSolidPath(
            solidPath, firstFileInfo, lastFileInfo);
    }

public:
    static bool IsRealtimePath(const std::string& path)
    { return path.find(FILE_SYSTEM_ROOT_LINK_NAME) != std::string::npos; }
    const std::string& GetRootPath() const { return mRootPath; }
    const FileSystemOptions& GetOptions() const { return mOptions; }

protected:
    virtual void DoSync(bool waitFinish) { }

    virtual void GetFileMetaOnDisk(const std::string& filePath, fslib::FileMeta& fileMeta) const;
    virtual bool IsDirOnDisk(const std::string& path) const;
    virtual void ListDirRecursiveOnDisk(const std::string& path,
            fslib::FileList& fileList, bool ignoreError = false,
            bool skipSecondaryRoot = false) const;
    virtual void ListDirOnDisk(const std::string& path,
                               fslib::FileList& fileList, bool ignoreError = false,
                               bool skipSecondaryRoot = false) const;

    virtual bool GetPackageOpenMeta(const std::string& filePath,
                                    PackageOpenMeta& packageOpenMeta);

    virtual bool DoRemoveFileInPackageFile(const std::string& filePath);
    virtual bool DoRemoveDirectoryInPackageFile(const std::string& dirPath);

protected:
    void OpenFileNode(FileNodePtr fileNode, const std::string& filePath, FSOpenType type);
    void DoListPackageFilePysicalPath(const std::string& dirPath, 
            fslib::FileList& fileList, bool recursive) const;
    bool GetPackageFileMountMeta(const std::string& filePath,
            PackageFileMeta::InnerFileMeta& innerMeta) const;

    void DoListFileInPackageFile(const std::string& dirPath, 
                                 fslib::FileList& fileList, bool recursive) const;

    // physicalPath = true: only list file paths on disk
    void DoListFileWithOnDisk(const std::string& dirPath, 
                              fslib::FileList& fileList, 
                              bool recursive, 
                              bool physicalPath, 
                              bool ignoreError,
                              bool skipSecondaryRoot) const;

    void DoRemoveDirectory(const std::string& dirPath, bool mayNonExist);

    void DoRemovePackageFileWriter(const std::string& dirPath);

    void SortAndUniqFileList(fslib::FileList& fileList) const;

    void GetMatchedPackageFileWriter(
            const std::string& dirPath, bool recursive,
            std::vector<PackageFileWriterPtr>& matchedPackageWriters) const;

    std::string GetFileLifecycle(const std::string& filePath) const;

    void DoRemoveFileInLifecycleTable(const std::string& path)
    {
        mLifecycleTable.RemoveFile(path);
    }
    void DoRemoveDirectoryInLifecycleTable(const std::string& path)
    {
        mLifecycleTable.RemoveDirectory(path);
    }

protected:
    typedef std::map<std::string, PackageFileWriterPtr> PackageFileWriterMap;

    PackageFileWriterMap mPackageFileWriterMap;
    StorageMetrics mStorageMetrics;
    FileNodeCachePtr mFileNodeCache;
    FSStorageType mStorageType;
    PackageFileMountTable mPackageFileMountTable;
    util::BlockMemoryQuotaControllerPtr mMemController;
    PathMetaContainerPtr mPathMetaContainer;
    std::string mRootPath;
    LifecycleTable mLifecycleTable;
    FileSystemOptions mOptions;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(Storage);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_STORAGE_H

