#ifndef __INDEXLIB_FILE_SYSTEM_INDEXLIB_FILE_SYSTEM_H
#define __INDEXLIB_FILE_SYSTEM_INDEXLIB_FILE_SYSTEM_H

#include <tr1/memory>
#include <fslib/fslib.h>
#include <future>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file_system_define.h"
#include "indexlib/file_system/file_system_options.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/file_system/slice_file_writer.h"
#include "indexlib/file_system/package_file_writer.h"
#include "indexlib/file_system/indexlib_file_system_metrics.h"
#include "indexlib/file_system/file_block_cache.h"
#include "indexlib/file_system/in_memory_file.h"

DECLARE_REFERENCE_CLASS(file_system, InMemoryFile);
DECLARE_REFERENCE_CLASS(file_system, FileReader);
DECLARE_REFERENCE_CLASS(file_system, SliceFileReader);
DECLARE_REFERENCE_CLASS(file_system, SliceFile);
DECLARE_REFERENCE_CLASS(file_system, SwapMmapFileReader);
DECLARE_REFERENCE_CLASS(file_system, SwapMmapFileWriter);
DECLARE_REFERENCE_CLASS(file_system, PackageStorage);
DECLARE_REFERENCE_CLASS(util, MemoryReserver);

IE_NAMESPACE_BEGIN(file_system);

class IndexlibFileSystem
{
public:
    IndexlibFileSystem() {};
    virtual ~IndexlibFileSystem() {};

public:
    virtual void Init(const FileSystemOptions& fileSystemOptions) = 0;
    virtual FileWriterPtr CreateFileWriter(
        const std::string& filePath,
        const FSWriterParam& param = FSWriterParam()) = 0;

    virtual FileReaderPtr CreateFileReader(const std::string& filePath, FSOpenType type) = 0;
    virtual FileReaderPtr CreateWritableFileReader(const std::string& filePath, FSOpenType type) = 0;
    virtual FileReaderPtr CreateFileReaderWithoutCache(
        const std::string& filePath, FSOpenType type)
        = 0;
    // if exist, return it
    virtual SliceFilePtr CreateSliceFile(const std::string& path, 
            uint64_t sliceLen, int32_t sliceNum) = 0;
    // if not exist, return null
    virtual SliceFilePtr GetSliceFile(const std::string& path) = 0;

    virtual SwapMmapFileReaderPtr CreateSwapMmapFileReader(const std::string& path) = 0;

    virtual SwapMmapFileWriterPtr CreateSwapMmapFileWriter(
            const std::string& path, size_t fileSize) = 0;

    // ignore exist error if recursive = true
    virtual InMemoryFilePtr CreateInMemoryFile(
        const std::string& path, uint64_t fileLength) = 0;
    virtual void MakeDirectory(const std::string& dirPath, 
                               bool recursive = true) = 0;
    virtual void RemoveFile(const std::string& filePath, bool mayNonExist = false) = 0;
    virtual void RemoveDirectory(const std::string& dirPath, bool mayNonExist = false) = 0;
    virtual size_t EstimateFileLockMemoryUse(const std::string& filePath,
            FSOpenType type) = 0;
    virtual bool IsExist(const std::string& path) const = 0;
    virtual bool IsExistInSecondaryPath(const std::string& path, bool ignoreTrash = true) const = 0;
    virtual bool IsExistInCache(const std::string& path) const = 0;
    virtual bool IsExistInPackageMountTable(const std::string& path) const = 0;
    virtual bool IsDir(const std::string& path) const = 0;
    virtual void ListFile(const std::string& dirPath, 
                          fslib::FileList& fileList,
                          bool recursive = false, 
                          bool physicalPath = false,
                          bool skipSecondaryRoot = false) const = 0;
    virtual size_t GetFileLength(const std::string& filePath) const = 0;
    virtual FSFileType GetFileType(const std::string& filePath) const = 0;
    virtual void GetFileStat(const std::string& filePath,
                             FileStat& fileStat) const = 0;
    virtual void GetFileMeta(const std::string& filePath,
                             fslib::FileMeta& fileMeta) const = 0;
    virtual FSStorageType GetStorageType(const std::string& path,
            bool throwExceptionIfNotExist) const = 0;
    virtual FSOpenType GetLoadConfigOpenType(const std::string& path) const = 0;
    virtual std::future<bool> Sync(bool waitFinish) = 0;
    virtual void CleanCache() = 0;
    virtual void CleanCache(FSStorageType type) = 0;
    // TODO: replace with GetFileSystemMetrics
    virtual const StorageMetrics& GetStorageMetrics(FSStorageType type) const = 0;
    virtual IndexlibFileSystemMetrics GetFileSystemMetrics() const = 0;
    virtual void ReportMetrics() = 0;
    virtual util::MemoryReserverPtr CreateMemoryReserver(const std::string& name) = 0;

    virtual const std::string& GetRootPath() const = 0;
    virtual const std::string& GetRootLinkPath() const = 0;
    virtual const std::string& GetSecondaryRootPath() const = 0;

    virtual void MountInMemStorage(const std::string& path) = 0;
    virtual void MountPackageStorage(const std::string& path, const std::string& description) = 0;
    virtual const PackageStoragePtr& GetPackageStorage() const = 0;
    
    virtual PackageFileWriterPtr CreatePackageFileWriter(
            const std::string& filePath) = 0;

    virtual PackageFileWriterPtr GetPackageFileWriter(
            const std::string& filePath) const = 0;

    virtual bool MountPackageFile(const std::string& filePath) = 0;

    virtual void EnableLoadSpeedSwitch() = 0;
    virtual void DisableLoadSpeedSwitch() = 0;

    virtual bool UseRootLink() const { return false; }
    virtual const FileBlockCachePtr& GetGlobalFileBlockCache() const = 0;
    virtual size_t GetFileSystemMemoryUse() const = 0;
    virtual void InsertToFileNodeCacheIfNotExist(const FileReaderPtr& fileReader) = 0;
    virtual bool OnlyPatialLock(const std::string& filePath) const = 0;

    virtual bool MatchSolidPath(const std::string& path) const = 0;

    virtual bool SetDirLifecycle(const std::string& path, const std::string& lifecycle) = 0;
    virtual bool AddPathFileInfo(const std::string& path, const FileInfo& fileInfos) = 0;
    virtual bool AddSolidPathFileInfos(const std::string& solidPath,
            const std::vector<FileInfo>::const_iterator& firstFileInfo,
            const std::vector<FileInfo>::const_iterator& lastFileInfo) = 0;
    virtual bool IsOffline() const = 0;
    virtual void Validate(const std::string& path, int64_t expectLength) const = 0;
    static void DeleteRootLinks(const std::string& rootPath);
    virtual bool GetAbsSecondaryPath(const std::string& path, std::string& absPath) const = 0;
    virtual bool UpdatePanguInlineFile(const std::string& path, const std::string& content) const = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexlibFileSystem);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FILE_SYSTEM_INDEXLIB_FILE_SYSTEM_H
