#ifndef __INDEXLIB_INDEXLIB_FILE_SYSTEM_IMPL_H
#define __INDEXLIB_INDEXLIB_FILE_SYSTEM_IMPL_H

#include <tr1/memory>
#include <autil/Lock.h>
#include "indexlib/misc/metric_provider.h"
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/memory_control/block_memory_quota_controller.h"
#include "indexlib/util/path_util.h"
#include "indexlib/util/memory_control/memory_reserver.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/file_system/in_mem_storage.h"
#include "indexlib/file_system/mount_table.h"
#include "indexlib/file_system/indexlib_file_system_metrics_reporter.h"

IE_NAMESPACE_BEGIN(file_system);

class IndexlibFileSystemImpl : public IndexlibFileSystem
{
public:
    IndexlibFileSystemImpl(const std::string& rootPath,
                           const std::string& secondaryRootPath = "",
                           misc::MetricProviderPtr metricProvider = misc::MetricProviderPtr());
    ~IndexlibFileSystemImpl();

public:
    void Init(const FileSystemOptions& fileSystemOptions) override;

    FileWriterPtr CreateFileWriter(const std::string& filePath,
                                   const FSWriterParam& param = FSWriterParam()) override;

    FileReaderPtr CreateFileReader(const std::string& filePath, 
                                   FSOpenType type) override;
    FileReaderPtr CreateWritableFileReader(const std::string& filePath, 
                                   FSOpenType type) override;
    FileReaderPtr CreateFileReaderWithoutCache(
        const std::string& filePath,FSOpenType type) override;
    SliceFilePtr CreateSliceFile(const std::string& filePath, 
                                 uint64_t sliceLen, int32_t sliceNum) override;
    SliceFilePtr GetSliceFile(const std::string& filePath) override;

    SwapMmapFileReaderPtr CreateSwapMmapFileReader(const std::string& path) override;

    SwapMmapFileWriterPtr CreateSwapMmapFileWriter(
            const std::string& path, size_t fileSize) override;

    void MakeDirectory(const std::string& dirPath, bool recursive = true) override;
    InMemoryFilePtr CreateInMemoryFile(
        const std::string& path, uint64_t fileLength) override;
    void RemoveFile(const std::string& filePath, bool mayNonExist) override;
    void RemoveDirectory(const std::string& dirPath, bool mayNonExist) override;

    bool IsExist(const std::string& path) const override;
    bool IsExistInSecondaryPath(const std::string& path,
                                bool ignoreTrash = false) const override;
    bool IsExistInCache(const std::string& path) const override;
    bool IsExistInPackageMountTable(const std::string& path) const override;
    bool IsDir(const std::string& path) const override;

    void ListFile(const std::string& path, fslib::FileList& fileList, 
                  bool recursive = false, bool physicalPath = false,
                  bool skipSecondaryRoot = false) const override;

    const std::string& GetRootPath() const override { return mRootPath; }
    const std::string& GetRootLinkPath() const override { return mRootLinkPath; }
    const std::string& GetSecondaryRootPath() const override { return mSecondaryRootPath; }
    
    size_t GetFileLength(const std::string& filePath) const override;
    FSFileType GetFileType(const std::string& filePath) const override;
    void GetFileStat(const std::string& filePath, 
                     FileStat& fileStat) const override;
    void GetFileMeta(const std::string& filePath,
                     fslib::FileMeta& fileMeta) const override;
    FSStorageType GetStorageType(const std::string& path,
                                 bool throwExceptionIfNotExist) const override;
    FSOpenType GetLoadConfigOpenType(const std::string& path) const override;
    std::future<bool> Sync(bool waitFinish) override;
    void CleanCache() override;
    void CleanCache(FSStorageType type) override;

    const StorageMetrics& GetStorageMetrics(FSStorageType type) const override;
    IndexlibFileSystemMetrics GetFileSystemMetrics() const override;
    void ReportMetrics() override;
    size_t EstimateFileLockMemoryUse(
        const std::string& filePath, FSOpenType type) override;
    util::MemoryReserverPtr CreateMemoryReserver(const std::string& name) override
    {
        return util::MemoryReserverPtr(
            new util::MemoryReserver(name, mMemController));
    }

    void MountInMemStorage(const std::string& path) override;
    void MountPackageStorage(const std::string& path, const std::string& description) override;
    const PackageStoragePtr& GetPackageStorage() const override;
    
    PackageFileWriterPtr CreatePackageFileWriter(
        const std::string& filePath) override;
    PackageFileWriterPtr GetPackageFileWriter(
        const std::string& filePath) const override;
    bool MountPackageFile(const std::string& filePath) override;

    void EnableLoadSpeedSwitch() override;
    void DisableLoadSpeedSwitch() override;

    bool UseRootLink() const override { return mOptions.useRootLink; }
    const FileBlockCachePtr& GetGlobalFileBlockCache() const override
    { return mOptions.fileBlockCache; }
    size_t GetFileSystemMemoryUse() const override
    { return mMemController->GetAllocatedQuota(); }

    void InsertToFileNodeCacheIfNotExist(
            const FileReaderPtr& fileReader) override;
    bool OnlyPatialLock(const std::string& filePath) const override;

    bool MatchSolidPath(const std::string& path) const override;
    
    bool AddPathFileInfo(const std::string& path, const FileInfo& fileInfo);
    bool AddSolidPathFileInfos(const std::string& solidPath,
                               const std::vector<FileInfo>::const_iterator& firstFileInfo,
                               const std::vector<FileInfo>::const_iterator& lastFileInfo) override;
    bool SetDirLifecycle(const std::string& path, const std::string& lifecycle) override;
    bool IsOffline() const override;
    void Validate(const std::string& path, int64_t expectLength) const override;
    bool GetAbsSecondaryPath(const std::string& path, std::string& absPath) const override;
    bool UpdatePanguInlineFile(const std::string& path, const std::string& content) const override;

private:
    void InitMetrics(FSMetricPreference metricPref);
    Storage* GetStorage(const std::string& normPath) const;
    bool IsRootPath(const std::string& normPath) const;
    void Close();
    void CheckInputPath(const std::string& normPath) const;
    std::future<bool> SyncInMemStorage(bool waitFinish);
    
public:
    //public for test
    const MountTablePtr& GetMountTable() const { return mMountTable; }

    const IndexlibFileSystemMetricsReporter& TEST_GetMetricsReporter() const
    { return mMetricsReporter; }
    
private:
    mutable autil::RecursiveThreadMutex mLock;
    std::string mRootPath;
    std::string mRootLinkPath;
    std::string mSecondaryRootPath;
    MountTablePtr mMountTable;
    FileSystemOptions mOptions;
    IndexlibFileSystemMetricsReporter mMetricsReporter;
    util::BlockMemoryQuotaControllerPtr mMemController;
    bool mIsClosed;

private:
    IE_LOG_DECLARE();
    friend class FileSystemInteTest;
    friend class IndexlibFileSystemTest;
};

DEFINE_SHARED_PTR(IndexlibFileSystemImpl);
//////////////////////////////////////////////////////////////////////////////
inline Storage* IndexlibFileSystemImpl::GetStorage(
        const std::string& normPath) const
{
    CheckInputPath(normPath);
    Storage* storage = mMountTable->GetStorage(normPath);
    assert(storage);
    return storage;
}

inline void IndexlibFileSystemImpl::CheckInputPath(const std::string& normPath) const
{
    if (!util::PathUtil::IsInRootPath(normPath, mRootPath))
    {
        INDEXLIB_FATAL_ERROR(BadParameter, "The path[%s] is not belong to "
                             "this file system", normPath.c_str());        
    }
}

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_INDEXLIB_FILE_SYSTEM_IMPL_H


