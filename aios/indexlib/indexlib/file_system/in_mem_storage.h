#ifndef __INDEXLIB_IN_MEM_STORAGE_H
#define __INDEXLIB_IN_MEM_STORAGE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/dump_scheduler.h"
#include "indexlib/file_system/storage.h"
#include "indexlib/file_system/flush_operation_queue.h"
#include "indexlib/file_system/file_node_creator.h"
#include "indexlib/file_system/file_system_options.h"
#include <future>

IE_NAMESPACE_BEGIN(file_system);

class InMemStorage : public Storage
{
private:
    using Storage::MountPackageFile;
public:
    InMemStorage(const std::string& rootPath,
                 const util::BlockMemoryQuotaControllerPtr& memController,
                 bool needFlush, bool asyncFlush = true);
    ~InMemStorage();

public:
    void Init(const FileSystemOptions& options);

    FileWriterPtr CreateFileWriter(
            const std::string& filePath,
            const FSWriterParam& param = FSWriterParam()) override;

    FileReaderPtr CreateFileReader(const std::string& filePath,
                                   FSOpenType type) override;
    FileReaderPtr CreateWritableFileReader(const std::string& filePath,
                                   FSOpenType type) override;    
    FileReaderPtr CreateFileReaderWithoutCache(const std::string& filePath,
                                               FSOpenType type) override;
    PackageFileWriterPtr CreatePackageFileWriter(
        const std::string& filePath) override;
    PackageFileWriterPtr GetPackageFileWriter(
        const std::string& filePath) override;
    void MakeDirectory(const std::string& dirPath,
                       bool recursive = true) override;
    void RemoveDirectory(const std::string& dirPath, bool mayNonExist) override;
    size_t EstimateFileLockMemoryUse(
        const std::string& filePath, FSOpenType type) override;
    bool IsExist(const std::string& path) const override;
    bool IsExistOnDisk(const std::string& path) const override;
    void GetFileMetaOnDisk(const std::string& filePath,
                           fslib::FileMeta& fileMeta) const override;
    
    void ListDirRecursiveOnDisk(const std::string& path,
            fslib::FileList& fileList, bool ignoreError = false,
            bool skipSecondaryRoot = false) const override;
    
    void ListDirOnDisk(const std::string& path,
                       fslib::FileList& fileList, bool ignoreError = false,
                       bool skipSecondaryRoot = false) const override;

    void ListFile(const std::string& dirPath,
                  fslib::FileList& fileList, 
                  bool recursive = false,
                  bool physicalPath = false,
                  bool skipSecondaryRoot = false) const override;
    void Close() override;
    void StoreFile(const FileNodePtr& fileNode,
                   const FSDumpParam& param = FSDumpParam()) override;

public:
    std::future<bool> Sync(bool waitFinish);
    void MakeRoot(const std::string& rootPath);
    bool GetFirstMatchRoot(const std::string& path, std::string& matchPath) const;
    void ListRoot(const std::string& supperRoot, fslib::FileList& fileList,
                  bool recursive);
    bool NeedFlush() const { return mFlushScheduler != NULL; }
    void AddFlushOperation(const FileFlushOperationPtr& fileFlushOperation);
    void MakeDirectoryRecursive(const std::string& dirPath, 
                                bool physicalPath = true);
    void MountPackageFile(const std::string& packageFilePath,
                          const PackageFileMetaPtr& fileMeta)
    {
        mPackageFileMountTable.MountPackageFile(packageFilePath, fileMeta, true);
    }


private:
    void DoMakeDirectory(const std::string& dirPath, bool needFlush);
    void DoSync(bool waitFinish) override { Sync(waitFinish); }

    FlushOperationQueuePtr CreateNewFlushOperationQueue();

private:
    FlushOperationQueuePtr mOperationQueue;
    DumpSchedulerPtr mFlushScheduler;
    FileNodeCreatorPtr mInMemFileNodeCreator;
    FileNodeCreatorPtr mDirectoryFileNodeCreator;
    FileNodeCachePtr mRootPathTable;
    uint32_t mFlushThreadCount;
    autil::ThreadMutex mLock;

private:
    IE_LOG_DECLARE();
    friend class InMemStorageTest;
};

DEFINE_SHARED_PTR(InMemStorage);

//////////////////////////////////////////////////////////////////
inline bool InMemStorage::GetFirstMatchRoot(
        const std::string& path, std::string& matchRoot) const
{
    autil::ScopedLock lock(*mRootPathTable->GetCacheLock());
    
    for (FileNodeCache::ConstIterator it = mRootPathTable->Begin(); 
         it != mRootPathTable->End(); ++it)
    {
        const std::string& root = it->first;
        bool isPrefix = false;
        
        size_t rootLen = mRootPath.length();
        if (path.size() == root.size())
        {
            isPrefix = (root.compare(rootLen, path.size() - rootLen, 
                                     path.c_str() + rootLen) == 0);
        }
        else if (path.size() < root.size())
        {
            isPrefix = (root.compare(rootLen, path.size() - rootLen, 
                                     path.c_str() + rootLen) == 0)
                && (root[path.size()] == '/');
        }
        else
        {
            isPrefix = (path.compare(rootLen, root.size() - rootLen, 
                                     root.c_str() + rootLen) == 0)
                && (path[root.size()] == '/');
        }
        
        if (isPrefix)
        {
            matchRoot = root;
            return true;
        }
    }
    return false;
}

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_IN_MEM_STORAGE_H
