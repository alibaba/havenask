#ifndef __INDEXLIB_MULTI_STORAGE_H
#define __INDEXLIB_MULTI_STORAGE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/in_mem_storage.h"
#include "indexlib/file_system/disk_storage.h"
#include "indexlib/file_system/package_storage.h"

IE_NAMESPACE_BEGIN(file_system);

class MultiStorage : public Storage
{
public:
    MultiStorage(const InMemStoragePtr& inMemStorage,
                 const DiskStoragePtr& diskStorage,
                 const PackageStoragePtr& packageStorage,
                 const util::BlockMemoryQuotaControllerPtr& memController);
    ~MultiStorage();

public:
    void Init() {}

    FileWriterPtr CreateFileWriter(
            const std::string& filePath,
            const FSWriterParam& param = FSWriterParam()) override;

    FileReaderPtr CreateFileReader(const std::string& filePath, FSOpenType type) override;
    FileReaderPtr CreateWritableFileReader(const std::string& filePath, FSOpenType type) override;
    FileReaderPtr CreateFileReaderWithoutCache(
        const std::string& filePath, FSOpenType type) override;

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
    void ListFile(const std::string& dirPath,
                  fslib::FileList& fileList, 
                  bool recursive = false, 
                  bool physicalPath = false,
                  bool skipSecondaryRoot = false) const override;
    void Close() override;

    void StoreFile(const FileNodePtr& fileNode,
                   const FSDumpParam& param = FSDumpParam()) override
    { assert(false); }
    void RemoveFile(const std::string& filePath, bool mayNonExist) override;

public:
    void CleanCache();

private:
    void ListInMemRootFile(const std::string& listDir, 
                           const std::string& rootPath, 
                           fslib::FileList& fileList, 
                           bool physicalPath) const;

private:
    const InMemStoragePtr& mInMemStorage;
    const DiskStoragePtr& mDiskStorage;
    const PackageStoragePtr& mPackageStorage;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiStorage);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_MULTI_STORAGE_H
