#ifndef __INDEXLIB_DISK_STORAGE_H
#define __INDEXLIB_DISK_STORAGE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/load_config/load_config_list.h"
#include "indexlib/file_system/slice_file_writer.h"
#include "indexlib/file_system/slice_file_reader.h"
#include "indexlib/file_system/storage.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/file_system/file_system_define.h"
#include "indexlib/file_system/file_node_creator.h"
#include "indexlib/file_system/storage_metrics.h"
#include "indexlib/file_system/block_cache_metrics.h"
#include "indexlib/file_system/block_file_node_creator.h"
#include "indexlib/file_system/file_system_options.h"

DECLARE_REFERENCE_CLASS(tools, IndexPrinterWorker);
IE_NAMESPACE_BEGIN(file_system);

class DiskStorage : public Storage
{
protected:
    typedef std::map<FSOpenType, FileNodeCreatorPtr> FileNodeCreatorMap; 
    typedef std::vector<FileNodeCreatorPtr> FileNodeCreatorVec;
    typedef std::vector<BlockFileNodeCreatorPtr> BlockFileNodeCreatorVec;
public:
    DiskStorage(const std::string rootPath,
                 const util::BlockMemoryQuotaControllerPtr& memController);
    ~DiskStorage();

public:
    void Init(const FileSystemOptions& options);

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
    void ListFile(const std::string& dirPath, 
                  fslib::FileList& fileList, 
                  bool recursive = false, 
                  bool physicalPath = false,
                  bool skipSecondaryRoot = false) const override;
    void Close() override;
    void StoreFile(const FileNodePtr& fileNode,
                   const FSDumpParam& param = FSDumpParam()) override;
    bool OnlyPatialLock(const std::string& filePath) const;
public:
    virtual FSOpenType GetLoadConfigOpenType(const std::string& path) const;
    
protected:
    virtual std::string GetRelativePath(const std::string& absPath) const;
    virtual const FileNodeCreatorPtr& DeduceFileNodeCreator(
        const FileNodeCreatorPtr& creator, FSOpenType type,
        const std::string& filePath) const;
    virtual void InitDefaultFileNodeCreator();
    virtual void InitPathMetaContainer();
    virtual FileNodeCreatorPtr CreateFileNodeCreator(
            const LoadConfig& loadConfig);
    virtual FileNodePtr CreateFileNode(const std::string& filePath, FSOpenType type, bool readOnly);

    virtual FileNodeCreator* CreateMmapFileNodeCreator(const LoadConfig& loadConfig) const;

protected:
    std::string AbsPathToRelativePath(const std::string& rootPath,
            const std::string& absPath) const;

    void DoInitDefaultFileNodeCreator(FileNodeCreatorMap& creatorMap,
            const LoadConfig& loadConfig);
    
    const FileNodeCreatorPtr& GetFileNodeCreator(const std::string& filePath,
            FSOpenType type) const;

private:
    FSOpenType ModifyOpenType(bool isRootInDfs, const FSOpenType& openType) const;
    FileReaderPtr CreateFileReader(const FileNodePtr& fileNode);
    FileReaderPtr DoCreateFileReader(const std::string& filePath, FSOpenType type, bool needWrite);

protected:
    std::string mRootPath;
    bool mSupportMmap;
    FileNodeCreatorMap mDefaultCreatorMap;

private:
    FileNodeCreatorVec mFileNodeCreators;
    BlockFileNodeCreatorVec mBlockFileNodeCreators;

private:
    IE_LOG_DECLARE();
    friend class tools::IndexPrinterWorker;
    friend class DiskStorageTest;
};

DEFINE_SHARED_PTR(DiskStorage);

/* typedef DiskStorage DiskStorage; */
/* typedef std::tr1::shared_ptr<DiskStorage> DiskStoragePtr; */

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_DISK_STORAGE_H
