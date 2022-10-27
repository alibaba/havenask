#ifndef __INDEXLIB_HYBRID_STORAGE_H
#define __INDEXLIB_HYBRID_STORAGE_H

#include <tr1/memory>
#include <type_traits>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/disk_storage.h"
#include "indexlib/file_system/directory_map_iterator.h"

IE_NAMESPACE_BEGIN(file_system);

// for online: searcher layer only operate primary path
// for offline: parallel build will operate both path
class HybridStorage final : public DiskStorage
{
public:
    HybridStorage(const std::string& rootPath,
                  const std::string& secondaryRootPath,
                  const util::BlockMemoryQuotaControllerPtr& mMemController);
    ~HybridStorage();
public:
    bool IsExistOnDisk(const std::string& path) const override;
    void RemoveFile(const std::string& filePath, bool mayNonExist) override;
    void RemoveDirectory(const std::string& dirPath, bool mayNonExist) override;
    void Validate(const std::string& path, int64_t expectLength) const override;
    bool IsExistInSecondaryPath(const std::string& path, bool ignoreTrash) const;
    void CleanCache() override;
    std::string GetAbsSecondaryPath(const std::string& path) const;

private:
    void GetFileMetaOnDisk(const std::string& filePath, fslib::FileMeta& fileMeta) const override;
    bool IsDirOnDisk(const std::string& path) const override;
    void ListDirRecursiveOnDisk(const std::string& path, fslib::FileList& fileList,
                                bool ignoreError = false, bool skipSecondaryRoot = false) const override;
    void ListDirOnDisk(const std::string& path, fslib::FileList& fileList,
                       bool ignoreError = false, bool skipSecondaryRoot = false) const override;

    std::string GetRelativePath(const std::string& absPath) const override;
    const FileNodeCreatorPtr& DeduceFileNodeCreator(
        const FileNodeCreatorPtr& creator, FSOpenType type,
        const std::string& filePath) const override;

    void InitDefaultFileNodeCreator() override;

    bool GetPackageOpenMeta(const std::string& filePath,
                            PackageOpenMeta& packageOpenMeta) override;

    bool DoRemoveFileInPackageFile(const std::string& filePath) override;
    bool DoRemoveDirectoryInPackageFile(const std::string& dirPath) override;

    FileNodeCreator* CreateMmapFileNodeCreator(const LoadConfig& loadConfig) const override;

    bool AddPathFileInfo(const std::string& path, const FileInfo& fileInfo) override;
    bool MatchSolidPath(const std::string& solidPath) const override;
    bool AddSolidPathFileInfos(const std::string& solidPath,
                               const std::vector<FileInfo>::const_iterator& firstFileInfo,
                               const std::vector<FileInfo>::const_iterator& lastFileInfo) override;
    FileNodePtr CreateFileNode(
        const std::string& filePath, FSOpenType type, bool readOnly) override;

private:
    std::string GetAbsPrimaryPath(const std::string& path) const;
    std::string GetPhysicalPath(const std::string& path, bool useCache = true) const;
    void ModifyPackageOpenMeta(PackageOpenMeta& packageOpenMeta);
    const LoadConfig& GetMatchedLoadConfig(const std::string& path) const;

private:
    std::string mSecondaryRootPath;
    bool mSecondarySupportMmap;
    FileNodeCreatorMap mSecondaryDefaultCreatorMap;

    typedef DirectoryMapIterator<FileCarrierPtr> PackageFileCarrierMapIter;
    typedef PackageFileCarrierMapIter::DirectoryMap PackageFileCarrierMap;
    PackageFileCarrierMap mPackageFileCarrierMap;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(HybridStorage);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_HYBRID_STORAGE_H
