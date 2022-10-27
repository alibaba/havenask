#ifndef __INDEXLIB_PACKAGE_STORAGE_H
#define __INDEXLIB_PACKAGE_STORAGE_H

#include <tr1/memory>
#include <queue>
#include <tuple>
#include <unordered_map>
#include <autil/Lock.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/path_util.h"
#include "indexlib/file_system/storage.h"
#include "indexlib/file_system/file_system_options.h"
#include "indexlib/file_system/buffered_file_output_stream.h"

DECLARE_REFERENCE_CLASS(storage, FileBuffer);

IE_NAMESPACE_BEGIN(file_system);

class PackageStorage : public Storage
{
public:
    PackageStorage(const std::string& rootPath, const std::string& description,
                   const util::BlockMemoryQuotaControllerPtr& memController);
    ~PackageStorage();
public:
    FileWriterPtr CreateFileWriter(const std::string& filePath,
                                   const FSWriterParam& param = FSWriterParam()) override;
    FileReaderPtr CreateFileReader(const std::string& filePath, FSOpenType type) override;
    FileReaderPtr CreateWritableFileReader(const std::string& filePath,
                                           FSOpenType type) override;
    FileReaderPtr CreateFileReaderWithoutCache(const std::string& filePath,
                                               FSOpenType type) override;
    PackageFileWriterPtr CreatePackageFileWriter(const std::string& filePath) override;
    PackageFileWriterPtr GetPackageFileWriter(const std::string& filePath) override;
    void MakeDirectory(const std::string& dirPath, bool recursive = true) override;
    void RemoveDirectory(const std::string& dirPath, bool mayNonExist) override;
    size_t EstimateFileLockMemoryUse(const std::string& filePath, FSOpenType type) override;
    bool IsExist(const std::string& path) const override;
    void ListFile(const std::string& dirPath, fslib::FileList& fileList, 
                  bool recursive = false, bool physicalPath = false,
                  bool skipSecondaryRoot = false) const override;
    void Close() override;
    void StoreFile(const FileNodePtr& fileNode,
                   const FSDumpParam& param = FSDumpParam()) override;
    void RemoveFile(const std::string& filePath, bool mayNonExist) override;
    bool IsExistOnDisk(const std::string& path) const override;
    
public:
    void Init(const FileSystemOptions& options);
    void MakeRoot(const std::string& rootPath);
    void Recover(const std::string& rootPath, int32_t recoverMetaId,
                 const fslib::FileList& fileListInRoot);

public:
    bool MatchRoot(const std::string& path, std::string& matchPath) const;
    std::pair<uint32_t, BufferedFileOutputStreamPtr> AllocatePhysicalStream(
            int32_t unitId, const std::string& logicalFilePath);
    void StoreLogicalFile(int32_t unitId, const std::string& logicalFilePath, 
                          storage::FileBuffer* buffer);
    void StoreLogicalFile(int32_t unitId, const std::string& logicalFilePath, 
                          uint32_t physicalFileId, size_t fileLength);
    // flush all small files which not yet allcate physical file writer
    bool Sync();
    // flush and seal all opened Physical File Writer
    int32_t Commit();
    void SetDescription(const std::string& description) { mDescription = description; }
    std::string GetDescription() { return mDescription; }
    typedef std::function<std::string(const std::string&)> TagFunction;
    void SetTagFunction(const TagFunction& tagFunction) { mTagFunction = tagFunction; }
    void AssertDescription(const std::string& description);

private:
    struct UnflushedFile
    {
        std::string logicalFilePath;
        storage::FileBuffer* buffer;
    };
    
    // a Unit reference to a segment
    struct Unit
    {
        Unit(const std::string& _rootPath): rootPath(_rootPath) {}
        
        std::unordered_map<std::string, std::queue<uint32_t>> freePhysicalFileIds;
        std::vector<std::tuple<std::string, BufferedFileOutputStreamPtr, std::string>> physicalStreams;
        std::vector<UnflushedFile> unflushedBuffers;
        // metas[KEY->VALUE]
        // FullPath ==
        // <PackageStorage.mRootPath> + KEY == 
        // <PackageStorage.mRootPath> + <Unit.rootPath> + "/" + <VALUE.mFilePath>
        std::map<std::string, PackageFileMeta::InnerFileMeta> metas;
        std::string rootPath;
        int32_t nextMetaId = 0;
        // bool needCommit = false;
    };
    
private:
    void CleanData(const Unit& unit, const std::set<std::string>& fileNames);
    void MakeDirectoryRecursive(Unit& unit, const std::string& relativePath);
    void DoMakeDirectory(Unit& unit, const std::string& relativePath);
    std::pair<uint32_t, BufferedFileOutputStreamPtr> DoAllocatePhysicalStream(
        Unit& unit, const std::string& logicalFilePath);

    int32_t MatchUnit(const std::string& relativePath) const;
    std::string GetRelativePath(const std::string& path) const
    {
        assert(path.compare(0, mRootPath.size(), mRootPath) == 0);
        return path.substr(mRootPath.size());
    }
    std::string JoinRoot(const std::string& path) const
    { return mRootPath + path; }

    std::string ExtractTag(const std::string& logicalFilePath) const;
    
private:
    TagFunction mTagFunction;
    std::vector<Unit> mUnits;
    std::string mDescription;    // instanceId_threadId
    size_t mFileAlignSize;
    bool mIsSynced;
    mutable autil::RecursiveThreadMutex mLock;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackageStorage);

//////////////////////////////////////////////////////////////////
inline bool PackageStorage::MatchRoot(const std::string& path, std::string& matchRoot) const
{
    autil::ScopedLock lock(mLock);
    int32_t unitId = MatchUnit(GetRelativePath(path));
    if (unitId >= 0)
    {
        matchRoot = JoinRoot(mUnits[unitId].rootPath);
        return true;
    }
    return false;
}
inline int32_t PackageStorage::MatchUnit(const std::string& relativePath) const
{
    int32_t matchUnitId = -1;
    size_t matchPathSize = 0;
    for (size_t i = 0; i < mUnits.size(); ++ i)
    {
        if (util::PathUtil::IsInRootPath(relativePath, mUnits[i].rootPath) &&
            mUnits[i].rootPath.size() >= matchPathSize)
        {
            matchUnitId = i;
            matchPathSize = mUnits[i].rootPath.size();
        }
    }
    return matchUnitId;
}

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_PACKAGE_STORAGE_H
