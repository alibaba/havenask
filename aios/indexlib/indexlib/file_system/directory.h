#ifndef __INDEXLIB_FS_DIRECTORY_H
#define __INDEXLIB_FS_DIRECTORY_H

#include <tr1/memory>
#include <fslib/fslib.h>
#include <future>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/file_system/file_system_define.h"

DECLARE_REFERENCE_CLASS(storage, ArchiveFolder);
DECLARE_REFERENCE_CLASS(file_system, FileReader);
DECLARE_REFERENCE_CLASS(file_system, SwapMmapFileReader);
DECLARE_REFERENCE_CLASS(file_system, FileWriter);
DECLARE_REFERENCE_CLASS(file_system, PackageFileWriter);
DECLARE_REFERENCE_CLASS(file_system, SwapMmapFileWriter);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(file_system, SliceFile);
DECLARE_REFERENCE_CLASS(file_system, PackDirectory);
DECLARE_REFERENCE_CLASS(file_system, LinkDirectory);
DECLARE_REFERENCE_CLASS(file_system, CompressFileWriter);
DECLARE_REFERENCE_CLASS(file_system, CompressFileReader);
DECLARE_REFERENCE_CLASS(file_system, CompressFileInfo);
DECLARE_REFERENCE_CLASS(file_system, InMemoryFile);
DECLARE_REFERENCE_CLASS(file_system, IndexlibFileSystem);

IE_NAMESPACE_BEGIN(file_system);

class Directory
{
public:
    Directory(const std::string& path, 
              const IndexlibFileSystemPtr& indexFileSystem);
    virtual ~Directory() {}

public:
    virtual FileWriterPtr CreateFileWriter(
            const std::string& filePath,
            const FSWriterParam& param = FSWriterParam()) = 0;

    virtual FileReaderPtr CreateFileReader(
        const std::string& filePath, FSOpenType type) = 0;

    virtual FileReaderPtr CreateWritableFileReader(const std::string& filePath, FSOpenType type)
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "can not support writable file reader");
        return FileReaderPtr();
    }

    virtual FileReaderPtr CreateFileReaderWithoutCache(
        const std::string& filePath, FSOpenType type) = 0;

    virtual PackageFileWriterPtr CreatePackageFileWriter(
            const std::string& filePath) = 0;

    virtual PackageFileWriterPtr GetPackageFileWriter(
            const std::string& filePath) const = 0;

    virtual size_t EstimateFileMemoryUse(const std::string& filePath, 
            FSOpenType type) = 0;
    virtual bool OnlyPatialLock(const std::string& filePath) const { return false; }
    virtual size_t EstimateIntegratedFileMemoryUse(const std::string& filePath);
    
    virtual DirectoryPtr MakeInMemDirectory(const std::string& dirName) = 0;

    //for write, make directory recursively & ignore exist error
    virtual DirectoryPtr MakeDirectory(const std::string& dirPath) = 0;
    //for read
    virtual DirectoryPtr GetDirectory(const std::string& dirPath,
            bool throwExceptionIfNotExist) = 0;
    virtual const std::string& GetPath() const { return mRootDir; }

    virtual void RemoveDirectory(const std::string& dirPath, bool mayNonExist = false);
    virtual void RemoveFile(const std::string& filePath, bool mayNonExist = false);

    virtual InMemoryFilePtr CreateInMemoryFile(
            const std::string& path, uint64_t fileLen);

    virtual void Rename(const std::string& srcPath, const DirectoryPtr& destDirectory,
                        const std::string& destPath = "") = 0;
    
public:
    // integrated file reader support GetBaseAddress()
    FileReaderPtr CreateIntegratedFileReader(const std::string& filePath);

    bool MountPackageFile(const std::string& filePath);

    LinkDirectoryPtr CreateLinkDirectory() const;

    void Load(const std::string& fileName, std::string& fileContent,
              bool insertToCacheIfNotExist = false);
    bool LoadMayNonExist(const std::string& fileName, std::string& fileContent,
                         bool cacheFileNode = false);

    void Store(const std::string& fileName,
               const std::string& fileContent,
               bool atomic = true);

    std::future<bool> Sync(bool waitFinish);

    const IndexlibFileSystemPtr& GetFileSystem() const 
    { return mFileSystem; }

    std::string GetRootLinkName() const;

public:
    CompressFileReaderPtr CreateCompressFileReader(
            const std::string& filePath, FSOpenType type);

    CompressFileWriterPtr CreateCompressFileWriter(
            const std::string& filePath,
            const std::string& compressorName,
            size_t compressBufferSize,
            const FSWriterParam& param = FSWriterParam());

    //physicalPath: list physical files on disk
    void ListFile(const std::string& path, 
                  fslib::FileList& fileList, 
                  bool recursive = false,
                  bool physicalPath = false,
                  bool skipSecondaryRoot = false) const;

    bool IsExist(const std::string& path);
    bool IsExistInCache(const std::string& path);
    bool IsExistInPackageMountTable(const std::string& path) const;
    bool IsDir(const std::string& path);
    size_t GetFileLength(const std::string& filePath) const;
    FSFileType GetFileType(const std::string& filePath) const;
    void GetFileMeta(const std::string& filePath, fslib::FileMeta& fileMeta) const;
    void Validate(const std::string& path, int64_t expectLength) const;

    SliceFilePtr CreateSliceFile(const std::string& path,
                                 uint64_t sliceLen, int32_t sliceNum);
    SliceFilePtr GetSliceFile(const std::string& path);

    SwapMmapFileReaderPtr CreateSwapMmapFileReader(const std::string& filePath);
    
    CompressFileReaderPtr CreateCompressSwapMmapFileReader(const std::string& filePath);

    SwapMmapFileWriterPtr CreateSwapMmapFileWriter(const std::string& filePath, size_t fileSize);
            
    void GetFileStat(const std::string& filePath, FileStat& fileStat);

    PackDirectoryPtr CreatePackDirectory(const std::string& packageFileName);

    FileReaderPtr CreateFileReaderWhenNotExistInCache(
            const std::string& filePath, FSOpenType type);

    CompressFileReaderPtr CreateCompressFileReaderWhenNotExistInCache(
            const std::string& filePath, FSOpenType type);

    CompressFileInfoPtr GetCompressFileInfo(const std::string& filePath);

    FSStorageType GetStorageType() const;
    
    void AddPathFileInfo(const FileInfo& fileInfo);
    void AddSolidPathFileInfos(const std::vector<FileInfo>::const_iterator& firstFileInfo,
                               const std::vector<FileInfo>::const_iterator& lastFileInfo);
    bool SetLifecycle(const std::string& lifecycle);
    storage::ArchiveFolderPtr GetArchiveFolder();

protected:
    virtual FSOpenType GetIntegratedOpenType(const std::string& filePath) = 0;
    void LoadCompressFileInfo(const std::string& filePath,
                              CompressFileInfo &compressInfo);

protected:
    std::string mRootDir;
    IndexlibFileSystemPtr mFileSystem;

private:    
    IE_LOG_DECLARE();
};

typedef std::vector<DirectoryPtr> DirectoryVector;

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FS_DIRECTORY_H
