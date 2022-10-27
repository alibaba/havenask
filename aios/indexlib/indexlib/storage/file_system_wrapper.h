#ifndef __INDEXLIB_FILE_SYSTEM_WRAPPER_H
#define __INDEXLIB_FILE_SYSTEM_WRAPPER_H

#include <tr1/memory>
#include <autil/Lock.h>
#include <fslib/fslib.h>
#include <fslib/fs/FileSystem.h>
#include <fslib/fs/File.h>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/exception.h"
#include "indexlib/storage/io_config.h"

DECLARE_REFERENCE_CLASS(storage, FileWrapper);
DECLARE_REFERENCE_CLASS(storage, BufferedFileWrapper);
DECLARE_REFERENCE_CLASS(util, ThreadPool);
DECLARE_REFERENCE_CLASS(misc, RegularExpression);

IE_NAMESPACE_BEGIN(storage);

class FileSystemWrapper
{
public:
    FileSystemWrapper();
    ~FileSystemWrapper() {}

public:
    static void AtomicStoreIgnoreExist(
        const std::string& filePath,
        const std::string& fileContent)
    {
        AtomicStoreIgnoreExist(filePath, fileContent.c_str(), fileContent.size());
    }

    static void AtomicStoreIgnoreExist(
            const std::string& filePath,
            const char* data, size_t len);

    static void AtomicStore(const std::string& filePath,
                            const std::string& fileContent)
    {
        AtomicStore(filePath, fileContent.c_str(), fileContent.size());
    }

    static void AtomicStore(const std::string& filePath,
                            const char* data, size_t len);
    static bool AtomicLoad(const std::string& filePath,
                           std::string& loadedFileCont,
                           bool ignoreErrorIfNotExit = false);
    static bool AtomicLoad(FileWrapper* file, std::string& fileContent);
    
    static bool Store(const std::string& filePath, const std::string& fileContent, bool mayExist);
    static bool Load(const std::string& filePath, std::string& fileContent, bool mayNonExist);

public:
    static FileWrapper* OpenFile(const std::string& fileName,
                                 fslib::Flag mode, 
                                 bool useDirectIO = false, ssize_t fileLength = -1);

    // deprecated, use BufferFileWriter instead
    static BufferedFileWrapper* OpenBufferedFile(
            const std::string& fileName, fslib::Flag mode, 
            uint32_t bufferSize, bool async = false);

    static fslib::fs::MMapFile* MmapFile(const std::string& fileName,
            fslib::Flag openMode, char* start, size_t length,
            int prot, int mapFlag, off_t offset, ssize_t fileLength);

public:
    static void DeleteFile(const std::string& filePath);
    static bool IsExist(const std::string& filePath);
    static bool IsExistIgnoreError(const std::string& filePath);

    static bool IsDir(const std::string& path);

    static void ListDir(const std::string& dirPath,
                        fslib::FileList& fileList,
                        bool ignoreError = false);

    static void ListDirRecursive(const std::string& path,
                                 fslib::FileList& fileList,
                                 bool ignoreError = false);

    static bool Delete(const std::string& path, bool mayNonExist);
    static void DeleteDir(const std::string& dirPath);
    static void DeleteIfExist(const std::string& path);
    static void MkDir(const std::string& dirPath,
                      bool recursive = false,
                      bool ignoreExist = false);
    static void MkDirIfNotExist(const std::string &dirPath);
    static void Rename(const std::string& srcName,
                       const std::string& dstName);
    static void Copy(const std::string& srcName,
                     const std::string& dstName);

    static void GetFileMeta(const std::string& fileName,
                            fslib::FileMeta& fileMeta);

    static size_t GetFileLength(const std::string& fileName,
                                bool ignoreError = false);

    static int64_t GetDirectorySize(const std::string& path);

    static std::string JoinPath(const std::string &path,
                                const std::string &name);

    static std::string NormalizeDir(const std::string &path);

    static bool NeedMkParentDirBeforeOpen(const std::string &path);

    static bool SymLink(const std::string& srcPath, const std::string& dstPath);

public:
    static void SetMergeIOConfig(const IOConfig &mergeIOConfig);
    static const IOConfig & GetMergeIOConfig();

    static void Reset();

    static const util::ThreadPoolPtr& GetWriteThreadPool();
    static const util::ThreadPoolPtr& GetReadThreadPool();

public:
    static std::string GetErrorString(fslib::ErrorCode ec);

public:
    // for test
    static void ClearError();
    static void AddFileErrorType(const std::string& fileName, uint32_t errorType);
    static void SetError(uint32_t errorType, const std::string& fileName = ".*",
                         uint32_t triggerTimes = 1);
    static bool IsError(const std::string& fileName, uint32_t errorType);
    static const util::ThreadPoolPtr& GetThreadPool(fslib::Flag mode);

private:
    static void AtomicStore(const std::string& filePath,
                            const char* data, size_t len,
                            bool removeIfExist);

    static void InitThreadPool(util::ThreadPoolPtr &threadPool, 
                               uint32_t threadNum, uint32_t queueSize,
                               const std::string& name);
    static bool IsTempFile(const std::string& fileName);

public:
    // for test
    struct TriggerErrorInfo {
        std::string filePattern;
        uint32_t errorType;
        uint32_t triggerTimes;
        misc::RegularExpressionPtr regex;
    };
    
    static const uint32_t OPEN_ERROR = 0x01;
    static const uint32_t DELETE_FILE_ERROR = 0x02;
    static const uint32_t IS_EXIST_ERROR = 0x04;
    static const uint32_t LIST_DIR_ERROR = 0x08;
    static const uint32_t MAKE_DIR_ERROR = 0x10;
    static const uint32_t RENAME_ERROR = 0x20;
    static const uint32_t COPY_ERROR = 0x40;
    static const uint32_t GET_FILE_META_ERROR = 0x80;
    static const uint32_t MMAP_ERROR = 0x0100;
    static const uint32_t MMAP_LOCK_ONLY = 0x0200;
    static const uint32_t IS_EXIST_IO_ERROR = 0x0400;

    static std::list<TriggerErrorInfo> mTriggerErrorInfos;
    static const char* TEMP_SUFFIX;

private:
    static IOConfig mMergeIOConfig;

    // thread pool for async read/write
    static util::ThreadPoolPtr mWritePool;
    static util::ThreadPoolPtr mReadPool;
    static autil::ThreadMutex mWritePoolLock;
    static autil::ThreadMutex mReadPoolLock;

private:
    IE_LOG_DECLARE();
};

//////////////////////////
inline std::string FileSystemWrapper::GetErrorString(fslib::ErrorCode ec)
{
    return fslib::fs::FileSystem::getErrorString(ec);
}

IE_NAMESPACE_END(storage);


#endif //__INDEXLIB_FILE_SYSTEM_WRAPPER_H
