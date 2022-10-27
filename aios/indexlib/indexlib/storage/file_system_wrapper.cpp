#include <sys/mman.h>
#include <unistd.h>
#include <list>
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/storage/async_buffered_file_wrapper.h"
#include "indexlib/storage/common_file_wrapper.h"
#include "indexlib/storage/write_control_file_wrapper.h"
#include "indexlib/storage/buffered_file_wrapper.h"
#include "indexlib/storage/common_file_wrapper.h"
#include "indexlib/storage/write_control_file_wrapper.h"
#include "indexlib/storage/buffered_file_wrapper.h"
#include "indexlib/util/thread_pool.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(storage);
IE_LOG_SETUP(storage, FileSystemWrapper);

list<FileSystemWrapper::TriggerErrorInfo> FileSystemWrapper::mTriggerErrorInfos = list<FileSystemWrapper::TriggerErrorInfo>();
const char* FileSystemWrapper::TEMP_SUFFIX = ".__tmp__";

IOConfig FileSystemWrapper::mMergeIOConfig = IOConfig();

util::ThreadPoolPtr FileSystemWrapper::mWritePool;
util::ThreadPoolPtr FileSystemWrapper::mReadPool;
autil::ThreadMutex FileSystemWrapper::mWritePoolLock;
autil::ThreadMutex FileSystemWrapper::mReadPoolLock;

FileSystemWrapper::FileSystemWrapper()
{
}

void FileSystemWrapper::Reset()
{
    FileSystemWrapper::mTriggerErrorInfos = list<FileSystemWrapper::TriggerErrorInfo>();
    FileSystemWrapper::mMergeIOConfig = IOConfig();
    FileSystemWrapper::mWritePool.reset();
    FileSystemWrapper::mReadPool.reset();
}

void FileSystemWrapper::AtomicStoreIgnoreExist(
        const string& filePath, const char* data, size_t len)
{
    AtomicStore(filePath, data, len, true);
}

void FileSystemWrapper::AtomicStore(const string& filePath,
                                    const char* data, size_t len)
{
    AtomicStore(filePath, data, len, false);
}

void FileSystemWrapper::AtomicStore(const string& filePath,
                                    const char* data, size_t len,
                                    bool removeIfExist)
{
    if (IsExistIgnoreError(filePath))
    {
        if (!removeIfExist)
        {
            INDEXLIB_FATAL_ERROR(Exist, "can't atomic store file: [%s], file already exist",
                    filePath.c_str());
        }

        IE_LOG(INFO, "delete exist file: %s", filePath.c_str());
        ErrorCode ec = FileSystem::remove(filePath);
        if (ec != EC_OK)
        {
            INDEXLIB_FATAL_ERROR(FileIO, "delete exist file: [%s] FAILED, error [%s]",
                    filePath.c_str(), GetErrorString(ec).c_str());
        }
    }
    string tempFilePath = filePath + TEMP_SUFFIX;
    if (IsExistIgnoreError(tempFilePath))
    {
        IE_LOG(WARN, "Remove temp file: %s.", tempFilePath.c_str());
        ErrorCode ec = FileSystem::remove(tempFilePath);
        if (ec != EC_OK)
        {
            INDEXLIB_FATAL_ERROR(FileIO, "Remove temp file: [%s] failed, error: %s",
                                    filePath.c_str(), GetErrorString(ec).c_str());
        }
    }
    FilePtr file(FileSystem::openFile(tempFilePath, WRITE));
    if (unlikely(!file))
    {
        INDEXLIB_FATAL_ERROR(FileIO, "open file: [%s] failed", tempFilePath.c_str());
    }
    if (!file->isOpened())
    {
        INDEXLIB_FATAL_ERROR(FileIO, "open file: [%s] failed",
                             tempFilePath.c_str());
    }

    ssize_t writeBytes = file->write(data, len);
    if (writeBytes != (ssize_t)len)
    {
        INDEXLIB_FATAL_ERROR(FileIO, "write file: [%s] failed, error: %s",
                             tempFilePath.c_str(), GetErrorString(file->getLastError()).c_str());
    }

    ErrorCode ec = file->close();
    if (ec != EC_OK)
    {
        INDEXLIB_FATAL_ERROR(FileIO, "close file: [%s] failed, error: %s",
                             tempFilePath.c_str(), GetErrorString(ec).c_str());

    }

    ec = FileSystem::rename(tempFilePath, filePath);
    if (ec != EC_OK)
    {
        INDEXLIB_FATAL_ERROR(FileIO, "rename file: [%s] to file: [%s] failed, error: %s",
                             tempFilePath.c_str(), filePath.c_str(), GetErrorString(ec).c_str());
    }
}

bool FileSystemWrapper::AtomicLoad(const string& filePath, string& fileContent, bool ignoreErrorIfNotExit)
{
    return Load(filePath, fileContent, ignoreErrorIfNotExit);
}

bool FileSystemWrapper::Load(const string& filePath, string& fileContent, bool mayNonExist)
{
    FilePtr file(FileSystem::openFile(filePath, READ));
    if (unlikely(!file))
    {
        INDEXLIB_FATAL_ERROR(FileIO, "Open file[%s] FAILED", filePath.c_str());
    }
    if (!file->isOpened())
    {
        ErrorCode ec = file->getLastError();
        if (ec == EC_NOENT)
        {
            if (mayNonExist)
            {
                IE_LOG(INFO, "file not exist [%s].", filePath.c_str());
                return false;
            }
            INDEXLIB_FATAL_ERROR(NonExist, "file not exist [%s].", filePath.c_str());
        }
        else
        {
            INDEXLIB_FATAL_ERROR(FileIO, "Open file[%s] FAILED, error: [%s]", filePath.c_str(), 
                    GetErrorString(file->getLastError()).c_str());
        }
    }

    fileContent.clear();
#define ATOMIC_LOAD_BUFF_SIZE (128 * 1024)
    char buffer[ATOMIC_LOAD_BUFF_SIZE];
    ssize_t readBytes = 0;
    do {
        readBytes = file->read(buffer, ATOMIC_LOAD_BUFF_SIZE);
        if (readBytes > 0)
        {
            if (fileContent.empty())
            {
                fileContent = string(buffer, readBytes);
            }
            else
            {
                fileContent.append(buffer, readBytes);
            }
        }
    } while (readBytes == ATOMIC_LOAD_BUFF_SIZE);
#undef ATOMIC_LOAD_BUFF_SIZE
    
    if (!file->isEof())
    {
        INDEXLIB_FATAL_ERROR(FileIO, "Read file[%s] FAILED, error: [%s]",filePath.c_str(), 
                             GetErrorString(file->getLastError()).c_str());
    }

    ErrorCode ret = file->close();
    if (ret != EC_OK)
    {
        INDEXLIB_FATAL_ERROR(FileIO, "Close file[%s] FAILED, error: [%s]",filePath.c_str(), 
               GetErrorString(ret).c_str());
    }
    return true;
}

bool FileSystemWrapper::AtomicLoad(FileWrapper* file, std::string& fileContent)
{
    fileContent.clear();
    if (!file)
    {
        return false;
    }
    int64_t readLength = 0;
    int64_t offset = 0;
    char buffer[131072];  // 128 * 1024
    while(true)
    {
        readLength = file->PRead(buffer, 128 * 1024, offset);
        if (readLength > 0) 
        {
            if (fileContent.empty())
            {
                fileContent = string(buffer, readLength);
            }
            else 
            {
                fileContent.append(buffer, readLength);
            }
            offset += readLength;
        }
        else 
        {
            break;
        }
    }
    return true;
}

bool FileSystemWrapper::Store(const string& filePath, const string& fileContent, bool mayExist)
{
    FilePtr file(FileSystem::openFile(filePath, WRITE));
    if (unlikely(!file))
    {
        INDEXLIB_FATAL_ERROR(FileIO, "open file: [%s] failed", filePath.c_str());        
    }
    if (!file->isOpened())
    {
        ErrorCode ec = file->getLastError();
        if (ec == EC_EXIST && mayExist)
        {
            return false;
        }
        INDEXLIB_FATAL_ERROR(FileIO, "open file: [%s] failed",
                             filePath.c_str());
    }

    ssize_t writeBytes = file->write(fileContent.data(), fileContent.size());
    if (writeBytes != (ssize_t)fileContent.size())
    {
        INDEXLIB_FATAL_ERROR(FileIO, "write file: [%s] failed, error: %s",
                             filePath.c_str(), GetErrorString(file->getLastError()).c_str());
    }

    ErrorCode ec = file->close();
    if (ec != EC_OK)
    {
        INDEXLIB_FATAL_ERROR(FileIO, "close file: [%s] failed, error: %s",
                             filePath.c_str(), GetErrorString(ec).c_str());

    }
    return true;
}

FileWrapper* FileSystemWrapper::OpenFile(const string& fileName, Flag mode,
                                         bool useDirectIO, ssize_t fileLength)
{
    // assert(fileLength < 0 || fileLength == (ssize_t)GetFileLength(fileName));
    File* file = FileSystem::openFile(fileName, mode, useDirectIO, fileLength);
    if (unlikely(!file))
    {
        INDEXLIB_FATAL_ERROR(FileIO, "Open file: [%s] FAILED", fileName.c_str());
    }
    if (!file->isOpened())
    {
        ErrorCode ec = file->getLastError();
        delete file;
        if (ec == EC_NOENT)
        {
            INDEXLIB_THROW_WARN(NonExist, "File [%s] not exist.",
                    fileName.c_str());
        }
        else
        {
            INDEXLIB_FATAL_IO_ERROR(ec, "Open file: [%s] FAILED",
                    fileName.c_str());
        }
    }
    return new CommonFileWrapper(file, useDirectIO);
}
BufferedFileWrapper* FileSystemWrapper::OpenBufferedFile(
        const string& fileName, Flag mode, 
        uint32_t bufferSize, bool async)
{
    File* file = FileSystem::openFile(fileName, mode);
    if (unlikely(!file))
    {
        INDEXLIB_FATAL_ERROR(FileIO, "Open file: [%s] FAILED", fileName.c_str());
    }
    if (!file->isOpened())
    {
        ErrorCode ec = file->getLastError();
        delete file;
        if (ec == EC_NOENT)
        {
            INDEXLIB_THROW_WARN(NonExist, "File [%s] not exist.",
                    fileName.c_str());
        }
        else
        {
            INDEXLIB_FATAL_IO_ERROR(ec, "Open file: [%s] FAILED",
                    fileName.c_str());
        }
    }
    int64_t fileLength = 0;
    if (mode == READ)
    {
        fileLength = GetFileLength(fileName);
    }
    if (!async)
    {
        return new BufferedFileWrapper(file, mode, fileLength, bufferSize);
    }
    else
    {
        const ThreadPoolPtr &threadPool = GetThreadPool(mode);
        return new AsyncBufferedFileWrapper(
                file, mode, fileLength, bufferSize, threadPool);
    }
}

const ThreadPoolPtr& FileSystemWrapper::GetThreadPool(Flag mode)
{
    if (mode == READ)
    {
        ScopedLock lock(mReadPoolLock);
        if (mReadPool)
        {
            return mReadPool;
        }
        InitThreadPool(mReadPool, mMergeIOConfig.readThreadNum, 
                       mMergeIOConfig.readQueueSize, "indexFSWRead");
        return mReadPool;
    }
    else
    {
        ScopedLock lock(mWritePoolLock);
        if (mWritePool)
        {
            return mWritePool;
        }
        InitThreadPool(mWritePool, mMergeIOConfig.writeThreadNum, 
                       mMergeIOConfig.writeQueueSize, "indexFSWWrite");
        return mWritePool;
    }
}

void FileSystemWrapper::InitThreadPool(
        ThreadPoolPtr &threadPool, uint32_t threadNum, uint32_t queueSize, const string& name)
{
    assert(!threadPool);
    threadPool.reset(new ThreadPool(threadNum, queueSize));
    threadPool->Start(name);
}

MMapFile* FileSystemWrapper::MmapFile(const string& fileName,
        Flag openMode, char* start, size_t length, 
        int prot, int mapFlag, off_t offset, ssize_t fileLength)
{
    // assert(fileLength < 0 || fileLength == (ssize_t)GetFileLength(fileName));
    if (openMode == READ)
    {
        if (length == (size_t)(-1))
        {
            length = fileLength > 0 ? fileLength : GetFileLength(fileName);
        }
        if (length == 0)
        {
            return new MMapFile(fileName, -1, NULL, 0, 0, EC_OK);
        }
    }
    assert(length != (size_t)-1 && length != 0);
    MMapFile* file = FileSystem::mmapFile(fileName, openMode, start, length,
                                          prot, mapFlag, offset, fileLength);
    if (!file)
    {
        INDEXLIB_FATAL_ERROR(FileIO, "Mmap file: [%s] FAILED", fileName.c_str());
    }
    if (!file->isOpened())
    {
        ErrorCode ec = file->getLastError();
        assert(ec != EC_OK);
        delete file;
        if (ec == EC_NOENT)
        {
            INDEXLIB_THROW_WARN(NonExist, "Mmap File: [%s] not exist.", fileName.c_str());
        }
        else if (ec != fslib::EC_OK)
        {
            INDEXLIB_FATAL_IO_ERROR(ec, "Mmap file: [%s] FAILED", fileName.c_str());
        }
    }
    return file;
}

bool FileSystemWrapper::Delete(const string& path, bool mayNonExist)
{
    ErrorCode ec = FileSystem::remove(path);
    if (ec == EC_NOENT)
    {
        if (mayNonExist)
        {
            return false;
        }
        INDEXLIB_FATAL_ERROR(NonExist, "File [%s] not exist.", path.c_str());
    }
    else if (ec != EC_OK)
    {
        INDEXLIB_FATAL_IO_ERROR(ec, "Delete file : [%s] FAILED", path.c_str());
    }
    return true;
}

void FileSystemWrapper::DeleteFile(const string& filePath)
{
    ErrorCode ec = FileSystem::remove(filePath);
    if (ec == EC_NOENT)
    {
        INDEXLIB_FATAL_ERROR(NonExist, "File [%s] not exist.",
                             filePath.c_str());
    }
    else if (ec != EC_OK)
    {
        INDEXLIB_FATAL_IO_ERROR(ec, "Delete file : [%s] FAILED",
                                filePath.c_str());
    }
}

bool FileSystemWrapper::IsExist(const string& filePath)
{
    IE_LOG(TRACE1, "IsExist [%s]", filePath.c_str());
    ErrorCode ec = FileSystem::isExist(filePath);
    if (ec == EC_TRUE)
    {
        return true;
    }
    else if (ec == EC_FALSE)
    {
        return false;
    }
    
    INDEXLIB_FATAL_ERROR(FileIO, "Determine existence of file [%s] "
                         "FAILED: [%s]", filePath.c_str(), 
                         FileSystemWrapper::GetErrorString(ec).c_str());
    return false;
}

bool FileSystemWrapper::IsExistIgnoreError(const string& filePath)
{
    IE_LOG(TRACE1, "IsExist [%s]", filePath.c_str());
    ErrorCode ec = FileSystem::isExist(filePath);
    if (ec == EC_TRUE)
    {
        return true;
    }
    else if (ec == EC_FALSE)
    {
        return false;
    }

    IE_LOG(ERROR, "Determine existence of file [%s] "
           "FAILED: [%s]", filePath.c_str(), 
           FileSystemWrapper::GetErrorString(ec).c_str());
    return false;
}

bool FileSystemWrapper::IsDir(const string& path)
{
    ErrorCode ec = FileSystem::isDirectory(path);
    if (ec == EC_TRUE)
    {
        return true;
    }
    else if (ec == EC_FALSE)
    {
        return false;
    }

    IE_LOG(ERROR, "Determine is directory of path [%s] "
           "FAILED: [%s]", path.c_str(), 
           FileSystemWrapper::GetErrorString(ec).c_str());
    return false;
}

bool FileSystemWrapper::IsTempFile(const string& filePath)
{
    size_t pos = filePath.rfind('.');
    return (pos != string::npos && filePath.substr(pos) == string(TEMP_SUFFIX));
}

void FileSystemWrapper::ListDirRecursive(
        const string &path, 
        FileList& fileList,
        bool ignoreError)
{
    list<string> pathList;
    string relativeDir;
    do {
        EntryList tmpList;
        string absoluteDir = JoinPath(path, relativeDir);
        ErrorCode ec = FileSystem::listDir(absoluteDir, tmpList);

        if (ec != EC_OK && !ignoreError)
        {
            if (ec == EC_NOENT)
            {
                INDEXLIB_FATAL_ERROR(NonExist, "Path [%s] not exist.",
                        absoluteDir.c_str());
            }
            if (ec == EC_NOTDIR)
            {
                INDEXLIB_FATAL_ERROR(InconsistentState, "Path [%s] is not a dir.",
                        absoluteDir.c_str());
            }
            INDEXLIB_FATAL_IO_ERROR(ec, "List directory : [%s] FAILED",
                    absoluteDir.c_str());
        }
        EntryList::iterator it = tmpList.begin();
        for (; it != tmpList.end(); it++)
        {
            if (IsTempFile(it->entryName))
            {
                continue;
            }
            string tmpName = JoinPath(relativeDir, it->entryName);
            if (it->isDir) {
                pathList.push_back(tmpName);
                fileList.push_back(NormalizeDir(tmpName));
            } else {
                fileList.push_back(tmpName);
            }
        }
        if (pathList.empty()) {
            break;
        }
        relativeDir = pathList.front();
        pathList.pop_front();
    } while (true);
}

void FileSystemWrapper::ListDir(const string& dirPath, 
                                FileList& fileList,
                                bool ignoreError)
{
    ErrorCode ec = FileSystem::listDir(dirPath, fileList);
    if (ignoreError)
    {
        return;
    }

    if (ec == EC_NOENT)
    {
        INDEXLIB_THROW_WARN(NonExist, "Directory [%s] not exist.",
                            dirPath.c_str());
    }
    else if (ec == EC_NOTDIR)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, "Path [%s] is not a dir.",
                             dirPath.c_str());
    }
    else if (ec != EC_OK)
    {
        INDEXLIB_FATAL_IO_ERROR(ec, "List directory : [%s] FAILED",
                                dirPath.c_str());
    }
}
    
void FileSystemWrapper::DeleteDir(const string& dirPath)
{
    ErrorCode ec = FileSystem::remove(dirPath);
    if (ec == EC_NOENT)
    {
        INDEXLIB_FATAL_ERROR(NonExist, "Directory [%s] not exist.",
                             dirPath.c_str());
    }
    else if (ec != EC_OK)
    {
        INDEXLIB_FATAL_IO_ERROR(ec, "Delete directory : [%s] FAILED",
                                dirPath.c_str());
    }
}

void FileSystemWrapper::DeleteIfExist(const string& path)
{
    ErrorCode ec = FileSystem::remove(path);
    if (ec == EC_NOENT)
    {
        IE_LOG(INFO, "target remove path [%s] not exist!", path.c_str());
        return;
    }
    
    if (ec != EC_OK)
    {
        INDEXLIB_FATAL_IO_ERROR(ec, "Delete : [%s] FAILED", path.c_str());
    }
    IE_LOG(INFO, "delete exist path: %s", path.c_str());
}

//TODO: merge MkDir & MkDirIfNotExist
void FileSystemWrapper::MkDir(const string& dirPath, bool recursive,
                              bool ignoreExist)
{
    ErrorCode ec = FileSystem::mkDir(dirPath, recursive);
    if (ec == EC_EXIST)
    {
        if (ignoreExist)
        {
            return;
        }
        INDEXLIB_FATAL_ERROR(Exist, "Directory [%s] already exist.",
                             dirPath.c_str());
    }
    else if (ec == EC_NOENT)
    {
        INDEXLIB_FATAL_ERROR(NonExist, "Parrent of directory [%s] not exist.",
                             dirPath.c_str());
    }
    else if (ec != EC_OK)
    {
        INDEXLIB_FATAL_IO_ERROR(ec, "Make directory : [%s] FAILED",
                                dirPath.c_str());
    }
}

void FileSystemWrapper::MkDirIfNotExist(const string& dirPath)
{
    ErrorCode ec = FileSystem::mkDir(dirPath, true);
    if (ec != EC_OK && ec != EC_EXIST)
    {
        INDEXLIB_FATAL_IO_ERROR(ec, "Make directory : [%s] FAILED",
                                dirPath.c_str());
    }
}

void FileSystemWrapper::Rename(const string& srcName, const string& dstName)
{
    ErrorCode ec = FileSystem::rename(srcName, dstName);
    if (ec == EC_NOENT)
    {
        INDEXLIB_FATAL_ERROR(NonExist, "Source file [%s] not exist.",
                             srcName.c_str());
    }
    else if (ec == EC_EXIST)
    {
        INDEXLIB_FATAL_ERROR(Exist, "Dest file [%s] already exist.",
                             dstName.c_str());
    }
    else if (ec != EC_OK)
    {
        INDEXLIB_FATAL_IO_ERROR(ec, "rename [%s] to [%s] FAILED",
                                srcName.c_str(), dstName.c_str());
    }
}

void FileSystemWrapper::Copy(const string& srcName, const string& dstName)
{
    ErrorCode ec = FileSystem::copy(srcName, dstName);
    if (ec == EC_NOENT)
    {
        INDEXLIB_FATAL_ERROR(NonExist, "Source file [%s] not exist.",
                             srcName.c_str());
    }
    else if (ec == EC_EXIST)
    {
        INDEXLIB_FATAL_ERROR(Exist, "Dest file [%s] already exist.",
                             dstName.c_str());
    }
    else if (ec != EC_OK)
    {
        INDEXLIB_FATAL_IO_ERROR(ec, "copy [%s] to [%s] FAILED",
                                srcName.c_str(), dstName.c_str());
    }
}
    
void FileSystemWrapper::GetFileMeta(const string& fileName, 
                                    FileMeta& fileMeta)
{
    IE_LOG(TRACE1, "GetFileMeta [%s]", fileName.c_str());
    ErrorCode ec = FileSystem::getFileMeta(fileName, fileMeta);
    if (ec == EC_NOENT)
    {
        INDEXLIB_THROW_WARN(NonExist, "File [%s] not exist.", fileName.c_str());
    }
    else if (ec != EC_OK)
    {
        INDEXLIB_FATAL_IO_ERROR(ec, "Get file : [%s] meta FAILED",
                                fileName.c_str());
    }
}

size_t FileSystemWrapper::GetFileLength(const string& fileName, bool ignoreError)
{
    FileMeta fileMeta;
    fileMeta.fileLength = 0;
    IE_LOG(TRACE1, "GetFileMeta [%s]", fileName.c_str());
    ErrorCode ec = FileSystem::getFileMeta(fileName, fileMeta);
    if (ec == EC_NOENT && !ignoreError)
    {
        INDEXLIB_THROW_WARN(NonExist, "File [%s] not exist.", fileName.c_str());
    }
    else if (ec != EC_OK && !ignoreError)
    {
        INDEXLIB_FATAL_IO_ERROR(ec, "Get file : [%s] meta FAILED",
                                fileName.c_str());
    }
    return (size_t)fileMeta.fileLength;
}

int64_t FileSystemWrapper::GetDirectorySize(const std::string& path)
{
    fslib::FileList fileList;
    FileSystemWrapper::ListDirRecursive(path, fileList, true);
    int64_t dirSize = 0;
    for (const auto& fileName : fileList)
    {
        string filePath = FileSystemWrapper::JoinPath(path, fileName);
        if (!FileSystemWrapper::IsDir(filePath))
        {
            dirSize += FileSystemWrapper::GetFileLength(filePath, true);
        }
    }
    return dirSize;
}

string FileSystemWrapper::JoinPath(const string &path, const string &name)
{
    if (path.empty()) 
    { 
        return name; 
    }

    if (*(path.rbegin()) != '/')
    {
        return path + "/" + name;
    } 

    return path + name;
}

string FileSystemWrapper::NormalizeDir(const string& dir)
{
    string tmpDir = dir;
    if (!tmpDir.empty() && *(tmpDir.rbegin()) != '/')
    {
        tmpDir += '/';
    }

    return tmpDir;
}

bool FileSystemWrapper::NeedMkParentDirBeforeOpen(const string &path)
{
    return FileSystem::getFsType(path) == FSLIB_FS_LOCAL_FILESYSTEM_NAME;
}

bool FileSystemWrapper::SymLink(const string& srcPath, const string& dstPath)
{
    if (FileSystem::getFsType(srcPath) != FSLIB_FS_LOCAL_FILESYSTEM_NAME)
    {
        IE_LOG(ERROR, "not support make symlink for dfs path[%s]", srcPath.c_str());
        return false;
    }
    
    int ret = symlink(srcPath.c_str(), dstPath.c_str());
    if (ret < 0)
    {
        IE_LOG(ERROR, "symlink src [%s] to dst [%s] fail, %s.",
               srcPath.c_str(), dstPath.c_str(), strerror(errno));
        return false;
    }
    return true;
}

void FileSystemWrapper::SetMergeIOConfig(const IOConfig &mergeIOConfig)
{
    mMergeIOConfig = mergeIOConfig;
}

const IOConfig & FileSystemWrapper::GetMergeIOConfig()
{
    return mMergeIOConfig;
}

const ThreadPoolPtr& FileSystemWrapper::GetWriteThreadPool()
{
    return mWritePool;
}

const ThreadPoolPtr& FileSystemWrapper::GetReadThreadPool()
{
    return mReadPool;
}

void FileSystemWrapper::ClearError()
{
}

void FileSystemWrapper::AddFileErrorType(const string& fileName, uint32_t errorType)
{
}

void FileSystemWrapper::SetError(uint32_t errorType, const string& fileName, uint32_t triggerTimes)
{
}

bool FileSystemWrapper::IsError(const string& fileName, uint32_t errorType)
{
    return false;
}

IE_NAMESPACE_END(storage);

