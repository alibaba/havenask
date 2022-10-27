#include <sys/mman.h>
#include <list>
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/storage/async_buffered_file_wrapper.h"
#include "indexlib/storage/exception_trigger.h"
#include "indexlib/storage/common_file_wrapper.h"
#include "indexlib/misc/regular_expression.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(storage);
IE_LOG_SETUP(storage, FileSystemWrapper);

static autil::ThreadMutex fileSystemWrapperMockLock;

FileSystemWrapper::FileSystemWrapper()
{
}

bool FileSystemWrapper::IsError(const string& fileName, uint32_t errorType)
{
    if (ExceptionTrigger::CanTriggerException())
    {
        IE_LOG(FATAL, "file [%s] trigger exception [%d]", fileName.c_str(), errorType);
        return true;
    }

    ScopedLock lock(fileSystemWrapperMockLock);
    for (auto it = mTriggerErrorInfos.begin(); it != mTriggerErrorInfos.end(); ++it)
    {
        if (it->errorType == errorType && it->regex->Match(fileName) && it->triggerTimes > 0)
        {
            it->triggerTimes -= 1;
            if (it->triggerTimes <= 0)
            {
                mTriggerErrorInfos.erase(it);
            }
            return true;
        }
    }
    return false;
}

void FileSystemWrapper::Reset()
{
    ScopedLock lock(fileSystemWrapperMockLock);
    FileSystemWrapper::mTriggerErrorInfos = list<TriggerErrorInfo>();
    FileSystemWrapper::mMergeIOConfig = IOConfig();
    FileSystemWrapper::mWritePool.reset();
    FileSystemWrapper::mReadPool.reset();
}

void FileSystemWrapper::AtomicStoreIgnoreExist(
        const string& filePath, const char* data, size_t len)
{
    if (IsError(filePath, OPEN_ERROR))
    {
        INDEXLIB_THROW(misc::FileIOException, "Test open file FAILED.");
    }
    if (IsExistIgnoreError(filePath))
    {
        ErrorCode ec = FileSystem::remove(filePath);
        if (ec != EC_OK)
        {
            INDEXLIB_FATAL_ERROR(FileIO, "Remove temp file[%s] FAILED, error: %s", 
                   filePath.c_str(), GetErrorString(ec).c_str());
        }
    }
    return AtomicStore(filePath, data, len);
}

void FileSystemWrapper::AtomicStore(const string& filePath,
                                    const char* data, size_t len)

{
    if (IsError(filePath, OPEN_ERROR))
    {
        INDEXLIB_THROW(misc::FileIOException, "Test open file FAILED.");
    }

    if (IsExistIgnoreError(filePath))
    {
        INDEXLIB_FATAL_ERROR(Exist, "can't atomic store file: [%s], file already exist",
                             filePath.c_str());
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

bool FileSystemWrapper::AtomicLoad(const string& filePath,
                                   string& fileContent, bool ignoreErrorIfNotExit)
{
    if (IsError(filePath, OPEN_ERROR))
    {
        INDEXLIB_THROW(misc::FileIOException, "Test open file FAILED.");
    }

    if (!ignoreErrorIfNotExit && IsError(filePath, IS_EXIST_ERROR))
    {
        INDEXLIB_THROW(misc::FileIOException, "Test is exist FAILED.");
    }

    if (!IsExistIgnoreError(filePath))
    {
        if (ignoreErrorIfNotExit)
        {
            return false;
        }
        INDEXLIB_FATAL_ERROR(NonExist, "file not exist [%s]", 
                             filePath.c_str());
    }

    FileMeta fileMeta;
    ErrorCode ret = FileSystem::getFileMeta(filePath, fileMeta);
    if (ret != EC_OK)
    {
        INDEXLIB_FATAL_ERROR(FileIO, "Get file meta of [%s] FAILED, error: [%s]", 
               filePath.c_str(), GetErrorString(ret).c_str());
    }
    FilePtr file(FileSystem::openFile(filePath, READ));
    if (unlikely(!file))
    {
        INDEXLIB_FATAL_ERROR(FileIO, "Open file[%s] FAILED", filePath.c_str());
    }
    if (!file->isOpened())
    {
        INDEXLIB_FATAL_ERROR(FileIO, "Open file[%s] FAILED, error: [%s]", filePath.c_str(), 
               GetErrorString(file->getLastError()).c_str());
    }
    size_t fileLength = fileMeta.fileLength;
    fileContent.resize(fileLength);
    char* data = const_cast<char*>(fileContent.c_str());

    ssize_t readBytes = file->read(data, fileLength);
    if (readBytes != (ssize_t)fileLength)
    {
        INDEXLIB_FATAL_ERROR(FileIO, "Read file[%s] FAILED, error: [%s]",filePath.c_str(), 
               GetErrorString(file->getLastError()).c_str());
    }

    ret = file->close();
    if (ret != EC_OK)
    {
        INDEXLIB_FATAL_ERROR(FileIO, "Close file[%s] FAILED, error: [%s]",filePath.c_str(), 
               GetErrorString(ret).c_str());
    }
    return true;
}

FileWrapper* FileSystemWrapper::OpenFile(const string& fileName, Flag mode,
                                         bool useDirectIO, ssize_t fileLength)
{
    if (IsError(fileName, OPEN_ERROR))
    {
        INDEXLIB_THROW(misc::FileIOException, "Test open file FAILED.");
    }

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
    return new CommonFileWrapper(file);
}

BufferedFileWrapper* FileSystemWrapper::OpenBufferedFile(
        const string& fileName, Flag mode, 
        uint32_t bufferSize, bool async)
{
    if (IsError(fileName, OPEN_ERROR))
    {
        INDEXLIB_THROW(misc::FileIOException, "Test open file FAILED.");
    }
    File* file = FileSystem::openFile(fileName, mode);
    if (unlikely(!file))
    {
        INDEXLIB_FATAL_ERROR(FileIO, "Open file[%s] FAILED", fileName.c_str());
    }
    if (!file->isOpened())
    {
        ErrorCode ec = file->getLastError();
        delete file;
        INDEXLIB_FATAL_IO_ERROR(ec,
                                "Open file: [%s] FAILED",
                                fileName.c_str());
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
        ScopedLock lock(fileSystemWrapperMockLock);
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
    if (IsError(fileName, MMAP_ERROR))
    {
        INDEXLIB_THROW(misc::FileIOException, "Test mmap file FAILED.");
    }
    if (length == (size_t)(-1))
    {
        FileMeta fileMeta;
        GetFileMeta(fileName, fileMeta);
        length = fileMeta.fileLength;
    }
    if (length == 0)
    {
        return new MMapFile(fileName, -1, NULL, 0, 0, EC_OK);
    }
    assert(length != (size_t)-1 && length != 0);
    MMapFile* file = FileSystem::mmapFile(fileName, openMode, start, length,
            prot, mapFlag, offset);
    if (!file)
    {
        INDEXLIB_FATAL_ERROR(FileIO, "Mmap file: [%s] FAILED", fileName.c_str());
    }
    ErrorCode ec = file->getLastError();
    if (ec != fslib::EC_OK) {
        delete file;
        INDEXLIB_FATAL_IO_ERROR(ec, "Mmap file: [%s] FAILED", fileName.c_str());
    }
    return file;
}

void FileSystemWrapper::DeleteFile(const string& filePath)
{
    if (IsError(filePath, DELETE_FILE_ERROR))
    {
        INDEXLIB_THROW(misc::FileIOException, "Test delete file FAILED.");
    }

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
    if (IsError(filePath, IS_EXIST_ERROR))
    {
        INDEXLIB_THROW(misc::NonExistException, "Trigger NonExistException when IsExist [%s].", filePath.c_str());
    }
    if (IsError(filePath, IS_EXIST_IO_ERROR))
    {
        INDEXLIB_THROW(misc::FileIOException, "Trigger FileIOException when IsExist [%s].", filePath.c_str());
    }

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
    if (IsError(path, IS_EXIST_ERROR))
    {
        INDEXLIB_THROW(misc::FileIOException, "Test is exsit FAILED.");
    }

    list<string> pathList;
    string relativeDir;
    do {
        EntryList tmpList;
        string absoluteDir = JoinPath(path, relativeDir);
        ErrorCode ec = 
            FileSystem::listDir(absoluteDir, tmpList);

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
    if (IsError(dirPath, LIST_DIR_ERROR) && !ignoreError)
    {
        INDEXLIB_THROW(misc::FileIOException, "Test list dir FAILED.");
    }

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
    if (IsError(dirPath, DELETE_FILE_ERROR))
    {
        INDEXLIB_THROW(misc::FileIOException, "Test delete dir FAILED.");
    }
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
    if (IsError(path, DELETE_FILE_ERROR))
    {
        INDEXLIB_THROW(misc::FileIOException, "Test delete dir FAILED.");
    }
    if (FileSystemWrapper::IsExist(path))
    {
        ErrorCode ec = FileSystem::remove(path);
        if (ec != EC_OK)
        {
            INDEXLIB_FATAL_IO_ERROR(ec, "Delete path[%s] FAILED",
                    path.c_str());
        }
    }
}

//TODO: merge MkDir & MkDirIfNotExist
void FileSystemWrapper::MkDir(const string& dirPath, bool recursive,
                              bool ignoreExist)
{
    if (IsError(dirPath, MAKE_DIR_ERROR))
    {
        INDEXLIB_THROW(misc::FileIOException, "Test make dir FAILED.");
    }
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
    if (IsError(dirPath, MAKE_DIR_ERROR))
    {
        INDEXLIB_THROW(misc::FileIOException, "Test make dir FAILED.");
    }

    ErrorCode ec = FileSystem::mkDir(dirPath, true);
    if (ec != EC_OK && ec != EC_EXIST)
    {
        INDEXLIB_FATAL_IO_ERROR(ec, "Make directory : [%s] FAILED",
                                dirPath.c_str());
    }
}

void FileSystemWrapper::Rename(const string& srcName, const string& dstName)
{
    if (IsError(srcName, RENAME_ERROR))
    {
        INDEXLIB_THROW(misc::FileIOException, "Test rename FAILED.");
    }

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
    if (IsError(srcName, COPY_ERROR))
    {
        INDEXLIB_THROW(misc::FileIOException, "Test copy FAILED.");
    }

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
    if (IsError(fileName, GET_FILE_META_ERROR))
    {
        INDEXLIB_THROW(misc::FileIOException, "Trigger FileIOException when GetFileMeta [%s].", fileName.c_str());
    }
    ErrorCode ec = FileSystem::getFileMeta(fileName, fileMeta);
    if (ec == EC_NOENT)
    {
        INDEXLIB_FATAL_ERROR(NonExist, "File [%s] not exist.",
                             fileName.c_str());
    }
    else if (ec != EC_OK)
    {
        INDEXLIB_FATAL_IO_ERROR(ec, "Get file : [%s] meta FAILED",
                                fileName.c_str());
    }
}

size_t FileSystemWrapper::GetFileLength(const string& fileName, bool ignoreError)
{
    if (IsError(fileName, GET_FILE_META_ERROR))
    {
        INDEXLIB_THROW(misc::FileIOException, "Trigger FileIOException when GetFileLength [%s].", fileName.c_str());
    }
    FileMeta fileMeta;
    fileMeta.fileLength = 0;
    ErrorCode ec = FileSystem::getFileMeta(fileName, fileMeta);
    if (ec == EC_NOENT && !ignoreError)
    {
        INDEXLIB_THROW_WARN(NonExist, "File [%s] not exist.",
                            fileName.c_str());
    }
    else if (ec != EC_OK && !ignoreError)
    {
        INDEXLIB_FATAL_IO_ERROR(ec, "Get file : [%s] meta FAILED",
                                fileName.c_str());
    }
    return (size_t)fileMeta.fileLength;
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
    ScopedLock lock(fileSystemWrapperMockLock);
    mTriggerErrorInfos.clear();
}

void FileSystemWrapper::AddFileErrorType(const string& fileName, uint32_t errorType)
{
}

void FileSystemWrapper::SetError(uint32_t errorType, const string& filePattern, uint32_t triggerTimes)
{
    TriggerErrorInfo info;
    info.filePattern = filePattern;
    info.errorType = errorType;
    info.triggerTimes = triggerTimes;
    info.regex.reset(new RegularExpression);
    info.regex->Init(info.filePattern);
    
    ScopedLock lock(fileSystemWrapperMockLock);
    mTriggerErrorInfos.push_back(info);
}

IE_NAMESPACE_END(storage);

