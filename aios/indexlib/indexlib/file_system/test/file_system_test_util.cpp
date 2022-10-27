#include <autil/StringUtil.h>
#include <fslib/fslib.h>
#include <autil/StringUtil.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <malloc.h>
#include "indexlib/file_system/test/file_system_test_util.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/path_util.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, FileSystemTestUtil);

FileSystemTestUtil::FileSystemTestUtil() 
{
}

FileSystemTestUtil::~FileSystemTestUtil() 
{
}

void FileSystemTestUtil::CreateDiskFile(string filePath, 
                                        string fileContext)
{
    FileSystemWrapper::AtomicStore(filePath, fileContext);
}

void FileSystemTestUtil::CreateDiskFiles(string dirPath, int fileCount,
        string fileContext)
{
    for (int i = 0; i < fileCount; i++)
    {
        string filePath = dirPath + StringUtil::toString(i);
        CreateDiskFile(filePath, fileContext);
    }
}

void FileSystemTestUtil::CreateInMemFile(
    InMemStoragePtr inMemStorage,
    string filePath,
    string fileContext,
    const FSWriterParam& param)
{
    FileWriterPtr fileWriter = 
        inMemStorage->CreateFileWriter(filePath, param); 
    if (fileContext.size() > 0)
    {
        fileWriter->Write(fileContext.data(), fileContext.size());
    }
    fileWriter->Close();
}

void FileSystemTestUtil::CreateInMemFiles(InMemStoragePtr inMemStorage,
        string dirPath, int fileCount, string fileContext)
{
    for (int i = 0; i < fileCount; i++)
    {
        string filePath = dirPath + StringUtil::toString(i);
        CreateInMemFile(inMemStorage, filePath, fileContext);
    }
}

size_t FileSystemTestUtil::GetFileLength(const string& fileName)
{
    FileMeta fileMeta;
    fileMeta.fileLength = 0;
    ErrorCode ec = FileSystem::getFileMeta(fileName, fileMeta);
    if (ec != EC_OK)
    {
        INDEXLIB_FATAL_IO_ERROR(ec, "Get file : [%s] meta FAILED",
                                fileName.c_str());
    }
    return (size_t)fileMeta.fileLength;
}

bool FileSystemTestUtil::CheckListFile(IndexlibFileSystemPtr ifs, string path, 
                                       bool recursive, string fileListStr)
{
    FileList expectFileList;
    StringUtil::fromString(fileListStr, expectFileList, ",");
    FileList actualFileList;
    ifs->ListFile(path, actualFileList, recursive);
    sort(expectFileList.begin(), expectFileList.end());

    if (actualFileList.size() != expectFileList.size())
    {
        IE_LOG(ERROR, "size not equal, actual[%lu], expect[%lu],\n"
               "ACTUAL [%s],\nEXPECT [%s]", 
               actualFileList.size(), expectFileList.size(),
               StringUtil::toString(actualFileList).c_str(),
               StringUtil::toString(expectFileList).c_str());
        return false;
    }

    for (size_t i = 0; i < expectFileList.size(); ++i)
    {
        if (actualFileList[i] != expectFileList[i])
        {
            IE_LOG(ERROR, "The fileList not match with index[%lu], "
                   "expectFile[%s], actualFile[%s]", 
                   i, expectFileList[i].c_str(), actualFileList[i].c_str());
            return false;
        }
    }

    return true;
}

#define CHECK_FILE_STAT_ITEM(NAME)                                      \
    if (NAME != fileStat.NAME)                                          \
    {                                                                   \
        IE_LOG(ERROR, #NAME" not equal. expect[%d], actual[%d]",        \
               (int)NAME, (int)fileStat.NAME);                          \
        return false;                                                   \
    }
bool FileSystemTestUtil::CheckFileStat(
        IndexlibFileSystemPtr ifs, string filePath, 
        FSStorageType storageType, FSFileType fileType, FSOpenType openType, 
        size_t fileLength, bool inCache, bool onDisk, bool isDirectory)
{
    FileStat fileStat;
    ifs->GetFileStat(filePath, fileStat);
    CHECK_FILE_STAT_ITEM(storageType);
    CHECK_FILE_STAT_ITEM(fileType);
    CHECK_FILE_STAT_ITEM(openType);
    CHECK_FILE_STAT_ITEM(fileLength);
    CHECK_FILE_STAT_ITEM(inCache);
    CHECK_FILE_STAT_ITEM(onDisk);
    CHECK_FILE_STAT_ITEM(isDirectory);
    if (fileLength != ifs->GetFileLength(filePath))
    {
        IE_LOG(ERROR, "GetFileLength  not equal. expect[%d], acutal[%d]"
               , (int)fileLength, (int)ifs->GetFileLength(filePath));
        return false;
    }
    if (fileType != ifs->GetFileType(filePath))
    {
        IE_LOG(ERROR, "GetFileType  not equal. expect[%d], acutal[%d]"
               , (int)fileType, (int)ifs->GetFileType(filePath));
        return false;
    }
    
    return true;
}

bool FileSystemTestUtil::IsOnDisk(IndexlibFileSystemPtr fileSystem,
                                  const string& filePath)
{
    FileStat fileStat;
    fileSystem->GetFileStat(filePath, fileStat);
    return fileStat.onDisk;
}

bool FileSystemTestUtil::ClearFileCache(const string& filePath)
{
    struct stat st;
    if (stat(filePath.c_str(), &st) < 0)
    {
        IE_LOG(ERROR, "stat localfile failed, path:%s", filePath.c_str());
        return false;
    }

    int fd = open(filePath.c_str(), O_RDONLY);
    if (fd < 0)
    {
        IE_LOG(ERROR, "open localfile failed, path:%s", filePath.c_str());
        return false;
    }

    //clear cache by posix_fadvise
    if (posix_fadvise(fd, 0, st.st_size, POSIX_FADV_DONTNEED) != 0)
    {
        IE_LOG(INFO, "Cache FADV_DONTNEED failed, %s", strerror(errno));
    }
    else
    {
        IE_LOG(INFO, "Cache FADV_DONTNEED done");
    }
    close(fd);
    return true;
}

int FileSystemTestUtil::CountPageInSystemCache(const string& filePath)
{
    struct stat st;
    if (stat(filePath.c_str(), &st) < 0)
    {
        IE_LOG(ERROR, "stat localfile failed, path:%s", filePath.c_str());
        return -1;
    }

    int fd = open(filePath.c_str(), O_RDONLY);
    if (fd < 0)
    {
        IE_LOG(ERROR, "open localfile failed, path:%s", filePath.c_str());
        return -1;
    }

    int page_size = getpagesize();
    size_t calloc_size = (st.st_size + page_size - 1) / page_size;
    uint8_t* mincore_vec = (uint8_t*)calloc(1, calloc_size);
    void* addr = mmap((void *)0, st.st_size, PROT_NONE, MAP_SHARED, fd, 0);    
    if (mincore(addr, st.st_size, mincore_vec) != 0)
    {
        IE_LOG(ERROR, "mincore localfile failed, path:%s, error[%s]",
               filePath.c_str(), strerror(errno));
        free(mincore_vec);
        close(fd);
        return -1;
    }
    int cached = 0;
    for (size_t i = 0; i < calloc_size; ++i)
    {
        if (mincore_vec[i] & 1)
        {
            ++cached;
        }
    }
    free(mincore_vec);
    close(fd);
    return cached;
}

void FileSystemTestUtil::CreateDiskFileDirectIO(
        const string& filePath, const string& fileContext)
{
    int fd = open(filePath.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0644);
    if (fd < 0)
    {
        IE_LOG(ERROR, "open localfile failed, path:%s", filePath.c_str());
        return;
    }

    // align by 512
    int alocate_size = (fileContext.size() + 512 - 1) / 512;
    void* write_buffer = memalign(512, 512 * alocate_size);
    if (!write_buffer)
    {
        IE_LOG(ERROR, "Failed to alloc write buffer");
        free(write_buffer);
        close(fd);
        return;
    }
    memcpy(write_buffer, fileContext.c_str(), fileContext.size());
    if (write(fd, write_buffer, 512 * alocate_size) < 0)
    {
        IE_LOG(ERROR, "write localfile failed, path:%s", filePath.c_str());
        free(write_buffer);
        close(fd);
        return;
    }

    free(write_buffer);
    close(fd);
    return;
}

IE_NAMESPACE_END(file_system);
