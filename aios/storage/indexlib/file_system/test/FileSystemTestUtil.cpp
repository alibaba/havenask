#include "indexlib/file_system/test/FileSystemTestUtil.h"

#include <algorithm>
#include <errno.h>
#include <fcntl.h>
#include <iosfwd>
#include <malloc.h>
#include <memory>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/ListOption.h"
#include "indexlib/file_system/Storage.h"
#include "indexlib/file_system/file/FileWriterImpl.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"

namespace indexlib { namespace file_system {
struct WriterOption;
}} // namespace indexlib::file_system

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, FileSystemTestUtil);

FileSystemTestUtil::FileSystemTestUtil() {}

FileSystemTestUtil::~FileSystemTestUtil() {}

void FileSystemTestUtil::CreateDiskFile(string filePath, string fileContext)
{
    auto ec = FslibWrapper::AtomicStore(filePath, fileContext).Code();
    (void)ec;
}

void FileSystemTestUtil::CreateDiskFiles(string dirPath, int fileCount, string fileContext)
{
    for (int i = 0; i < fileCount; i++) {
        string filePath = PathUtil::JoinPath(dirPath, StringUtil::toString(i));
        CreateDiskFile(filePath, fileContext);
    }
}
void FileSystemTestUtil::CreateInMemFile(std::shared_ptr<Storage> memStorage, const std::string& root,
                                         const std::string& filePath, const std::string& fileContext,
                                         const WriterOption& param)
{
    FileWriterImpl::UpdateFileSizeFunc emptyUpdateFileSizeFunc = [](int64_t size) {};

    auto physicalPath = PathUtil::JoinPath(root, filePath);
    auto ret = memStorage->CreateFileWriter(filePath, physicalPath, param);
    if (ret.ec != FSEC_OK) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "Return error when create file writer for file [%s] in memory storage",
                             filePath.c_str());
    }

    auto fileWriter = ret.result;
    if (fileContext.size() > 0) {
        fileWriter->Write(fileContext.data(), fileContext.size()).GetOrThrow();
    }
    fileWriter->Close().GetOrThrow();
}

void FileSystemTestUtil::CreateInMemFiles(std::shared_ptr<Storage> memStorage, const std::string& root,
                                          const std::string& dirPath, int fileCount, const std::string& fileContext)
{
    for (int i = 0; i < fileCount; i++) {
        string filePath = dirPath + StringUtil::toString(i);
        CreateInMemFile(memStorage, root, filePath, fileContext);
    }
}

size_t FileSystemTestUtil::GetFileLength(const string& fileName)
{
    fslib::FileMeta fileMeta;
    fileMeta.fileLength = 0;
    fslib::ErrorCode ec = fslib::fs::FileSystem::getFileMeta(fileName, fileMeta);
    if (ec != fslib::EC_OK) {
        INDEXLIB_FATAL_ERROR(FileIO, "get filemeta [%s] meta failed, fslibec[%d]", fileName.c_str(), ec);
    }
    return (size_t)fileMeta.fileLength;
}

bool FileSystemTestUtil::CheckListFile(std::shared_ptr<IFileSystem> ifs, string path, bool recursive,
                                       string fileListStr)
{
    FileList expectFileList;
    StringUtil::fromString(fileListStr, expectFileList, ",");
    FileList actualFileList;
    if (ifs->ListDir(path, ListOption::Recursive(recursive), actualFileList)) {
        AUTIL_LOG(ERROR, "ListDir path[%s] failed", path.c_str());
        return false;
    }
    sort(expectFileList.begin(), expectFileList.end());

    if (actualFileList.size() != expectFileList.size()) {
        AUTIL_LOG(ERROR,
                  "size not equal, actual[%lu], expect[%lu],\n"
                  "ACTUAL [%s],\nEXPECT [%s]",
                  actualFileList.size(), expectFileList.size(), StringUtil::toString(actualFileList).c_str(),
                  StringUtil::toString(expectFileList).c_str());
        return false;
    }

    for (size_t i = 0; i < expectFileList.size(); ++i) {
        if (actualFileList[i] != expectFileList[i]) {
            AUTIL_LOG(ERROR,
                      "The fileList not match with index[%lu], "
                      "expectFile[%s], actualFile[%s]",
                      i, expectFileList[i].c_str(), actualFileList[i].c_str());
            return false;
        }
    }

    return true;
}

#define CHECK_FILE_STAT_ITEM(NAME)                                                                                     \
    if (NAME != fileStat.NAME) {                                                                                       \
        AUTIL_LOG(ERROR, #NAME " not equal. expect[%d], actual[%d]", (int)NAME, (int)fileStat.NAME);                   \
        return false;                                                                                                  \
    }

bool FileSystemTestUtil::CheckFileStat(std::shared_ptr<IFileSystem> ifs, string filePath, FSStorageType storageType,
                                       FSFileType fileType, FSOpenType openType, size_t fileLength, bool inCache,
                                       bool onDisk, bool isDirectory)
{
    // FileStat fileStat;
    // ifs->TEST_GetFileStat(filePath, fileStat);
    // CHECK_FILE_STAT_ITEM(storageType);
    // CHECK_FILE_STAT_ITEM(fileType);
    // CHECK_FILE_STAT_ITEM(openType);
    // CHECK_FILE_STAT_ITEM(fileLength);
    // CHECK_FILE_STAT_ITEM(inCache);
    // CHECK_FILE_STAT_ITEM(onDisk);
    // CHECK_FILE_STAT_ITEM(isDirectory);

    if (fileLength != ifs->GetFileLength(filePath).GetOrThrow()) {
        AUTIL_LOG(ERROR, "GetFileLength  not equal. expect[%d], acutal[%d]", (int)fileLength,
                  (int)ifs->GetFileLength(filePath).GetOrThrow());
        return false;
    }

    // if (fileType != ifs->GetFileType(filePath))
    // {
    //     AUTIL_LOG(ERROR, "GetFileType  not equal. expect[%d], acutal[%d]", (int)fileType,
    //     (int)ifs->GetFileType(filePath)); return false;
    // }

    return true;
}

bool FileSystemTestUtil::ClearFileCache(const string& filePath)
{
    struct stat st;
    if (stat(filePath.c_str(), &st) < 0) {
        AUTIL_LOG(ERROR, "stat localfile failed, path:%s", filePath.c_str());
        return false;
    }

    int fd = open(filePath.c_str(), O_RDONLY);
    if (fd < 0) {
        AUTIL_LOG(ERROR, "open localfile failed, path:%s", filePath.c_str());
        return false;
    }

    // clear cache by posix_fadvise
    if (posix_fadvise(fd, 0, st.st_size, POSIX_FADV_DONTNEED) != 0) {
        AUTIL_LOG(INFO, "Cache FADV_DONTNEED failed, %s", strerror(errno));
    } else {
        AUTIL_LOG(INFO, "Cache FADV_DONTNEED done");
    }
    close(fd);
    return true;
}

int FileSystemTestUtil::CountPageInSystemCache(const string& filePath)
{
    struct stat st;
    if (stat(filePath.c_str(), &st) < 0) {
        AUTIL_LOG(ERROR, "stat localfile failed, path:%s", filePath.c_str());
        return -1;
    }

    int fd = open(filePath.c_str(), O_RDONLY);
    if (fd < 0) {
        AUTIL_LOG(ERROR, "open localfile failed, path:%s", filePath.c_str());
        return -1;
    }

    int page_size = getpagesize();
    size_t calloc_size = (st.st_size + page_size - 1) / page_size;
    uint8_t* mincore_vec = (uint8_t*)calloc(1, calloc_size);
    void* addr = mmap((void*)0, st.st_size, PROT_NONE, MAP_SHARED, fd, 0);
    if (mincore(addr, st.st_size, mincore_vec) != 0) {
        AUTIL_LOG(ERROR, "mincore localfile failed, path:%s, error[%s]", filePath.c_str(), strerror(errno));
        free(mincore_vec);
        close(fd);
        return -1;
    }
    int cached = 0;
    for (size_t i = 0; i < calloc_size; ++i) {
        if (mincore_vec[i] & 1) {
            ++cached;
        }
    }
    free(mincore_vec);
    close(fd);
    return cached;
}

void FileSystemTestUtil::CreateDiskFileDirectIO(const string& filePath, const string& fileContext)
{
    int fd = open(filePath.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0644);
    if (fd < 0) {
        AUTIL_LOG(ERROR, "open localfile failed, path:%s", filePath.c_str());
        return;
    }

    // align by 512
    int alocate_size = (fileContext.size() + 512 - 1) / 512;
    void* write_buffer = memalign(512, 512 * alocate_size);
    if (!write_buffer) {
        AUTIL_LOG(ERROR, "Failed to alloc write buffer");
        free(write_buffer);
        close(fd);
        return;
    }
    memcpy(write_buffer, fileContext.c_str(), fileContext.size());
    if (write(fd, write_buffer, 512 * alocate_size) < 0) {
        AUTIL_LOG(ERROR, "write localfile failed, path:%s", filePath.c_str());
        free(write_buffer);
        close(fd);
        return;
    }

    free(write_buffer);
    close(fd);
    return;
}

void FileSystemTestUtil::CleanEntryTables(const std::string& dirPath)
{
    fslib::FileList fileList;
    FslibWrapper::ListDirE(dirPath, fileList);
    for (const std::string& name : fileList) {
        if (autil::StringUtil::startsWith(name, "entry_table")) {
            FslibWrapper::DeleteFileE(dirPath + "/" + name, DeleteOption::NoFence(false));
        }
    }
}
}} // namespace indexlib::file_system
