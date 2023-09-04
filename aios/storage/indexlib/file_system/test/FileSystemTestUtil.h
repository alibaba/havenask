#pragma once

#include <stddef.h>
#include <string>

#include "autil/Log.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/WriterOption.h"

namespace indexlib { namespace file_system {
struct WriterOption;
}} // namespace indexlib::file_system

namespace indexlib { namespace file_system {

class FileSystemTestUtil
{
public:
    FileSystemTestUtil();
    ~FileSystemTestUtil();

public:
    static void CreateDiskFile(std::string filePath, std::string fileContext);
    static void CreateDiskFileDirectIO(const std::string& filePath, const std::string& fileContext);
    static void CreateDiskFiles(std::string dirPath, int fileCount, std::string fileContext);

    static void CreateInMemFile(std::shared_ptr<Storage> memStorage, const std::string& root,
                                const std::string& filePath, const std::string& fileContext,
                                const WriterOption& param = WriterOption());

    static void CreateInMemFiles(std::shared_ptr<Storage> memStorage, const std::string& root,
                                 const std::string& dirPath, int fileCount, const std::string& fileContext);

    static size_t GetFileLength(const std::string& fileName);

    static bool ClearFileCache(const std::string& filePath); // not sure
    static int CountPageInSystemCache(const std::string& filePath);

public:
    static bool CheckListFile(std::shared_ptr<IFileSystem> ifs, std::string path, bool recursive,
                              std::string fileListStr);
    static bool CheckFileStat(std::shared_ptr<IFileSystem> ifs, std::string path, FSStorageType storageType,
                              FSFileType fileType, FSOpenType openType, size_t fileLength, bool inCache, bool onDisk,
                              bool isDirectory);

    static void CleanEntryTables(const std::string& dirPath);

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FileSystemTestUtil> FileSystemTestUtilPtr;
}} // namespace indexlib::file_system
