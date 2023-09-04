#pragma once

#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/file/SliceFileReader.h"

namespace indexlib { namespace file_system {
struct WriterOption;
}} // namespace indexlib::file_system

namespace indexlib { namespace file_system {

#define DEFAULT_DIRECTORY "default_path"
#define DEFAULT_FILE_NAME "default_file"
#define DEFAULT_PACKAGE_FILE_NAME "default_package_file"
static const int32_t DEFAULT_SLICE_LEN = 4 * 1024;
static const int32_t DEFAULT_SLICE_NUM = 1;

class TestFileCreator
{
public:
    TestFileCreator();
    ~TestFileCreator();

public:
    static std::shared_ptr<FileWriter> CreateWriter(const std::shared_ptr<IFileSystem>& fileSystem,
                                                    FSStorageType storageType, FSOpenType openType,
                                                    const std::string& content = "", const std::string& path = "",
                                                    const WriterOption& writerParam = WriterOption());

    static std::shared_ptr<FileReader> CreateReader(const std::shared_ptr<IFileSystem>& fileSystem,
                                                    FSStorageType storageType, FSOpenType openType,
                                                    const std::string& content = "", const std::string& path = "");

    static std::shared_ptr<SliceFileReader> CreateSliceFileReader(const std::shared_ptr<IFileSystem>& fileSystem,
                                                                  FSStorageType storageType,
                                                                  const std::string& content = "",
                                                                  const std::string& path = "");

    // path is empty: path = DEFAULT_DIRECTORY + DEFAULT_FILE_NAME
    // fileCount == 1: filePath = $path
    // fileCount >  1: filePath = $path$fileCount
    static void CreateFiles(const std::shared_ptr<IFileSystem>& fileSystem, FSStorageType storageType,
                            FSOpenType openType, const std::string& fileContext = "", const std::string& path = "",
                            int32_t fileCount = 1);

    static void CreateFile(const std::shared_ptr<IFileSystem>& fileSystem, FSStorageType storageType,
                           FSOpenType openType, const std::string& fileContext = "", const std::string& path = "");

    // Example: #:local,inmem^I,file^F; #/local:local_0; #/inmem:inmem_0
    // Root
    // |-- file
    // |-- inmem
    // |   `-- inmem_0
    // `-- local
    //     `-- local_0
    //
    // # : root of filesystem
    // ^I: it's InMemStorage Directory
    // ^F: it's file name
    static void CreateFileTree(const std::shared_ptr<IFileSystem>& fileSystem, const std::string& pathsStr);

private:
    static std::string GetDefaultFilePath(const std::string& path);
    static void CreateOneDirectory(const std::shared_ptr<IFileSystem>& fileSystem, const std::string& dirEntry);
    static void CreateOnePath(const std::shared_ptr<IFileSystem>& fileSystem, const std::string& pathEntry);

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<TestFileCreator> TestFileCreatorPtr;
}} // namespace indexlib::file_system
