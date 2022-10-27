#ifndef __INDEXLIB_TEST_FILE_CREATOR_H
#define __INDEXLIB_TEST_FILE_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/indexlib_file_system.h"

IE_NAMESPACE_BEGIN(file_system);

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
    static FileWriterPtr CreateWriter(
            const IndexlibFileSystemPtr& fileSystem,
            FSStorageType storageType, FSOpenType openType, 
            const std::string& content = "",
            const std::string& path = "",
            const FSWriterParam& writerParam = FSWriterParam());

    static FileReaderPtr CreateReader(const IndexlibFileSystemPtr& fileSystem,
            FSStorageType storageType, FSOpenType openType, 
            const std::string& content = "",
            const std::string& path = ""); 

    static std::vector<FileReaderPtr> CreateInPackageFileReader(
            const IndexlibFileSystemPtr& fileSystem,
            FSStorageType storageType, FSOpenType openType, 
            const std::vector<std::string>& innerFileContents,
            const std::string& path = ""); 

    static SliceFileReaderPtr CreateSliceFileReader(
            const IndexlibFileSystemPtr& fileSystem, FSStorageType storageType, 
            const std::string& content = "", const std::string& path = "");

    // path is empty: path = GetRootPath() + DEFAULT_DIRECTORY + DEFAULT_FILE_NAME
    // fileCount == 1: filePath = $path
    // fileCount >  1: filePath = $path$fileCount
    static void CreateFiles(const IndexlibFileSystemPtr& fileSystem,
                            FSStorageType storageType, FSOpenType openType,
                            const std::string& fileContext = "",
                            const std::string& path = "",
                            int32_t fileCount = 1);

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
    static void CreateFileTree(const IndexlibFileSystemPtr& fileSystem,
                               const std::string& pathsStr);

private:
    static std::string GetDefaultFilePath(const std::string& path);
    static void CreateOneDirectory(const IndexlibFileSystemPtr& fileSystem,
                                   const std::string& dirEntry);
    static void CreateOnePath(const IndexlibFileSystemPtr& fileSystem,
                              const std::string& pathEntry);

    static void CreatePackageFile(
            const IndexlibFileSystemPtr& fileSystem,
            FSStorageType storageType, 
            const std::vector<std::string>& content,
            const std::string& path);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TestFileCreator);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_TEST_FILE_CREATOR_H
