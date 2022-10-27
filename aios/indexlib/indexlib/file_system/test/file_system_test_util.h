#ifndef __INDEXLIB_FILE_SYSTEM_TEST_UTIL_H
#define __INDEXLIB_FILE_SYSTEM_TEST_UTIL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/in_mem_storage.h"
#include "indexlib/file_system/indexlib_file_system.h"

IE_NAMESPACE_BEGIN(file_system);

class FileSystemTestUtil
{
public:
    FileSystemTestUtil();
    ~FileSystemTestUtil();

public:
    static void CreateDiskFile(std::string filePath, std::string fileContext);
    static void CreateDiskFileDirectIO(const std::string& filePath,
            const std::string& fileContext);
    static void CreateDiskFiles(std::string dirPath, int fileCount, 
                                std::string fileContext);
    
    static void CreateInMemFile(InMemStoragePtr inMemStorage,
                                std::string filePath,
                                std::string fileContext,
                                const FSWriterParam& param = FSWriterParam());
    
    static void CreateInMemFiles(InMemStoragePtr inMemStorage,
                                 std::string dirPath, 
                                 int fileCount, std::string fileContext);
    
    static size_t GetFileLength(const std::string& fileName);

    static bool ClearFileCache(const std::string& filePath); // not sure
    static int CountPageInSystemCache(const std::string& filePath);

public: 
    static bool CheckListFile(IndexlibFileSystemPtr ifs, std::string path,
                              bool recursive, std::string fileListStr);
    static bool CheckFileStat(IndexlibFileSystemPtr ifs, std::string path, 
                              FSStorageType storageType,
                              FSFileType fileType, FSOpenType openType, 
                              size_t fileLength, bool inCache, bool onDisk,
                              bool isDirectory);
    static bool IsOnDisk(IndexlibFileSystemPtr fileSystem,
                         const std::string& filePath);
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FileSystemTestUtil);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FILE_SYSTEM_TEST_UTIL_H
