#ifndef __INDEXLIB_MOCK_DIRECTORY_H
#define __INDEXLIB_MOCK_DIRECTORY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/directory.h"

IE_NAMESPACE_BEGIN(file_system);

class MockDirectory : public Directory
{
public:
    MockDirectory(const std::string& path)
        : Directory(path, IndexlibFileSystemPtr())
    {}
    ~MockDirectory() {}
    
public:
    MOCK_METHOD2(CreateFileWriter, FileWriterPtr(
                    const std::string& filePath,
                    const FSWriterParam& writerParam));
    MOCK_METHOD2(CreateFileReader, FileReaderPtr(
                    const std::string& filePath, FSOpenType type));
    MOCK_METHOD2(CreateFileReaderWithoutCache, FileReaderPtr(
                    const std::string& filePath, FSOpenType type));
    MOCK_METHOD1(CreateIntegratedFileReader, FileReaderPtr(
                    const std::string& filePath));

    MOCK_METHOD3(ListFile, void(const std::string& path, 
                                fslib::FileList& fileList, 
                                bool recursive));

    MOCK_METHOD1(MakeDirectory, DirectoryPtr(const std::string& dirPath));
    MOCK_METHOD1(MakeInMemDirectory, DirectoryPtr(const std::string& dirName));

    MOCK_METHOD2(GetDirectory, DirectoryPtr(const std::string& dirPath, bool throwExceptionIfNotExist));

    MOCK_CONST_METHOD2(GetFileMeta, void(
                    const std::string& filePath, 
                    fslib::FileMeta& fileMeta));

    MOCK_METHOD1(GetIntegratedOpenType, FSOpenType(const std::string&));

    MOCK_METHOD1(CreatePackageFileWriter, PackageFileWriterPtr(
                    const std::string& filePath));

    MOCK_CONST_METHOD1(GetPackageFileWriter, PackageFileWriterPtr(
                    const std::string& filePath));

    MOCK_METHOD1(MountPackageFile, void(const std::string& filePath));
    MOCK_METHOD2(EstimateFileMemoryUse, size_t(const std::string& filePath, 
                    FSOpenType type));
    MOCK_METHOD3(Rename, void(const std::string& srcPath, const DirectoryPtr& destDirectory,
                              const std::string& destPath));
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MockDirectory);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_MOCK_DIRECTORY_H
