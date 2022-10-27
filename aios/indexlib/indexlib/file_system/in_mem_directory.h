#ifndef __INDEXLIB_FS_IN_MEM_DIRECTORY_H
#define __INDEXLIB_FS_IN_MEM_DIRECTORY_H

#include <tr1/memory>
#include <fslib/fslib.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/directory.h"

IE_NAMESPACE_BEGIN(file_system);

class InMemDirectory : public Directory
{
public:
    InMemDirectory(const std::string& path, 
              const IndexlibFileSystemPtr& indexFileSystem);
    ~InMemDirectory();

public:
    FileWriterPtr CreateFileWriter(
            const std::string& filePath,
            const FSWriterParam& param = FSWriterParam()) override;
    
    FileReaderPtr CreateFileReader(
            const std::string& filePath, FSOpenType type) override;
    FileReaderPtr CreateWritableFileReader(const std::string& filePath, FSOpenType type) override
    {
        return CreateFileReader(filePath, type);
    }
    FileReaderPtr CreateFileReaderWithoutCache(const std::string& filePath, 
            FSOpenType type) override;
    DirectoryPtr MakeInMemDirectory(const std::string& dirName) override;
    DirectoryPtr MakeDirectory(const std::string& dirPath) override;
    DirectoryPtr GetDirectory(const std::string& dirPath,
                                            bool throwExceptionIfNotExist) override;

    PackageFileWriterPtr CreatePackageFileWriter(
            const std::string& filePath) override;

    PackageFileWriterPtr GetPackageFileWriter(
            const std::string& filePath) const override;

    size_t EstimateFileMemoryUse(const std::string& filePath,
            FSOpenType type) override;

    void RemoveDirectory(const std::string& dirPath, bool mayNonExist) override;
    void RemoveFile(const std::string& filePath, bool mayNonExist) override;
    void Rename(const std::string& srcPath, const DirectoryPtr& destDirectory,
                const std::string& destPath = "") override;
    
protected:
    FSOpenType GetIntegratedOpenType(const std::string& filePath) override;

private:    
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemDirectory);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FS_IN_MEM_DIRECTORY_H
