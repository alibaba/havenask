#ifndef __INDEXLIB_PACK_DIRECTORY_H
#define __INDEXLIB_PACK_DIRECTORY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/directory.h"

DECLARE_REFERENCE_CLASS(file_system, PackageFileWriter);
IE_NAMESPACE_BEGIN(file_system);

class PackDirectory : public Directory
{
public:
    PackDirectory(const std::string& path, 
                  const IndexlibFileSystemPtr& indexFileSystem);

    ~PackDirectory();

public:
    void Init(const PackageFileWriterPtr& packageFileWriter);

    FileWriterPtr CreateFileWriter(
            const std::string& filePath,
            const FSWriterParam& param = FSWriterParam()) override;
    
    FileReaderPtr CreateFileReader(
            const std::string& filePath, FSOpenType type) override;

    FileReaderPtr CreateFileReaderWithoutCache(
            const std::string& filePath, FSOpenType type) override;

    PackageFileWriterPtr CreatePackageFileWriter(
            const std::string& filePath) override;

    PackageFileWriterPtr GetPackageFileWriter(
            const std::string& filePath) const override;

    DirectoryPtr MakeInMemDirectory(const std::string& dirName) override;

    DirectoryPtr MakeDirectory(const std::string& dirPath) override;

    DirectoryPtr GetDirectory(const std::string& dirPath,
                                             bool throwExceptionIfNotExist) override;

    size_t EstimateFileMemoryUse(const std::string& filePath, 
            FSOpenType type) override;

    InMemoryFilePtr CreateInMemoryFile(
            const std::string& path, uint64_t fileLen) override;

    void Rename(const std::string& srcPath, const DirectoryPtr& destDirectory,
                const std::string& destPath = "") override;

public:
    void ClosePackageFileWriter();

protected:
    FSOpenType GetIntegratedOpenType(const std::string& filePath) override;

    bool GetRelativePath(const std::string& packFileParentPath, 
                         const std::string& path,
                         std::string& relativePath) const;

private:
    friend class PackDirectoryTest;
    PackageFileWriterPtr mPackageFileWriter;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackDirectory);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_PACK_DIRECTORY_H
