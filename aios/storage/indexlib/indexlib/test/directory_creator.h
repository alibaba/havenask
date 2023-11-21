#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemOptions.h"

DECLARE_REFERENCE_CLASS(file_system, IFileSystem);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index_base, SegmentDirectory);

namespace indexlib { namespace test {

class DirectoryCreator
{
public:
    DirectoryCreator();
    ~DirectoryCreator();

public:
    // old api, todo rm
    static file_system::DirectoryPtr Create(const file_system::IFileSystemPtr& fileSystem, const std::string& path,
                                            bool isAbsPath = true);

    static file_system::DirectoryPtr Get(const file_system::IFileSystemPtr& fileSystem, const std::string& path,
                                         bool throwExceptionIfNotExist, bool isAbsPath = true);

    /**
     * @param isOffline: this will create a OfflineFileSystem which is optimized for remote FileSystem access
     **/
    static file_system::DirectoryPtr Create(const std::string& absPath, bool useCache = false, bool isOffline = false);

    static file_system::DirectoryPtr Create(const std::string& absPath,
                                            const file_system::FileSystemOptions& fsOptions);

public:
    // new api
    // dirPath = "" for GetRootDirectory
    static file_system::DirectoryPtr GetDirectory(const file_system::IFileSystemPtr& fileSystem,
                                                  const std::string& dirPath = "");
    static file_system::DirectoryPtr CreateOfflineRootDir(const std::string& rootPath);

private:
    static file_system::DirectoryPtr CreateDirectory(const file_system::IFileSystemPtr& fileSystem,
                                                     const std::string& path, bool isAbsPath);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DirectoryCreator);
}} // namespace indexlib::test
