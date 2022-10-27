#ifndef __INDEXLIB_DIRECTORY_CREATOR_H
#define __INDEXLIB_DIRECTORY_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file_system_define.h"

DECLARE_REFERENCE_CLASS(file_system, IndexlibFileSystem);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index_base, SegmentDirectory);

IE_NAMESPACE_BEGIN(file_system);

class DirectoryCreator
{
public:
    DirectoryCreator();
    ~DirectoryCreator();
public:
    static DirectoryPtr Create(
            const IndexlibFileSystemPtr& fileSystem,
            const std::string& absPath,
            FSStorageType storageType);

    static DirectoryPtr Get(
            const IndexlibFileSystemPtr& fileSystem,
            const std::string& absPath,
            bool throwExceptionIfNotExist);

    /**
     * @param enablePathMetaContainer: this will enable meta cache for FileSystem
     * @param isOffline: this will create a OfflineFileSystem which is optimized for remote FileSystem access 
     **/
    static DirectoryPtr Create(const std::string& absPath, bool useCache = false,
                               bool enablePathMetaContainer = true,
                               bool isOffline = false);
    static DirectoryPtr Create(const IndexlibFileSystemPtr& fileSystem,
                               const std::string& absPath);

private:
    static DirectoryPtr CreateDirectory(
            const IndexlibFileSystemPtr& fileSystem,
            const std::string& absPath,
            FSStorageType storageType);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DirectoryCreator);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_DIRECTORY_CREATOR_H
