#include "indexlib/file_system/link_directory.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, LinkDirectory);

LinkDirectory::LinkDirectory(const string& path, 
                             const IndexlibFileSystemPtr& indexFileSystem)
    : LocalDirectory(path, indexFileSystem)
{
    assert(path.find(FILE_SYSTEM_ROOT_LINK_NAME) != string::npos);
    assert(mFileSystem->IsExist(path));
}

LinkDirectory::~LinkDirectory() 
{
}

DirectoryPtr LinkDirectory::MakeInMemDirectory(const string& dirName)
{
    INDEXLIB_FATAL_ERROR(UnSupported,
                         "LinkDirectory [%s] not support MakeInMemDirectory",
                         mRootDir.c_str());
    return DirectoryPtr();
}

DirectoryPtr LinkDirectory::MakeDirectory(const string& dirPath)
{
    INDEXLIB_FATAL_ERROR(UnSupported,
                         "LinkDirectory [%s] not support MakeDirectory",
                         mRootDir.c_str());
    return DirectoryPtr();
}

DirectoryPtr LinkDirectory::GetDirectory(const string& dirPath,
        bool throwExceptionIfNotExist)
{
    string absPath = FileSystemWrapper::JoinPath(mRootDir, dirPath);
    if (!mFileSystem->IsExist(absPath))
    {
        if (throwExceptionIfNotExist)
        {
            INDEXLIB_THROW(misc::NonExistException, "directory [%s] does not exist.", 
                           absPath.c_str());
        }
        IE_LOG(DEBUG, "directory [%s] not exist", absPath.c_str());
        return DirectoryPtr();
    }
    if (!mFileSystem->IsDir(absPath))
    {
        INDEXLIB_THROW(misc::NonExistException, "[%s] does exist. but it is not directory.", 
                       absPath.c_str());
    }
    return DirectoryPtr(new LinkDirectory(absPath, mFileSystem));
}

FileWriterPtr LinkDirectory::CreateFileWriter(const string& filePath,
        const FSWriterParam& param)
{
    INDEXLIB_FATAL_ERROR(UnSupported,
                         "LinkDirectory [%s] not support CreateFileWriter",
                         mRootDir.c_str());
    return FileWriterPtr();
}

IE_NAMESPACE_END(file_system);

