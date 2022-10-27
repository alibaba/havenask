#include "indexlib/file_system/in_mem_directory.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/file_system/link_directory.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/path_util.h"

using namespace std;
using namespace fslib;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, InMemDirectory);

InMemDirectory::InMemDirectory(const string& path, 
                     const IndexlibFileSystemPtr& indexFileSystem)
    : Directory(path, indexFileSystem)
{
}

InMemDirectory::~InMemDirectory() 
{
}

FileWriterPtr InMemDirectory::CreateFileWriter(
    const string& filePath, const FSWriterParam& param)
{
    string absPath = FileSystemWrapper::JoinPath(mRootDir, filePath);
    return mFileSystem->CreateFileWriter(absPath, param);
}

FileReaderPtr InMemDirectory::CreateFileReaderWithoutCache(const string& filePath, 
                                                               FSOpenType type)
{
    string absPath = FileSystemWrapper::JoinPath(mRootDir, filePath);
    return mFileSystem->CreateFileReaderWithoutCache(absPath, FSOT_IN_MEM);    
}

FileReaderPtr InMemDirectory::CreateFileReader(const string& filePath, 
        FSOpenType type)
{
    string absPath = FileSystemWrapper::JoinPath(mRootDir, filePath);
    return mFileSystem->CreateFileReader(absPath, FSOT_IN_MEM);
}

DirectoryPtr InMemDirectory::MakeDirectory(const string& dirPath)
{
    string absPath = FileSystemWrapper::JoinPath(mRootDir, dirPath);
    mFileSystem->MakeDirectory(absPath);

    return DirectoryPtr(new InMemDirectory(absPath, mFileSystem));
}

size_t InMemDirectory::EstimateFileMemoryUse(const string& filePath, FSOpenType type)
{
    if (!IsExist(filePath))
    {
        return 0;
    }
    string absPath = FileSystemWrapper::JoinPath(mRootDir, filePath);
    return mFileSystem->EstimateFileLockMemoryUse(absPath, FSOT_IN_MEM);
}

DirectoryPtr InMemDirectory::MakeInMemDirectory(const std::string& dirName)
{
    return MakeDirectory(dirName);
}

DirectoryPtr InMemDirectory::GetDirectory(const string& dirPath,
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
    return DirectoryPtr(new InMemDirectory(absPath, mFileSystem));
}

FSOpenType InMemDirectory::GetIntegratedOpenType(const string& filePath)
{
    return FSOT_IN_MEM;
}

PackageFileWriterPtr InMemDirectory::CreatePackageFileWriter(
            const string& filePath)
{
    string absPath = FileSystemWrapper::JoinPath(mRootDir, filePath);
    assert(mFileSystem);
    return mFileSystem->CreatePackageFileWriter(absPath);
}

PackageFileWriterPtr InMemDirectory::GetPackageFileWriter(
            const string& filePath) const
{
    string absPath = FileSystemWrapper::JoinPath(mRootDir, filePath);
    assert(mFileSystem);
    return mFileSystem->GetPackageFileWriter(absPath);
}

void InMemDirectory::RemoveDirectory(const string& dirPath, bool mayNonExist)
{
    DirectoryPtr linkDir = CreateLinkDirectory();
    if (linkDir && linkDir->IsExist(dirPath))
    {
        IE_LOG(INFO, "Remove directory [%s] in link path [%s]",
               dirPath.c_str(), linkDir->GetPath().c_str());
        Sync(true);
        linkDir->RemoveDirectory(dirPath, mayNonExist);
        if (IsExist(dirPath))
        {
            Directory::RemoveDirectory(dirPath, mayNonExist);
        }
    }
    else
    {
        Directory::RemoveDirectory(dirPath, mayNonExist);
    }
}

void InMemDirectory::RemoveFile(const string& filePath, bool mayNonExist)
{
    DirectoryPtr linkDir = CreateLinkDirectory();
    if (linkDir && linkDir->IsExist(filePath))
    {
        IE_LOG(INFO, "Remove file [%s] in link path [%s]",
               filePath.c_str(), linkDir->GetPath().c_str());
        Sync(true);
        linkDir->RemoveFile(filePath, false);
        Directory::RemoveFile(filePath, true);
    }
    else
    {
        Directory::RemoveFile(filePath, mayNonExist);
    }
}

void InMemDirectory::Rename(const string& srcPath, const DirectoryPtr& destDirectory,
                            const string& destPath)
{
    assert(false);

    string absSrcPath = FileSystemWrapper::JoinPath(mRootDir, srcPath);
    string absDestPath = FileSystemWrapper::JoinPath(destDirectory->GetPath(),
            (destPath.empty() ? srcPath : destPath));
    INDEXLIB_FATAL_ERROR(UnSupported,
                         "Only support LocalDirectory, rename from [%s] to [%s]",
                         absSrcPath.c_str(), absDestPath.c_str());
}

IE_NAMESPACE_END(file_system);

