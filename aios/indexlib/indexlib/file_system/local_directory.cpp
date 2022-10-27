#include "indexlib/file_system/local_directory.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/misc/exception.h"
#include "indexlib/file_system/in_mem_directory.h"
#include "indexlib/file_system/directory_creator.h"

using namespace std;
using namespace fslib;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, LocalDirectory);

LocalDirectory::LocalDirectory(const string& path, 
                     const IndexlibFileSystemPtr& indexFileSystem)
    : Directory(path, indexFileSystem)
{
}

LocalDirectory::~LocalDirectory() 
{
}

FileWriterPtr LocalDirectory::CreateFileWriter(
    const string& filePath, const FSWriterParam& param)
{
    string absPath = FileSystemWrapper::JoinPath(mRootDir, filePath);
    return mFileSystem->CreateFileWriter(absPath, param);
}

FileReaderPtr LocalDirectory::CreateFileReader(const string& filePath, 
        FSOpenType type)
{
    string absPath = FileSystemWrapper::JoinPath(mRootDir, filePath);
    return mFileSystem->CreateFileReader(absPath, type);
}

FileReaderPtr LocalDirectory::CreateWritableFileReader(const string& filePath, 
        FSOpenType type)
{
    string absPath = FileSystemWrapper::JoinPath(mRootDir, filePath);
    return mFileSystem->CreateWritableFileReader(absPath, type);
}

FileReaderPtr LocalDirectory::CreateFileReaderWithoutCache(const string& filePath, 
            FSOpenType type)
{
    string absPath = FileSystemWrapper::JoinPath(mRootDir, filePath);
    return mFileSystem->CreateFileReaderWithoutCache(absPath, type);
}

size_t LocalDirectory::EstimateFileMemoryUse(
        const string& filePath, FSOpenType type)
{
    if (!IsExist(filePath))
    {
        return 0;
    }
    string absPath = FileSystemWrapper::JoinPath(mRootDir, filePath);
    return mFileSystem->EstimateFileLockMemoryUse(absPath, type);
}

bool LocalDirectory::OnlyPatialLock(const std::string& filePath) const
{
    string absPath = FileSystemWrapper::JoinPath(mRootDir, filePath);
    return mFileSystem->OnlyPatialLock(absPath);
}

FSOpenType LocalDirectory::GetIntegratedOpenType(const string& filePath)
{
    string absPath = FileSystemWrapper::JoinPath(mRootDir, filePath);
    FSOpenType openType = mFileSystem->GetLoadConfigOpenType(absPath);
    if (openType == FSOT_CACHE)
    {
        IE_LOG(DEBUG, "Failed to create fileReader on file [%s]," 
               "FSOT_LOAD_CONFIG does not support GetBaseAddress()",
               filePath.c_str());
        return FSOT_IN_MEM;
    }
    else if (openType == FSOT_MMAP || openType == FSOT_IN_MEM)
    {
        return FSOT_LOAD_CONFIG;
    }
    INDEXLIB_FATAL_ERROR(InconsistentState, 
                         "FileSystem return open type[%d]", openType);
    return FSOT_LOAD_CONFIG;
}

DirectoryPtr LocalDirectory::MakeDirectory(const string& dirPath)
{
    string absPath = FileSystemWrapper::JoinPath(mRootDir, dirPath);
    mFileSystem->MakeDirectory(absPath);
    return DirectoryPtr(new LocalDirectory(absPath, mFileSystem));
}

DirectoryPtr LocalDirectory::GetDirectory(const string& dirPath,
        bool throwExceptionIfNotExist)
{
    string absPath = FileSystemWrapper::JoinPath(mRootDir, dirPath);
    return DirectoryCreator::Get(mFileSystem, absPath, throwExceptionIfNotExist);
}

DirectoryPtr LocalDirectory::MakeInMemDirectory(const std::string& dirName)
{
    //TODO:
    string absPath = FileSystemWrapper::JoinPath(mRootDir, dirName);
    mFileSystem->MountInMemStorage(absPath);
    return DirectoryPtr(new InMemDirectory(absPath, mFileSystem));
}

PackageFileWriterPtr LocalDirectory::CreatePackageFileWriter(
            const string& filePath)
{
    INDEXLIB_FATAL_ERROR(
            UnSupported,
            "LocalDirectory not support CreatePackageFileWriter, for [%s]",
            filePath.c_str());
    return PackageFileWriterPtr();
}

PackageFileWriterPtr LocalDirectory::GetPackageFileWriter(
            const string& filePath) const
{
    INDEXLIB_FATAL_ERROR(
            UnSupported,
            "LocalDirectory not support GetPackageFileWriter, for [%s]",
            filePath.c_str());
    return PackageFileWriterPtr();
}

void LocalDirectory::Rename(const string& srcPath, const DirectoryPtr& destDirectory,
                            const string& destPath)
{
    string absSrcPath = FileSystemWrapper::JoinPath(mRootDir, srcPath);
    string absDestPath = FileSystemWrapper::JoinPath(destDirectory->GetPath(),
            (destPath.empty() ? srcPath : destPath));
    if (!DYNAMIC_POINTER_CAST(LocalDirectory, destDirectory))
    {
        assert(false);
        INDEXLIB_FATAL_ERROR(UnSupported,
                             "Only support LocalDirectory, rename from [%s] to [%s]",
                             absSrcPath.c_str(), absDestPath.c_str());
    }
    FileSystemWrapper::Rename(absSrcPath, absDestPath);
}

IE_NAMESPACE_END(file_system);

