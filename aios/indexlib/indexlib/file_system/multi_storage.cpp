#include "indexlib/file_system/multi_storage.h"
#include "indexlib/util/path_util.h"

using namespace std;
using namespace fslib;

IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, MultiStorage);

MultiStorage::MultiStorage(const InMemStoragePtr& inMemStorage, 
                           const DiskStoragePtr& diskStorage,
                           const PackageStoragePtr& packageStorage,
                           const util::BlockMemoryQuotaControllerPtr& memController) 
    : Storage(inMemStorage->GetRootPath(), FSST_LOCAL, memController)
    , mInMemStorage(inMemStorage)
    , mDiskStorage(diskStorage)
    , mPackageStorage(packageStorage)
{
    assert(inMemStorage);
    assert(diskStorage);
    assert(inMemStorage->GetRootPath() == diskStorage->GetRootPath());
}

MultiStorage::~MultiStorage() 
{
}

FileWriterPtr MultiStorage::CreateFileWriter(
    const string& filePath, const FSWriterParam& param)
{
    INDEXLIB_FATAL_ERROR(InconsistentState, "File [%s]", filePath.c_str());    
    return FileWriterPtr();
}

PackageFileWriterPtr MultiStorage::CreatePackageFileWriter(
            const string& filePath)
{
    INDEXLIB_FATAL_ERROR(UnSupported,
                         "MultiStorage not support CreatePackageFileWriter"
                         ", filePath [%s]", filePath.c_str());
    return PackageFileWriterPtr();
}

PackageFileWriterPtr MultiStorage::GetPackageFileWriter(
            const string& filePath)
{
    INDEXLIB_FATAL_ERROR(UnSupported,
                         "MultiStorage not support GetPackageFileWriter"
                         ", filePath [%s]", filePath.c_str());
    return PackageFileWriterPtr();
}

FileReaderPtr MultiStorage::CreateFileReader(const string& filePath,
        FSOpenType type)
{
    INDEXLIB_FATAL_ERROR(InconsistentState, "File [%s]", filePath.c_str());
    return FileReaderPtr();
}

FileReaderPtr MultiStorage::CreateWritableFileReader(const string& filePath,
        FSOpenType type)
{
    INDEXLIB_FATAL_ERROR(InconsistentState, "File [%s]", filePath.c_str());
    return FileReaderPtr();
}

FileReaderPtr MultiStorage::CreateFileReaderWithoutCache(const std::string& filePath,
        FSOpenType type)
{
    INDEXLIB_FATAL_ERROR(InconsistentState, "File [%s]", filePath.c_str());
    return FileReaderPtr();
}

void MultiStorage::MakeDirectory(const string& dirPath, bool recursive)
{
    INDEXLIB_FATAL_ERROR(Exist, "Dir [%s] has exist", dirPath.c_str());
}

void MultiStorage::RemoveFile(const std::string& filePath, bool mayNonExist)
{
    INDEXLIB_FATAL_ERROR(InconsistentState, "File [%s]", filePath.c_str());
}

void MultiStorage::RemoveDirectory(const string& dirPath, bool mayNonExist)
{
    assert(!mPackageStorage);
    string normPath = PathUtil::NormalizePath(dirPath);
    FileList rootList;
    mInMemStorage->ListRoot(normPath, rootList, true);
    for (size_t i = 0; i < rootList.size(); ++i)
    {
        string absRoot = PathUtil::JoinPath(normPath, rootList[i]);
        mInMemStorage->RemoveDirectory(absRoot, mayNonExist);
    }
    
    mDiskStorage->RemoveDirectory(normPath, mayNonExist);
}

size_t MultiStorage::EstimateFileLockMemoryUse(const string& path,
        FSOpenType type)
{
    INDEXLIB_FATAL_ERROR(InconsistentState, "File [%s]", path.c_str());
    return 0;
}

bool MultiStorage::IsExist(const string& path) const
{
    assert(!mPackageStorage);
    return mDiskStorage->IsExist(path) || mInMemStorage->IsExist(path);
}

void MultiStorage::ListFile(const string& dirPath, FileList& fileList, 
                            bool recursive, bool physicalPath, bool skipSecondaryRoot) const
{
    assert(!mPackageStorage);
    assert(mInMemStorage);
    assert(mDiskStorage);

    string normPath = PathUtil::NormalizePath(dirPath);
    mDiskStorage->ListFile(normPath, fileList, recursive, physicalPath, skipSecondaryRoot);

    FileList rootList;
    mInMemStorage->ListRoot(normPath, rootList, recursive);
    fileList.insert(fileList.end(), rootList.begin(), rootList.end());

    if (recursive)
    {
        for (size_t i = 0; i < rootList.size(); ++i)
        {
            FileList inMemList;
            ListInMemRootFile(normPath, rootList[i], inMemList, physicalPath);
            fileList.insert(fileList.end(), inMemList.begin(), inMemList.end());
        }
    }
    SortAndUniqFileList(fileList);
}

void MultiStorage::ListInMemRootFile(const string& listDir,
                                     const string& rootPath,
                                     FileList& fileList, 
                                     bool physicalPath) const 
{
    string absRoot = PathUtil::JoinPath(listDir, rootPath);
    mInMemStorage->ListFile(absRoot, fileList, true, physicalPath);
    for (size_t i = 0; i < fileList.size(); ++i)
    {
        const string& path = fileList[i];
        fileList[i] = PathUtil::JoinPath(rootPath, path);
    }
}

void MultiStorage::CleanCache()
{
    INDEXLIB_FATAL_ERROR(UnSupported, 
                         "multi storage does need clean cache");
}

void MultiStorage::Close()
{
}

IE_NAMESPACE_END(file_system);

