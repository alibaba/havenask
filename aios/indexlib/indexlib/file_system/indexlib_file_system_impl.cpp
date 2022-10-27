#include <autil/TimeUtility.h>
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/indexlib_file_system_impl.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/file_system/file_system_define.h"
#include "indexlib/file_system/file_node_cache.h"
#include "indexlib/file_system/in_mem_file_node.h"
#include "indexlib/file_system/hybrid_storage.h"
#include "indexlib/file_system/swap_mmap_file_reader.h"
#include "indexlib/file_system/swap_mmap_file_writer.h"
#include "indexlib/file_system/slice_file_reader.h"
#include "indexlib/file_system/slice_file.h"
#include "indexlib/file_system/in_memory_file.h"

using namespace std;
using namespace fslib;
using namespace autil;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, IndexlibFileSystemImpl);

IndexlibFileSystemImpl::IndexlibFileSystemImpl(const string& rootPath,
        const std::string& secondaryRootPath,
        MetricProviderPtr metricProvider)
    : mRootPath(rootPath)
    , mSecondaryRootPath(secondaryRootPath)
    , mMetricsReporter(metricProvider)
    , mIsClosed(true)
{
    mMountTable.reset(new MountTable);
}

IndexlibFileSystemImpl::~IndexlibFileSystemImpl() 
{
    // ATTENTION: do not throw FileIO exception in destructor
    try
    {
        Close();
    }
    catch (const ExceptionBase& e)
    {
        IE_LOG(ERROR, "Caught exception %s when Close",
               e.GetMessage().c_str());
    }
    catch (...)
    {
        IE_LOG(ERROR, "Caught unknown exception when Close");
    }
    mMemController.reset();
}

void IndexlibFileSystemImpl::Init(const FileSystemOptions& options)
{
    IE_LOG(INFO, "RootPath[%s], SecondaryRootPath[%s], NeedFlush[%d],"
           "AsyncFlush[%d], UseCache[%d], UseRootLink[%d]",
           mRootPath.c_str(), mSecondaryRootPath.c_str(), options.needFlush,
           options.enableAsyncFlush, options.useCache, options.useRootLink);

    mMemController.reset(new BlockMemoryQuotaController(
                    options.memoryQuotaController, "file_system"));
    mOptions = options;
    mRootPath = PathUtil::NormalizePath(mRootPath);
    mSecondaryRootPath = PathUtil::NormalizePath(mSecondaryRootPath);

    if (!mSecondaryRootPath.empty() &&
        !FileSystemWrapper::IsExistIgnoreError(mSecondaryRootPath))
    {
        INDEXLIB_FATAL_ERROR(NonExist, "SecondaryRootPath[%s] does not exist.",
                             mSecondaryRootPath.c_str());
    }

    if (!FileSystemWrapper::IsExistIgnoreError(mRootPath) && mSecondaryRootPath.empty())
    {
        INDEXLIB_FATAL_ERROR(NonExist, "Root[%s] does not exist.",
                             mRootPath.c_str());
    }

    string rootLinkName = FILE_SYSTEM_ROOT_LINK_NAME;
    if (mOptions.rootLinkWithTs)
    {
        rootLinkName = rootLinkName + "@" +
                       StringUtil::toString(TimeUtility::currentTimeInSeconds());
    }

    mRootLinkPath = PathUtil::JoinPath(mRootPath, rootLinkName);
    if (mOptions.useRootLink)
    {
        IndexlibFileSystem::DeleteRootLinks(mRootPath);
        IE_LOG(INFO, "make symlink src[%s] to dst[%s]",
               mRootPath.c_str(), mRootLinkPath.c_str());
        if (!FileSystemWrapper::SymLink(mRootPath, mRootLinkPath))
        {
            INDEXLIB_FATAL_ERROR(FileIO, "make symlink src[%s] to dst[%s] fail!",
                    mRootPath.c_str(), mRootLinkPath.c_str());
        }
    }

    mMountTable->Init(mRootPath, mSecondaryRootPath, mOptions, mMemController);
    InitMetrics(mOptions.metricPref);
    mIsClosed = false;
}

void IndexlibFileSystemImpl::InitMetrics(FSMetricPreference metricPref)
{
    mMetricsReporter.DeclareMetrics(GetFileSystemMetrics(), metricPref);
}

FileWriterPtr IndexlibFileSystemImpl::CreateFileWriter(
    const string& filePath, const FSWriterParam& param)
{
    string normPath = PathUtil::NormalizePath(filePath);
    IE_LOG(DEBUG, "Create File Writer[%s]", normPath.c_str());

    FSWriterParam writerParam = param;
    writerParam.prohibitInMemDump = param.prohibitInMemDump || mOptions.prohibitInMemDump;
    if (!writerParam.raidConfig)
    {
        writerParam.raidConfig = mOptions.raidConfig;
    }
    ScopedLock lock(mLock);

    Storage* storage = GetStorage(normPath);
    assert(storage);
    if (storage->GetStorageType() != FSST_IN_MEM)
    {
        SyncInMemStorage(true);
    }
    return storage->CreateFileWriter(normPath, writerParam);
}

FileReaderPtr IndexlibFileSystemImpl::CreateFileReaderWithoutCache(
        const std::string& filePath, FSOpenType type)
{
    string normPath = PathUtil::NormalizePath(filePath);
    IE_LOG(DEBUG, "File[%s], OpenType[%d]", normPath.c_str(), type);
    FileReaderPtr reader;
    {
        ScopedLock lock(mLock);
        
        Storage* storage = GetStorage(normPath);
        assert(storage);
        reader = storage->CreateFileReaderWithoutCache(normPath, type);
    }
    assert(reader);
    reader->Open();
    return reader;
}

FileReaderPtr IndexlibFileSystemImpl::CreateFileReader(const string& filePath,
        FSOpenType type)
{
    string normPath = PathUtil::NormalizePath(filePath);
    IE_LOG(DEBUG, "File[%s], OpenType[%d]", normPath.c_str(), type);
    FileReaderPtr reader;
    {
        ScopedLock lock(mLock);
        
        Storage* storage = GetStorage(normPath);
        assert(storage);
        reader = storage->CreateFileReader(normPath, type);
    }
    assert(reader);
    reader->Open();
    return reader;
}

FileReaderPtr IndexlibFileSystemImpl::CreateWritableFileReader(
    const std::string& filePath, FSOpenType type)
{
    string normPath = PathUtil::NormalizePath(filePath);
    IE_LOG(DEBUG, "File[%s], OpenType[%d]", normPath.c_str(), type);
    FileReaderPtr reader;
    {
        ScopedLock lock(mLock);
        
        Storage* storage = GetStorage(normPath);
        assert(storage);
        reader = storage->CreateWritableFileReader(normPath, type);
    }
    assert(reader);
    reader->Open();
    return reader;    
}

SliceFilePtr IndexlibFileSystemImpl::CreateSliceFile(
        const std::string& filePath, uint64_t sliceLen, int32_t sliceNum)
{
    string normPath = PathUtil::NormalizePath(filePath);
    IE_LOG(DEBUG, "File[%s], sliceLen[%lu], sliceNum[%d]", 
           normPath.c_str(), sliceLen, sliceNum);

    ScopedLock lock(mLock);

    Storage* storage = GetStorage(normPath);
    assert(storage);
    // slice file always in cache, does need sync
    return storage->CreateSliceFile(normPath, sliceLen, sliceNum);
}

SliceFilePtr IndexlibFileSystemImpl::GetSliceFile(const std::string& filePath)
{
    string normPath = PathUtil::NormalizePath(filePath);
    IE_LOG(DEBUG, "File[%s]", normPath.c_str());

    ScopedLock lock(mLock);

    Storage* storage = GetStorage(normPath);
    assert(storage);
    return storage->GetSliceFile(normPath);
}

SwapMmapFileReaderPtr IndexlibFileSystemImpl::CreateSwapMmapFileReader(
        const string& path)
{
    string normPath = PathUtil::NormalizePath(path);
    ScopedLock lock(mLock);

    Storage* storage = GetStorage(normPath);
    assert(storage);
    return storage->CreateSwapMmapFileReader(normPath);
}

SwapMmapFileWriterPtr IndexlibFileSystemImpl::CreateSwapMmapFileWriter(
            const string& path, size_t fileSize)
{
    string normPath = PathUtil::NormalizePath(path);
    ScopedLock lock(mLock);

    Storage* storage = GetStorage(normPath);
    assert(storage);
    return storage->CreateSwapMmapFileWriter(normPath, fileSize);
}

InMemoryFilePtr IndexlibFileSystemImpl::CreateInMemoryFile(
    const std::string& path, uint64_t fileLength)
{
    string normPath = PathUtil::NormalizePath(path);
    IE_LOG(DEBUG, "File[%s], fileLen[%lu]", 
           normPath.c_str(), fileLength);

    ScopedLock lock(mLock);

    Storage* storage = GetStorage(normPath);
    assert(storage);
    // slice file always in cache, does need sync
    return storage->CreateInMemoryFile(normPath, fileLength);
}

void IndexlibFileSystemImpl::MakeDirectory(const string& dirPath, bool recursive)
{
    string normPath = PathUtil::NormalizePath(dirPath);
    IE_LOG(DEBUG, "Dir[%s], recursive[%d]", normPath.c_str(), recursive);

    ScopedLock lock(mLock);

    Storage* storage = GetStorage(normPath);
    assert(storage);
    if (storage->GetStorageType() != FSST_IN_MEM)
    {
        SyncInMemStorage(true);
    }
    storage->MakeDirectory(normPath, recursive);
}

void IndexlibFileSystemImpl::RemoveFile(const string& filePath, bool mayNonExist)
{
    string normPath = PathUtil::NormalizePath(filePath);
    IE_LOG(INFO, "Remove File[%s]", normPath.c_str());

    ScopedLock lock(mLock);

    Storage* storage = GetStorage(normPath);
    assert(storage);
    storage->RemoveFile(normPath, mayNonExist);
}

void IndexlibFileSystemImpl::RemoveDirectory(const string& dirPath, bool mayNonExist)
{
    string normPath = PathUtil::NormalizePath(dirPath);
    IE_LOG(INFO, "Remove Dir[%s]", normPath.c_str());

    ScopedLock lock(mLock);

    if (IsRootPath(normPath))
    {
        INDEXLIB_FATAL_ERROR(BadParameter, "Can not remove root path[%s]",
                             normPath.c_str());
    }
    SyncInMemStorage(true);

    Storage* storage = GetStorage(normPath);
    assert(storage);
    storage->RemoveDirectory(normPath, mayNonExist);
}

size_t IndexlibFileSystemImpl::EstimateFileLockMemoryUse(const string& filePath,
        FSOpenType type)
{
    string normPath = PathUtil::NormalizePath(filePath);
    IE_LOG(DEBUG, "File[%s], OpenType[%d]", normPath.c_str(), type);

    ScopedLock lock(mLock);

    Storage* storage = GetStorage(normPath);
    assert(storage);
    return storage->EstimateFileLockMemoryUse(normPath, type);
}

bool IndexlibFileSystemImpl::IsExist(const string& path) const
{
    string normPath = PathUtil::NormalizePath(path);

    ScopedLock lock(mLock);

    Storage* storage = GetStorage(normPath);
    assert(storage);
    return storage->IsExist(normPath);
}

bool IndexlibFileSystemImpl::IsExistInPackageMountTable(const string& path) const
{
    string normPath = PathUtil::NormalizePath(path);

    ScopedLock lock(mLock);

    Storage* storage = GetStorage(normPath);
    assert(storage);
    return storage->IsExistInPackageMountTable(normPath);
}

bool IndexlibFileSystemImpl::IsExistInSecondaryPath(const std::string& path,
                                                    bool ignoreTrash) const
{
    string normPath = PathUtil::NormalizePath(path);

    ScopedLock lock(mLock);
    CheckInputPath(normPath);
    auto diskStorage = mMountTable->GetDiskStorage();
    auto hybridStorage = DYNAMIC_POINTER_CAST(file_system::HybridStorage, diskStorage);
    if (!hybridStorage)
    {
        return false;
    }
    return hybridStorage->IsExistInSecondaryPath(normPath, ignoreTrash);
}

bool IndexlibFileSystemImpl::IsExistInCache(const string& path) const
{
    string normPath = PathUtil::NormalizePath(path);

    ScopedLock lock(mLock);

    Storage* storage = GetStorage(normPath);
    assert(storage);
    return storage->IsExistInCache(normPath);
}

bool IndexlibFileSystemImpl::MatchSolidPath(const string& path) const
{
    string normPath = PathUtil::NormalizePath(path);

    ScopedLock lock(mLock);

    Storage* storage = GetStorage(normPath);
    assert(storage);
    return storage->MatchSolidPath(normPath);
}
    
bool IndexlibFileSystemImpl::AddPathFileInfo(const string& path, const FileInfo& fileInfo)
{
    string normPath = PathUtil::NormalizePath(path);
    ScopedLock lock(mLock);
    Storage* storage = GetStorage(PathUtil::JoinPath(normPath, fileInfo.filePath));
    assert(storage);
    return storage->AddPathFileInfo(normPath, fileInfo);
}

bool IndexlibFileSystemImpl::AddSolidPathFileInfos(const string& solidPath,
        const vector<FileInfo>::const_iterator& firstFileInfo,
        const vector<FileInfo>::const_iterator& lastFileInfo)
{
    string normPath = PathUtil::NormalizePath(solidPath);

    ScopedLock lock(mLock);

    Storage* storage = GetStorage(normPath);
    assert(storage);
    return storage->AddSolidPathFileInfos(normPath, firstFileInfo, lastFileInfo);
}

bool IndexlibFileSystemImpl::SetDirLifecycle(const std::string& path, const std::string& lifecycle)
{
    string normPath = PathUtil::NormalizePath(path);
    
    ScopedLock lock(mLock);
    
    Storage* storage = GetStorage(normPath);
    assert(storage);
    return storage->SetDirLifecycle(normPath, lifecycle);
}

bool IndexlibFileSystemImpl::IsDir(const string& path) const
{
    string normPath = PathUtil::NormalizePath(path);

    ScopedLock lock(mLock);

    Storage* storage = GetStorage(normPath);
    assert(storage);
    return storage->IsDir(normPath);
}

void IndexlibFileSystemImpl::ListFile(const string& dirPath, FileList& fileList, 
                                      bool recursive, bool physicalPath,
                                      bool skipSecondaryRoot) const
{
    string normPath = PathUtil::NormalizePath(dirPath);
    IE_LOG(DEBUG, "Dir[%s], recursive[%d]", normPath.c_str(), recursive);

    ScopedLock lock(mLock);

    Storage* storage = GetStorage(normPath);
    assert(storage);
    storage->ListFile(normPath, fileList, recursive, physicalPath, skipSecondaryRoot);
    for(FileList::iterator it = fileList.begin(); 
        it != fileList.end(); )
    {
        const string& path = *it;
        size_t pos = path.rfind('.');
        if(pos != string::npos
           && path.substr(pos) == TEMP_FILE_SUFFIX)
        {
            it = fileList.erase(it);
        }
        else
        {
            ++it;
        }
    }
    sort(fileList.begin(), fileList.end());
}

size_t IndexlibFileSystemImpl::GetFileLength(const string& filePath) const
{
    string normPath = PathUtil::NormalizePath(filePath);
    ScopedLock lock(mLock);

    Storage* storage = GetStorage(normPath);
    assert(storage);
    return storage->GetFileLength(normPath);
}

FSFileType IndexlibFileSystemImpl::GetFileType(const string& filePath) const
{
    string normPath = PathUtil::NormalizePath(filePath);
    ScopedLock lock(mLock);

    Storage* storage = GetStorage(normPath);
    assert(storage);
    return storage->GetFileType(normPath);
}

void IndexlibFileSystemImpl::GetFileStat(const string& filePath, 
        FileStat& fileStat) const
{
    string normPath = PathUtil::NormalizePath(filePath);
    ScopedLock lock(mLock);

    Storage* storage = GetStorage(normPath);
    assert(storage);
    storage->GetFileStat(normPath, fileStat);
}

void IndexlibFileSystemImpl::GetFileMeta(const std::string& filePath,
        fslib::FileMeta& fileMeta) const
{
    string normPath = PathUtil::NormalizePath(filePath);
    ScopedLock lock(mLock);

    Storage* storage = GetStorage(normPath);
    assert(storage);
    storage->GetFileMeta(normPath, fileMeta);
}

FSStorageType IndexlibFileSystemImpl::GetStorageType(
        const string& path, bool throwExceptionIfNotExist) const
{
    string normPath = PathUtil::NormalizePath(path);

    ScopedLock lock(mLock);

    Storage* storage = GetStorage(normPath);
    assert(storage);
    if (!storage->IsExist(normPath))
    {
        if (throwExceptionIfNotExist)
        {
            INDEXLIB_THROW(misc::NonExistException, "Path [%s] does not exist.", 
                           normPath.c_str());
        }
        return FSST_UNKNOWN;
    }
    return storage->GetStorageType();
}

FSOpenType IndexlibFileSystemImpl::GetLoadConfigOpenType(
        const std::string& path) const
{
    string normPath = PathUtil::NormalizePath(path);

    ScopedLock lock(mLock);
    if (GetStorageType(normPath, true) != FSST_LOCAL)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, 
                             "Only local storage has LoadConfig, path [%s]", 
                             normPath.c_str());
    }
    const DiskStoragePtr& diskStorage = mMountTable->GetDiskStorage();
    return diskStorage->GetLoadConfigOpenType(normPath);
}

bool IndexlibFileSystemImpl::OnlyPatialLock(const std::string& filePath) const
{
    string normPath = PathUtil::NormalizePath(filePath);

    ScopedLock lock(mLock);
    const DiskStoragePtr& diskStorage = mMountTable->GetDiskStorage();
    return diskStorage->OnlyPatialLock(normPath);
}

std::future<bool> IndexlibFileSystemImpl::Sync(bool waitFinish)
{
    IE_LOG(INFO, "Sync[%d], RootPath[%s]", waitFinish, GetRootPath().c_str());
    ScopedLock lock(mLock);
    return SyncInMemStorage(waitFinish);
}

std::future<bool> IndexlibFileSystemImpl::SyncInMemStorage(bool waitFinish)
{
    InMemStoragePtr inMemStorage = mMountTable->GetInMemStorage();
    assert(inMemStorage);
    return inMemStorage->Sync(waitFinish);
}

void IndexlibFileSystemImpl::CleanCache()
{
    IE_LOG(DEBUG, "Clean");
    ScopedLock lock(mLock);

    const InMemStoragePtr& inMemStorage = mMountTable->GetInMemStorage();
    const DiskStoragePtr& diskStorage = mMountTable->GetDiskStorage();
    assert(inMemStorage && diskStorage);
    inMemStorage->CleanCache();
    diskStorage->CleanCache();
}

void IndexlibFileSystemImpl::CleanCache(FSStorageType type)
{
    IE_LOG(DEBUG, "Clean storage [%d]", type);
    ScopedLock lock(mLock);

    if (type == FSST_IN_MEM)
    {
        const InMemStoragePtr& inMemStorage = mMountTable->GetInMemStorage();
        assert(inMemStorage);
        inMemStorage->CleanCache();
    }
    else if (type == FSST_LOCAL)
    {
        const DiskStoragePtr& diskStorage = mMountTable->GetDiskStorage();
        assert(diskStorage);
        diskStorage->CleanCache();
    }
}

void IndexlibFileSystemImpl::Close()
{
    ScopedLock lock(mLock);
    mIsClosed = true;
    IE_LOG(INFO, "Close");
    mMountTable.reset();
}

void IndexlibFileSystemImpl::MountInMemStorage(const string& path)
{
    string normPath = PathUtil::NormalizePath(path);

    ScopedLock lock(mLock);

    if (unlikely(!mOptions.useCache))
    {
        //in mem storage must cache 
        INDEXLIB_FATAL_ERROR(UnSupported, "in mem storage must use cache");
    }

    CheckInputPath(normPath);
    mMountTable->Mount(normPath, FSST_IN_MEM);
}

void IndexlibFileSystemImpl::MountPackageStorage(
        const string& path, const string& description)
{
    string normPath = PathUtil::NormalizePath(path);
    IE_LOG(INFO, "mount package storage [%s] with description [%s]",
           path.c_str(), description.c_str());
    
    ScopedLock lock(mLock);
    
    CheckInputPath(normPath);
    mMountTable->Mount(normPath, FSST_PACKAGE, description);
}

const PackageStoragePtr& IndexlibFileSystemImpl::GetPackageStorage() const
{
    return mMountTable->GetPackageStorage();
}

bool IndexlibFileSystemImpl::IsRootPath(const string& normPath) const
{
    return mRootPath.compare(normPath) == 0;
}

const StorageMetrics& IndexlibFileSystemImpl::GetStorageMetrics(
        FSStorageType type) const
{
    const Storage* storage = mMountTable->GetStorage(type);
    assert(storage);
    return storage->GetMetrics();
}

IndexlibFileSystemMetrics IndexlibFileSystemImpl::GetFileSystemMetrics() const
{
    const StorageMetrics& inMemStorageMetrics = GetStorageMetrics(FSST_IN_MEM);
    const StorageMetrics& localStorageMetrics = GetStorageMetrics(FSST_LOCAL);
    
    return IndexlibFileSystemMetrics(
            inMemStorageMetrics, localStorageMetrics);
}

void IndexlibFileSystemImpl::ReportMetrics()
{
    mMetricsReporter.ReportMetrics(GetFileSystemMetrics());
    if (mMemController)
    {
        mMetricsReporter.ReportMemoryQuotaUse(mMemController->GetUsedQuota());
    }
}

PackageFileWriterPtr IndexlibFileSystemImpl::CreatePackageFileWriter(
        const string& filePath)
{
    string normPath = PathUtil::NormalizePath(filePath);
    IE_LOG(DEBUG, "Create Package File Writer[%s]", normPath.c_str());

    ScopedLock lock(mLock);

    Storage* storage = GetStorage(normPath);
    assert(storage);
    if (storage->GetStorageType() != FSST_IN_MEM)
    {
        SyncInMemStorage(true);
    }
    return storage->CreatePackageFileWriter(normPath);
}

PackageFileWriterPtr IndexlibFileSystemImpl::GetPackageFileWriter(
            const string& filePath) const
{
    string normPath = PathUtil::NormalizePath(filePath);
    IE_LOG(DEBUG, "Get Package File Writer[%s]", normPath.c_str());

    ScopedLock lock(mLock);
    Storage* storage = GetStorage(normPath);
    assert(storage);
    return storage->GetPackageFileWriter(normPath);
}

bool IndexlibFileSystemImpl::MountPackageFile(const string& filePath)
{
    string normPath = PathUtil::NormalizePath(filePath);
    IE_LOG(DEBUG, "Mount PackageFile[%s]", normPath.c_str());

    ScopedLock lock(mLock);

    Storage* storage = GetStorage(normPath);
    assert(storage);
    return storage->MountPackageFile(normPath);
}

void IndexlibFileSystemImpl::EnableLoadSpeedSwitch()
{
    mOptions.loadConfigList.EnableLoadSpeedLimit();
}

void IndexlibFileSystemImpl::DisableLoadSpeedSwitch()
{
    mOptions.loadConfigList.DisableLoadSpeedLimit();
}

void IndexlibFileSystemImpl::InsertToFileNodeCacheIfNotExist(
        const FileReaderPtr& fileReader)
{
    assert(fileReader);
    string filePath = PathUtil::NormalizePath(fileReader->GetPath());
    ScopedLock lock(mLock);
    Storage* storage = GetStorage(filePath);
    assert(storage);
    if (storage->IsExistInCache(filePath))
    {
        return;
    }
    storage->StoreWhenNonExist(fileReader->GetFileNode());
}

bool IndexlibFileSystemImpl::IsOffline() const
{
    return mOptions.isOffline;
}

void IndexlibFileSystemImpl::Validate(const std::string& path, int64_t expectLength) const
{
    assert(*(path.rbegin()) != '/' || expectLength == -1);
    string normPath = PathUtil::NormalizePath(path);

    ScopedLock lock(mLock);

    Storage* storage = GetStorage(normPath);
    assert(storage);
    return storage->Validate(normPath, expectLength);
}

bool IndexlibFileSystemImpl::GetAbsSecondaryPath(
    const std::string& path, std::string& absPath) const
{
    string normPath = PathUtil::NormalizePath(path);

    ScopedLock lock(mLock);
    CheckInputPath(normPath);
    auto diskStorage = mMountTable->GetDiskStorage();
    auto hybridStorage = DYNAMIC_POINTER_CAST(file_system::HybridStorage, diskStorage);
    if (!hybridStorage)
    {
        return false;
    }
    absPath = hybridStorage->GetAbsSecondaryPath(normPath);
    return true;
}

bool IndexlibFileSystemImpl::UpdatePanguInlineFile(
    const string& path, const string& content) const
{
    constexpr const char* UPDATE_INLINE_COMMAND = "pangu_UpdateInlinefile";
    static string out;
    fslib::ErrorCode ec = fslib::fs::FileSystem::forward(UPDATE_INLINE_COMMAND, path, content, out);
    if (ec != fslib::EC_OK) {
        IE_LOG(WARN, "update pangu inline file [%s] with content [%s] failed, error [%d]",
               path.c_str(), content.c_str(), ec);
        return false;
    }
    return true;
}

IE_NAMESPACE_END(file_system);
