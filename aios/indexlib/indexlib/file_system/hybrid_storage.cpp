#include <functional>
#include "indexlib/file_system/file_carrier.h"
#include "indexlib/file_system/hybrid_storage.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/in_mem_file_node_creator.h"
#include "indexlib/util/path_util.h"
#include "indexlib/file_system/mmap_file_node_creator.h"
#include "indexlib/file_system/in_mem_file_node_creator.h"

using namespace std;
using namespace fslib;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, HybridStorage);

HybridStorage::HybridStorage(
        const string& rootPath,
        const string& secondaryRootPath,
        const util::BlockMemoryQuotaControllerPtr& memController)
    : DiskStorage(rootPath, memController)
    , mSecondaryRootPath(secondaryRootPath)
    , mSecondarySupportMmap(PathUtil::SupportMmap(secondaryRootPath))
{
    // rooPath and secondaryRootPath must be normalized
    mPackageFileMountTable.SetGetPhysicalPathFunc(
        bind(&HybridStorage::GetPhysicalPath, this, placeholders::_1, false));
    // assert(!PathUtil::IsInRootPath(secondaryRootPath, rootPath));
}

HybridStorage::~HybridStorage() 
{
}

string HybridStorage::GetAbsPrimaryPath(const string& path) const
{
    string relativePath = GetRelativePath(path);
    return PathUtil::JoinPath(mRootPath, relativePath);
}

string HybridStorage::GetAbsSecondaryPath(const string& path) const
{
    string relativePath = AbsPathToRelativePath(mRootPath, path);
    return PathUtil::JoinPath(mSecondaryRootPath, relativePath);
}

string HybridStorage::GetPhysicalPath(const string& path, bool useCache) const
{
    assert(!IsExistInPackageFile(path));
    if (useCache)
    {
        FileNodePtr fileNode = mFileNodeCache->Find(path);
        if (fileNode && (fileNode->GetType() == FSFT_SLICE
                         || fileNode->GetType() == FSFT_IN_MEM))
        {
            return path;
        }
    }
    if (IsRealtimePath(path))
    {
        return path;
    }
    
    if (mOptions.isOffline)
    {
        if (DiskStorage::IsExistOnDisk(path))
        {
            return path;
        }
        return GetAbsSecondaryPath(path);
    }
    if (FileSystemWrapper::IsExist(path))
    {
        return path;
    }
    return GetAbsSecondaryPath(path);
}

string HybridStorage::GetRelativePath(const std::string& absPath) const
{
    if (PathUtil::IsInRootPath(absPath, mRootPath))
    {
        return AbsPathToRelativePath(mRootPath, absPath);
    }
    else
    {
        return AbsPathToRelativePath(mSecondaryRootPath, absPath);
    }
}

FileNodePtr HybridStorage::CreateFileNode(const string& filePath, FSOpenType type, bool readOnly)    
{
    PackageOpenMeta packageOpenMeta;
    bool inPackage = GetPackageOpenMeta(filePath, packageOpenMeta);
    FSOpenType realType = type;
    if (inPackage && packageOpenMeta.physicalFileCarrier &&
        packageOpenMeta.physicalFileCarrier->GetMmapLockFileNode())
    {
        IE_LOG(DEBUG, "file[%s] in package[%s] change FSOpenType[%d] to FSOT_MMAP",
               filePath.c_str(), packageOpenMeta.GetPhysicalFilePath().c_str(), type);
        realType = FSOT_MMAP;
    }
    const FileNodeCreatorPtr& fileNodeCreator = GetFileNodeCreator(filePath, realType);
    FileNodePtr fileNode = fileNodeCreator->CreateFileNode(filePath, realType, readOnly);
    if (inPackage)
    {
        if (type != realType)
        {
            fileNode->SetOriginalOpenType(type);
        }
        fileNode->Open(packageOpenMeta, realType);
        return fileNode;
    }
    
    string realPath = filePath;
    if ((fileNodeCreator->IsRemote() && !IsRealtimePath(filePath) && PathUtil::IsInRootPath(filePath, mRootPath)) ||
        // !DiskStorage::IsExistOnDisk(filePath))
        (mOptions.isOffline && !DiskStorage::IsExistOnDisk(filePath)) ||
        (!mOptions.isOffline && !storage::FileSystemWrapper::IsExist(filePath)))
    {
        realPath = GetAbsSecondaryPath(filePath);
    }

    FileInfo info;
    if (mPathMetaContainer && mPathMetaContainer->GetFileInfo(filePath, info))
    {
        FileMeta fileMeta;
        fileMeta.fileLength = info.fileLength;
        fileMeta.createTime = info.modifyTime;
        fileMeta.lastModifyTime = info.modifyTime;
        fileNode->Open(filePath, realPath, fileMeta, realType);
    }
    else
    {
        FileMeta fileMeta;
        fileMeta.fileLength = -1;
        fileNode->Open(filePath, realPath, fileMeta, realType);
        if (mPathMetaContainer)
        {
            // -1 means invalid ts
            mPathMetaContainer->AddFileInfo(filePath, fileNode->GetLength(), -1, false);
        }
    }
    return fileNode;
}

FileNodeCreator* HybridStorage::CreateMmapFileNodeCreator(const LoadConfig& loadConfig) const
{
    if (loadConfig.IsRemote())
    {
        if (!mSecondarySupportMmap)
        {
            IE_LOG(WARN, "LoadConfig[%s] is remote, but secondary path [%s] not support mmap, change to inmem",
                   loadConfig.GetName().c_str(), mSecondaryRootPath.c_str());
            return new InMemFileNodeCreator();
        }
        return new MmapFileNodeCreator();
    }
    return DiskStorage::CreateMmapFileNodeCreator(loadConfig);
}

void HybridStorage::InitDefaultFileNodeCreator()
{
    DiskStorage::InitDefaultFileNodeCreator();

    LoadConfig loadConfig;
    loadConfig.SetRemote(true);
    DoInitDefaultFileNodeCreator(mSecondaryDefaultCreatorMap, loadConfig);

    LoadConfig defaultLoadConfig = mOptions.loadConfigList.GetDefaultLoadConfig();
    defaultLoadConfig.SetRemote(true);

    if (!mSecondarySupportMmap && defaultLoadConfig.GetLoadStrategyName() == READ_MODE_MMAP)
    {
        FileNodeCreatorPtr inMemFileNodeCreator(new InMemFileNodeCreator());
        inMemFileNodeCreator->Init(defaultLoadConfig, mMemController);
        mSecondaryDefaultCreatorMap[FSOT_LOAD_CONFIG] = inMemFileNodeCreator;
    }
    else
    {
        mSecondaryDefaultCreatorMap[FSOT_LOAD_CONFIG] = CreateFileNodeCreator(defaultLoadConfig);
    }
}

const FileNodeCreatorPtr& HybridStorage::DeduceFileNodeCreator(
        const FileNodeCreatorPtr& creator, FSOpenType type,
        const string& filePath) const
{
    assert(creator);
    bool useSecondPath = creator->IsRemote() && !IsRealtimePath(filePath);

    if (useSecondPath
        && mSecondarySupportMmap
        && creator->GetDefaultOpenType() == FSOT_MMAP
        && creator->MatchType(type))
    {
        return creator;
    }

    FSOpenType deducedType = type;
    if (type == FSOT_MMAP)
    {
        bool supportMmap = useSecondPath ? mSecondarySupportMmap : mSupportMmap;
        if (!supportMmap)
        {
            deducedType = FSOT_IN_MEM;
        }
    }    

    IE_LOG(DEBUG, "File [%s] type [%d], use deduced open type [%d]",
           filePath.c_str(), type, deducedType);
    const FileNodeCreatorMap& creatorMap =
        useSecondPath ? mSecondaryDefaultCreatorMap : mDefaultCreatorMap;
    FileNodeCreatorMap::const_iterator it = creatorMap.find(deducedType);
    if (it == creatorMap.end())
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "File [%s], "
                             "Default not support Open deducedType [%d]",
                             filePath.c_str(), deducedType);
    }
    return it->second;
}

bool HybridStorage::IsExistInSecondaryPath(const std::string& path, bool ignoreTrash) const
{
    assert(!IsRealtimePath(path));
    string secondaryPath = GetAbsSecondaryPath(path);
    if (ignoreTrash)
    {
        secondaryPath = PathUtil::SetFsConfig(secondaryPath, "ignore_trash=true");
    }
    return DiskStorage::IsExistOnDisk(secondaryPath);
}

void HybridStorage::CleanCache()
{
    DiskStorage::CleanCache();
    for (auto it = mPackageFileCarrierMap.begin(); it != mPackageFileCarrierMap.end(); )
    {
        if ((!it->second) || (it->second->GetMmapLockFileNodeUseCount() <= 1))
        {
            it = mPackageFileCarrierMap.erase(it);
        }
        else
        {
            ++it;
        }
    }
}

bool HybridStorage::IsExistOnDisk(const string& path) const
{
    if (mOptions.isOffline)
    {
        return DiskStorage::IsExistOnDisk(path) ||
               DiskStorage::IsExistOnDisk(GetAbsSecondaryPath(path));
    }
    if (mPathMetaContainer)
    {
        if (mPathMetaContainer->IsExist(path))
        {
            return true;
        }
        if (mPathMetaContainer->MatchSolidPath(path))
        {
            return false;
        }
    }
    if (FileSystemWrapper::IsExist(path))
    {
        return true;
    }
    if (GetMatchedLoadConfig(path).IsRemote() &&
        FileSystemWrapper::IsExist(GetAbsSecondaryPath(path)))
    {
        return true;
    }
    return false;
}

const LoadConfig& HybridStorage::GetMatchedLoadConfig(const std::string& path) const
{
    assert(!mOptions.isOffline);
    string relativePath = GetRelativePath(path);
    auto lifecycle = GetFileLifecycle(GetAbsPrimaryPath(path));
    for (size_t i = 0; i < mOptions.loadConfigList.Size(); ++i)
    {
        const auto& loadConfig = mOptions.loadConfigList.GetLoadConfig(i);
        if (loadConfig.Match(relativePath, lifecycle))
        {
            return loadConfig;
        }
    }
    relativePath += "/"; // for rule "summary/"
    for (size_t i = 0; i < mOptions.loadConfigList.Size(); ++i)
    {
        const auto& loadConfig = mOptions.loadConfigList.GetLoadConfig(i);
        if (loadConfig.Match(relativePath, lifecycle))
        {
            return loadConfig;
        }
    }
    return mOptions.loadConfigList.GetDefaultLoadConfig();
}

void HybridStorage::GetFileMetaOnDisk(const std::string& filePath, fslib::FileMeta& fileMeta) const
{
    if (mOptions.isOffline)
    {
        return DiskStorage::GetFileMetaOnDisk(GetPhysicalPath(filePath), fileMeta);
    }
    if (mPathMetaContainer)
    {
        FileInfo info;
        if (mPathMetaContainer->GetFileInfo(filePath, info))
        {
            // attention : createTime & modifyTime may not be same with dfs
            fileMeta.fileLength = info.fileLength;
            fileMeta.createTime = info.modifyTime;
            fileMeta.lastModifyTime = info.modifyTime;
            return;
        }
    }
    return FileSystemWrapper::GetFileMeta(GetPhysicalPath(filePath), fileMeta);
}

bool HybridStorage::IsDirOnDisk(const std::string& path) const
{
    if (mOptions.isOffline)
    {
        return DiskStorage::IsDirOnDisk(GetPhysicalPath(path));
    }
    if (mPathMetaContainer)
    {
        // optimize: avoid check existance from hdfs path
        FileInfo info;
        if (mPathMetaContainer->GetFileInfo(path, info))
        {
            return info.isDirectory();
        }
        if (mPathMetaContainer->MatchSolidPath(path))
        {
            return false;
        }
    }
    return FileSystemWrapper::IsDir(GetPhysicalPath(path));
}

void HybridStorage::ListDirRecursiveOnDisk(const std::string& path,
        fslib::FileList& fileList, bool ignoreError, bool skipSecondaryRoot) const
{
    if (DiskStorage::IsExistOnDisk(path))
    {
        DiskStorage::ListDirRecursiveOnDisk(path, fileList, ignoreError, skipSecondaryRoot);
    }
    if (skipSecondaryRoot) {
        return;
    }
    string secondPath = GetAbsSecondaryPath(path);
    if (DiskStorage::IsExistOnDisk(secondPath))
    {
        fslib::FileList tmpFileList;
        DiskStorage::ListDirRecursiveOnDisk(secondPath, tmpFileList, ignoreError, skipSecondaryRoot);
        fileList.insert(fileList.end(),
                        tmpFileList.begin(), tmpFileList.end());
    }
    SortAndUniqFileList(fileList);
}

void HybridStorage::ListDirOnDisk(const std::string& path,
                                  fslib::FileList& fileList,
                                  bool ignoreError, bool skipSecondaryRoot) const
{
    if (DiskStorage::IsExistOnDisk(path))
    {
        DiskStorage::ListDirOnDisk(path, fileList, ignoreError, skipSecondaryRoot);
    }
    if (skipSecondaryRoot) {
        return;
    }
    string secondPath = GetAbsSecondaryPath(path);
    if (DiskStorage::IsExistOnDisk(secondPath))
    {
        fslib::FileList tmpFileList;
        DiskStorage::ListDirOnDisk(secondPath, tmpFileList, ignoreError, skipSecondaryRoot);
        fileList.insert(fileList.end(), tmpFileList.begin(), tmpFileList.end());
    }
    SortAndUniqFileList(fileList);
}

// secondary path is always read only, any modification in secondary is illegal
void HybridStorage::RemoveFile(const std::string& filePath, bool mayNonExist)
{
    if (!mayNonExist && !IsExist(filePath))
    {
        INDEXLIB_FATAL_ERROR(NonExist, "File [%s] not exist.", filePath.c_str());
    }
    DiskStorage::RemoveFile(filePath, true);
}

void HybridStorage::RemoveDirectory(const std::string& dirPath, bool mayNonExist)
{
    if (!mayNonExist && !IsExist(dirPath))
    {
        INDEXLIB_FATAL_ERROR(NonExist, "Directory [%s] not exist.", dirPath.c_str());
    }
    DiskStorage::RemoveDirectory(dirPath, true);
}

void HybridStorage::ModifyPackageOpenMeta(PackageOpenMeta& packageOpenMeta)
{
    const string& filePath = packageOpenMeta.GetPhysicalFilePath();
    const string& relativePath = GetRelativePath(filePath);
    const string& primaryPath = PathUtil::JoinPath(mRootPath, relativePath);

    auto it = mPackageFileCarrierMap.find(primaryPath);
    if (it != mPackageFileCarrierMap.end())
    {
        packageOpenMeta.physicalFileCarrier = it->second;
        return;
    }
    assert(!packageOpenMeta.physicalFileCarrier);

    FileNodeCreatorPtr fileNodeCreator = GetFileNodeCreator(primaryPath, FSOT_LOAD_CONFIG);
    bool isMMapLock = false;
    if (fileNodeCreator->GetDefaultOpenType() == FSOT_MMAP)
    {
        MmapFileNodeCreatorPtr mmapFileNodeCreator =
            DYNAMIC_POINTER_CAST(MmapFileNodeCreator, fileNodeCreator);
        isMMapLock = mmapFileNodeCreator && mmapFileNodeCreator->IsLock();
    }
    if (!isMMapLock)
    {
        // only support mmap lock
        packageOpenMeta.physicalFileCarrier = FileCarrierPtr();
        mPackageFileCarrierMap.insert(it, make_pair(primaryPath, packageOpenMeta.physicalFileCarrier));
        IE_LOG(INFO, "logical file[%s] in package[%s] do not use share, OpenType[%d]",
               packageOpenMeta.GetLogicalFilePath().c_str(), filePath.c_str(),
               fileNodeCreator->GetDefaultOpenType());
        return;
    }

    IE_LOG(DEBUG, "logical file[%s] in package[%s] use share, OpenType[%d]",
           packageOpenMeta.GetLogicalFilePath().c_str(), filePath.c_str(),
           fileNodeCreator->GetDefaultOpenType());

    FileNodePtr fileNode = fileNodeCreator->CreateFileNode(primaryPath, FSOT_MMAP, false);
    fslib::FileMeta fileMeta;
    GetFileMetaOnDisk(primaryPath, fileMeta);
    fileNode->Open(primaryPath, filePath, fileMeta, FSOT_MMAP);
    FileCarrierPtr fileCarrier(new FileCarrier());
    fileCarrier->SetMmapLockFileNode(DYNAMIC_POINTER_CAST(MmapFileNode, fileNode));
    mPackageFileCarrierMap.insert(it, make_pair(primaryPath, fileCarrier));
    packageOpenMeta.physicalFileCarrier = mPackageFileCarrierMap[primaryPath];
}

bool HybridStorage::GetPackageOpenMeta(const std::string& filePath,
                                       PackageOpenMeta& packageOpenMeta)
{
    if (mPackageFileMountTable.GetMountMeta(filePath, packageOpenMeta.innerFileMeta))
    {
        size_t physicalFileLength = (size_t)-1;
        bool ret = mPackageFileMountTable.GetPackageFileLength(
            GetAbsPrimaryPath(packageOpenMeta.GetPhysicalFilePath()), physicalFileLength);
        assert(ret);(void)ret;
        packageOpenMeta.physicalFileLength = (ssize_t)physicalFileLength;
        packageOpenMeta.SetLogicalFilePath(filePath);
        ModifyPackageOpenMeta(packageOpenMeta);
        return true;
    }
    return false;
}

bool HybridStorage::DoRemoveFileInPackageFile(const string& filePath)
{
    return mPackageFileMountTable.RemoveFile(filePath, !mOptions.isOffline);
    // how about data file
}

bool HybridStorage::DoRemoveDirectoryInPackageFile(const string& dirPath)
{
    bool removeDir = mPackageFileMountTable.RemoveDirectory(dirPath, !mOptions.isOffline);
    if (!mPackageFileCarrierMap.empty())
    {
        PackageFileCarrierMapIter dirIter(mPackageFileCarrierMap, dirPath);
        while (dirIter.HasNext())
        {
            PackageFileCarrierMapIter::Iterator it = dirIter.Next();
            IE_LOG(INFO, "release package data file node[%s]", it->first.c_str());
            dirIter.Erase(it);
        }
        assert(mPackageFileCarrierMap.count(dirPath) == 0);
    }
    return removeDir;
}

bool HybridStorage::AddPathFileInfo(const string& path, const FileInfo& fileInfo)
{
    if (likely(!mOptions.isOffline))
    {
        return DiskStorage::AddPathFileInfo(path, fileInfo);
    }
    return DiskStorage::AddPathFileInfo(GetPhysicalPath(path), fileInfo);
}

bool HybridStorage::MatchSolidPath(const string& path) const
{
    if (!mOptions.isOffline)
    {
        return DiskStorage::MatchSolidPath(path);    
    }
    return DiskStorage::MatchSolidPath(GetPhysicalPath(path));
}

// for online: solidPath may exist in both primary path or secondary path
//             => pathMetaContainer have to hold control path ("primary path")
// for offline: solidPath only exist ether primary path or secondary path
//             => pathMetaContainer have to hold physical path
bool HybridStorage::AddSolidPathFileInfos(
    const string& solidPath, const vector<FileInfo>::const_iterator& firstFileInfo,
    const vector<FileInfo>::const_iterator& lastFileInfo)
{
    if (!mOptions.isOffline)
    {
        return DiskStorage::AddSolidPathFileInfos(solidPath, firstFileInfo, lastFileInfo);
    }
    string physicalPath = GetPhysicalPath(solidPath);
    if (physicalPath == solidPath)
    {
        return DiskStorage::AddSolidPathFileInfos(solidPath, firstFileInfo, lastFileInfo);
    }

    if (mPathMetaContainer)
    {
        mPathMetaContainer->MarkNotExistSolidPath(solidPath);
    }
    return DiskStorage::AddSolidPathFileInfos(physicalPath, firstFileInfo, lastFileInfo);
}

void HybridStorage::Validate(const std::string& path, int64_t expectLength) const
{
    Storage::Validate(path, expectLength);
}

IE_NAMESPACE_END(file_system);

