#include "indexlib/util/path_util.h"
#include "indexlib/index_define.h"
#include "indexlib/file_system/load_config/cache_load_strategy.h"
#include "indexlib/file_system/disk_storage.h"
#include "indexlib/file_system/in_mem_file_node_creator.h"
#include "indexlib/file_system/mmap_file_node_creator.h"
#include "indexlib/file_system/buffered_file_node_creator.h"
#include "indexlib/file_system/buffered_file_reader.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/file_system/normal_file_reader.h"
#include "indexlib/file_system/load_config/mmap_load_strategy.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, DiskStorage);

DiskStorage::DiskStorage(const std::string rootPath,
            const util::BlockMemoryQuotaControllerPtr& memController)
    : Storage(rootPath, FSST_LOCAL, memController)
    , mRootPath(rootPath) // rootPath must has been normalized
    , mSupportMmap(PathUtil::SupportMmap(rootPath))
{
}

DiskStorage::~DiskStorage() 
{
}

void DiskStorage::Init(const FileSystemOptions& options)
{
    mOptions = options;
    InitDefaultFileNodeCreator();

    if (options.enablePathMetaContainer)
    {
        InitPathMetaContainer();
    }
    
    for (size_t i = 0; i < mOptions.loadConfigList.Size(); ++i)
    {
        const LoadConfig& loadConfig = mOptions.loadConfigList.GetLoadConfig(i);
        FileNodeCreatorPtr fileNodeCreator = CreateFileNodeCreator(loadConfig);
        mFileNodeCreators.push_back(fileNodeCreator);
    }
}

void DiskStorage::InitDefaultFileNodeCreator()
{
    DoInitDefaultFileNodeCreator(mDefaultCreatorMap, LoadConfig());

    const auto& defaultLoadConfig = mOptions.loadConfigList.GetDefaultLoadConfig(); 
    if (!mSupportMmap && defaultLoadConfig.GetLoadStrategyName() == READ_MODE_MMAP)
    {
        FileNodeCreatorPtr inMemFileNodeCreator(new InMemFileNodeCreator());
        inMemFileNodeCreator->Init(defaultLoadConfig, mMemController);
        mDefaultCreatorMap[FSOT_LOAD_CONFIG] = inMemFileNodeCreator;
    }
    else
    {
        mDefaultCreatorMap[FSOT_LOAD_CONFIG] = CreateFileNodeCreator(defaultLoadConfig);
    }
}

void DiskStorage::DoInitDefaultFileNodeCreator(FileNodeCreatorMap& creatorMap,
                                               const LoadConfig& loadConfig)
{
    IE_LOG(DEBUG, "Init default file node creator");

    SessionFileCachePtr fileCache;
    // online can not use SessionFileCache without SessionFileCache clean logic, in particular fslib prefetech scene
    if (mOptions.enablePathMetaContainer && mOptions.isOffline)
    {
        fileCache.reset(new SessionFileCache);
    }
        
    FileNodeCreatorPtr inMemFileNodeCreator(
            new InMemFileNodeCreator(fileCache));
    inMemFileNodeCreator->Init(loadConfig, mMemController);
    creatorMap[FSOT_IN_MEM] = inMemFileNodeCreator;

    FileNodeCreatorPtr mmapFileNodeCreator(
            new MmapFileNodeCreator());
    mmapFileNodeCreator->Init(loadConfig, mMemController);

    creatorMap[FSOT_MMAP] = mmapFileNodeCreator; 

    // default should not support cache file reader creator
    FileNodeCreatorPtr bufferedFileNodeCreator(
            new BufferedFileNodeCreator(fileCache));
    bufferedFileNodeCreator->Init(loadConfig, mMemController);
    creatorMap[FSOT_BUFFERED] = bufferedFileNodeCreator;
}

FileReaderPtr DiskStorage::CreateFileReaderWithoutCache(const std::string& filePath,
        FSOpenType type)
{
    // ModifiType
    FileNodePtr fileNode = CreateFileNode(filePath, type, true);
    return CreateFileReader(fileNode);
}

FileReaderPtr DiskStorage::CreateFileReader(const string& filePath,
        FSOpenType type)
{
    return DoCreateFileReader(filePath, type, false);
}

FileReaderPtr DiskStorage::CreateWritableFileReader(const string& filePath,
        FSOpenType type)
{
    return DoCreateFileReader(filePath, type, true);
}

FileReaderPtr DiskStorage::DoCreateFileReader(const std::string& filePath,
                                              FSOpenType type, bool needWrite)
{
    FileNodePtr fileNode = mFileNodeCache->Find(filePath);
    if (fileNode)
    {
        FSOpenType cacheOpenType = fileNode->GetOpenType();
        FSFileType cacheFileType = fileNode->GetType();
        if (fileNode->MatchType(type, needWrite))
        {
            IE_LOG(DEBUG, "File [%s] cache hit, Open type [%d], "
                   "Cache open type [%d], Cache file type [%d]",
                   filePath.c_str(), type, cacheOpenType, cacheFileType);
            mStorageMetrics.IncreaseFileCacheHit();
            return CreateFileReader(fileNode);
        }

        if (type == FSOT_BUFFERED)
        {
            fileNode = CreateFileNode(filePath, type, !needWrite);
            assert(fileNode);
            return CreateFileReader(fileNode);
        }
        // replace it
        IE_LOG(DEBUG, "File [%s] cache replace, Open type [%d], "
               "Cache open type [%d], Cache file type [%d]", 
               filePath.c_str(), type, cacheOpenType, cacheFileType);
        if (fileNode->GetType() == FSFT_SLICE)
        {
            INDEXLIB_FATAL_ERROR(InconsistentState, "File[%s] is slice file",
                    filePath.c_str());
        }
        if (fileNode.use_count() > 2)
        {
            INDEXLIB_FATAL_ERROR(InconsistentState, "File[%s] is using [%ld]",
                    filePath.c_str(), fileNode.use_count());
        }
        mStorageMetrics.IncreaseFileCacheReplace();
        fileNode.reset();
    }
    else
    {
        IE_LOG(DEBUG, "File [%s] cache miss, Open type [%d]",
               filePath.c_str(), type);
        mStorageMetrics.IncreaseFileCacheMiss();
    }
        
    fileNode = CreateFileNode(filePath, type, !needWrite);
    assert(fileNode);

    if (mOptions.useCache)
    {
        mFileNodeCache->Insert(fileNode);
    }
    return CreateFileReader(fileNode);    
}

void DiskStorage::MakeDirectory(const string& dirPath, bool recursive)
{
    IE_LOG(DEBUG, "Make dir[%s], recursive[%d]", dirPath.c_str(), recursive);
    if (recursive)
    {
        // recursive & ignore EXIST exception
        FileSystemWrapper::MkDirIfNotExist(dirPath);
    }
    else
    {
        FileSystemWrapper::MkDir(dirPath, false);
    }
    if (mPathMetaContainer)
    {
        mPathMetaContainer->AddFileInfo(dirPath, -1, -1, true);
    }
}

void DiskStorage::RemoveDirectory(const string& dirPath, bool mayNonExist)
{
    // Even though dir does not exist in cache, the files in it will be removed
    DoRemoveDirectory(dirPath, mayNonExist);
}

size_t DiskStorage::EstimateFileLockMemoryUse(const string& filePath, 
        FSOpenType type)
{
    // TODO: hybrid
    if (type == FSOT_SLICE)
    {
        return SliceFileNode::EstimateFileLockMemoryUse(GetFileLength(filePath));
    }
    const FileNodeCreatorPtr& fileNodeCreator = GetFileNodeCreator(
            filePath, type);
    return fileNodeCreator->EstimateFileLockMemoryUse(GetFileLength(filePath));
}

bool DiskStorage::IsExist(const string& path) const
{
    if (IsExistInPackageFile(path))
    {
        return true;
    }

    if (IsExistOnDisk(path))
    {
        return true;
    }
    
    FileNodePtr fileNode = mFileNodeCache->Find(path);
    if (fileNode && (fileNode->GetType() == FSFT_SLICE
                     || fileNode->GetType() == FSFT_IN_MEM))
    {
        return true;
    }
    
    return false;
}

void DiskStorage::ListFile(const string& dirPath, FileList& fileList, 
                           bool recursive, bool physicalPath, bool skipSecondaryRoot) const
{
    if (physicalPath)
    {
        DoListPackageFilePysicalPath(dirPath, fileList, recursive);
    }
    else
    {
        DoListFileInPackageFile(dirPath, fileList, recursive);
    }
    DoListFileWithOnDisk(dirPath, fileList, 
                         recursive, physicalPath, 
                         IsExistInPackageFile(dirPath),
                         skipSecondaryRoot);
    SortAndUniqFileList(fileList);
}

void DiskStorage::Close()
{
}

void DiskStorage::StoreFile(const FileNodePtr& fileNode, const FSDumpParam& param)
{
    assert(fileNode);
    if (mOptions.useCache)
    {
        StoreWhenNonExist(fileNode);
    }

    if (fileNode->GetType() == FSFT_IN_MEM)
    {
        const string& filePath = fileNode->GetPath();
        if (FileSystemWrapper::IsExist(filePath))
        {
            IE_LOG(ERROR, "file [%s] already exist!", filePath.c_str());
            return;
        }
        FSWriterParam fsWriterParam(param.atomicDump);
        BufferedFileWriter writer(fsWriterParam);
        writer.Open(filePath);
        writer.Write(fileNode->GetBaseAddress(), fileNode->GetLength());
        writer.Close();
    }
}

FSOpenType DiskStorage::GetLoadConfigOpenType(const std::string& path) const
{
    FileNodeCreatorPtr fileNodeCreator = 
        GetFileNodeCreator(path, FSOT_LOAD_CONFIG);
    
    if (!fileNodeCreator)
    {
        return FSOT_UNKNOWN;
    }

    return fileNodeCreator->GetDefaultOpenType();
}

bool DiskStorage::OnlyPatialLock(const std::string& filePath) const
{
    FileNodeCreatorPtr fileNodeCreator = 
        GetFileNodeCreator(filePath, FSOT_LOAD_CONFIG);
    
    if (!fileNodeCreator)
    {
        assert(false);
        return false;
    }

    return fileNodeCreator->OnlyPatialLock();
}

FileNodePtr DiskStorage::CreateFileNode(const string& filePath, FSOpenType type, bool readOnly)
{
    const FileNodeCreatorPtr& fileNodeCreator = 
        GetFileNodeCreator(filePath, type);
    FileNodePtr fileNode = fileNodeCreator->CreateFileNode(filePath, type, readOnly);
    OpenFileNode(fileNode, filePath, type);
    return fileNode;
}

FileReaderPtr DiskStorage::CreateFileReader(const FileNodePtr& fileNode)
{
    assert(fileNode);

    if (fileNode->GetOpenType() == FSOT_BUFFERED)
    {
        return BufferedFileReaderPtr(new BufferedFileReader(fileNode));
    }

    return NormalFileReaderPtr(new NormalFileReader(fileNode));
}

FileNodeCreatorPtr DiskStorage::CreateFileNodeCreator(
        const LoadConfig& loadConfig)
{
    IE_LOG(DEBUG, "Create file node creator with LoadConfig[%s]",
           ToJsonString(loadConfig).c_str());

    FileNodeCreatorPtr fileNodeCreator;
    if (loadConfig.GetLoadStrategyName() == READ_MODE_MMAP)
    {
        // TODO: use AutoMmapFileNodeCreator
        fileNodeCreator.reset(CreateMmapFileNodeCreator(loadConfig));
    }
    else if (loadConfig.GetLoadStrategyName() == READ_MODE_CACHE)
    {
        BlockFileNodeCreatorPtr blockFileNodeCreator(
                new BlockFileNodeCreator());
        fileNodeCreator = blockFileNodeCreator;
        mBlockFileNodeCreators.push_back(blockFileNodeCreator);
    }
    else if (loadConfig.GetLoadStrategyName() == READ_MODE_GLOBAL_CACHE)
    {
        fileNodeCreator.reset(new BlockFileNodeCreator(mOptions.fileBlockCache));
    }
    else
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "Unknow load config mode[%s]",
                             loadConfig.GetLoadStrategyName().c_str());
    }

    assert(fileNodeCreator);

    if (!fileNodeCreator->Init(loadConfig, mMemController))
    {
        INDEXLIB_FATAL_ERROR(BadParameter, "init fail with load config[%s]",
                             ToJsonString(loadConfig).c_str());
    }
    return fileNodeCreator;
}

FileNodeCreator* DiskStorage::CreateMmapFileNodeCreator(const LoadConfig& loadConfig) const
{
    if (mSupportMmap)
    {
        return new MmapFileNodeCreator();
    }
    return new InMemFileNodeCreator();
}

string DiskStorage::AbsPathToRelativePath(const string& rootDir,
        const string& absPath) const
{
    if (absPath.length() <= rootDir.length())
    {
        return string("");
    }
    // caller gurantee absPath is Normalized
    const string& normPath = absPath;
    assert(rootDir.size() > 1);
    assert(normPath.compare(0, rootDir.length(), rootDir) == 0);

    // +1 to trim '/'
    return normPath.substr(rootDir.size() + 1);
}

string DiskStorage::GetRelativePath(const string& absPath) const
{
    return AbsPathToRelativePath(mRootPath, absPath);
}

const FileNodeCreatorPtr& DiskStorage::GetFileNodeCreator(
        const string& filePath, FSOpenType type) const
{
    // load config macro(eg. _ATTRIBUTE_) need path without root
    string relativePath = GetRelativePath(filePath);
    size_t matchedIdx = 0;
    string lifecycle = GetFileLifecycle(filePath);

    for (; matchedIdx < mFileNodeCreators.size(); ++matchedIdx)
    {
        if (mFileNodeCreators[matchedIdx]->Match(relativePath, lifecycle))
        {
            IE_LOG(DEBUG, "File [%s] use load config [%d], default open type [%d]",
                   filePath.c_str(), (int)matchedIdx, mFileNodeCreators[matchedIdx]->GetDefaultOpenType());
            break;
        }
    }

    if (matchedIdx < mFileNodeCreators.size())
    {
        const auto& matchCreator = mFileNodeCreators[matchedIdx];
        IE_LOG(DEBUG, "File [%s] check reduce: type [%d], MatchType [%d], GetDefaultOpenType [%d], IsRemote [%d]",
               filePath.c_str(), type, matchCreator->MatchType(type), matchCreator->GetDefaultOpenType(), matchCreator->IsRemote());
        if (matchCreator->MatchType(type)
            && !(matchCreator->GetDefaultOpenType() == FSOT_MMAP && matchCreator->IsRemote()))
        {
            return matchCreator;
        }
        return DeduceFileNodeCreator(matchCreator, type, filePath);
    }
    return DeduceFileNodeCreator(mDefaultCreatorMap.at(FSOT_LOAD_CONFIG), type, filePath);
}

const FileNodeCreatorPtr& DiskStorage::DeduceFileNodeCreator(
        const FileNodeCreatorPtr& creator, FSOpenType type,
        const string& filePath) const
{
    FSOpenType deducedType = type;
    if (!mSupportMmap && type == FSOT_MMAP)
    {
        deducedType = FSOT_IN_MEM;
    }
    IE_LOG(DEBUG, "File [%s] type [%d], use deduced open type [%d]",
           filePath.c_str(), type, deducedType);
    FileNodeCreatorMap::const_iterator it = mDefaultCreatorMap.find(deducedType);
    if (it == mDefaultCreatorMap.end())
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "File [%s], "
                             "Default not support Open deducedType [%d]",
                             filePath.c_str(), deducedType);
    }
    return it->second;
}

PackageFileWriterPtr DiskStorage::CreatePackageFileWriter(
            const string& filePath)
{
    INDEXLIB_FATAL_ERROR(UnSupported,
                         "DiskStorage not support CreatePackageFileWriter"
                         ", filePath [%s]", filePath.c_str());
    return PackageFileWriterPtr();
}

PackageFileWriterPtr DiskStorage::GetPackageFileWriter(
            const string& filePath)
{
    INDEXLIB_FATAL_ERROR(UnSupported,
                         "DiskStorage not support GetPackageFileWriter"
                         ", filePath [%s]", filePath.c_str());
    return PackageFileWriterPtr();
}

void DiskStorage::InitPathMetaContainer()
{
    IE_LOG(INFO, "Init path meta cache!");
    mPathMetaContainer.reset(new PathMetaContainer);
    mPathMetaContainer->AddFileInfo(mRootPath, -1, -1, true);
}

IE_NAMESPACE_END(file_system);

