#include "indexlib/index_define.h"
#include "indexlib/merger/merge_file_system.h"
#include "indexlib/merger/package_merge_file_system.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/util/path_util.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/indexlib_file_system_creator.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/merger/index_partition_merger.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, MergeFileSystem);

MergeFileSystemPtr MergeFileSystem::Create(const string& rootPath, const MergeConfig& mergeConfig,
    uint32_t instanceId, const storage::RaidConfigPtr& raidConfig)
{
    assert(StringUtil::endsWith(rootPath, IndexPartitionMerger::MERGE_INSTANCE_DIR_PREFIX
                                + StringUtil::toString(instanceId)));
    if (mergeConfig.GetEnablePackageFile())
    {
        PackageMergeFileSystemPtr packageMergeFileSystem(
            new PackageMergeFileSystem(rootPath, mergeConfig, instanceId, raidConfig));
        return packageMergeFileSystem;
    }
    MergeFileSystemPtr mergeFileSystem(
        new MergeFileSystem(rootPath, mergeConfig, instanceId, raidConfig));
    return mergeFileSystem;
}

MergeFileSystem::MergeFileSystem(const string& rootPath, const MergeConfig& mergeConfig,
                                 uint32_t instanceId, const storage::RaidConfigPtr& raidConfig)
    : mRootPath(PathUtil::NormalizePath(rootPath))
    , mMergeConfig(mergeConfig)
    , mMetricProvider(NULL)
    , mInstanceId(instanceId)
    , mThreadFileSystem(new ThreadLocalPtr([](void* p){ delete (ThreadFileSystem*)p; } ))
{
    mFileSystemOptions.enablePathMetaContainer = true;
    mFileSystemOptions.isOffline = true;
    mFileSystemOptions.enableAsyncFlush = mergeConfig.mergeIOConfig.enableAsyncWrite;
    mFileSystemOptions.useCache = false;
    mFileSystemOptions.raidConfig = raidConfig;
    // TODO: quota controller need a quota.
    mFileSystemOptions.memoryQuotaController =
        MemoryQuotaControllerCreator::CreatePartitionMemoryController();
}

MergeFileSystem::~MergeFileSystem() 
{
}

void MergeFileSystem::Init(const vector<string>& targetSegmentPaths)
{
    for (auto segmentPath : targetSegmentPaths)
    {
        string path = PathUtil::JoinPath(mRootPath, segmentPath);
        FileSystemWrapper::MkDirIfNotExist(path);
    }
    string checkPointDir = FileSystemWrapper::JoinPath(
            mRootPath, MERGE_ITEM_CHECK_POINT_DIR_NAME);
    FileSystemWrapper::MkDirIfNotExist(checkPointDir);
}

void MergeFileSystem::Commit()
{
    IE_LOG(INFO, "commit file system");
}

void MergeFileSystem::Close()
{
    IE_LOG(INFO, "close file system");
    for (auto iter = mArchiveFolders.begin();
         iter != mArchiveFolders.end(); iter++)
    {
        iter->second->Close();
    }
}

void MergeFileSystem::MakeDirectory(const string& absolutePath, bool mayExist)
{
    FileSystemWrapper::MkDir(absolutePath, true, mayExist);
}

DirectoryPtr MergeFileSystem::GetDirectory(const string& absolutePath)
{
    assert(PathUtil::IsInRootPath(absolutePath, mRootPath));
    return DirectoryCreator::Get(GetFileSystem(), absolutePath, true);
}

void MergeFileSystem::MakeCheckpoint(const string& fileName)
{
    string checkpointPath = FileSystemWrapper::JoinPath(
        mRootPath, MERGE_ITEM_CHECK_POINT_DIR_NAME);
    ArchiveFolderPtr folder = GetArchiveFolder(checkpointPath, true);
    FileWrapperPtr fileWrapper = folder->GetInnerFile(fileName, fslib::WRITE);
    fileWrapper->Close();
    //FileSystemWrapper::AtomicStoreIgnoreExist(checkpointPath, "");
}

bool MergeFileSystem::HasCheckpoint(const string& fileName)
{
    if (fileName.empty())
    {
        return true;
    }
    string checkpointPath = FileSystemWrapper::JoinPath(
            mRootPath, MERGE_ITEM_CHECK_POINT_DIR_NAME);
    ArchiveFolderPtr folder = GetArchiveFolder(checkpointPath, true);
    FileWrapperPtr fileWrapper = folder->GetInnerFile(fileName, fslib::READ);
    return fileWrapper != FileWrapperPtr();
}

MergeFileSystem::ThreadFileSystem* MergeFileSystem::CreateFileSystem()
{
    ThreadFileSystem* tfs = new ThreadFileSystem();
    tfs->fileSystem = IndexlibFileSystemCreator::Create(mRootPath, "", mMetricProvider, mFileSystemOptions);
    IE_LOG(INFO, "create file system [%s]", mRootPath.c_str());
    return tfs;
}

MergeFileSystem::ThreadFileSystem* MergeFileSystem::GetThreadFileSystem()
{
    auto tfs = static_cast<ThreadFileSystem*>(mThreadFileSystem->Get());
    if (tfs == nullptr)
    {
        tfs = CreateFileSystem();
        mThreadFileSystem->Reset(tfs);
    }
    return tfs;
}

ArchiveFolderPtr MergeFileSystem::GetArchiveFolder(const std::string& absolutePath,
                                                   bool forceUseArchiveFile)
{
    ScopedLock lock(mLock);
    FileSystemWrapper::MkDirIfNotExist(absolutePath);
    if (mArchiveFolders.find(absolutePath) != mArchiveFolders.end())
    {
        return mArchiveFolders.find(absolutePath)->second;
    }
    bool legacyMode = forceUseArchiveFile ? false : !mMergeConfig.IsArchiveFileEnable();
    ArchiveFolderPtr archiveFolder(new ArchiveFolder(legacyMode));
    archiveFolder->Open(absolutePath, StringUtil::toString(mInstanceId));
    mArchiveFolders[absolutePath] = archiveFolder;
    return archiveFolder;
}


const IndexlibFileSystemPtr& MergeFileSystem::GetFileSystem()
{
    return GetThreadFileSystem()->fileSystem;
}

const string& MergeFileSystem::GetDescription()
{
    return GetThreadFileSystem()->description;
}

IE_NAMESPACE_END(merger);

