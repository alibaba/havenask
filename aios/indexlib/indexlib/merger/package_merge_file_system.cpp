#include <autil/TimeUtility.h>
#include <autil/StringUtil.h>
#include "indexlib/index_define.h"
#include "indexlib/merger/package_merge_file_system.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/util/thread_local.h"
#include "indexlib/util/path_util.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/package_storage.h"
#include "indexlib/file_system/indexlib_file_system_creator.h"
#include "indexlib/config/package_file_tag_config_list.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, PackageMergeFileSystem);

const string PackageMergeFileSystem::PACKAGE_MERGE_CHECKPOINT = "package_merge_checkpoint.";

void PackageMergeFileSystem::CheckpointManager::Jsonize(JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON)
    {
        vector<string> checkpointsVector(mCheckpoints.begin(), mCheckpoints.end());
        json.Jsonize("checkpoints", checkpointsVector);
    }
    else
    {
        vector<string> checkpointsVector;
        json.Jsonize("checkpoints", checkpointsVector);
        mCheckpoints.insert(checkpointsVector.begin(), checkpointsVector.end());
    }
    json.Jsonize("description_to_version", mDescriptionToMetaId, mDescriptionToMetaId);
    json.Jsonize("thread_count", mThreadCount, mThreadCount);
}

void PackageMergeFileSystem::CheckpointManager::Store(const ArchiveFolderPtr& folder)
{
    FileWrapperPtr checkpointFile = 
        folder->GetInnerFile(PACKAGE_MERGE_CHECKPOINT, fslib::WRITE);
    string checkpointContent = ToJsonString(*this);
    checkpointFile->Write(checkpointContent.c_str(), checkpointContent.size());
    checkpointFile->Close();
}

void PackageMergeFileSystem::CheckpointManager::Load(const ArchiveFolderPtr& folder)
{
    string content;
    FileWrapperPtr checkpointFile = 
        folder->GetInnerFile(PACKAGE_MERGE_CHECKPOINT, fslib::READ);
    if (checkpointFile)
    {
        FileSystemWrapper::AtomicLoad(checkpointFile.get(), content);
        FromJsonString(*this, content);
    }
}

void PackageMergeFileSystem::CheckpointManager::Recover(
        const ArchiveFolderPtr& folder, uint32_t threadCount)
{
    mThreadCount = threadCount;
    Load(folder);
}

void PackageMergeFileSystem::CheckpointManager::MakeCheckpoint(const string& fileName)
{
    mCheckpoints.insert(fileName);
}

bool PackageMergeFileSystem::CheckpointManager::HasCheckpoint(const string& fileName) const
{
    return mCheckpoints.count(fileName) > 0;
}

void PackageMergeFileSystem::CheckpointManager::Commit(const ArchiveFolderPtr& folder,
        const string& description, int32_t metaId)
{
    mDescriptionToMetaId[description] = metaId;
    Store(folder);
}

int32_t PackageMergeFileSystem::CheckpointManager::GetMetaId(const std::string& description) const
{
    int32_t metaId = -1;
    auto it = mDescriptionToMetaId.find(description);
    if (it != mDescriptionToMetaId.end())
    {
        metaId = it->second;
    }
    return metaId;
}

//////////////////////////////////////////////////////////////////////

PackageMergeFileSystem::PackageMergeFileSystem(const string& rootPath,
    const MergeConfig& mergeConfig, uint32_t instanceId, const storage::RaidConfigPtr& raidConfig)
    : MergeFileSystem(rootPath, mergeConfig, instanceId, raidConfig)
    , mInstanceId(instanceId)
    , mNextThreadId(0)
    , mThreadLastTimestamp(new ThreadLocalPtr([](void* p) { delete (int64_t*)p; }))
    , mThreadCheckpoints(new ThreadLocalPtr([](void* p) { delete (vector<string>*)p; }))
{
    assert(mergeConfig.GetEnablePackageFile());
}

PackageMergeFileSystem::~PackageMergeFileSystem() 
{
}

void PackageMergeFileSystem::Init(const vector<string>& targetSegmentPaths)
{
    ScopedLock lock(mLock);
    mTargetSegmentPaths = targetSegmentPaths;
    for (auto segmentPath : mTargetSegmentPaths)
    {
        string path = PathUtil::JoinPath(mRootPath, segmentPath);
        if (!FileSystemWrapper::IsExist(path))
        {
            assert(mTargetSegmentFileList.count(segmentPath) == 0);
            FileSystemWrapper::MkDir(path, true);
        }
        else
        {
            IE_LOG(INFO, "need recover for segment path[%s]", path.c_str());
            FileSystemWrapper::ListDir(path, mTargetSegmentFileList[segmentPath], false);
        }
    }
    string checkpointPath = PathUtil::JoinPath(mRootPath, MERGE_ITEM_CHECK_POINT_DIR_NAME);
    ArchiveFolderPtr folder = GetArchiveFolder(checkpointPath, true);
    mCheckpointManager.Recover(folder, mMergeConfig.mergeThreadCount);
    if (mMergeConfig.mergeThreadCount != mCheckpointManager.GetThreadCount())
    {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "merge thread count changed from [%u] to [%u]",
                             mMergeConfig.mergeThreadCount, mCheckpointManager.GetThreadCount());
    }
}

void PackageMergeFileSystem::Commit()
{
    ThreadFileSystem* tfs = GetThreadFileSystem();
    int32_t metaId = Commit(tfs);
    IE_LOG(INFO, "close file system, desc[%s], metaId[%d]",
           tfs->description.c_str(), metaId);    
}

void PackageMergeFileSystem::MakeDirectory(const string& absolutePath, bool mayExist)
{
    assert(mNextThreadId == 0);
    ScopedLock lock(mLock);
    if (mNeedMakeDirectoryPaths.find(absolutePath) != mNeedMakeDirectoryPaths.end())
    {
        if (mayExist)
        {
            return;
        }
        INDEXLIB_FATAL_ERROR(Exist, "directory [%s] already exist", absolutePath.c_str());
    }
    mNeedMakeDirectoryPaths.insert(absolutePath);
}

int32_t PackageMergeFileSystem::Commit(ThreadFileSystem* tfs)
{
    const PackageStoragePtr& storage = tfs->fileSystem->GetPackageStorage();
    int32_t metaId = storage->Commit();
    {
        ScopedLock lock(mLock);
        vector<string> threadCheckpoints;
        PopThreadCheckpoints(threadCheckpoints);
        for (const string& checkpoint : threadCheckpoints)
        {
            mCheckpointManager.MakeCheckpoint(checkpoint);
        }
        string checkpointPath = PathUtil::JoinPath(mRootPath, MERGE_ITEM_CHECK_POINT_DIR_NAME);
        ArchiveFolderPtr folder = GetArchiveFolder(checkpointPath, true);
        mCheckpointManager.Commit(folder, tfs->description, metaId);
    }
    SetLastTimestamp(TimeUtility::currentTimeInSeconds());
    return metaId;
}

void PackageMergeFileSystem::MakeCheckpoint(const string& fileName)
{
    AddThreadCheckpoint(fileName);
    ThreadFileSystem* tfs = GetThreadFileSystem();
    int64_t lastTimestamp = GetLastTimestamp();
    int64_t curTimestamp = TimeUtility::currentTimeInSeconds();
    if (curTimestamp - lastTimestamp < mMergeConfig.GetCheckpointInterval())
    {
        const PackageStoragePtr& storage = tfs->fileSystem->GetPackageStorage();
        storage->Sync();
        return;
    }
    int32_t metaId = Commit(tfs);
    IE_LOG(INFO, "commit [%ld - %ld >= %u] for desc[%s], metaId[%d]",
           curTimestamp, lastTimestamp, mMergeConfig.GetCheckpointInterval(),
           tfs->description.c_str(), metaId);
}

bool PackageMergeFileSystem::HasCheckpoint(const string& fileName)
{
    ScopedLock lock(mLock);
    return mCheckpointManager.HasCheckpoint(fileName);
}

uint32_t PackageMergeFileSystem::GetThreadId()
{
    ScopedLock lock(mLock);
    return mNextThreadId++;
}

// FULLPATH = mRootPath + / + relativeFilePath
MergeFileSystem::ThreadFileSystem* PackageMergeFileSystem::CreateFileSystem()
{
    ThreadFileSystem* tfs = new ThreadFileSystem();
    tfs->fileSystem = IndexlibFileSystemCreator::Create(mRootPath, "", mMetricProvider, mFileSystemOptions);
    tfs->description = "i" + StringUtil::toString(mInstanceId) +
                       "t" + StringUtil::toString(GetThreadId());
    int32_t metaId = mCheckpointManager.GetMetaId(tfs->description);
    for (auto segmentPath : mTargetSegmentPaths)
    {
        string path = PathUtil::JoinPath(mRootPath, segmentPath);
        tfs->fileSystem->MountPackageStorage(path, tfs->description);
        const PackageStoragePtr& storage = tfs->fileSystem->GetPackageStorage();
        if (mTargetSegmentFileList.count(segmentPath) > 0)
        {
            storage->Recover(path, metaId, mTargetSegmentFileList[segmentPath]);
        }
    }
    for (auto absPath : mNeedMakeDirectoryPaths)
    {
        tfs->fileSystem->MakeDirectory(absPath);
    }

    const PackageStoragePtr& storage = tfs->fileSystem->GetPackageStorage();
    storage->SetTagFunction([this](const string& relativeFilePath){
                // FULLPATH = mRootPath + / + relativeFilePath
                return mMergeConfig.GetPackageFileTagConfigList().Match(relativeFilePath, "");
            });
    SetLastTimestamp(TimeUtility::currentTimeInSeconds());
    IE_LOG(INFO, "create file system [%s] with package [%s]",
           mRootPath.c_str(), tfs->description.c_str());
    return tfs;
}

int64_t PackageMergeFileSystem::GetLastTimestamp()
{
    int64_t* lastTimeStamp = static_cast<int64_t*>(mThreadLastTimestamp->Get());
    if (lastTimeStamp == nullptr)
    {
        mThreadLastTimestamp->Reset(new int64_t(0));
        return 0;
    }
    return *lastTimeStamp;
}

void PackageMergeFileSystem::SetLastTimestamp(int64_t timestamp)
{
    int64_t* lastTimeStamp = static_cast<int64_t*>(mThreadLastTimestamp->Get());
    if (lastTimeStamp == nullptr)
    {
        mThreadLastTimestamp->Reset(new int64_t(timestamp));
    }
    else
    {
        *lastTimeStamp = timestamp;
    }
}

void PackageMergeFileSystem::AddThreadCheckpoint(const string& fileName)
{
    vector<string>* checkpoints = static_cast<vector<string>*>(mThreadCheckpoints->Get());
    if (checkpoints == nullptr)
    {
        mThreadCheckpoints->Reset(new vector<string>({fileName}));
    }
    else
    {
        checkpoints->push_back(fileName);
    }
}

void PackageMergeFileSystem::PopThreadCheckpoints(vector<string>& dest)
{
    vector<string>* checkpoints = static_cast<vector<string>*>(mThreadCheckpoints->Get());
    if (checkpoints == nullptr)
    {
        return;
    }
    dest.insert(dest.end(), checkpoints->begin(), checkpoints->end());
    checkpoints->clear();
}

IE_NAMESPACE_END(merger);

