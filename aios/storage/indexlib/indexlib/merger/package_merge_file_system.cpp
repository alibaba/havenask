/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/merger/package_merge_file_system.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <future>
#include <utility>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/ThreadLocal.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/file_system/DirectoryOption.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/ListOption.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/fslib/IoConfig.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index_define.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::index;
using namespace indexlib::index_base;
using namespace indexlib::file_system;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, PackageMergeFileSystem);

const string PackageMergeFileSystem::PACKAGE_MERGE_CHECKPOINT = "package_merge_checkpoint.";

void PackageMergeFileSystem::CheckpointManager::Jsonize(JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) {
        vector<string> checkpointsVector(mCheckpoints.begin(), mCheckpoints.end());
        json.Jsonize("checkpoints", checkpointsVector);
    } else {
        vector<string> checkpointsVector;
        json.Jsonize("checkpoints", checkpointsVector);
        mCheckpoints.insert(checkpointsVector.begin(), checkpointsVector.end());
    }
    json.Jsonize("description_to_version", mDescriptionToMetaId, mDescriptionToMetaId);
    json.Jsonize("thread_count", mThreadCount, mThreadCount);
}

void PackageMergeFileSystem::CheckpointManager::Store(const ArchiveFolderPtr& folder)
{
    FileWriterPtr checkpointFile = folder->CreateFileWriter(PACKAGE_MERGE_CHECKPOINT).GetOrThrow();
    string checkpointContent = ToJsonString(*this);
    checkpointFile->Write(checkpointContent.c_str(), checkpointContent.size()).GetOrThrow();
    checkpointFile->Close().GetOrThrow();
}

void PackageMergeFileSystem::CheckpointManager::Load(const ArchiveFolderPtr& folder)
{
    assert(folder);
    FileReaderPtr checkpointFile = folder->CreateFileReader(PACKAGE_MERGE_CHECKPOINT).GetOrThrow();
    if (checkpointFile) {
        auto len = checkpointFile->GetLength();
        unique_ptr<char[]> buf(new char[len]);
        checkpointFile->Read(buf.get(), len).GetOrThrow();
        string content = string(buf.get(), len);
        FromJsonString(*this, content);
    }
}

void PackageMergeFileSystem::CheckpointManager::Recover(const ArchiveFolderPtr& folder, uint32_t threadCount)
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

void PackageMergeFileSystem::CheckpointManager::Commit(const ArchiveFolderPtr& folder, const string& description,
                                                       int32_t metaId)
{
    mDescriptionToMetaId[description] = metaId;
    Store(folder);
}

int32_t PackageMergeFileSystem::CheckpointManager::GetMetaId(const std::string& description) const
{
    int32_t metaId = -1;
    auto it = mDescriptionToMetaId.find(description);
    if (it != mDescriptionToMetaId.end()) {
        metaId = it->second;
    }
    return metaId;
}

//////////////////////////////////////////////////////////////////////

PackageMergeFileSystem::PackageMergeFileSystem(const string& rootPath, const MergeConfig& mergeConfig,
                                               uint32_t instanceId, const file_system::IFileSystemPtr& fileSystem)
    : MergeFileSystem(rootPath, mergeConfig, instanceId, fileSystem)
    , mNextThreadId(0)
    , mIsRecoverd(false)
    , mThreadLastTimestamp(new ThreadLocalPtr([](void* p) { delete (int64_t*)p; }))
    , mThreadCheckpoints(new ThreadLocalPtr([](void* p) { delete (vector<string>*)p; }))
{
    assert(mergeConfig.GetEnablePackageFile());
}

PackageMergeFileSystem::~PackageMergeFileSystem() {}

void PackageMergeFileSystem::Init(const vector<string>& targetSegmentPaths)
{
    ScopedLock lock(mLock);
    mTargetSegmentPaths = targetSegmentPaths;
    for (auto segmentPath : mTargetSegmentPaths) {
        if (!mFileSystem->IsExist(segmentPath).GetOrThrow(segmentPath)) {
            // attention!! there must be fslibWrapper rather than threadFs
            string path = PathUtil::JoinPath(mFileSystem->GetOutputRootPath(), segmentPath);
            file_system::ErrorCode mkdirEC = FslibWrapper::MkDir(path, true).Code();
            mTargetSegmentFileList[segmentPath] = {};
            THROW_IF_FS_ERROR(mkdirEC, "Make target segment dir [%s] failed", path.c_str());
        } else {
            IE_LOG(INFO, "need recover for segment path[%s]", mFileSystem->GetPhysicalPath(segmentPath).result.c_str());
            THROW_IF_FS_ERROR(
                mFileSystem->ListDir(segmentPath, ListOption::Recursive(false), mTargetSegmentFileList[segmentPath]),
                "list [%s] failed", segmentPath.c_str());
        }
    }
    ArchiveFolderPtr folder = CreateArchiveFolder(GetDirectory(""), MERGE_ITEM_CHECK_POINT_DIR_NAME, false);
    mCheckpointManager.Recover(folder, mMergeConfig.mergeThreadCount);
    if (mMergeConfig.mergeThreadCount != mCheckpointManager.GetThreadCount()) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "merge thread count changed from [%u] to [%u]",
                             mMergeConfig.mergeThreadCount, mCheckpointManager.GetThreadCount());
    }
    Recover();
}

void PackageMergeFileSystem::Commit()
{
    ThreadFileSystem* tfs = GetThreadFileSystem();
    int32_t metaId = Commit(tfs);
    IE_LOG(INFO, "close file system, desc[%s], metaId[%d]", tfs->description.c_str(), metaId);
}

void PackageMergeFileSystem::MakeDirectory(const string& absolutePath)
{
    assert(mNextThreadId == 0);
    ScopedLock lock(mLock);
    if (mNeedMakeDirectoryPaths.find(absolutePath) != mNeedMakeDirectoryPaths.end()) {
        return;
    }
    mNeedMakeDirectoryPaths.insert(absolutePath);
}

int32_t PackageMergeFileSystem::Commit(ThreadFileSystem* tfs)
{
    FSResult<int32_t> metaIdRet = tfs->fileSystem->CommitPackage();
    THROW_IF_FS_ERROR(metaIdRet.ec, "Make checkpoint failed, fs [%s] in [%s]", tfs->description.c_str(),
                      mRootPath.c_str());
    {
        ScopedLock lock(mLock);
        vector<string> threadCheckpoints;
        PopThreadCheckpoints(threadCheckpoints);
        for (const string& checkpoint : threadCheckpoints) {
            mCheckpointManager.MakeCheckpoint(checkpoint);
        }
        ArchiveFolderPtr folder = CreateArchiveFolder(GetDirectory(""), MERGE_ITEM_CHECK_POINT_DIR_NAME, false);
        mCheckpointManager.Commit(folder, tfs->description, metaIdRet.result);
    }
    SetLastTimestamp(TimeUtility::currentTimeInSeconds());
    return metaIdRet.result;
}

// just for recover, mock every threadLoalFileSystem to recover
void PackageMergeFileSystem::Recover()
{
    assert(!mIsRecoverd);
    mNextThreadId = 0;
    mThreadFileSystem.reset(new ThreadLocalPtr([](void* p) { delete (ThreadFileSystem*)p; }));
    for (int32_t threadId = 0; threadId < mCheckpointManager.GetThreadCount(); ++threadId) {
        string description = "i" + StringUtil::toString(mInstanceId) + "t" + StringUtil::toString(threadId);
        int32_t metaId = mCheckpointManager.GetMetaId(description);
        auto fs = GetFileSystem();
        for (auto segmentPath : mTargetSegmentPaths) {
            // reocver every dir even filelist is empty to recover package hint dir
            THROW_IF_FS_ERROR(fs->RecoverPackage(metaId, segmentPath, mTargetSegmentFileList[segmentPath]),
                              "recover failed, segmentPath[%s]", segmentPath.c_str());
        }
        mThreadFileSystems.emplace_back(fs);
        mThreadFileSystem.reset(new ThreadLocalPtr([](void* p) { delete (ThreadFileSystem*)p; }));
    }
    mNextThreadId = 0;
    mIsRecoverd = true;
}

void PackageMergeFileSystem::MakeCheckpoint(const string& fileName)
{
    AddThreadCheckpoint(fileName);
    ThreadFileSystem* tfs = GetThreadFileSystem();
    int64_t lastTimestamp = GetLastTimestamp();
    int64_t curTimestamp = TimeUtility::currentTimeInSeconds();
    if (curTimestamp - lastTimestamp < mMergeConfig.GetCheckpointInterval()) {
        tfs->fileSystem->Sync(true).GetOrThrow();
        return;
    }
    int32_t metaId = Commit(tfs);
    IE_LOG(INFO, "commit [%ld - %ld >= %u] for desc[%s], metaId[%d]", curTimestamp, lastTimestamp,
           mMergeConfig.GetCheckpointInterval(), tfs->description.c_str(), metaId);
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
    uint32_t threadId = GetThreadId();
    tfs->description = "i" + StringUtil::toString(mInstanceId) + "t" + StringUtil::toString(threadId);
    if (mIsRecoverd) {
        assert(mThreadFileSystems.size() > threadId);
        tfs->fileSystem = mThreadFileSystems[threadId];
        for (auto segmentPath : mTargetSegmentPaths) {
            THROW_IF_FS_ERROR(tfs->fileSystem->MakeDirectory(segmentPath, DirectoryOption::Package()),
                              "mkdir [%s] failed", segmentPath.c_str());
        }
        for (auto absPath : mNeedMakeDirectoryPaths) {
            THROW_IF_FS_ERROR(tfs->fileSystem->MakeDirectory(absPath, DirectoryOption()), "mkdir [%s] failed",
                              absPath.c_str());
        }
    } else {
        try {
            tfs->fileSystem = std::move(mFileSystem->CreateThreadOwnFileSystem(tfs->description).GetOrThrow());
            for (auto segmentPath : mTargetSegmentPaths) {
                THROW_IF_FS_ERROR(tfs->fileSystem->MakeDirectory(segmentPath, DirectoryOption::Package()),
                                  "mkdir [%s] failed", segmentPath.c_str());
            }
            for (auto absPath : mNeedMakeDirectoryPaths) {
                THROW_IF_FS_ERROR(tfs->fileSystem->MakeDirectory(absPath, DirectoryOption()), "mkdir [%s] failed",
                                  absPath.c_str());
            }
        } catch (...) {
            AUTIL_LOG(ERROR, "create merge thread filesystem [%s] failed", mRootPath.c_str());
            delete tfs;
            throw;
        }
    }
    SetLastTimestamp(TimeUtility::currentTimeInSeconds());
    IE_LOG(INFO, "create file system [%s] with package [%s]", mRootPath.c_str(), tfs->description.c_str());
    return tfs;
}

int64_t PackageMergeFileSystem::GetLastTimestamp()
{
    int64_t* lastTimeStamp = static_cast<int64_t*>(mThreadLastTimestamp->Get());
    if (lastTimeStamp == nullptr) {
        mThreadLastTimestamp->Reset(new int64_t(0));
        return 0;
    }
    return *lastTimeStamp;
}

void PackageMergeFileSystem::SetLastTimestamp(int64_t timestamp)
{
    int64_t* lastTimeStamp = static_cast<int64_t*>(mThreadLastTimestamp->Get());
    if (lastTimeStamp == nullptr) {
        mThreadLastTimestamp->Reset(new int64_t(timestamp));
    } else {
        *lastTimeStamp = timestamp;
    }
}

void PackageMergeFileSystem::AddThreadCheckpoint(const string& fileName)
{
    vector<string>* checkpoints = static_cast<vector<string>*>(mThreadCheckpoints->Get());
    if (checkpoints == nullptr) {
        mThreadCheckpoints->Reset(new vector<string>({fileName}));
    } else {
        checkpoints->push_back(fileName);
    }
}

void PackageMergeFileSystem::PopThreadCheckpoints(vector<string>& dest)
{
    vector<string>* checkpoints = static_cast<vector<string>*>(mThreadCheckpoints->Get());
    if (checkpoints == nullptr) {
        return;
    }
    dest.insert(dest.end(), checkpoints->begin(), checkpoints->end());
    checkpoints->clear();
}
}} // namespace indexlib::merger
