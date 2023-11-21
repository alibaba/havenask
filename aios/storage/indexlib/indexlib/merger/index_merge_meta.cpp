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
#include "indexlib/merger/index_merge_meta.h"

#include <cstddef>
#include <map>
#include <utility>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "indexlib/base/Constant.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/fslib/DeleteOption.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/index_meta/segment_temperature_meta.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/merge_task_resource_manager.h"
#include "indexlib/merger/merge_define.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/counter/CounterMap.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace autil;
using namespace autil::legacy;

using namespace indexlib::index;
using namespace indexlib::index_base;
using namespace indexlib::file_system;
using namespace indexlib::util;

using namespace indexlib::util;
namespace indexlib { namespace merger {

IE_LOG_SETUP(merger, IndexMergeMeta);

const string IndexMergeMeta::MERGE_PLAN_DIR_PREFIX = "merge_plan_";
const string IndexMergeMeta::MERGE_META_VERSION = "merge_meta_version";
const string IndexMergeMeta::MERGE_TIMESTAMP_FILE = "merge_timestamp";
const string IndexMergeMeta::MERGE_TASK_RESOURCE_FILE = "merge_task_resource_meta";

IndexMergeMeta::IndexMergeMeta()
    : mMergeTimestamp(0)
    , mCounterMapContent(util::CounterMap::EMPTY_COUNTER_MAP_JSON)
    , mIsPackMeta(false)
{
    mIsPackMeta = autil::EnvUtil::getEnv("INDEXLIB_PACKAGE_MERGE_META", false);
}

IndexMergeMeta::~IndexMergeMeta() {}

void IndexMergeMeta::StoreMergeMetaVersion(const DirectoryPtr& directory) const
{
    FileWriterPtr mergeMetaVersion = directory->CreateFileWriter(MERGE_META_VERSION);
    string metaVersion = std::to_string(define::INDEX_MERGE_META_BINARY_VERSION);
    if (mergeMetaVersion->Write(metaVersion.c_str(), metaVersion.size()).GetOrThrow() != metaVersion.size()) {
        INDEXLIB_FATAL_ERROR(FileIO, "store mergeMetaVersion[%s] to %s%s failed.", metaVersion.c_str(),
                             directory->DebugString().c_str(), MERGE_META_VERSION.c_str());
    }
    mergeMetaVersion->Close().GetOrThrow();
}

void IndexMergeMeta::StoreTargetVersion(const DirectoryPtr& directory) const
{
    FileWriterPtr versionFile = directory->CreateFileWriter(VERSION_FILE_NAME_PREFIX);
    string versionStr = mTargetVersion.ToString();
    if (versionFile->Write(versionStr.c_str(), versionStr.size()).GetOrThrow() != versionStr.size()) {
        INDEXLIB_FATAL_ERROR(FileIO, "store targetVersion[%s] to %s%s failed.", versionStr.c_str(),
                             directory->DebugString().c_str(), VERSION_FILE_NAME_PREFIX.c_str());
    }
    versionFile->Close().GetOrThrow();
}

IFileSystemPtr IndexMergeMeta::CreateFileSystem(const string& rootPath, bool isOverride) const
{
    if (isOverride) {
        auto ec = FslibWrapper::DeleteDir(rootPath, DeleteOption::NoFence(true)).Code();
        THROW_IF_FS_ERROR(ec, "clean index merge meta root dir failed, path [%s]", rootPath.c_str());
    }
    // prepare FileSystem
    FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    fsOptions.useCache = false;
    fsOptions.outputStorage = mIsPackMeta ? FSST_PACKAGE_MEM : FSST_DISK;
    auto fs = BranchFS::Create(rootPath);
    fs->Init(nullptr, fsOptions);
    if (unlikely(fs == nullptr)) {
        INDEXLIB_FATAL_ERROR(FileIO, "create file_system failed, path[%s].", rootPath.c_str());
    }
    return fs->GetFileSystem();
}

void IndexMergeMeta::InnerStore(const string& mergeMetaPath) const
{
    bool isOverride = true;
    IFileSystemPtr fs = CreateFileSystem(mergeMetaPath, isOverride);
    DirectoryPtr mergeMetaDir = Directory::Get(fs);
    StoreMergeMetaVersion(mergeMetaDir);
    StoreTargetVersion(mergeMetaDir);
    StoreMergeTaskItems(mergeMetaDir);
    StoreMergeTaskResourceMeta(mergeMetaDir);

    assert(mMergePlans.size() == mMergePlanResources.size());
    for (uint32_t i = 0; i < mMergePlans.size(); ++i) {
        DirectoryPtr planDir = mergeMetaDir->MakeDirectory(MERGE_PLAN_DIR_PREFIX + StringUtil::toString(i));
        mMergePlans[i].Store(planDir);

        const MergePlanResource& resource = mMergePlanResources[i];
        if (resource.reclaimMap) {
            resource.reclaimMap->StoreForMerge(planDir, ReclaimMap::RECLAIM_MAP_FILE_NAME,
                                               mMergePlans[i].GetSegmentMergeInfos());
        }
        if (resource.subReclaimMap) {
            resource.subReclaimMap->StoreForMerge(planDir, "sub_" + ReclaimMap::RECLAIM_MAP_FILE_NAME,
                                                  mMergePlans[i].GetSubSegmentMergeInfos());
        }
        StoreBucketMaps(planDir, resource.bucketMaps);
        planDir->Close();
    }
    FileWriterPtr tsWriter = mergeMetaDir->CreateFileWriter(MERGE_TIMESTAMP_FILE);
    string mergeTs = StringUtil::toString(mMergeTimestamp);
    tsWriter->Write(mergeTs.c_str(), mergeTs.size()).GetOrThrow();
    tsWriter->Close().GetOrThrow();
    FileWriterPtr counterWriter = mergeMetaDir->CreateFileWriter(COUNTER_FILE_NAME);
    counterWriter->Write(mCounterMapContent.c_str(), mCounterMapContent.size()).GetOrThrow();
    counterWriter->Close().GetOrThrow();

    mergeMetaDir->Sync(true);
}

void IndexMergeMeta::Store(const string& mergeMetaPath, FenceContext* fenceContext) const
{
    if (!fenceContext) {
        return InnerStore(mergeMetaPath);
    }

    // make fence directory-granularity, not file-granularity, because
    // (1) package file rename is not supported
    // (2) avoid too many fence check
    string mergeMetaTempPath = mergeMetaPath;
    PathUtil::TrimLastDelim(mergeMetaTempPath);
    mergeMetaTempPath = mergeMetaTempPath + "." + fenceContext->epochId + TEMP_FILE_SUFFIX;
    InnerStore(mergeMetaTempPath);
    auto ec = FslibWrapper::DeleteDir(mergeMetaPath, DeleteOption::Fence(fenceContext, true)).Code();
    THROW_IF_FS_ERROR(ec, "clean index merge meta root dir failed, path [%s]", mergeMetaPath.c_str());

    ec = FslibWrapper::RenameWithFenceContext(mergeMetaTempPath, mergeMetaPath, fenceContext).Code();
    THROW_IF_FS_ERROR(ec, "merge meta rename failed");
}

bool IndexMergeMeta::Load(const string& mergeMetaPath) { return Load(mergeMetaPath, true); }

void IndexMergeMeta::StoreMergeTaskItems(const DirectoryPtr& directory) const
{
    string content = ToJsonString(mMergeTaskItemsVec);
    FileWriterPtr writer = directory->CreateFileWriter(MergeTaskItem::MERGE_TASK_ITEMS_FILE_NAME);
    if (writer->Write(content.c_str(), content.size()).GetOrThrow() != content.size()) {
        INDEXLIB_FATAL_ERROR(FileIO, "store mergeTaskItems[%s] to %s/%s failed.", content.c_str(),
                             directory->DebugString().c_str(), MergeTaskItem::MERGE_TASK_ITEMS_FILE_NAME.c_str());
    }
    writer->Close().GetOrThrow();
}

bool IndexMergeMeta::LoadMergeTaskItems(const DirectoryPtr& directory)
{
    string content;
    directory->Load(MergeTaskItem::MERGE_TASK_ITEMS_FILE_NAME, content);
    FromJsonString(mMergeTaskItemsVec, content);
    return true;
}

void IndexMergeMeta::StoreMergeTaskResourceMeta(const DirectoryPtr& directory) const
{
    if (mMergeTaskResourceVec.empty()) {
        return;
    }
    string content = ToJsonString(mMergeTaskResourceVec);
    FileWriterPtr writer = directory->CreateFileWriter(MERGE_TASK_RESOURCE_FILE);
    if (writer->Write(content.c_str(), content.size()).GetOrThrow() != content.size()) {
        INDEXLIB_FATAL_ERROR(FileIO, "store MergeTaskResourceMeta[%s] to %s%s failed.", content.c_str(),
                             directory->DebugString().c_str(), MERGE_TASK_RESOURCE_FILE.c_str());
    }
    writer->Close().GetOrThrow();
}

void IndexMergeMeta::LoadMergeTaskResourceMeta(const DirectoryPtr& directory)
{
    if (!directory->IsExist(MERGE_TASK_RESOURCE_FILE)) {
        IE_LOG(INFO, "no merge task resource file: [%s/%s]!", directory->DebugString().c_str(),
               MERGE_TASK_RESOURCE_FILE.c_str());
        return;
    }
    string content;
    directory->Load(MERGE_TASK_RESOURCE_FILE, content);
    FromJsonString(mMergeTaskResourceVec, content);
}

bool IndexMergeMeta::LoadBasicInfo(const string& mergeMetaPath) { return Load(mergeMetaPath, false); }

bool IndexMergeMeta::LoadMergeMetaVersion(const DirectoryPtr& directory, int64_t& mergeMetaBinaryVersion)
{
    if (!directory->IsExist(MERGE_META_VERSION)) {
        IE_LOG(ERROR, "merge meta version[%s/%s] is not exist.", directory->DebugString().c_str(),
               MERGE_META_VERSION.c_str());
        return false;
    }
    string versionStr;
    directory->Load(MERGE_META_VERSION, versionStr);
    return StringUtil::fromString(versionStr, mergeMetaBinaryVersion);
}

bool IndexMergeMeta::Load(const string& rootPath, bool loadFullMeta)
{
    int64_t mergeMetaBinaryVersion = -1;
    IFileSystemPtr fs = CreateFileSystem(rootPath);
    DirectoryPtr mergeMetaDir = Directory::Get(fs);
    mergeMetaDir->MountPackage();

    if (!LoadMergeMetaVersion(mergeMetaDir, mergeMetaBinaryVersion)) {
        return false;
    }
    if (!mTargetVersion.Load(mergeMetaDir, VERSION_FILE_NAME_PREFIX)) {
        return false;
    }
    if (!LoadMergeTaskItems(mergeMetaDir)) {
        return false;
    }
    LoadMergeTaskResourceMeta(mergeMetaDir);

    FileList fileList;
    mergeMetaDir->ListDir("", fileList);
    for (size_t i = 0; i < fileList.size(); ++i) {
        if (fileList[i].find(MERGE_PLAN_DIR_PREFIX) != 0) {
            continue;
        }
        string planDirName = fileList[i];
        uint32_t mergePlanIdx = StringUtil::fromString<uint32_t>(planDirName.substr(MERGE_PLAN_DIR_PREFIX.size()));
        DirectoryPtr planDir = mergeMetaDir->GetDirectory(planDirName, true);

        if (mergePlanIdx >= mMergePlans.size()) {
            mMergePlans.resize(mergePlanIdx + 1);
            if (loadFullMeta) {
                mMergePlanResources.resize(mergePlanIdx + 1);
            }
        }
        if (!mMergePlans[mergePlanIdx].Load(planDir, mergeMetaBinaryVersion)) {
            return false;
        }

        if (loadFullMeta) {
            MergePlanResource& resource = mMergePlanResources[mergePlanIdx];
            const SegmentMergeInfos& segMergeInfos = mMergePlans[mergePlanIdx].GetSegmentMergeInfos();
            const SegmentMergeInfos& subSegMergeInfos = mMergePlans[mergePlanIdx].GetSubSegmentMergeInfos();
            if (planDir->IsExist(ReclaimMap::RECLAIM_MAP_FILE_NAME)) {
                resource.reclaimMap.reset(new ReclaimMap());
                if (!resource.reclaimMap->LoadForMerge(planDir, ReclaimMap::RECLAIM_MAP_FILE_NAME, segMergeInfos,
                                                       mergeMetaBinaryVersion)) {
                    return false;
                }
            }
            string subRclaimMapPath = "sub_" + ReclaimMap::RECLAIM_MAP_FILE_NAME;
            if (planDir->IsExist(subRclaimMapPath)) {
                resource.subReclaimMap.reset(new ReclaimMap());
                if (!resource.subReclaimMap->LoadForMerge(planDir, subRclaimMapPath, subSegMergeInfos,
                                                          mergeMetaBinaryVersion)) {
                    return false;
                }
            }
            if (!LoadBucketMaps(planDir, resource.bucketMaps)) {
                return false;
            }
        }
    }

    if (!mergeMetaDir->IsExist(MERGE_TIMESTAMP_FILE)) {
        IE_LOG(ERROR, "merge_meta_timestamp[%s/%s] is not exist.", mergeMetaDir->DebugString().c_str(),
               MERGE_TIMESTAMP_FILE.c_str());
        return false;
    }
    string mergeTsStr;
    mergeMetaDir->Load(MERGE_TIMESTAMP_FILE, mergeTsStr);
    mMergeTimestamp = StringUtil::fromString<int64_t>(mergeTsStr);

    if (!mergeMetaDir->IsExist(COUNTER_FILE_NAME)) {
        IE_LOG(ERROR, "counter[%s/%s] is not exist.", mergeMetaDir->DebugString().c_str(), COUNTER_FILE_NAME.c_str());
        return false;
    }
    mergeMetaDir->Load(COUNTER_FILE_NAME, mCounterMapContent);
    return true;
}

vector<ReclaimMapPtr> IndexMergeMeta::GetReclaimMapVec() const
{
    vector<ReclaimMapPtr> vec;
    for (size_t i = 0; i < mMergePlanResources.size(); ++i) {
        vec.push_back(mMergePlanResources[i].reclaimMap);
    }
    return vec;
}

vector<ReclaimMapPtr> IndexMergeMeta::GetSubPartReclaimMapVec() const
{
    vector<ReclaimMapPtr> vec;
    for (size_t i = 0; i < mMergePlanResources.size(); ++i) {
        vec.push_back(mMergePlanResources[i].subReclaimMap);
    }
    return vec;
}

void IndexMergeMeta::StoreBucketMaps(const DirectoryPtr& directory, const index::legacy::BucketMaps& bucketMaps)
{
    for (index::legacy::BucketMaps::const_iterator it = bucketMaps.begin(); it != bucketMaps.end(); ++it) {
        const string& truncateName = it->first;
        const auto& bucketMapPtr = it->second;
        bucketMapPtr->Store(directory, index::legacy::BucketMap::BUKCET_MAP_FILE_NAME_PREFIX + truncateName);
    }
}

bool IndexMergeMeta::LoadBucketMaps(const DirectoryPtr& directory, index::legacy::BucketMaps& bucketMaps)
{
    FileList fileList;
    directory->ListDir("", fileList);
    for (FileList::const_iterator it = fileList.begin(); it != fileList.end(); ++it) {
        const string& fileName = *it;
        if (fileName.find(index::legacy::BucketMap::BUKCET_MAP_FILE_NAME_PREFIX) != 0) {
            continue;
        }
        string truncateName = fileName.substr(index::legacy::BucketMap::BUKCET_MAP_FILE_NAME_PREFIX.size());
        auto bucketMapPtr = std::make_shared<index::legacy::BucketMap>();
        if (!bucketMapPtr->Load(directory, fileName)) {
            return false;
        }
        bucketMaps[truncateName] = bucketMapPtr;
    }
    return true;
}

int64_t IndexMergeMeta::EstimateMemoryUse() const
{
    int64_t memUse = 0;
    for (size_t i = 0; i < mMergePlanResources.size(); i++) {
        const MergePlanResource& resource = mMergePlanResources[i];
        if (resource.reclaimMap) {
            memUse += resource.reclaimMap->EstimateMemoryUse();
        }
        if (resource.subReclaimMap) {
            memUse += resource.subReclaimMap->EstimateMemoryUse();
        }
        for (index::legacy::BucketMaps::const_iterator it = resource.bucketMaps.begin();
             it != resource.bucketMaps.end(); it++) {
            const auto& bucketMap = it->second;
            memUse += bucketMap->EstimateMemoryUse();
        }
    }
    return memUse;
}

MergeTaskResourceManagerPtr IndexMergeMeta::CreateMergeTaskResourceManager() const
{
    MergeTaskResourceManagerPtr mgr(new MergeTaskResourceManager);
    mgr->Init(mMergeTaskResourceVec);
    return mgr;
}
}} // namespace indexlib::merger
