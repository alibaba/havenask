#include "indexlib/merger/index_merge_meta.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/indexlib_file_system_creator.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/package_storage.h"
#include "indexlib/file_system/package_file_meta.h"
#include "indexlib/index_define.h"
#include "indexlib/merger/merge_define.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/util/path_util.h"
#include "indexlib/util/env_util.h"
#include "indexlib/index_base/index_meta/merge_task_resource.h"
#include "indexlib/index_base/merge_task_resource_manager.h"
#include <fslib/fs/FileSystem.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace autil;
using namespace autil::legacy;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(merger);

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
    mIsPackMeta = EnvUtil::GetEnv("INDEXLIB_PACKAGE_MERGE_META", false);
}

IndexMergeMeta::~IndexMergeMeta() 
{
}

void IndexMergeMeta::StoreMergeMetaVersion(const DirectoryPtr& directory) const
{
    FileWriterPtr mergeMetaVersion = directory->CreateFileWriter(MERGE_META_VERSION);
    string metaVersion = std::to_string(define::INDEX_MERGE_META_BINARY_VERSION);
    if (mergeMetaVersion->Write(metaVersion.c_str(), metaVersion.size()) != metaVersion.size())
    {
        INDEXLIB_FATAL_ERROR(FileIO, "store mergeMetaVersion[%s] to %s%s failed.",
                             metaVersion.c_str(), directory->GetPath().c_str(),
                             MERGE_META_VERSION.c_str());
    }
    mergeMetaVersion->Close();
}

void IndexMergeMeta::StoreTargetVersion(const DirectoryPtr& directory) const
{
    FileWriterPtr versionFile = directory->CreateFileWriter(VERSION_FILE_NAME_PREFIX);
    string versionStr = mTargetVersion.ToString();
    if (versionFile->Write(versionStr.c_str(), versionStr.size()) != versionStr.size())
    {
        INDEXLIB_FATAL_ERROR(FileIO, "store targetVersion[%s] to %s%s failed.",
                             versionStr.c_str(), directory->GetPath().c_str(),
                             VERSION_FILE_NAME_PREFIX);
    }
    versionFile->Close();
}

IndexlibFileSystemPtr IndexMergeMeta::CreateFileSystem(const string& rootPath) const
{
    string parentPath = PathUtil::GetParentDirPath(rootPath);
    string relativePath;
    PathUtil::GetRelativePath(parentPath, rootPath, relativePath);
    //prepare FileSystem
    IndexlibFileSystemPtr fs = IndexlibFileSystemCreator::Create(parentPath);
    return fs;
}

void IndexMergeMeta::Store(const string &rootPath) const
{
    if (FileSystemWrapper::IsExist(rootPath))
    {
        FileSystemWrapper::DeleteDir(rootPath);
    }
    FileSystemWrapper::MkDir(rootPath, true);
    IndexlibFileSystemPtr fs = CreateFileSystem(rootPath);
    if (mIsPackMeta)
    {
        fs->MountPackageStorage(rootPath, "");
    }
    DirectoryPtr mergeMetaDir = DirectoryCreator::Get(fs, rootPath, true);
    
    StoreMergeMetaVersion(mergeMetaDir);
    StoreTargetVersion(mergeMetaDir);
    StoreMergeTaskItems(mergeMetaDir);
    StoreMergeTaskResourceMeta(mergeMetaDir);

    assert(mMergePlans.size() == mMergePlanResources.size());
    for (uint32_t i = 0; i < mMergePlans.size(); ++i) 
    {
        DirectoryPtr planDir =
            mergeMetaDir->MakeDirectory(MERGE_PLAN_DIR_PREFIX + StringUtil::toString(i));
        mMergePlans[i].Store(planDir);

        const MergePlanResource &resource = mMergePlanResources[i];
        if (resource.reclaimMap)
        {
            resource.reclaimMap->StoreForMerge(
                planDir, ReclaimMap::RECLAIM_MAP_FILE_NAME,
                mMergePlans[i].GetSegmentMergeInfos());
        }
        if (resource.subReclaimMap)
        {
            resource.subReclaimMap->StoreForMerge(
                planDir, "sub_" + ReclaimMap::RECLAIM_MAP_FILE_NAME,
                mMergePlans[i].GetSubSegmentMergeInfos());
        }
        StoreBucketMaps(planDir, resource.bucketMaps);
    }
    FileWriterPtr tsWriter = mergeMetaDir->CreateFileWriter(MERGE_TIMESTAMP_FILE);
    string mergeTs = StringUtil::toString(mMergeTimestamp);
    tsWriter->Write(mergeTs.c_str(), mergeTs.size());
    tsWriter->Close();
    FileWriterPtr counterWriter = mergeMetaDir->CreateFileWriter(COUNTER_FILE_NAME);
    counterWriter->Write(mCounterMapContent.c_str(), mCounterMapContent.size());
    counterWriter->Close();
    if (mIsPackMeta)
    {
        fs->GetPackageStorage()->Commit();
    }
}

bool IndexMergeMeta::Load(const string &rootPath)
{
    return Load(rootPath, true);
}

void IndexMergeMeta::StoreMergeTaskItems(const DirectoryPtr& directory) const
{
    string content = ToJsonString(mMergeTaskItemsVec);
    FileWriterPtr writer = directory->CreateFileWriter(MergeTaskItem::MERGE_TASK_ITEMS_FILE_NAME);
    if (writer->Write(content.c_str(), content.size()) != content.size())
    {
        INDEXLIB_FATAL_ERROR(FileIO, "store mergeTaskItems[%s] to %s/%s failed.",
                             content.c_str(), directory->GetPath().c_str(),
                             MergeTaskItem::MERGE_TASK_ITEMS_FILE_NAME.c_str());
    }
    writer->Close();
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
    if (mMergeTaskResourceVec.empty())
    {
        return;
    }
    string content = ToJsonString(mMergeTaskResourceVec);
    FileWriterPtr writer = directory->CreateFileWriter(MERGE_TASK_RESOURCE_FILE);
    if (writer->Write(content.c_str(), content.size()) != content.size())
    {
        INDEXLIB_FATAL_ERROR(FileIO, "store MergeTaskResourceMeta[%s] to %s%s failed.",
                             content.c_str(), directory->GetPath().c_str(),
                             MERGE_TASK_RESOURCE_FILE.c_str());
    }
    writer->Close();
}

void IndexMergeMeta::LoadMergeTaskResourceMeta(const DirectoryPtr& directory)
{
    if (!directory->IsExist(MERGE_TASK_RESOURCE_FILE))
    {
        IE_LOG(INFO, "no merge task resource file: [%s/%s]!",
               directory->GetPath().c_str(), MERGE_TASK_RESOURCE_FILE.c_str());
        return;
    }
    string content;
    directory->Load(MERGE_TASK_RESOURCE_FILE, content);
    FromJsonString(mMergeTaskResourceVec, content);
}

bool IndexMergeMeta::LoadBasicInfo(const string &rootPath)
{
    return Load(rootPath, false);
}

bool IndexMergeMeta::LoadMergeMetaVersion(const DirectoryPtr &directory,
                                          int64_t &mergeMetaBinaryVersion) {
    if (!directory->IsExist(MERGE_META_VERSION))
    {
        IE_LOG(ERROR, "merge meta version[%s/%s] is not exist.",
               directory->GetPath().c_str(), MERGE_META_VERSION.c_str());
        return false;
    }
    string versionStr;
    directory->Load(MERGE_META_VERSION, versionStr);
    return StringUtil::fromString(versionStr, mergeMetaBinaryVersion);
}

bool IndexMergeMeta::Load(const string &rootPath, bool loadFullMeta)
{
    int64_t mergeMetaBinaryVersion = -1;
    IndexlibFileSystemPtr fs = CreateFileSystem(rootPath);
    DirectoryPtr mergeMetaDir = DirectoryCreator::Get(fs, rootPath, true);
    mergeMetaDir->MountPackageFile(PACKAGE_FILE_PREFIX);

    if (!LoadMergeMetaVersion(mergeMetaDir, mergeMetaBinaryVersion))
    {
        return false;
    }
    if (!mTargetVersion.Load(mergeMetaDir, VERSION_FILE_NAME_PREFIX))
    {
        return false;
    }
    if (!LoadMergeTaskItems(mergeMetaDir))
    {
        return false;
    }
    LoadMergeTaskResourceMeta(mergeMetaDir);

    FileList fileList;
    mergeMetaDir->ListFile("", fileList);
    for (size_t i = 0; i < fileList.size(); ++i)
    {
        if (fileList[i].find(MERGE_PLAN_DIR_PREFIX) != 0)
        {
            continue;
        }
        string planDirName = fileList[i];
        uint32_t mergePlanIdx = StringUtil::fromString<uint32_t>(
                planDirName.substr(MERGE_PLAN_DIR_PREFIX.size()));
        DirectoryPtr planDir = mergeMetaDir->GetDirectory(planDirName, true);
        
        if (mergePlanIdx >= mMergePlans.size())
        {
            mMergePlans.resize(mergePlanIdx + 1);
            if (loadFullMeta)
            {
                mMergePlanResources.resize(mergePlanIdx + 1);
            }
        }
        if (!mMergePlans[mergePlanIdx].Load(planDir, mergeMetaBinaryVersion))
        {
            return false;
        }

        if (loadFullMeta)
        {
            MergePlanResource &resource = mMergePlanResources[mergePlanIdx];
            const SegmentMergeInfos &segMergeInfos = mMergePlans[mergePlanIdx].GetSegmentMergeInfos();
            const SegmentMergeInfos &subSegMergeInfos = mMergePlans[mergePlanIdx].GetSubSegmentMergeInfos();
            if (planDir->IsExist(ReclaimMap::RECLAIM_MAP_FILE_NAME))
            {
                resource.reclaimMap.reset(new ReclaimMap());
                if (!resource.reclaimMap->LoadForMerge(planDir, ReclaimMap::RECLAIM_MAP_FILE_NAME, segMergeInfos, mergeMetaBinaryVersion)) {
                    return false;
                }
            }
            string subRclaimMapPath = "sub_" + ReclaimMap::RECLAIM_MAP_FILE_NAME;
            if (planDir->IsExist(subRclaimMapPath))
            {
                resource.subReclaimMap.reset(new ReclaimMap());
                if (!resource.subReclaimMap->LoadForMerge(planDir, subRclaimMapPath, subSegMergeInfos, mergeMetaBinaryVersion)) {
                    return false;
                }
            }
            if (!LoadBucketMaps(planDir, resource.bucketMaps))
            {
                return false;
            }
        }
    }

    if (!mergeMetaDir->IsExist(MERGE_TIMESTAMP_FILE))
    {
        IE_LOG(ERROR, "merge_meta_timestamp[%s/%s] is not exist.",
               mergeMetaDir->GetPath().c_str(), MERGE_TIMESTAMP_FILE.c_str());
        return false;
    }
    string mergeTsStr;
    mergeMetaDir->Load(MERGE_TIMESTAMP_FILE, mergeTsStr);
    mMergeTimestamp = StringUtil::fromString<int64_t>(mergeTsStr);
    
    if (!mergeMetaDir->IsExist(COUNTER_FILE_NAME))
    {
        IE_LOG(ERROR, "counter[%s/%s] is not exist.",
               mergeMetaDir->GetPath().c_str(), COUNTER_FILE_NAME);
        return false;
    }
    mergeMetaDir->Load(COUNTER_FILE_NAME, mCounterMapContent);
    return true;
}

vector<ReclaimMapPtr> IndexMergeMeta::GetReclaimMapVec() const
{
    vector<ReclaimMapPtr> vec;
    for (size_t i = 0; i < mMergePlanResources.size(); ++i)
    {
        vec.push_back(mMergePlanResources[i].reclaimMap);
    }
    return vec;
}

vector<ReclaimMapPtr> IndexMergeMeta::GetSubPartReclaimMapVec() const
{
    vector<ReclaimMapPtr> vec;
    for (size_t i = 0; i < mMergePlanResources.size(); ++i)
    {
        vec.push_back(mMergePlanResources[i].subReclaimMap);
    }
    return vec;
}

void IndexMergeMeta::StoreBucketMaps(const DirectoryPtr &directory, const BucketMaps &bucketMaps)
{
    for (BucketMaps::const_iterator it = bucketMaps.begin();
         it != bucketMaps.end(); ++it)
    {
        const string &truncateName = it->first;
        const BucketMapPtr &bucketMapPtr = it->second;
        bucketMapPtr->Store(directory, BucketMap::BUKCET_MAP_FILE_NAME_PREFIX + truncateName);
    }
}

bool IndexMergeMeta::LoadBucketMaps(const DirectoryPtr &directory, BucketMaps &bucketMaps) 
{
    FileList fileList;
    directory->ListFile("", fileList);
    for (FileList::const_iterator it = fileList.begin();
         it != fileList.end(); ++it)
    {
        const string &fileName = *it;
        if (fileName.find(BucketMap::BUKCET_MAP_FILE_NAME_PREFIX) != 0)
        {
            continue;
        }
        string truncateName = fileName.substr(BucketMap::BUKCET_MAP_FILE_NAME_PREFIX.size());
        BucketMapPtr bucketMapPtr(new BucketMap);
        if (!bucketMapPtr->Load(directory, fileName))
        {
            return false;
        }
        bucketMaps[truncateName] = bucketMapPtr;
    }
    return true;
}

int64_t IndexMergeMeta::EstimateMemoryUse() const
{
    int64_t memUse = 0;
    for (size_t i = 0; i < mMergePlanResources.size(); i++)
    {
        const MergePlanResource &resource = mMergePlanResources[i];
        if (resource.reclaimMap)
        {
            memUse += resource.reclaimMap->EstimateMemoryUse();
        }
        if (resource.subReclaimMap)
        {
            memUse += resource.subReclaimMap->EstimateMemoryUse();
        }
        for (BucketMaps::const_iterator it = resource.bucketMaps.begin();
             it != resource.bucketMaps.end(); it++)
        {
            const BucketMapPtr &bucketMap = it->second;
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

IE_NAMESPACE_END(merger);

