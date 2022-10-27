#include "indexlib/merger/table_merge_meta.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/file_system/package_storage.h"
#include "indexlib/table/table_merge_plan.h"
#include "indexlib/table/table_merge_plan_meta.h"
#include "indexlib/table/table_merge_plan_resource.h"

#include <autil/StringUtil.h>
#include <fslib/fs/FileSystem.h>

using namespace std;
using namespace autil::legacy;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(table);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, TableMergeMeta);

const string TableMergeMeta::MERGE_META_VERSION = "merge_meta_version";
const string TableMergeMeta::MERGE_META_BINARY_VERSION = "1";
const string TableMergeMeta::TASK_EXECUTE_META_FILE = "task_execute_meta";
const string TableMergeMeta::MERGE_PLAN_DIR_PREFIX = "merge_plan_";
const string TableMergeMeta::MERGE_PLAN_RESOURCE_DIR_PREFIX = "plan_resource";
const string TableMergeMeta::MERGE_PLAN_FILE = "merge_plan";
const string TableMergeMeta::MERGE_PLAN_META_FILE = "merge_plan_meta";
const string TableMergeMeta::MERGE_PLAN_TASK_DESCRIPTION_FILE = "merge_tasks";

TableMergeMeta::TableMergeMeta(const table::MergePolicyPtr& mergePolicy)
    : mMergePolicy(mergePolicy)
{
}

TableMergeMeta::~TableMergeMeta() 
{
}

void TableMergeMeta::Store(const std::string &rootPath) const
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
    mergeMetaDir->Store(MERGE_META_VERSION, MERGE_META_BINARY_VERSION, false);
    StoreTargetVersion(mergeMetaDir);

    string taskExecuteMetaContent = ToJsonString(instanceExecMetas);
    mergeMetaDir->Store(TASK_EXECUTE_META_FILE, taskExecuteMetaContent, false);

    for (size_t planIdx = 0; planIdx < mergePlans.size(); ++planIdx)
    {
        DirectoryPtr planDir = mergeMetaDir->MakeDirectory(
            MERGE_PLAN_DIR_PREFIX + StringUtil::toString(planIdx));

        if (!StoreSingleMergePlan(planIdx, planDir))
        {
            INDEXLIB_FATAL_ERROR(Runtime, "fail to store mergePlan[%zu] to dir[%s]",
                                 planIdx, planDir->GetPath().c_str());
        }
    }
    if (mIsPackMeta)
    {
        fs->GetPackageStorage()->Commit();
    }
}

bool TableMergeMeta::Load(const std::string &rootPath, bool loadFullMeta)
{
    string mergeMetaBinaryVersion;
    IndexlibFileSystemPtr fs = CreateFileSystem(rootPath);
    DirectoryPtr mergeMetaDir = DirectoryCreator::Get(fs, rootPath, true);
    mergeMetaDir->MountPackageFile(PACKAGE_FILE_PREFIX);

    mergeMetaDir->Load(MERGE_META_VERSION, mergeMetaBinaryVersion);
    if (mergeMetaBinaryVersion != MERGE_META_BINARY_VERSION)
    {
        IE_LOG(ERROR, "load failed, incompatible merge meta binary version[%s]",
               mergeMetaBinaryVersion.c_str());
    }

    if (!mTargetVersion.Load(
            mergeMetaDir, VERSION_FILE_NAME_PREFIX))
    {
        return false;
    }

    string taskExecuteMetaContent;
    mergeMetaDir->Load(TASK_EXECUTE_META_FILE, taskExecuteMetaContent);
    FromJsonString(instanceExecMetas, taskExecuteMetaContent);

    fslib::FileList fileList;
    mergeMetaDir->ListFile("", fileList);
    size_t mergePlanCount = 0;
    for (size_t i = 0; i < fileList.size(); ++i)
    {
        if (fileList[i].find(MERGE_PLAN_DIR_PREFIX) == 0)
        {
            mergePlanCount++;
        }
    }
    mergePlans.resize(mergePlanCount);
    mergePlanMetas.resize(mergePlanCount);
    mergePlanResources.resize(mergePlanCount);
    mergeTaskDescriptions.resize(mergePlanCount);

    for (size_t i = 0; i < fileList.size(); ++i)
    {
        if (fileList[i].find(MERGE_PLAN_DIR_PREFIX) != 0)
        {
            continue;
        }
        uint32_t mergePlanIdx = StringUtil::fromString<uint32_t>(
                fileList[i].substr(MERGE_PLAN_DIR_PREFIX.size()));

        if (mergePlanIdx > mergePlanCount) {
            IE_LOG(ERROR, "Unexpected error: mergePlanIdx[%u] exceeds mergePlanCount[%zu]",
                   mergePlanIdx, mergePlanCount);
            return false;
        }
        DirectoryPtr planDir = mergeMetaDir->GetDirectory(fileList[i], true);
        if (!LoadSingleMergePlan(mergePlanIdx, planDir, loadFullMeta))
        {
            IE_LOG(ERROR, "load MergePlan[%u] failed from dir[%s]",
                   mergePlanIdx, planDir->GetPath().c_str());
            return false;
        }
    }
    return true;
}

bool TableMergeMeta::StoreSingleMergePlan(
    size_t mergePlanIdx, const DirectoryPtr& planDir) const
{
    planDir->Store(MERGE_PLAN_FILE, ToJsonString(*mergePlans[mergePlanIdx]), false);
    planDir->Store(MERGE_PLAN_META_FILE, ToJsonString(*mergePlanMetas[mergePlanIdx]), false);
    planDir->Store(MERGE_PLAN_TASK_DESCRIPTION_FILE,
                   ToJsonString(mergeTaskDescriptions[mergePlanIdx]), false);

    if (!mergePlanResources[mergePlanIdx])
    {
        return true;
    }
    DirectoryPtr resourceDir = planDir->MakeDirectory(MERGE_PLAN_RESOURCE_DIR_PREFIX);
    if (!mergePlanResources[mergePlanIdx]->Store(resourceDir))
    {
        IE_LOG(ERROR, "fail to store planResource[%zu] to dir[%s]",
               mergePlanIdx, resourceDir->GetPath().c_str());
        return false;
    }
    return true;
}

bool TableMergeMeta::LoadSingleMergePlan(size_t mergePlanIdx,
                                         const DirectoryPtr& planDir,
                                         bool loadResource)
{
    string mergePlanContent;
    planDir->Load(MERGE_PLAN_FILE, mergePlanContent);
    string mergePlanMetaContent;
    planDir->Load(MERGE_PLAN_META_FILE, mergePlanMetaContent);
    string mergeTaskDescriptionsContent;
    planDir->Load(MERGE_PLAN_TASK_DESCRIPTION_FILE, mergeTaskDescriptionsContent);

    mergePlans[mergePlanIdx].reset(new TableMergePlan());
    FromJsonString(*mergePlans[mergePlanIdx], mergePlanContent);
    mergePlanMetas[mergePlanIdx].reset(new TableMergePlanMeta());
    FromJsonString(*mergePlanMetas[mergePlanIdx], mergePlanMetaContent);
    FromJsonString(mergeTaskDescriptions[mergePlanIdx], mergeTaskDescriptionsContent); 

    if (!loadResource || !mMergePolicy.get())
    {
        return true;
    }
    if (!planDir->IsExist(MERGE_PLAN_RESOURCE_DIR_PREFIX))
    {
        IE_LOG(INFO, "resource not found in [%s/%s]", planDir->GetPath().c_str(),
               MERGE_PLAN_RESOURCE_DIR_PREFIX.c_str());
        return true;
    }
    DirectoryPtr resourceDir = planDir->GetDirectory(MERGE_PLAN_RESOURCE_DIR_PREFIX, true);
    TableMergePlanResourcePtr planResource(mMergePolicy->CreateMergePlanResource());
    if (!planResource)
    {
        IE_LOG(WARN, "Get NULL TableMergePlanResource from MergePolicy");
    }
    else
    {
        if (!planResource->Load(resourceDir))
        {
            IE_LOG(ERROR, "fail to load planResource[%zu] from dir[%s]",
                   mergePlanIdx, resourceDir->GetPath().c_str());
            return false;
        }        
    }
    mergePlanResources[mergePlanIdx] = planResource;
    return true;    
}

int64_t TableMergeMeta::EstimateMemoryUse() const
{
    int64_t resourceMemUse = 0;
    for (const auto& resource: mergePlanResources)
    {
        if (resource)
        {
            resourceMemUse += static_cast<int64_t>(resource->EstimateMemoryUse());
        }
    }
    return resourceMemUse;
}

vector<segmentid_t> TableMergeMeta::GetTargetSegmentIds(size_t planIdx) const
{
    return { mergePlanMetas[planIdx]->targetSegmentId };
}

IE_NAMESPACE_END(merger);
