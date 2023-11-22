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
#include "indexlib/merger/table_merge_meta.h"

#include <cstddef>
#include <ext/alloc_traits.h>
#include <memory>

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IFileSystem.h"
// #include "indexlib/file_system/package_storage.h"
#include "alog/Logger.h"
#include "autil/LongHashValue.h"
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "indexlib/base/Constant.h"
#include "indexlib/file_system/DirectoryOption.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/fslib/FenceContext.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/table/table_merge_plan.h"
#include "indexlib/table/table_merge_plan_meta.h"
#include "indexlib/table/table_merge_plan_resource.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

using namespace indexlib::file_system;
using namespace indexlib::table;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, TableMergeMeta);

const string TableMergeMeta::MERGE_META_VERSION = "merge_meta_version";
const string TableMergeMeta::MERGE_META_BINARY_VERSION = "1";
const string TableMergeMeta::TASK_EXECUTE_META_FILE = "task_execute_meta";
const string TableMergeMeta::MERGE_PLAN_DIR_PREFIX = "merge_plan_";
const string TableMergeMeta::MERGE_PLAN_RESOURCE_DIR_PREFIX = "plan_resource";
const string TableMergeMeta::MERGE_PLAN_FILE = "merge_plan";
const string TableMergeMeta::MERGE_PLAN_META_FILE = "merge_plan_meta";
const string TableMergeMeta::MERGE_PLAN_TASK_DESCRIPTION_FILE = "merge_tasks";

TableMergeMeta::TableMergeMeta(const table::MergePolicyPtr& mergePolicy) : mMergePolicy(mergePolicy) {}

TableMergeMeta::~TableMergeMeta() {}

void TableMergeMeta::InnerStore(const std::string& mergeMetaPath) const
{
    bool isOverride = true;
    IFileSystemPtr fs = CreateFileSystem(mergeMetaPath, isOverride);
    DirectoryPtr mergeMetaDir = Directory::Get(fs);
    mergeMetaDir->Store(MERGE_META_VERSION, MERGE_META_BINARY_VERSION, WriterOption());
    StoreTargetVersion(mergeMetaDir);

    string taskExecuteMetaContent = ToJsonString(instanceExecMetas);
    mergeMetaDir->Store(TASK_EXECUTE_META_FILE, taskExecuteMetaContent, WriterOption());

    for (size_t planIdx = 0; planIdx < mergePlans.size(); ++planIdx) {
        DirectoryPtr planDir = mergeMetaDir->MakeDirectory(MERGE_PLAN_DIR_PREFIX + StringUtil::toString(planIdx),
                                                           DirectoryOption::Package());
        if (!StoreSingleMergePlan(planIdx, planDir)) {
            INDEXLIB_FATAL_ERROR(Runtime, "fail to store mergePlan[%zu] to dir[%s]", planIdx,
                                 planDir->DebugString().c_str());
        }
        planDir->Close();
    }
    mergeMetaDir->FlushPackage();
    mergeMetaDir->Sync(true);
}

bool TableMergeMeta::Load(const std::string& mergeMetaPath, bool loadFullMeta)
{
    string mergeMetaBinaryVersion;
    IFileSystemPtr fs = CreateFileSystem(mergeMetaPath);
    DirectoryPtr mergeMetaDir = Directory::Get(fs);
    mergeMetaDir->MountPackage();

    mergeMetaDir->Load(MERGE_META_VERSION, mergeMetaBinaryVersion);
    if (mergeMetaBinaryVersion != MERGE_META_BINARY_VERSION) {
        IE_LOG(ERROR, "load failed, incompatible merge meta binary version[%s]", mergeMetaBinaryVersion.c_str());
    }

    if (!mTargetVersion.Load(mergeMetaDir, VERSION_FILE_NAME_PREFIX)) {
        return false;
    }

    string taskExecuteMetaContent;
    mergeMetaDir->Load(TASK_EXECUTE_META_FILE, taskExecuteMetaContent);
    FromJsonString(instanceExecMetas, taskExecuteMetaContent);

    fslib::FileList fileList;
    mergeMetaDir->ListDir("", fileList);
    size_t mergePlanCount = 0;
    for (size_t i = 0; i < fileList.size(); ++i) {
        if (fileList[i].find(MERGE_PLAN_DIR_PREFIX) == 0) {
            mergePlanCount++;
        }
    }
    mergePlans.resize(mergePlanCount);
    mergePlanMetas.resize(mergePlanCount);
    mergePlanResources.resize(mergePlanCount);
    mergeTaskDescriptions.resize(mergePlanCount);

    for (size_t i = 0; i < fileList.size(); ++i) {
        if (fileList[i].find(MERGE_PLAN_DIR_PREFIX) != 0) {
            continue;
        }
        uint32_t mergePlanIdx = StringUtil::fromString<uint32_t>(fileList[i].substr(MERGE_PLAN_DIR_PREFIX.size()));

        if (mergePlanIdx > mergePlanCount) {
            IE_LOG(ERROR, "Unexpected error: mergePlanIdx[%u] exceeds mergePlanCount[%zu]", mergePlanIdx,
                   mergePlanCount);
            return false;
        }
        DirectoryPtr planDir = mergeMetaDir->GetDirectory(fileList[i], true);
        if (!LoadSingleMergePlan(mergePlanIdx, planDir, loadFullMeta)) {
            IE_LOG(ERROR, "load MergePlan[%u] failed from dir[%s]", mergePlanIdx, planDir->DebugString().c_str());
            return false;
        }
    }
    return true;
}

bool TableMergeMeta::StoreSingleMergePlan(size_t mergePlanIdx, const DirectoryPtr& planDir) const
{
    planDir->Store(MERGE_PLAN_FILE, ToJsonString(*mergePlans[mergePlanIdx]), WriterOption());
    planDir->Store(MERGE_PLAN_META_FILE, ToJsonString(*mergePlanMetas[mergePlanIdx]), WriterOption());
    planDir->Store(MERGE_PLAN_TASK_DESCRIPTION_FILE, ToJsonString(mergeTaskDescriptions[mergePlanIdx]), WriterOption());

    if (!mergePlanResources[mergePlanIdx]) {
        return true;
    }
    DirectoryPtr resourceDir = planDir->MakeDirectory(MERGE_PLAN_RESOURCE_DIR_PREFIX);
    if (!mergePlanResources[mergePlanIdx]->Store(resourceDir)) {
        IE_LOG(ERROR, "fail to store planResource[%zu] to dir[%s]", mergePlanIdx, resourceDir->DebugString().c_str());
        return false;
    }
    return true;
}

bool TableMergeMeta::LoadSingleMergePlan(size_t mergePlanIdx, const DirectoryPtr& planDir, bool loadResource)
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

    if (!loadResource || !mMergePolicy.get()) {
        return true;
    }
    if (!planDir->IsExist(MERGE_PLAN_RESOURCE_DIR_PREFIX)) {
        IE_LOG(INFO, "resource not found in [%s/%s]", planDir->DebugString().c_str(),
               MERGE_PLAN_RESOURCE_DIR_PREFIX.c_str());
        return true;
    }
    DirectoryPtr resourceDir = planDir->GetDirectory(MERGE_PLAN_RESOURCE_DIR_PREFIX, true);
    TableMergePlanResourcePtr planResource(mMergePolicy->CreateMergePlanResource());
    if (!planResource) {
        IE_LOG(WARN, "Get NULL TableMergePlanResource from MergePolicy");
    } else {
        if (!planResource->Load(resourceDir)) {
            IE_LOG(ERROR, "fail to load planResource[%zu] from dir[%s]", mergePlanIdx,
                   resourceDir->DebugString().c_str());
            return false;
        }
    }
    mergePlanResources[mergePlanIdx] = planResource;
    return true;
}

int64_t TableMergeMeta::EstimateMemoryUse() const
{
    int64_t resourceMemUse = 0;
    for (const auto& resource : mergePlanResources) {
        if (resource) {
            resourceMemUse += static_cast<int64_t>(resource->EstimateMemoryUse());
        }
    }
    return resourceMemUse;
}

vector<segmentid_t> TableMergeMeta::GetTargetSegmentIds(size_t planIdx) const
{
    return {mergePlanMetas[planIdx]->targetSegmentId};
}
}} // namespace indexlib::merger
