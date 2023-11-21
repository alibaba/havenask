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
#pragma once

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "indexlib/base/Types.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/index_merge_meta.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"
#include "indexlib/table/merge_policy.h"
#include "indexlib/table/merge_task_description.h"
#include "indexlib/table/table_merge_plan.h"
#include "indexlib/table/table_merge_plan_resource.h"
#include "indexlib/table/task_execute_meta.h"

DECLARE_REFERENCE_CLASS(table, TableMergePlanMeta);

namespace indexlib { namespace merger {

// TODO: inherites from MergeMeta
class TableMergeMeta : public IndexMergeMeta
{
public:
    TableMergeMeta(const table::MergePolicyPtr& mergePolicy);
    ~TableMergeMeta();

public:
    static const std::string MERGE_META_VERSION;
    static const std::string MERGE_META_BINARY_VERSION;
    static const std::string TASK_EXECUTE_META_FILE;
    static const std::string MERGE_PLAN_DIR_PREFIX;
    static const std::string MERGE_PLAN_RESOURCE_DIR_PREFIX;
    static const std::string MERGE_PLAN_FILE;
    static const std::string MERGE_PLAN_META_FILE;
    static const std::string MERGE_PLAN_TASK_DESCRIPTION_FILE;

public:
    bool Load(const std::string& mergeMetaPath) override { return Load(mergeMetaPath, true); }
    bool LoadBasicInfo(const std::string& mergeMetaPath) override { return Load(mergeMetaPath, false); }
    size_t GetMergePlanCount() const override { return mergePlans.size(); }
    std::vector<segmentid_t> GetTargetSegmentIds(size_t planIdx) const override;

    int64_t EstimateMemoryUse() const override;
    table::MergePolicyPtr GetMergePolicy() const { return mMergePolicy; }

private:
    void InnerStore(const std::string& mergeMetaPath) const override;
    bool Load(const std::string& rootPath, bool loadFullMeta);
    bool StoreSingleMergePlan(size_t planIdx, const file_system::DirectoryPtr& planDir) const;
    bool LoadSingleMergePlan(size_t planIdx, const file_system::DirectoryPtr& planDir, bool loadResource);
    bool LoadMergeMetaVersion(const std::string& path, std::string& mergeMetaBinaryVersion);

public:
    // for each mergePlan
    std::vector<table::TableMergePlanPtr> mergePlans;
    std::vector<table::TableMergePlanMetaPtr> mergePlanMetas;
    std::vector<table::TableMergePlanResourcePtr> mergePlanResources;
    std::vector<table::MergeTaskDescriptions> mergeTaskDescriptions;
    // for each merge instance
    std::vector<table::TaskExecuteMetas> instanceExecMetas;

private:
    table::MergePolicyPtr mMergePolicy;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TableMergeMeta);
}} // namespace indexlib::merger
