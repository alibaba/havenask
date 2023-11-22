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

#include <vector>

#include "indexlib/index_base/index_meta/merge_task_resource.h"
#include "indexlib/merger/merge_task_item.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(merger, IndexMergeMeta);
DECLARE_REFERENCE_CLASS(index, DeletionMapMerger);
DECLARE_REFERENCE_CLASS(index, SummaryMerger);
DECLARE_REFERENCE_CLASS(index, AttributeMerger);
DECLARE_REFERENCE_CLASS(index, IndexMerger);
DECLARE_REFERENCE_CLASS(index_base, MergeTaskResourceManager);

namespace indexlib { namespace merger {

class ParallelEndMergeExecutor
{
public:
    ParallelEndMergeExecutor(const config::IndexPartitionSchemaPtr& schema,
                             const plugin::PluginManagerPtr& pluginManager,
                             const file_system::DirectoryPtr& rootDirectory);

    virtual ~ParallelEndMergeExecutor();

public:
    void ParallelEndMerge(const IndexMergeMetaPtr& mergeMeta);

private:
    static std::vector<MergeTaskItems> ExtractParallelMergeTaskGroup(const IndexMergeMetaPtr& mergeMeta);

    static void CheckOneTaskGroup(const MergeTaskItems& items);

    void SingleGroupParallelEndMerge(const IndexMergeMetaPtr& mergeMeta, const MergeTaskItems& taskGroup);

    virtual index::SummaryMergerPtr CreateSummaryMerger(const MergeTaskItem& item,
                                                        const config::IndexPartitionSchemaPtr& schema) const;

    virtual index::AttributeMergerPtr CreateAttributeMerger(const MergeTaskItem& item,
                                                            const config::IndexPartitionSchemaPtr& schema) const;

    virtual index::IndexMergerPtr CreateIndexMerger(const MergeTaskItem& item,
                                                    const config::IndexPartitionSchemaPtr& schema) const;

    std::vector<index_base::MergeTaskResourceVector>
    ExtractMergeTaskResource(const index_base::MergeTaskResourceManagerPtr& mgr, const MergeTaskItems& taskGroup) const;

private:
    config::IndexPartitionSchemaPtr mSchema;
    plugin::PluginManagerPtr mPluginManager;
    file_system::DirectoryPtr mRootDirectory;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ParallelEndMergeExecutor);
}} // namespace indexlib::merger
