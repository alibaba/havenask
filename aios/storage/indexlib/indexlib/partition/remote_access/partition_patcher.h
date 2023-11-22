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

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/remote_access/attribute_data_patcher.h"
#include "indexlib/partition/remote_access/index_data_patcher.h"

DECLARE_REFERENCE_CLASS(partition, OfflinePartition);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(config, IndexConfig);
DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace partition {

class PartitionPatcher
{
private:
    static const size_t DEFAULT_INDEX_INIT_TERM_COUNT = 4 * 1024;

public:
    PartitionPatcher() {}
    ~PartitionPatcher() {}

public:
    bool Init(const OfflinePartitionPtr& offlinePartition, const config::IndexPartitionSchemaPtr& newSchema,
              const file_system::DirectoryPtr& patchDir, const plugin::PluginManagerPtr& pluginManager);

    AttributeDataPatcherPtr CreateSingleAttributePatcher(const std::string& attrName, segmentid_t segmentId);

    IndexDataPatcherPtr CreateSingleIndexPatcher(const std::string& indexName, segmentid_t segmentId,
                                                 size_t initDistinctTermCount = DEFAULT_INDEX_INIT_TERM_COUNT);

private:
    config::AttributeConfigPtr GetAlterAttributeConfig(const std::string& attrName) const;
    config::IndexConfigPtr GetAlterIndexConfig(const std::string& indexName) const;

private:
    config::IndexPartitionSchemaPtr mOldSchema;
    config::IndexPartitionSchemaPtr mNewSchema;
    config::MergeIOConfig mMergeIOConfig;
    std::vector<config::AttributeConfigPtr> mAlterAttributes;
    std::vector<config::IndexConfigPtr> mAlterIndexes;
    index_base::PartitionDataPtr mPartitionData;
    file_system::DirectoryPtr mPatchDir;
    plugin::PluginManagerPtr mPluginManager;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionPatcher);
}} // namespace indexlib::partition
