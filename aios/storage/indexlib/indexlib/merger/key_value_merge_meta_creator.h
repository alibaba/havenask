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
#ifndef __INDEXLIB_KEY_VALUE_MERGE_META_CREATOR_H
#define __INDEXLIB_KEY_VALUE_MERGE_META_CREATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/merge_meta_creator.h"

namespace indexlib { namespace merger {

class KeyValueMergeMetaCreator : public MergeMetaCreator
{
public:
    KeyValueMergeMetaCreator(const config::IndexPartitionSchemaPtr& schema);
    ~KeyValueMergeMetaCreator();

public:
    void Init(const merger::SegmentDirectoryPtr& segmentDirectory, const index_base::SegmentMergeInfos& mergeInfos,
              const config::MergeConfig& mergeConfig, const merger::DumpStrategyPtr& dumpStrategy,
              const plugin::PluginManagerPtr& pluginManager, uint32_t instanceCount) override;
    IndexMergeMeta* Create(merger::MergeTask& task) override;

private:
    void CreateMergePlans(IndexMergeMeta& meta, const merger::MergeTask& task);
    void CreateMergeTaskItems(IndexMergeMeta& meta);
    void UpdateLevelCursor(const merger::MergePlan& mergePlan, const merger::DumpStrategyPtr& dumpStrategy);
    std::string GetMergeResourceRootPath() const;

private:
    config::IndexPartitionSchemaPtr mSchema;
    merger::SegmentDirectoryPtr mSegmentDirectory;
    index_base::SegmentMergeInfos mSegMergeInfos;
    config::MergeConfig mMergeConfig;
    merger::DumpStrategyPtr mDumpStrategy;
    plugin::PluginManagerPtr mPluginManager;
    uint32_t mInstanceCount;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KeyValueMergeMetaCreator);
}} // namespace indexlib::merger

#endif //__INDEXLIB_KEY_VALUE_MERGE_META_CREATOR_H
