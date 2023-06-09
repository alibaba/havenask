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
#ifndef __INDEXLIB_MERGE_META_CREATOR_H
#define __INDEXLIB_MERGE_META_CREATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/index_merge_meta.h"

DECLARE_REFERENCE_CLASS(merger, MergeTask);
DECLARE_REFERENCE_CLASS(merger, SegmentDirectory);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, MergeConfig);
DECLARE_REFERENCE_CLASS(merger, DumpStrategy);
DECLARE_REFERENCE_CLASS(index, ReclaimMapCreator);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);

namespace indexlib { namespace merger {

class MergeMetaCreator
{
public:
    MergeMetaCreator();
    virtual ~MergeMetaCreator();

public:
    virtual void Init(const merger::SegmentDirectoryPtr& segmentDirectory,
                      const index_base::SegmentMergeInfos& mergeInfos, const config::MergeConfig& mergeConfig,
                      const merger::DumpStrategyPtr& dumpStrategy, const plugin::PluginManagerPtr& pluginManager,
                      uint32_t instanceCount) = 0;
    virtual IndexMergeMeta* Create(MergeTask& task) = 0;

    static index_base::SegmentMergeInfos CreateSegmentMergeInfos(const config::IndexPartitionSchemaPtr& schema,
                                                                 const config::MergeConfig& mergeConfig,
                                                                 const merger::SegmentDirectoryPtr& segDir);

private:
    static void ReWriteSegmentMetric(const index_base::Version& version, index_base::SegmentMergeInfos& mergeInfos);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeMetaCreator);
}} // namespace indexlib::merger

#endif //__INDEXLIB_MERGE_META_CREATOR_H
