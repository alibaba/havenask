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
#ifndef __INDEXLIB_MERGE_STRATEGY_H
#define __INDEXLIB_MERGE_STRATEGY_H

#include <memory>
#include <string>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/merge_strategy_parameter.h"
#include "indexlib/framework/LevelInfo.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/merge_task.h"
#include "indexlib/merger/segment_directory.h"

namespace indexlib { namespace merger {

class MergeStrategy
{
public:
    MergeStrategy(const merger::SegmentDirectoryPtr& segDir, const config::IndexPartitionSchemaPtr& schema)
        : mSegDir(segDir)
        , mSchema(schema)
    {
    }

    virtual ~MergeStrategy() {}

public:
    virtual std::string GetIdentifier() const = 0;
    virtual void SetParameter(const config::MergeStrategyParameter& param) = 0;
    virtual std::string GetParameter() const = 0;
    virtual MergeTask CreateMergeTask(const index_base::SegmentMergeInfos& segMergeInfos,
                                      const indexlibv2::framework::LevelInfo& levelInfo) = 0;
    virtual MergeTask CreateMergeTaskForOptimize(const index_base::SegmentMergeInfos& segMergeInfos,
                                                 const indexlibv2::framework::LevelInfo& levelInfo) = 0;

protected:
    SegmentDirectoryPtr mSegDir;
    config::IndexPartitionSchemaPtr mSchema;
};

DEFINE_SHARED_PTR(MergeStrategy);
}} // namespace indexlib::merger

#endif //__INDEXLIB_MERGE_STRATEGY_H
