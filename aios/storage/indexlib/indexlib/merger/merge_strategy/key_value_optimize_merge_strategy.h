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
#ifndef __INDEXLIB_KEY_VALUE_OPTIMIZE_MERGE_STRATEGY_H
#define __INDEXLIB_KEY_VALUE_OPTIMIZE_MERGE_STRATEGY_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/merge_strategy/merge_strategy.h"

namespace indexlib { namespace merger {

class KeyValueOptimizeMergeStrategy : public MergeStrategy
{
public:
    KeyValueOptimizeMergeStrategy(const merger::SegmentDirectoryPtr& segDir,
                                  const config::IndexPartitionSchemaPtr& schema);

    ~KeyValueOptimizeMergeStrategy();

public:
    std::string GetIdentifier() const override { return OPTIMIZE_MERGE_STRATEGY_STR; }

    void SetParameter(const config::MergeStrategyParameter& param) override;
    std::string GetParameter() const override;
    MergeTask CreateMergeTask(const index_base::SegmentMergeInfos& segMergeInfos,
                              const indexlibv2::framework::LevelInfo& levelInfo) override;
    MergeTask CreateMergeTaskForOptimize(const index_base::SegmentMergeInfos& segMergeInfos,
                                         const indexlibv2::framework::LevelInfo& levelInfo) override;

private:
    bool NeedMerge(const index_base::SegmentMergeInfos& segMergeInfos,
                   const indexlibv2::framework::LevelInfo& levelInfo) const;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KeyValueOptimizeMergeStrategy);
}} // namespace indexlib::merger

#endif //__INDEXLIB_KEY_VALUE_OPTIMIZE_MERGE_STRATEGY_H
