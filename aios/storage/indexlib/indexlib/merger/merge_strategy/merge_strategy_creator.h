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
#include <string>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/merge_strategy/merge_strategy.h"
#include "indexlib/merger/segment_directory.h"

namespace indexlib { namespace merger {

#define DECLARE_MERGE_STRATEGY_CREATOR(classname, identifier)                                                          \
    std::string GetIdentifier() const override { return identifier; }                                                  \
    class Creator : public MergeStrategyCreator                                                                        \
    {                                                                                                                  \
    public:                                                                                                            \
        std::string GetIdentifier() const { return identifier; }                                                       \
        MergeStrategyPtr Create(const SegmentDirectoryPtr& segDir,                                                     \
                                const config::IndexPartitionSchemaPtr& schema) const                                   \
        {                                                                                                              \
            return MergeStrategyPtr(new classname(segDir, schema));                                                    \
        }                                                                                                              \
    };

class MergeStrategyCreator
{
public:
    MergeStrategyCreator() {}
    virtual ~MergeStrategyCreator() {}

public:
    virtual std::string GetIdentifier() const = 0;
    virtual MergeStrategyPtr Create(const SegmentDirectoryPtr& segDir,
                                    const config::IndexPartitionSchemaPtr& schema) const = 0;
};

typedef std::shared_ptr<MergeStrategyCreator> MergeStrategyCreatorPtr;
}} // namespace indexlib::merger
